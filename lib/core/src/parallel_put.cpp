#include "irods/parallel_put.hpp"

#include "irods/authentication_plugin_framework.hpp"
#include "irods/dataObjClose.h"
#include "irods/dataObjInpOut.h"
#include "irods/dataObjLseek.h"
#include "irods/dataObjOpen.h"
#include "irods/dataObjRepl.h"
#include "irods/dataObjWrite.h"
#include "irods/getRodsEnv.h"
#include "irods/irods_at_scope_exit.hpp"
#include "irods/rcConnect.h"
#include "irods/rcGlobalExtern.h"
#include "irods/rcMisc.h"
#include "irods/replica_close.h"
#include "irods/replica_open.h"
#include "irods/rodsErrorTable.h"
#include "irods/rodsKeyWdDef.h"
#include "irods/rodsLog.h"
#include "irods/transfer_layout.hpp"

#include <nlohmann/json.hpp>

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace
{
    using json = nlohmann::json;

    // One connection writing one part of the data object.
    struct part_stream
    {
        RcComm* conn = nullptr;
        int fd = -1;
        std::int64_t offset = 0;
        std::int64_t length = 0;
        int status = 0;
    };

    // Shared by the threads to report progress the way the portal put does.
    struct progress
    {
        RcComm& comm;
        std::mutex mutex;
    };

    auto connect_to(const RcComm& _comm, const std::string& _host, const char* _auth_scheme) -> RcComm*
    {
        namespace ia = irods::authentication;

        rErrMsg_t error{};

        RcComm* conn = _rcConnect(_host.c_str(),
                                  _comm.portNum,
                                  _comm.proxyUser.userName,
                                  _comm.proxyUser.rodsZone,
                                  _comm.clientUser.userName,
                                  _comm.clientUser.rodsZone,
                                  &error,
                                  0,
                                  NO_RECONN);
        if (!conn) {
            rodsLog(LOG_ERROR, "parallel_put: could not connect to [%s]: %s", _host.c_str(), error.msg);
            return nullptr;
        }

        if (const auto ec = ia::authenticate_client(*conn, json{{ia::scheme_name, _auth_scheme}}); ec != 0) {
            rodsLogError(LOG_ERROR, ec, "parallel_put: could not authenticate to [%s]", _host.c_str());
            rcDisconnect(conn);
            return nullptr;
        }

        return conn;
    } // connect_to

    // Copies the put input into an open input, adding the keywords every open of the transfer needs.
    auto make_open_input(const DataObjInp& _inp,
                         const std::string& _hierarchy,
                         const std::string& _layout,
                         int _number_of_threads,
                         DataObjInp& _open_inp) -> void
    {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
        replDataObjInp(const_cast<DataObjInp*>(&_inp), &_open_inp);

        rmKeyVal(&_open_inp.condInput, DATA_INCLUDED_KW);

        _open_inp.oprType = PUT_OPR;
        _open_inp.numThreads = _number_of_threads;

        addKeyVal(&_open_inp.condInput, RESC_HIER_STR_KW, _hierarchy.c_str());
        addKeyVal(&_open_inp.condInput, NUM_THREADS_KW, std::to_string(_number_of_threads).c_str());
        addKeyVal(&_open_inp.condInput, DATA_SIZE_KW, std::to_string(_inp.dataSize).c_str());
        addKeyVal(&_open_inp.condInput, TRANSFER_LAYOUT_KW, _layout.c_str());
    } // make_open_input

    // Writes one part of the local file to the data object.
    auto write_part(part_stream& _part,
                    const char* _local_path,
                    std::size_t _buffer_size,
                    progress& _progress,
                    std::atomic<bool>& _failed) -> void
    {
        const int local_fd = open(_local_path, O_RDONLY);
        if (local_fd < 0) {
            _part.status = UNIX_FILE_OPEN_ERR - errno;
            _failed = true;
            return;
        }
        const auto close_local_fd = irods::at_scope_exit{[local_fd] { close(local_fd); }};

        openedDataObjInp_t seek_inp{};
        seek_inp.l1descInx = _part.fd;
        seek_inp.offset = _part.offset;
        seek_inp.whence = SEEK_SET;

        fileLseekOut_t* seek_out{};
        if (const auto ec = rcDataObjLseek(_part.conn, &seek_inp, &seek_out); ec < 0) {
            _part.status = ec;
            _failed = true;
            return;
        }
        std::free(seek_out); // NOLINT(cppcoreguidelines-no-malloc, cppcoreguidelines-owning-memory)

        std::vector<char> buffer(_buffer_size);
        std::int64_t remaining = _part.length;
        std::int64_t offset = _part.offset;

        while (remaining > 0 && !_failed) {
            const auto to_read = static_cast<std::size_t>(std::min<std::int64_t>(remaining, buffer.size()));
            const auto bytes_read = pread(local_fd, buffer.data(), to_read, offset);

            if (bytes_read <= 0) {
                _part.status = bytes_read < 0 ? UNIX_FILE_READ_ERR - errno : SYS_COPY_LEN_ERR;
                _failed = true;
                return;
            }

            openedDataObjInp_t write_inp{};
            write_inp.l1descInx = _part.fd;
            write_inp.len = static_cast<int>(bytes_read);

            BytesBuf write_buf{};
            write_buf.buf = buffer.data();
            write_buf.len = static_cast<int>(bytes_read);

            if (const auto bytes_written = rcDataObjWrite(_part.conn, &write_inp, &write_buf);
                bytes_written != static_cast<int>(bytes_read))
            {
                _part.status = bytes_written < 0 ? bytes_written : SYS_COPY_LEN_ERR;
                _failed = true;
                return;
            }

            remaining -= bytes_read;
            offset += bytes_read;

            std::lock_guard lock{_progress.mutex};
            _progress.comm.transStat.bytesWritten += bytes_read;
            if (gGuiProgressCB) {
                _progress.comm.operProgress.curFileSizeDone += bytes_read;
                gGuiProgressCB(&_progress.comm.operProgress);
            }
        }
    } // write_part
} // anonymous namespace

namespace irods
{
    auto parallel_put(RcComm& _comm,
                      const DataObjInp& _inp,
                      const char* _local_path,
                      const std::string& _host,
                      const std::string& _hierarchy,
                      int _number_of_threads) -> int
    {
        if (_number_of_threads < 1 || _number_of_threads > MAX_NUM_CONFIG_TRAN_THR) {
            rodsLog(LOG_ERROR, "parallel_put: invalid number of threads [%d]", _number_of_threads);
            return SYS_INVALID_INPUT_PARAM;
        }

        rodsEnv env{};
        if (const auto ec = getRodsEnv(&env); ec < 0) {
            return ec;
        }
        const auto buffer_size = static_cast<std::size_t>(env.irodsTransBufferSizeForParaTrans) * 1024 * 1024;

        const auto layout = transfer_layout::make_layout(_inp.dataSize, _number_of_threads);
        const auto layout_string = layout.dump();

        std::vector<part_stream> parts(_number_of_threads);
        for (int i = 0; i < _number_of_threads; ++i) {
            parts[i].offset = layout.at("parts").at(i).at("offset").get<std::int64_t>();
            parts[i].length = layout.at("parts").at(i).at("length").get<std::int64_t>();
        }

        // Every part uses its own connection so that, on failure, the connections can be dropped
        // without closing the replica. The server then marks it stale.
        const auto disconnect = at_scope_exit{[&parts] {
            for (auto& part : parts) {
                if (part.conn) {
                    rcDisconnect(part.conn);
                }
            }
        }};

        _comm.transStat.numThreads = _number_of_threads;
        _comm.transStat.bytesWritten = 0;

        // The first part creates or truncates the replica.
        if (parts[0].conn = connect_to(_comm, _host, env.rodsAuthScheme); !parts[0].conn) {
            return USER_SOCK_CONNECT_ERR;
        }

        DataObjInp primary_inp{};
        const auto clear_primary_inp = at_scope_exit{[&primary_inp] { clearKeyVal(&primary_inp.condInput); }};
        make_open_input(_inp, _hierarchy, layout_string, _number_of_threads, primary_inp);
        primary_inp.openFlags = O_CREAT | O_WRONLY | O_TRUNC;

        // rc_replica_open also returns the replica token the other parts need.
        std::string token;
        {
            char* fd_info{};
            const auto free_fd_info = at_scope_exit{[&fd_info] { std::free(fd_info); }}; // NOLINT(cppcoreguidelines-no-malloc)

            if (parts[0].fd = rc_replica_open(parts[0].conn, &primary_inp, &fd_info); parts[0].fd < 0) {
                return parts[0].fd;
            }

            try {
                token = json::parse(fd_info).at("replica_token").get<std::string>();
            }
            catch (const json::exception& e) {
                rodsLog(LOG_ERROR, "parallel_put: could not get the replica token: %s", e.what());
                return SYS_INTERNAL_ERR;
            }
        }

        // The other parts open the same replica with its replica token.
        if (_number_of_threads > 1) {

            for (int i = 1; i < _number_of_threads; ++i) {
                if (parts[i].conn = connect_to(_comm, _host, env.rodsAuthScheme); !parts[i].conn) {
                    return USER_SOCK_CONNECT_ERR;
                }

                DataObjInp inp{};
                const auto clear_inp = at_scope_exit{[&inp] { clearKeyVal(&inp.condInput); }};
                std::strncpy(inp.objPath, _inp.objPath, MAX_NAME_LEN - 1);
                inp.dataSize = _inp.dataSize;
                inp.openFlags = O_WRONLY;
                inp.oprType = PUT_OPR;
                inp.numThreads = _number_of_threads;
                addKeyVal(&inp.condInput, RESC_HIER_STR_KW, _hierarchy.c_str());
                addKeyVal(&inp.condInput, REPLICA_TOKEN_KW, token.c_str());
                addKeyVal(&inp.condInput, NUM_THREADS_KW, std::to_string(_number_of_threads).c_str());
                addKeyVal(&inp.condInput, DATA_SIZE_KW, std::to_string(_inp.dataSize).c_str());
                addKeyVal(&inp.condInput, TRANSFER_LAYOUT_KW, layout_string.c_str());

                if (parts[i].fd = rcDataObjOpen(parts[i].conn, &inp); parts[i].fd < 0) {
                    return parts[i].fd;
                }
            }
        }

        if (getValByKey(&_inp.condInput, VERY_VERBOSE_KW)) {
            printf("Parallel put: NumThreads=%d, host:%s, hierarchy:%s\n",
                   _number_of_threads, _host.c_str(), _hierarchy.c_str());
        }

        if (gGuiProgressCB) {
            _comm.operProgress.flag = 1;
        }

        progress progress{_comm, {}};
        std::atomic<bool> failed{false};

        std::vector<std::thread> threads;
        threads.reserve(_number_of_threads);
        for (auto& part : parts) {
            threads.emplace_back(write_part, std::ref(part), _local_path, buffer_size, std::ref(progress), std::ref(failed));
        }
        for (auto& thread : threads) {
            thread.join();
        }

        for (const auto& part : parts) {
            if (part.status < 0) {
                return part.status;
            }
        }

        // Close the other parts without touching the catalog, then finalize with the first part.
        for (int i = 1; i < _number_of_threads; ++i) {
            const auto close_input = json{{"fd", parts[i].fd},
                                          {"update_size", false},
                                          {"update_status", false},
                                          {"compute_checksum", false},
                                          {"send_notifications", false}};

            if (const auto ec = rc_replica_close(parts[i].conn, close_input.dump().c_str()); ec < 0) {
                return ec;
            }
            parts[i].fd = -1;
        }

        openedDataObjInp_t close_inp{};
        close_inp.l1descInx = parts[0].fd;
        close_inp.bytesWritten = _inp.dataSize;

        if (const auto ec = rcDataObjClose(parts[0].conn, &close_inp); ec < 0) {
            return ec;
        }

        // Update the other replicas, as a put does (see rsDataObjPut).
        if (getValByKey(&_inp.condInput, ALL_KW)) {
            DataObjInp repl_inp{};
            const auto clear_repl_inp = at_scope_exit{[&repl_inp] { clearKeyVal(&repl_inp.condInput); }};
            std::strncpy(repl_inp.objPath, _inp.objPath, MAX_NAME_LEN - 1);
            addKeyVal(&repl_inp.condInput, UPDATE_REPL_KW, "");
            addKeyVal(&repl_inp.condInput, ALL_KW, "");

            if (const auto ec = rcDataObjRepl(&_comm, &repl_inp); ec < 0) {
                rodsLogError(LOG_ERROR, ec, "parallel_put: could not update the other replicas of [%s]", _inp.objPath);
                return ec;
            }
        }

        return 0;
    } // parallel_put
} // namespace irods
