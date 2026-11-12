#ifndef IRODS_PARALLEL_PUT_HPP
#define IRODS_PARALLEL_PUT_HPP

/// \file
///
/// \brief Uploads a local file over several connections, each writing one part of the data object.
///
/// \since 5.1.90

#include <string>

struct RcComm;
struct DataObjInp;

namespace irods
{
    /// Uploads a local file over \p _number_of_threads connections.
    ///
    /// The function opens the replica on \p _number_of_threads new connections to \p _host and passes
    /// the transfer layout (see irods/transfer_layout.hpp) on every open. The first connection
    /// creates or truncates the replica and is closed last, which finalizes the replica (checksums,
    /// metadata, ACLs and size checks happen there, as with a put). The other connections open
    /// the replica with its replica token and are closed without updating the catalog.
    ///
    /// If ALL_KW is set, the other replicas are then updated with rcDataObjRepl(), as with a put.
    ///
    /// If any part fails, the connections are dropped without closing the replica. The server
    /// then marks the replica stale.
    ///
    /// \param[in] _comm              An existing connection. Its users and port are used for the new
    ///                               connections, and its operProgress and transStat are updated as
    ///                               data is written.
    /// \param[in] _inp               The put input as it would be passed to rcDataObjPut(). objPath
    ///                               and dataSize must be set.
    /// \param[in] _local_path        The local file to upload.
    /// \param[in] _host              The server to connect to, usually the "host" returned by
    ///                               rc_get_resource_info_for_operation().
    /// \param[in] _hierarchy         The resource hierarchy to open the replica in.
    /// \param[in] _number_of_threads The number of connections to use, from 1 to
    ///                               MAX_NUM_CONFIG_TRAN_THR.
    ///
    /// \return An integer.
    /// \retval 0        On success.
    /// \retval Non-zero On failure.
    ///
    /// \since 5.1.90
    auto parallel_put(RcComm& _comm,
                      const DataObjInp& _inp,
                      const char* _local_path,
                      const std::string& _host,
                      const std::string& _hierarchy,
                      int _number_of_threads) -> int;
} // namespace irods

#endif // IRODS_PARALLEL_PUT_HPP
