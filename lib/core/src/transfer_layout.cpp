#include "irods/transfer_layout.hpp"

#include "irods/rodsDef.h"
#include "irods/rodsErrorTable.h"
#include "irods/rodsLog.h"

#include <cstdint>

namespace irods::transfer_layout
{
    auto make_layout(std::int64_t _size, int _number_of_threads) -> nlohmann::json
    {
        const std::int64_t bytes_per_thread = _size / _number_of_threads;

        auto parts = nlohmann::json::array();

        for (int i = 0; i < _number_of_threads; ++i) {
            const std::int64_t offset = bytes_per_thread * i;
            const std::int64_t length = (i < _number_of_threads - 1) ? bytes_per_thread : _size - offset;
            parts.push_back({{"offset", offset}, {"length", length}});
        }

        return {{"size", _size}, {"parts", parts}};
    } // make_layout

    auto validate_layout(const nlohmann::json& _layout,
                         std::int64_t _expected_size,
                         int _expected_number_of_parts) -> int
    {
        try {
            if (_layout.at("size").get<std::int64_t>() != _expected_size) {
                rodsLog(LOG_ERROR, "%s: layout size does not match the data size [%lld].", __func__, _expected_size);
                return SYS_INVALID_INPUT_PARAM;
            }

            const auto& parts = _layout.at("parts");

            if (!parts.is_array() || parts.empty() || parts.size() > MAX_NUM_CONFIG_TRAN_THR ||
                static_cast<int>(parts.size()) != _expected_number_of_parts)
            {
                rodsLog(LOG_ERROR,
                        "%s: layout does not have the expected number of parts [%d].",
                        __func__,
                        _expected_number_of_parts);
                return SYS_INVALID_INPUT_PARAM;
            }

            std::int64_t next_offset = 0;

            for (const auto& part : parts) {
                const auto offset = part.at("offset").get<std::int64_t>();
                const auto length = part.at("length").get<std::int64_t>();

                if (offset != next_offset || length < 0) {
                    rodsLog(LOG_ERROR, "%s: layout parts are not contiguous at offset [%lld].", __func__, offset);
                    return SYS_INVALID_INPUT_PARAM;
                }

                next_offset += length;
            }

            if (next_offset != _expected_size) {
                rodsLog(LOG_ERROR, "%s: layout parts do not cover the data size [%lld].", __func__, _expected_size);
                return SYS_INVALID_INPUT_PARAM;
            }
        }
        catch (const nlohmann::json::exception& e) {
            rodsLog(LOG_ERROR, "%s: invalid layout: %s", __func__, e.what());
            return SYS_INVALID_INPUT_PARAM;
        }

        return 0;
    } // validate_layout
} // namespace irods::transfer_layout
