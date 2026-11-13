#ifndef IRODS_TRANSFER_LAYOUT_HPP
#define IRODS_TRANSFER_LAYOUT_HPP

/// \file
///
/// \brief Functions for describing how a client splits a parallel upload.
///
/// A transfer layout is a JSON object describing the byte range each client thread writes:
///
/// \code{.js}
/// {
///     "size": 1073741824,
///     "parts": [
///         {"offset": 0, "length": 536870912},
///         {"offset": 536870912, "length": 536870912}
///     ]
/// }
/// \endcode
///
/// Each entry in \p parts belongs to one thread, so the number of threads is the number of entries.
///
/// A client that writes a replica over several connections passes the layout as the value of
/// TRANSFER_LAYOUT_KW on every open of the replica, along with NUM_THREADS_KW and DATA_SIZE_KW.
/// The server validates it and stores it in the L1 descriptor so that resource plugins can use it.
/// Each connection writes the part starting at the offset of its first write.
///
/// Servers that do not support transfer layouts ignore the keyword.
///
/// iput gets the number of threads from rc_get_resource_info_for_operation() and builds the
/// layout with make_layout().
///
/// \since 5.1.90

#include <nlohmann/json.hpp>

#include <cstdint>

namespace irods::transfer_layout
{
    /// Builds a layout which splits \p _size bytes evenly over \p _number_of_threads threads,
    /// with the last thread receiving any remainder.
    ///
    /// \param[in] _size              The number of bytes in the transfer.
    /// \param[in] _number_of_threads The number of threads. Must be greater than zero.
    ///
    /// \return The layout.
    ///
    /// \since 5.1.90
    auto make_layout(std::int64_t _size, int _number_of_threads) -> nlohmann::json;

    /// Checks that a layout is well formed and describes the expected transfer.
    ///
    /// The parts must be sorted by offset and must cover the range [0, \p _expected_size)
    /// exactly, with no gaps or overlaps. There must be at least one part and no more than
    /// MAX_NUM_CONFIG_TRAN_THR.
    ///
    /// \param[in] _layout                   The layout.
    /// \param[in] _expected_size            The number of bytes in the transfer.
    /// \param[in] _expected_number_of_parts The number of threads in the transfer.
    ///
    /// \return 0 if the layout is valid, SYS_INVALID_INPUT_PARAM otherwise.
    ///
    /// \since 5.1.90
    auto validate_layout(const nlohmann::json& _layout,
                         std::int64_t _expected_size,
                         int _expected_number_of_parts) -> int;
} // namespace irods::transfer_layout

#endif // IRODS_TRANSFER_LAYOUT_HPP
