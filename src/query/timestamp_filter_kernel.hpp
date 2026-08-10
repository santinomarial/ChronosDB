#ifndef CHRONOS_QUERY_TIMESTAMP_FILTER_KERNEL_HPP_
#define CHRONOS_QUERY_TIMESTAMP_FILTER_KERNEL_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/query/timestamp_range.hpp"

#include <cstddef>
#include <cstdint>
#include <span>

namespace chronos::query::detail {

enum class TimestampFilterKernel : std::uint8_t { kScalar, kAvx2, kNeon };

[[nodiscard]] bool timestamp_filter_kernel_available(TimestampFilterKernel kernel) noexcept;
[[nodiscard]] TimestampFilterKernel preferred_timestamp_filter_kernel() noexcept;

// Compacts an identity selection to canonical little-endian TIMESTAMP_NS values matching predicate.
// The caller has validated values.size() == indices.size() * 8 and indices initially contain
// 0..N-1. The operation preserves row order, performs no allocation, and returns the retained
// prefix length. An unavailable requested acceleration falls back to the scalar reference kernel.
[[nodiscard]] std::size_t compact_identity_timestamps(
    common::ByteView values, std::span<std::uint32_t> indices,
    const TimestampRangePredicate& predicate,
    TimestampFilterKernel kernel = preferred_timestamp_filter_kernel()) noexcept;

} // namespace chronos::query::detail

#endif // CHRONOS_QUERY_TIMESTAMP_FILTER_KERNEL_HPP_
