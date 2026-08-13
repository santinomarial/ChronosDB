#include "chronos/query/value.hpp"

namespace {
[[maybe_unused]] constexpr auto kComparison = &chronos::query::compare_scalar_values;
[[maybe_unused]] constexpr auto kCanonicalComparison =
    &chronos::query::compare_canonical_scalar_bytes;
} // namespace
