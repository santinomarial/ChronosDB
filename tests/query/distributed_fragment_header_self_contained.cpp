#include "chronos/query/distributed_fragment.hpp"

namespace {
[[maybe_unused]] auto* const kEncodeDistributedFragment =
    &chronos::query::encode_distributed_aggregate_fragment;
[[maybe_unused]] auto* const kDecodeDistributedFragment =
    &chronos::query::decode_distributed_aggregate_fragment_exact;
} // namespace
