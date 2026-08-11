#include "chronos/query/distributed_fragment_dispatch.hpp"

namespace {
[[maybe_unused]] auto* const kEncodeDistributedFragmentDispatch =
    &chronos::query::encode_distributed_aggregate_fragment_dispatch;
[[maybe_unused]] auto* const kDecodeDistributedFragmentDispatch =
    &chronos::query::decode_distributed_aggregate_fragment_dispatch_exact;
} // namespace
