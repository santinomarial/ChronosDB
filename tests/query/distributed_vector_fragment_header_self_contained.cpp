#include "chronos/query/distributed_vector_fragment.hpp"
#include "chronos/query/distributed_vector_fragment_v2.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::DistributedVectorFragmentDispatch>);
static_assert(
    !std::is_copy_constructible_v<chronos::query::EncodedDistributedVectorFragmentDispatch>);
static_assert(!std::is_move_constructible_v<chronos::query::DistributedVectorFragmentReader>);
static_assert(std::is_move_constructible_v<chronos::query::DistributedVectorFragmentWriteCursor>);
static_assert(std::is_aggregate_v<chronos::query::DistributedVectorFragmentDispatchV2>);
static_assert(
    !std::is_copy_constructible_v<chronos::query::EncodedDistributedVectorFragmentDispatchV2>);

namespace {
[[maybe_unused]] const auto kEncodeV2 =
    &chronos::query::encode_distributed_vector_fragment_dispatch_v2;
[[maybe_unused]] const auto kDecodeV2 =
    &chronos::query::decode_distributed_vector_fragment_dispatch_v2_exact;
} // namespace
