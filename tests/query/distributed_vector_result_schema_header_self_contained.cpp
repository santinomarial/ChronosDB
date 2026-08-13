#include "chronos/query/distributed_vector_result_schema.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::DistributedVectorResultSchema>);
static_assert(!std::is_copy_constructible_v<chronos::query::EncodedDistributedVectorResultSchema>);

namespace {
[[maybe_unused]] const auto kEncode = &chronos::query::encode_distributed_vector_result_schema;
[[maybe_unused]] const auto kDecode =
    &chronos::query::decode_distributed_vector_result_schema_exact;
[[maybe_unused]] const auto kValidateValue =
    &chronos::query::validate_distributed_vector_result_schema_value;
[[maybe_unused]] const auto kValidate = &chronos::query::validate_distributed_vector_result_schema;
} // namespace
