#include "chronos/query/distributed_vector_fragment_v2.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <ranges>
#include <span>
#include <vector>

namespace {

[[nodiscard]] chronos::common::Uuid uuid(const std::uint8_t seed) {
  chronos::common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return chronos::common::Uuid{bytes};
}

[[nodiscard]] chronos::query::DistributedVectorFragmentV2DecodeLimits limits() {
  chronos::query::DistributedVectorFragmentV2DecodeLimits value;
  value.maximum_frame_length = 65'536U;
  value.dispatch.maximum_frame_length = 32'768U;
  value.dispatch.maximum_projection_columns = 64U;
  value.dispatch.plan.maximum_input_columns = 64U;
  value.dispatch.plan.maximum_output_columns = 64U;
  value.dispatch.plan.maximum_row_outputs = 64U;
  value.dispatch.plan.maximum_group_keys = 64U;
  value.dispatch.plan.maximum_aggregates = 64U;
  value.dispatch.plan.maximum_order_keys = 64U;
  value.result_schema.maximum_frame_length = 32'768U;
  value.result_schema.maximum_columns = 64U;
  value.result_schema.maximum_name_length = 256U;
  return value;
}

void exercise_bytes(const chronos::common::ByteView bytes) {
  using namespace chronos::query;
  const DistributedVectorFragmentV2DecodeLimits decode_limits = limits();
  const auto decoded = decode_distributed_vector_fragment_dispatch_v2_exact(bytes, decode_limits);
  if (decoded.has_value()) {
    const auto encoded = encode_distributed_vector_fragment_dispatch_v2(*decoded);
    if (!encoded.has_value() || !std::ranges::equal(encoded->bytes(), bytes))
      std::abort();
  }

  DistributedVectorFragmentV2Reader reader{decode_limits};
  const std::size_t split =
      bytes.empty()
          ? 0U
          : std::min(static_cast<std::size_t>(std::to_integer<std::uint8_t>(bytes.front())),
                     bytes.size());
  const auto prefix = reader.consume(bytes.first(split));
  if (!prefix.has_value())
    return;
  const auto suffix = reader.consume(bytes.subspan(split));
  if (!suffix.has_value())
    return;
  if (prefix->dispatch.has_value() && suffix->dispatch.has_value())
    std::abort();
}

void exercise_canonical_mutation(const chronos::common::ByteView input) {
  using namespace chronos;
  const schema::LogicalType type =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  const query::DistributedVectorFragmentDispatchV2 value{
      .dispatch = {.query_id = uuid(1U),
                   .database_id = manifest::DatabaseId::from_uuid(uuid(2U)).value(),
                   .table_id = schema::TableId::from_uuid(uuid(3U)).value(),
                   .tablet_id = schema::TabletId::from_uuid(uuid(4U)).value(),
                   .destination_schema_id = schema::SchemaId::from_uuid(uuid(5U)).value(),
                   .raft_group_id = uuid(6U),
                   .snapshot_generation = 1U,
                   .serving_node = 7U,
                   .placement_epoch = 1U,
                   .read_policy = {.consistency = query::DistributedReadConsistency::kLocalEventual,
                                   .maximum_staleness_positions = std::nullopt},
                   .linearizable_barrier = std::nullopt,
                   .destination_column_ordinals = {0U},
                   .event_time_predicate = std::nullopt,
                   .plan = {.mode = query::DistributedVectorPlanMode::kRows,
                            .row_output_indices = {0U},
                            .group_key_input_indices = {},
                            .aggregates = {},
                            .order_keys = {},
                            .limit = std::nullopt}},
      .result_schema = {.columns = {{"value", type, false}}}};
  const auto encoded = query::encode_distributed_vector_fragment_dispatch_v2(value);
  if (!encoded.has_value())
    std::abort();
  std::vector<std::byte> candidate(encoded->bytes().begin(), encoded->bytes().end());
  if (!input.empty()) {
    const std::size_t index =
        static_cast<std::size_t>(std::to_integer<std::uint8_t>(input.front())) % candidate.size();
    const std::byte mask = input.size() > 1U ? input[1U] : std::byte{1U};
    candidate[index] ^= mask == std::byte{} ? std::byte{1U} : mask;
  }
  exercise_bytes(candidate);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const chronos::common::ByteView bytes = std::as_bytes(std::span{data, size});
  exercise_bytes(bytes);
  exercise_canonical_mutation(bytes);
  return 0;
}
