#include "chronos/cluster/distributed_vector_query_transport_v2.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <ranges>
#include <span>
#include <utility>
#include <vector>

namespace {

[[nodiscard]] chronos::common::Uuid uuid(const std::uint8_t seed) {
  chronos::common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return chronos::common::Uuid{bytes};
}

[[nodiscard]] chronos::query::DistributedVectorResultSchema result_schema() {
  return {
      .columns = {
          {"value",
           chronos::schema::LogicalType::create(chronos::schema::LogicalTypeKind::kInt64).value(),
           false}}};
}

[[nodiscard]] chronos::query::DistributedVectorFragmentDispatchV2 dispatch() {
  using namespace chronos;
  return {
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
      .result_schema = result_schema()};
}

[[nodiscard]] std::size_t split_at(const chronos::common::ByteView bytes) {
  return bytes.empty()
             ? 0U
             : std::min(static_cast<std::size_t>(std::to_integer<std::uint8_t>(bytes.front())),
                        bytes.size());
}

void exercise_request_bytes(const chronos::common::ByteView bytes) {
  using namespace chronos::cluster;
  const auto decoded = decode_distributed_vector_query_request_v2_exact(bytes);
  if (decoded.has_value()) {
    const auto encoded = encode_distributed_vector_query_request_v2(*decoded);
    if (!encoded.has_value() || !std::ranges::equal(*encoded, bytes))
      std::abort();
  }

  DistributedVectorQueryRequestV2Reader reader{65'536U};
  const std::size_t split = split_at(bytes);
  const auto prefix = reader.consume(bytes.first(split));
  if (!prefix.has_value())
    return;
  const auto suffix = reader.consume(bytes.subspan(split));
  if (!suffix.has_value())
    return;
  if (prefix->request.has_value() && suffix->request.has_value())
    std::abort();
}

void exercise_response_bytes(const chronos::common::ByteView bytes) {
  using namespace chronos::cluster;
  chronos::query::DistributedVectorResultSchema schema = result_schema();
  const auto decoded = decode_distributed_vector_query_response_v2_exact(bytes, schema);
  if (decoded.has_value()) {
    const auto encoded = encode_distributed_vector_query_response_v2(*decoded, schema);
    if (!encoded.has_value() || !std::ranges::equal(*encoded, bytes))
      std::abort();
  }

  DistributedVectorQueryResponseV2Reader reader{std::move(schema), 65'536U};
  const std::size_t split = split_at(bytes);
  const auto prefix = reader.consume(bytes.first(split));
  if (!prefix.has_value())
    return;
  const auto suffix = reader.consume(bytes.subspan(split));
  if (!suffix.has_value())
    return;
  if (prefix->response.has_value() && suffix->response.has_value())
    std::abort();
}

void mutate(std::vector<std::byte>& candidate, const chronos::common::ByteView input) {
  if (input.empty())
    return;
  const std::size_t index =
      static_cast<std::size_t>(std::to_integer<std::uint8_t>(input.front())) % candidate.size();
  const std::byte mask = input.size() > 1U ? input[1U] : std::byte{1U};
  candidate[index] ^= mask == std::byte{} ? std::byte{1U} : mask;
}

void exercise_canonical_mutations(const chronos::common::ByteView input) {
  using namespace chronos;
  const cluster::DistributedVectorQueryRequestV2 request{
      .source_node_id = 8U, .target_node_id = 7U, .dispatch = dispatch()};
  auto encoded_request = cluster::encode_distributed_vector_query_request_v2(request);
  if (!encoded_request.has_value())
    std::abort();
  mutate(*encoded_request, input);
  exercise_request_bytes(*encoded_request);

  const query::DistributedVectorResultSchema schema = result_schema();
  const cluster::DistributedVectorQueryResponseV2 response{
      .source_node_id = 7U,
      .target_node_id = 8U,
      .query_id = request.dispatch.dispatch.query_id,
      .tablet_id = request.dispatch.dispatch.tablet_id,
      .status_code = common::StatusCode::kUnavailable,
      .payload = std::nullopt,
      .leader_hint = cluster::DistributedQueryLeaderHint{.node_id = 9U, .placement_epoch = 2U}};
  auto encoded_response = cluster::encode_distributed_vector_query_response_v2(response, schema);
  if (!encoded_response.has_value())
    std::abort();
  mutate(*encoded_response, input);
  exercise_response_bytes(*encoded_response);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const chronos::common::ByteView bytes = std::as_bytes(std::span{data, size});
  exercise_request_bytes(bytes);
  exercise_response_bytes(bytes);
  exercise_canonical_mutations(bytes);
  return 0;
}
