#include "chronos/cluster/distributed_vector_result_exchange.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <span>
#include <utility>
#include <vector>

namespace {

[[nodiscard]] chronos::query::DistributedVectorResultSchema
schema_from_input(const chronos::common::ByteView bytes) {
  using chronos::schema::LogicalType;
  using chronos::schema::LogicalTypeKind;
  const bool text = !bytes.empty() && (std::to_integer<std::uint8_t>(bytes.front()) & 1U) != 0U;
  const bool nullable = bytes.size() > 1U && (std::to_integer<std::uint8_t>(bytes[1U]) & 1U) != 0U;
  return {
      .columns = {
          {"value",
           LogicalType::create(text ? LogicalTypeKind::kString : LogicalTypeKind::kInt64).value(),
           nullable}}};
}

void exercise_bytes(const chronos::common::ByteView bytes) {
  using namespace chronos::cluster;
  chronos::query::DistributedVectorResultSchema schema = schema_from_input(bytes);
  DistributedVectorResultExchangeDecodeLimits limits;
  limits.maximum_frame_length = 65'536U;
  limits.result_batch.protocol.maximum_payload_size = 65'452U;
  limits.result_batch.maximum_rows = 4096U;
  limits.result_batch.maximum_columns = 16U;
  limits.result_batch.maximum_column_name_bytes = 64U;

  const auto decoded =
      decode_distributed_vector_result_exchange_message_v2_exact(bytes, schema, limits);
  if (decoded.has_value()) {
    const auto encoded = encode_distributed_vector_result_exchange_message_v2(*decoded, schema);
    if (!encoded.has_value() || !std::ranges::equal(encoded->bytes(), bytes))
      std::abort();
  }

  DistributedVectorResultExchangeReader reader{std::move(schema), limits};
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
  if (prefix->message.has_value() && suffix->message.has_value())
    std::abort();
}

void exercise_canonical_mutation(const chronos::common::ByteView input) {
  using namespace chronos;
  const query::DistributedVectorResultSchema result_schema = schema_from_input(input);
  const std::array<network::QueryResultColumn, 1U> columns{
      network::QueryResultColumn{result_schema.columns[0].name, result_schema.columns[0].type,
                                 result_schema.columns[0].nullable}};
  const auto batch = network::encode_query_result_batch(0U, columns, {});
  if (!batch.has_value())
    std::abort();
  common::Uuid::Bytes query_bytes{};
  common::Uuid::Bytes tablet_bytes{};
  query_bytes.front() = std::byte{1U};
  tablet_bytes.front() = std::byte{2U};
  const auto tablet_id = schema::TabletId::from_bytes(tablet_bytes);
  if (!tablet_id.has_value())
    std::abort();
  const auto encoded = cluster::encode_distributed_vector_result_exchange_message_v2(
      {.query_id = common::Uuid{query_bytes},
       .tablet_id = *tablet_id,
       .sequence = 1U,
       .terminal = true,
       .encoded_result_batch = *batch},
      result_schema);
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
