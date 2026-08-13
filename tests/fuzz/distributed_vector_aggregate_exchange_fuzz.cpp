#include "chronos/query/distributed_vector_aggregate_exchange.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <ranges>
#include <span>
#include <vector>

namespace {

[[nodiscard]] std::array<chronos::query::VectorAggregateDefinition, 1U> definitions() {
  return {
      {{.operation = chronos::query::VectorAggregateOperation::kCountStar, .input = std::nullopt}}};
}

[[nodiscard]] chronos::query::DistributedVectorAggregateExchangeDecodeLimits limits() {
  chronos::query::DistributedVectorAggregateExchangeDecodeLimits value;
  value.maximum_frame_length = 65'536U;
  value.state.maximum_frame_length = 65'436U;
  value.state.maximum_variable_extremum_bytes = 32'768U;
  return value;
}

void exercise_bytes(const chronos::common::ByteView bytes) {
  using namespace chronos::query;
  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  const auto expected = definitions();
  const auto decode_limits = limits();
  const auto decoded = decode_distributed_vector_aggregate_exchange_message_exact(
      bytes, expected, resources, decode_limits);
  if (decoded.has_value()) {
    const auto encoded = encode_distributed_vector_aggregate_exchange_message(*decoded, expected);
    if (!encoded.has_value() || !std::ranges::equal(encoded->bytes(), bytes))
      std::abort();
  }

  DistributedVectorAggregateExchangeReader reader{
      std::vector<VectorAggregateDefinition>{expected.begin(), expected.end()}, resources,
      decode_limits};
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
  const auto expected = definitions();
  auto state = query::MergeableVectorAggregateState::create(expected[0]).value();
  const std::size_t additions =
      input.empty() ? 0U : std::to_integer<std::uint8_t>(input.front()) % 8U;
  for (std::size_t index = 0U; index < additions; ++index) {
    if (!state.accumulate_count_star().has_value())
      std::abort();
  }
  common::Uuid::Bytes query_bytes{};
  common::Uuid::Bytes tablet_bytes{};
  query_bytes.front() = std::byte{1U};
  tablet_bytes.front() = std::byte{2U};
  const auto tablet_id = schema::TabletId::from_bytes(tablet_bytes);
  if (!tablet_id.has_value())
    std::abort();
  const query::DistributedVectorAggregateExchangeMessage message{
      {.query_id = common::Uuid{query_bytes},
       .tablet_id = *tablet_id,
       .sequence = 1U,
       .aggregate_ordinal = 0U,
       .terminal = true},
      std::move(state)};
  const auto encoded =
      query::encode_distributed_vector_aggregate_exchange_message(message, expected);
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
