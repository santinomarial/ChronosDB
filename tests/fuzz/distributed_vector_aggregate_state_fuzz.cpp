#include "chronos/query/distributed_vector_aggregate_state.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <ranges>
#include <span>
#include <vector>

namespace {

[[nodiscard]] chronos::query::DistributedVectorAggregateStateDecodeLimits limits() {
  return {.maximum_frame_length = 65'536U, .maximum_variable_extremum_bytes = 32'768U};
}

void exercise_bytes(const chronos::common::ByteView bytes) {
  using namespace chronos::query;
  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  const DistributedVectorAggregateStateDecodeLimits decode_limits = limits();
  const auto decoded =
      decode_mergeable_vector_aggregate_state_exact(bytes, resources, decode_limits);
  if (decoded.has_value()) {
    const auto encoded = encode_mergeable_vector_aggregate_state(*decoded);
    if (!encoded.has_value() || !std::ranges::equal(encoded->bytes(), bytes))
      std::abort();
  }

  MergeableVectorAggregateStateReader reader{resources, decode_limits};
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
  if (prefix->state.has_value() && suffix->state.has_value())
    std::abort();
}

void exercise_canonical_mutation(const chronos::common::ByteView input) {
  using namespace chronos::query;
  auto state = MergeableVectorAggregateState::create(
                   {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt})
                   .value();
  const std::size_t additions =
      input.empty() ? 0U : std::to_integer<std::uint8_t>(input.front()) % 8U;
  for (std::size_t index = 0U; index < additions; ++index) {
    if (!state.accumulate_count_star().has_value())
      std::abort();
  }
  const auto encoded = encode_mergeable_vector_aggregate_state(state);
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
