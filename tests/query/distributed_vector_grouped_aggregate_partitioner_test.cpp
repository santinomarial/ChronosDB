#include "chronos/query/distributed_vector_grouped_aggregate_partitioner.hpp"

#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <variant>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  return Identifier::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] std::vector<VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U, .type = type(schema::LogicalTypeKind::kString), .nullable = false},
          {.column_ordinal = 1U, .type = type(schema::LogicalTypeKind::kBool), .nullable = true}};
}

[[nodiscard]] std::vector<VectorAggregateDefinition> aggregates() {
  return {{.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt},
          {.operation = VectorAggregateOperation::kCount,
           .input = VectorAggregateInput{.column_ordinal = 2U,
                                         .type = type(schema::LogicalTypeKind::kInt64),
                                         .nullable = true}}};
}

[[nodiscard]] std::vector<MergeableVectorAggregateState>
states(const std::span<const VectorAggregateDefinition> definitions) {
  std::vector<MergeableVectorAggregateState> result;
  result.reserve(definitions.size());
  for (const VectorAggregateDefinition& definition : definitions)
    result.push_back(MergeableVectorAggregateState::create(definition).value());
  EXPECT_TRUE(result.front().accumulate_count_star().has_value());
  EXPECT_TRUE(result.front().accumulate_count_star().has_value());
  return result;
}

[[nodiscard]] EncodedDistributedVectorGroupedAggregateExchangeMessage
encoded_group(const std::vector<VectorGroupKeyDefinition>& expected_keys,
              const std::vector<VectorAggregateDefinition>& expected_aggregates,
              const std::uint32_t ordinal, const std::uint32_t count, std::string label,
              const std::optional<bool> enabled) {
  std::vector<ScalarValue> values;
  values.push_back(ScalarValue::text(expected_keys[0].type, std::move(label)).value());
  values.push_back(enabled.has_value() ? ScalarValue::boolean(*enabled).value()
                                       : ScalarValue::null(expected_keys[1].type));
  return encode_distributed_vector_grouped_aggregate_exchange_message(
             {.query_id = uuid(1U),
              .tablet_id = id<schema::TabletId>(2U),
              .sequence = static_cast<std::uint64_t>(ordinal) + 1U,
              .group_ordinal = ordinal,
              .group_count = count,
              .terminal = ordinal + 1U == count,
              .empty = false},
             values, states(expected_aggregates), expected_keys, expected_aggregates)
      .value();
}

[[nodiscard]] std::vector<EncodedDistributedVectorGroupedAggregateExchangeMessage>
encoded_groups(const std::vector<VectorGroupKeyDefinition>& expected_keys,
               const std::vector<VectorAggregateDefinition>& expected_aggregates) {
  std::vector<EncodedDistributedVectorGroupedAggregateExchangeMessage> result;
  result.push_back(encoded_group(expected_keys, expected_aggregates, 0U, 3U, "alpha", false));
  result.push_back(encoded_group(expected_keys, expected_aggregates, 1U, 3U, "beta", std::nullopt));
  result.push_back(encoded_group(expected_keys, expected_aggregates, 2U, 3U, "gamma", true));
  return result;
}

TEST(DistributedVectorGroupedAggregatePartitionerTest,
     RoutesCanonicalKeysAndEmitsCompleteStreamForEveryPartition) {
  const auto expected_keys = keys();
  const auto expected_aggregates = aggregates();
  QueryResourceContext resources = QueryResourceContext::create(8U << 20U).value();
  auto partitioner = DistributedVectorGroupedAggregatePartitioner::create(
                         expected_keys, expected_aggregates, resources, 8U)
                         .value();
  const auto input = encoded_groups(expected_keys, expected_aggregates);
  auto output = partitioner.partition(input);
  ASSERT_TRUE(output.has_value()) << output.error().to_string();
  ASSERT_EQ(output->size(), 8U);

  std::set<std::string> observed;
  std::size_t total_bytes{};
  for (std::size_t partition = 0U; partition < output->size(); ++partition) {
    const auto& stream = (*output)[partition];
    EXPECT_EQ(stream.partition_id, partition);
    ASSERT_FALSE(stream.messages.empty());
    std::size_t stream_bytes{};
    for (std::size_t ordinal = 0U; ordinal < stream.messages.size(); ++ordinal) {
      stream_bytes += stream.messages[ordinal].bytes().size();
      auto decoded = decode_distributed_vector_grouped_aggregate_exchange_message_exact(
          stream.messages[ordinal].bytes(), expected_keys, expected_aggregates, resources);
      ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
      const auto& position = decoded->position();
      EXPECT_EQ(position.query_id, uuid(1U));
      EXPECT_EQ(position.tablet_id, id<schema::TabletId>(2U));
      if (position.empty) {
        EXPECT_EQ(stream.messages.size(), 1U);
        EXPECT_EQ(position.group_count, 0U);
        EXPECT_EQ(position.sequence, 1U);
        EXPECT_TRUE(position.terminal);
        continue;
      }
      EXPECT_EQ(position.group_ordinal, ordinal);
      EXPECT_EQ(position.group_count, stream.messages.size());
      EXPECT_EQ(position.sequence, ordinal + 1U);
      EXPECT_EQ(position.terminal, ordinal + 1U == stream.messages.size());
      const auto hash = canonical_vector_group_key_hash_v1(expected_keys, decoded->keys());
      ASSERT_TRUE(hash.has_value());
      EXPECT_EQ(*hash % output->size(), partition);
      observed.insert(std::get<std::string>(decoded->keys()[0].storage()));
    }
    EXPECT_EQ(stream.encoded_bytes, stream_bytes);
    total_bytes += stream_bytes;
  }
  EXPECT_EQ(observed, (std::set<std::string>{"alpha", "beta", "gamma"}));
  EXPECT_GE(total_bytes,
            input[0].bytes().size() + input[1].bytes().size() + input[2].bytes().size());
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);

  std::vector<EncodedDistributedVectorGroupedAggregateExchangeMessage> empty_input;
  empty_input.push_back(encode_distributed_vector_grouped_aggregate_exchange_message(
                            {.query_id = uuid(1U),
                             .tablet_id = id<schema::TabletId>(2U),
                             .sequence = 1U,
                             .group_ordinal = 0U,
                             .group_count = 0U,
                             .terminal = true,
                             .empty = true},
                            {}, {}, expected_keys, expected_aggregates)
                            .value());
  auto empty_output = partitioner.partition(empty_input);
  ASSERT_TRUE(empty_output.has_value());
  ASSERT_EQ(empty_output->size(), 8U);
  for (const auto& stream : *empty_output) {
    ASSERT_EQ(stream.messages.size(), 1U);
    auto decoded = decode_distributed_vector_grouped_aggregate_exchange_message_exact(
        stream.messages[0].bytes(), expected_keys, expected_aggregates, resources);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(decoded->position().empty);
  }
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(DistributedVectorGroupedAggregatePartitionerTest,
     HashV1MatchesSignedZeroNanAndNullGroupingEquivalence) {
  const std::vector<VectorGroupKeyDefinition> float_key{
      {.column_ordinal = 0U, .type = type(schema::LogicalTypeKind::kFloat64), .nullable = true}};
  const double first_nan = std::bit_cast<double>(std::uint64_t{0x7ff8000000000001ULL});
  const double second_nan = std::bit_cast<double>(std::uint64_t{0xfff8000000000042ULL});
  const std::array<ScalarValue, 1U> positive_zero{ScalarValue::float64(0.0).value()};
  const std::array<ScalarValue, 1U> negative_zero{ScalarValue::float64(-0.0).value()};
  const std::array<ScalarValue, 1U> nan_one{ScalarValue::float64(first_nan).value()};
  const std::array<ScalarValue, 1U> nan_two{ScalarValue::float64(second_nan).value()};
  const std::array<ScalarValue, 1U> null_one{ScalarValue::null(float_key[0].type)};
  EXPECT_EQ(canonical_vector_group_key_hash_v1(float_key, positive_zero).value(),
            canonical_vector_group_key_hash_v1(float_key, negative_zero).value());
  EXPECT_EQ(canonical_vector_group_key_hash_v1(float_key, nan_one).value(),
            canonical_vector_group_key_hash_v1(float_key, nan_two).value());
  EXPECT_TRUE(canonical_vector_group_key_hash_v1(float_key, null_one).has_value());

  auto non_nullable = float_key;
  non_nullable[0].nullable = false;
  EXPECT_EQ(canonical_vector_group_key_hash_v1(non_nullable, null_one).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorGroupedAggregatePartitionerTest,
     RejectsIncompleteInputSkewAndTotalAmplificationAtomically) {
  const auto expected_keys = keys();
  const auto expected_aggregates = aggregates();
  QueryResourceContext resources = QueryResourceContext::create(8U << 20U).value();

  std::vector<EncodedDistributedVectorGroupedAggregateExchangeMessage> incomplete;
  incomplete.push_back(
      encoded_group(expected_keys, expected_aggregates, 0U, 2U, "only-first", false));
  auto normal = DistributedVectorGroupedAggregatePartitioner::create(
                    expected_keys, expected_aggregates, resources, 2U)
                    .value();
  EXPECT_EQ(normal.partition(incomplete).error().code(), common::StatusCode::kInvalidArgument);

  DistributedVectorGroupedAggregatePartitionerLimits skew_limits;
  skew_limits.maximum_groups_per_partition = 1U;
  auto skew = DistributedVectorGroupedAggregatePartitioner::create(
                  expected_keys, expected_aggregates, resources, 1U, skew_limits)
                  .value();
  const auto input = encoded_groups(expected_keys, expected_aggregates);
  EXPECT_EQ(skew.partition(input).error().code(), common::StatusCode::kResourceExhausted);

  DistributedVectorGroupedAggregatePartitionerLimits byte_limits;
  byte_limits.maximum_total_output_encoded_bytes =
      distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength;
  auto bounded = DistributedVectorGroupedAggregatePartitioner::create(
                     expected_keys, expected_aggregates, resources, 2U, byte_limits)
                     .value();
  EXPECT_EQ(bounded.partition(input).error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(normal.partition(input).has_value());

  byte_limits.maximum_total_output_encoded_bytes =
      kMaximumDistributedVectorGroupedPartitionBytes + 1U;
  EXPECT_EQ(DistributedVectorGroupedAggregatePartitioner::create(expected_keys, expected_aggregates,
                                                                 resources, 2U, byte_limits)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::query
