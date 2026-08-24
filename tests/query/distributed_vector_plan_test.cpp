#include "chronos/common/crc32c.hpp"
#include "chronos/query/distributed_vector_plan.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::query {
namespace {

void store_u32_le(std::vector<std::byte>& bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t index = 0U; index < 4U; ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

void rewrite_checksums(std::vector<std::byte>& bytes) {
  store_u32_le(bytes, 44U, common::crc32c(common::ByteView{bytes}.first(44U)));
  store_u32_le(bytes, bytes.size() - 4U,
               common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

[[nodiscard]] DistributedVectorPlanIntent grouped_plan() {
  return {.mode = DistributedVectorPlanMode::kGroupedAggregate,
          .group_key_input_indices = {2U, 0U},
          .aggregates = {{.operation = VectorAggregateOperation::kCountStar},
                         {.operation = VectorAggregateOperation::kSum, .input_index = 1U},
                         {.operation = VectorAggregateOperation::kMinimum, .input_index = 2U}},
          .order_keys = {{.output_index = 3U,
                          .direction = PhysicalSortDirection::kDescending,
                          .null_placement = ScalarNullPlacement::kFirst},
                         {.output_index = 0U}},
          .limit = 0U};
}

TEST(DistributedVectorPlanTest, RoundTripsRowsAggregatesOrderingAndPresentZeroLimit) {
  const DistributedVectorPlanIntent grouped = grouped_plan();
  const auto encoded = encode_distributed_vector_plan_intent(grouped);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->bytes().size(), 48U + 2U * 4U + 3U * 8U + 2U * 8U + 8U + 4U);
  const auto decoded = decode_distributed_vector_plan_intent_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, grouped);
  EXPECT_TRUE(decoded->row_output_indices.empty());
  EXPECT_EQ(decoded->limit, 0U);

  const DistributedVectorPlanIntent rows{.mode = DistributedVectorPlanMode::kRows,
                                         .row_output_indices = {2U, 0U, 2U},
                                         .order_keys = {{.output_index = 0U}},
                                         .limit = 17U};
  const auto encoded_rows = encode_distributed_vector_plan_intent(rows);
  ASSERT_TRUE(encoded_rows.has_value());
  const auto decoded_rows = decode_distributed_vector_plan_intent_exact(encoded_rows->bytes());
  ASSERT_TRUE(decoded_rows.has_value());
  EXPECT_EQ(*decoded_rows, rows);
  EXPECT_TRUE(decoded_rows->group_key_input_indices.empty());
  EXPECT_TRUE(decoded_rows->aggregates.empty());

  const DistributedVectorPlanIntent hidden_order{
      .mode = DistributedVectorPlanMode::kRows,
      .row_output_indices = {2U, 0U},
      .visible_row_output_indices = {0U},
      .order_keys = {{.output_index = 1U, .direction = PhysicalSortDirection::kDescending}}};
  const auto encoded_hidden = encode_distributed_vector_plan_intent(hidden_order);
  ASSERT_TRUE(encoded_hidden.has_value()) << encoded_hidden.error().to_string();
  EXPECT_EQ(encoded_hidden->bytes().size(), 48U + 2U * 4U + 1U * 4U + 1U * 8U + 4U);
  EXPECT_EQ(encoded_hidden->bytes()[10U], std::byte{1U});
  const auto decoded_hidden = decode_distributed_vector_plan_intent_exact(encoded_hidden->bytes());
  ASSERT_TRUE(decoded_hidden.has_value()) << decoded_hidden.error().to_string();
  EXPECT_EQ(*decoded_hidden, hidden_order);
  EXPECT_EQ(decode_distributed_vector_plan_intent_exact(encoded_hidden->bytes(),
                                                        {.maximum_visible_row_outputs = 0U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);

  const DistributedVectorPlanIntent ungrouped{
      .mode = DistributedVectorPlanMode::kUngroupedAggregate,
      .aggregates = {{.operation = VectorAggregateOperation::kCount, .input_index = 1U}}};
  const auto encoded_ungrouped = encode_distributed_vector_plan_intent(ungrouped);
  ASSERT_TRUE(encoded_ungrouped.has_value());
  EXPECT_EQ(decode_distributed_vector_plan_intent_exact(encoded_ungrouped->bytes()).value(),
            ungrouped);
}

TEST(DistributedVectorPlanTest, RejectsDamageNoncanonicalDescriptorsAndBounds) {
  const DistributedVectorPlanIntent grouped = grouped_plan();
  const auto encoded = encode_distributed_vector_plan_intent(grouped);
  ASSERT_TRUE(encoded.has_value());
  std::vector<std::byte> bytes(encoded->bytes().begin(), encoded->bytes().end());

  EXPECT_EQ(
      decode_distributed_vector_plan_intent_exact(common::ByteView{bytes}.first(bytes.size() - 1U))
          .error()
          .code(),
      common::StatusCode::kCorruption);

  std::vector<std::byte> unsupported = bytes;
  unsupported[8U] = std::byte{2U};
  rewrite_checksums(unsupported);
  EXPECT_EQ(decode_distributed_vector_plan_intent_exact(unsupported).error().code(),
            common::StatusCode::kNotSupported);

  std::vector<std::byte> unsupported_minor = bytes;
  unsupported_minor[10U] = std::byte{2U};
  rewrite_checksums(unsupported_minor);
  EXPECT_EQ(decode_distributed_vector_plan_intent_exact(unsupported_minor).error().code(),
            common::StatusCode::kNotSupported);

  std::vector<std::byte> noncanonical_minor = bytes;
  noncanonical_minor[10U] = std::byte{1U};
  rewrite_checksums(noncanonical_minor);
  EXPECT_EQ(decode_distributed_vector_plan_intent_exact(noncanonical_minor).error().code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> absent_input_nonzero = bytes;
  absent_input_nonzero[60U] = std::byte{1U};
  rewrite_checksums(absent_input_nonzero);
  EXPECT_EQ(decode_distributed_vector_plan_intent_exact(absent_input_nonzero).error().code(),
            common::StatusCode::kCorruption);

  EXPECT_EQ(
      decode_distributed_vector_plan_intent_exact(encoded->bytes(), {.maximum_group_keys = 1U})
          .error()
          .code(),
      common::StatusCode::kResourceExhausted);
  EXPECT_EQ(
      decode_distributed_vector_plan_intent_exact(encoded->bytes(), {.maximum_input_columns = 2U})
          .error()
          .code(),
      common::StatusCode::kResourceExhausted);
  EXPECT_EQ(
      decode_distributed_vector_plan_intent_exact(encoded->bytes(), {.maximum_output_columns = 4U})
          .error()
          .code(),
      common::StatusCode::kResourceExhausted);

  DistributedVectorPlanIntent duplicate_group = grouped;
  duplicate_group.group_key_input_indices = {2U, 2U};
  EXPECT_EQ(encode_distributed_vector_plan_intent(duplicate_group).error().code(),
            common::StatusCode::kInvalidArgument);
  DistributedVectorPlanIntent invalid_count = grouped;
  invalid_count.aggregates.front().input_index = 0U;
  EXPECT_EQ(encode_distributed_vector_plan_intent(invalid_count).error().code(),
            common::StatusCode::kInvalidArgument);
  DistributedVectorPlanIntent invalid_order = grouped;
  invalid_order.order_keys.front().output_index = 5U;
  EXPECT_EQ(encode_distributed_vector_plan_intent(invalid_order).error().code(),
            common::StatusCode::kInvalidArgument);
  DistributedVectorPlanIntent invalid_visible{.mode = DistributedVectorPlanMode::kRows,
                                              .row_output_indices = {0U, 1U},
                                              .visible_row_output_indices = {0U, 0U}};
  EXPECT_EQ(encode_distributed_vector_plan_intent(invalid_visible).error().code(),
            common::StatusCode::kInvalidArgument);
  invalid_visible.visible_row_output_indices = {2U};
  EXPECT_EQ(encode_distributed_vector_plan_intent(invalid_visible).error().code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::query
