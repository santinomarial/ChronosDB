#include "chronos/cluster/distributed_vector_aggregate_rows_finalization_v2.hpp"
#include "chronos/common/byte_reader.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

struct TestRow {
  std::int64_t score{};
  std::optional<std::string> label;
};

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet(const std::uint8_t seed) {
  return schema::TabletId::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] query::DistributedVectorResultSchema input_schema() {
  return {.columns = {{"score", type(schema::LogicalTypeKind::kInt64), false},
                      {"label", type(schema::LogicalTypeKind::kString), true}}};
}

[[nodiscard]] query::DistributedVectorResultSchema output_schema() {
  return {.columns = {{"count_all", type(schema::LogicalTypeKind::kInt64), false},
                      {"count_label", type(schema::LogicalTypeKind::kInt64), false},
                      {"sum_score", type(schema::LogicalTypeKind::kInt64), true},
                      {"avg_score", type(schema::LogicalTypeKind::kFloat64), true},
                      {"min_label", type(schema::LogicalTypeKind::kString), true},
                      {"var_pop_score", type(schema::LogicalTypeKind::kFloat64), true}}};
}

[[nodiscard]] query::DistributedVectorPlanIntent aggregate_plan() {
  return {
      .mode = query::DistributedVectorPlanMode::kUngroupedAggregate,
      .aggregates = {
          {.operation = query::VectorAggregateOperation::kCountStar},
          {.operation = query::VectorAggregateOperation::kCount, .input_index = 1U},
          {.operation = query::VectorAggregateOperation::kSum, .input_index = 0U},
          {.operation = query::VectorAggregateOperation::kAverage, .input_index = 0U},
          {.operation = query::VectorAggregateOperation::kMinimum, .input_index = 1U},
          {.operation = query::VectorAggregateOperation::kVariancePopulation, .input_index = 0U}}};
}

[[nodiscard]] std::array<std::byte, 8U> signed_bytes(const std::int64_t value) {
  const std::uint64_t bits = std::bit_cast<std::uint64_t>(value);
  std::array<std::byte, 8U> bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index)
    bytes[index] = static_cast<std::byte>((bits >> (index * 8U)) & 0xffU);
  return bytes;
}

[[nodiscard]] std::vector<std::byte> encode_rows(const std::vector<TestRow>& rows,
                                                 const std::string& label_name = "label") {
  const std::array<network::QueryResultColumn, 2U> columns{
      network::QueryResultColumn{
          .name = "score", .type = type(schema::LogicalTypeKind::kInt64), .nullable = false},
      network::QueryResultColumn{
          .name = label_name, .type = type(schema::LogicalTypeKind::kString), .nullable = true}};
  std::vector<std::array<std::byte, 8U>> scores;
  std::vector<network::QueryResultCell> cells;
  scores.reserve(rows.size());
  cells.reserve(rows.size() * columns.size());
  for (const TestRow& row : rows)
    scores.push_back(signed_bytes(row.score));
  for (std::size_t index = 0U; index < rows.size(); ++index) {
    cells.push_back({.value = scores[index]});
    if (!rows[index].label.has_value()) {
      cells.push_back({.is_null = true});
    } else {
      const std::string& value = *rows[index].label;
      cells.push_back({.value = std::as_bytes(std::span{value.data(), value.size()})});
    }
  }
  return network::encode_query_result_batch(static_cast<std::uint32_t>(rows.size()), columns, cells)
      .value();
}

[[nodiscard]] DistributedVectorResultExchangeMessage message(const std::uint8_t tablet_seed,
                                                             const std::uint64_t sequence,
                                                             const bool terminal,
                                                             std::vector<std::byte> batch) {
  return {.query_id = uuid(1U),
          .tablet_id = tablet(tablet_seed),
          .sequence = sequence,
          .terminal = terminal,
          .encoded_result_batch = std::move(batch)};
}

[[nodiscard]] DistributedVectorQueryExecutionResultV2
execution_result(std::vector<DistributedVectorResultExchangeMessage> messages) {
  return {.plan = {.mode = query::DistributedVectorPlanMode::kRows, .row_output_indices = {0U, 1U}},
          .result = {.result_schema = input_schema(), .messages = std::move(messages)}};
}

[[nodiscard]] std::uint64_t bits_cell(const network::QueryResultBatchView& batch,
                                      const std::uint32_t column) {
  const network::QueryResultCell* cell = batch.cell(0U, column);
  EXPECT_NE(cell, nullptr);
  if (cell == nullptr)
    return 0U;
  common::ByteReader reader{cell->value};
  const auto bits = reader.read_u64_le();
  EXPECT_TRUE(bits.has_value());
  return bits.value_or(0U);
}

TEST(DistributedVectorAggregateRowsFinalizationV2Test,
     AggregatesCompleteTabletRowsAndPublishesOneCanonicalResult) {
  auto input =
      execution_result({message(2U, 1U, true, encode_rows({{1, "z"}, {3, std::nullopt}})),
                        message(3U, 1U, true, encode_rows({{5, "a"}})), message(4U, 1U, true, {})});
  auto plan = aggregate_plan();
  auto finalized =
      finalize_distributed_vector_aggregate_rows_v2(std::move(input), plan, output_schema());
  ASSERT_TRUE(finalized.has_value()) << finalized.error().to_string();
  EXPECT_EQ(finalized->row_count, 1U);
  EXPECT_EQ(finalized->result_schema, output_schema());
  auto batch = network::decode_query_result_batch(finalized->encoded_batch);
  ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
  ASSERT_EQ(batch->row_count(), 1U);
  EXPECT_EQ(std::bit_cast<std::int64_t>(bits_cell(*batch, 0U)), 3);
  EXPECT_EQ(std::bit_cast<std::int64_t>(bits_cell(*batch, 1U)), 2);
  EXPECT_EQ(std::bit_cast<std::int64_t>(bits_cell(*batch, 2U)), 9);
  EXPECT_DOUBLE_EQ(std::bit_cast<double>(bits_cell(*batch, 3U)), 3.0);
  EXPECT_DOUBLE_EQ(std::bit_cast<double>(bits_cell(*batch, 5U)), 8.0 / 3.0);
  const network::QueryResultCell* minimum = batch->cell(0U, 4U);
  ASSERT_NE(minimum, nullptr);
  const std::string expected = "a";
  const auto expected_bytes = std::as_bytes(std::span{expected.data(), expected.size()});
  EXPECT_EQ(minimum->value.size(), expected_bytes.size());
  EXPECT_TRUE(std::equal(minimum->value.begin(), minimum->value.end(), expected_bytes.begin(),
                         expected_bytes.end()));
}

TEST(DistributedVectorAggregateRowsFinalizationV2Test,
     EmptyTabletSetProducesCountZeroAndNullableAggregateNulls) {
  auto input = execution_result({message(2U, 1U, true, {}), message(3U, 1U, true, {})});
  auto plan = aggregate_plan();
  auto finalized =
      finalize_distributed_vector_aggregate_rows_v2(std::move(input), plan, output_schema());
  ASSERT_TRUE(finalized.has_value()) << finalized.error().to_string();
  auto batch = network::decode_query_result_batch(finalized->encoded_batch);
  ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
  ASSERT_EQ(batch->row_count(), 1U);
  EXPECT_EQ(std::bit_cast<std::int64_t>(bits_cell(*batch, 0U)), 0);
  EXPECT_EQ(std::bit_cast<std::int64_t>(bits_cell(*batch, 1U)), 0);
  for (std::uint32_t column = 2U; column < 6U; ++column) {
    const network::QueryResultCell* cell = batch->cell(0U, column);
    ASSERT_NE(cell, nullptr);
    EXPECT_TRUE(cell->is_null);
    EXPECT_TRUE(cell->value.empty());
  }
}

TEST(DistributedVectorAggregateRowsFinalizationV2Test,
     AppliesGlobalLimitAndRejectsIncompleteOrMismatchedInput) {
  auto zero_input = execution_result({message(2U, 1U, true, encode_rows({{1, "x"}}))});
  auto zero_plan = aggregate_plan();
  zero_plan.limit = 0U;
  auto zero = finalize_distributed_vector_aggregate_rows_v2(std::move(zero_input), zero_plan,
                                                            output_schema());
  ASSERT_TRUE(zero.has_value()) << zero.error().to_string();
  EXPECT_EQ(zero->row_count, 0U);

  auto plan = aggregate_plan();
  auto incomplete = execution_result({message(2U, 1U, false, encode_rows({{1, "x"}}))});
  EXPECT_EQ(
      finalize_distributed_vector_aggregate_rows_v2(std::move(incomplete), plan, output_schema())
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);

  auto wrong_descriptor =
      execution_result({message(2U, 1U, true, encode_rows({{1, "x"}}, "wrong"))});
  EXPECT_EQ(finalize_distributed_vector_aggregate_rows_v2(std::move(wrong_descriptor), plan,
                                                          output_schema())
                .error()
                .code(),
            common::StatusCode::kCorruption);

  auto bounded = execution_result({message(2U, 1U, true, encode_rows({{1, "x"}, {2, "y"}}))});
  EXPECT_EQ(finalize_distributed_vector_aggregate_rows_v2(std::move(bounded), plan, output_schema(),
                                                          {.maximum_input_rows = 1U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);

  auto working = execution_result({message(2U, 1U, true, encode_rows({{1, "x"}}))});
  EXPECT_EQ(finalize_distributed_vector_aggregate_rows_v2(std::move(working), plan, output_schema(),
                                                          {.maximum_working_bytes = 1U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);

  auto duplicate = execution_result({message(2U, 1U, true, encode_rows({{1, "x"}})),
                                     message(3U, 1U, true, encode_rows({{2, "y"}})),
                                     message(2U, 1U, true, encode_rows({{3, "z"}}))});
  EXPECT_EQ(
      finalize_distributed_vector_aggregate_rows_v2(std::move(duplicate), plan, output_schema())
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);

  auto nonidentity = execution_result({message(2U, 1U, true, encode_rows({{1, "x"}}))});
  nonidentity.plan.row_output_indices = {1U, 0U};
  EXPECT_EQ(
      finalize_distributed_vector_aggregate_rows_v2(std::move(nonidentity), plan, output_schema())
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cluster
