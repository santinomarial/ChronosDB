#include "chronos/cluster/distributed_vector_row_finalization_v2.hpp"
#include "chronos/common/byte_reader.hpp"

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

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {.columns = {{"score", type(schema::LogicalTypeKind::kInt64), false},
                      {"label", type(schema::LogicalTypeKind::kString), true}}};
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
      // Guarded by the presence check above.
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
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

[[nodiscard]] query::DistributedVectorPlanIntent row_plan() {
  return {.mode = query::DistributedVectorPlanMode::kRows, .row_output_indices = {0U, 1U}};
}

[[nodiscard]] DistributedVectorQueryExecutionResultV2
execution_result(query::DistributedVectorPlanIntent plan,
                 std::vector<DistributedVectorResultExchangeMessage> messages) {
  return {.plan = std::move(plan),
          .result = {.result_schema = result_schema(), .messages = std::move(messages)}};
}

[[nodiscard]] std::vector<TestRow>
decode_rows(const DistributedVectorRowsFinalizedResultV2& result) {
  std::vector<TestRow> rows;
  for (const std::vector<std::byte>& encoded : result.encoded_batches) {
    const auto batch = network::decode_query_result_batch(encoded);
    EXPECT_TRUE(batch.has_value());
    if (!batch.has_value())
      return {};
    for (std::uint32_t row = 0U; row < batch->row_count(); ++row) {
      const network::QueryResultCell* score = batch->cell(row, 0U);
      const network::QueryResultCell* label = batch->cell(row, 1U);
      EXPECT_NE(score, nullptr);
      EXPECT_NE(label, nullptr);
      if (score == nullptr || label == nullptr)
        return {};
      common::ByteReader score_reader{score->value};
      const auto bits = score_reader.read_u64_le();
      EXPECT_TRUE(bits.has_value());
      if (!bits.has_value())
        return {};
      TestRow decoded{.score = std::bit_cast<std::int64_t>(*bits)};
      if (!label->is_null) {
        // Character bytes may be inspected through char by the C++ aliasing rules.
        // NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast)
        decoded.label =
            std::string{reinterpret_cast<const char*>(label->value.data()), label->value.size()};
        // NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast)
      }
      rows.push_back(std::move(decoded));
    }
  }
  return rows;
}

TEST(DistributedVectorRowFinalizationV2Test, OrdersAndLimitsOnlyAfterAllTabletStreamsClose) {
  auto plan = row_plan();
  plan.order_keys = {{.output_index = 0U,
                      .direction = query::PhysicalSortDirection::kDescending,
                      .null_placement = query::ScalarNullPlacement::kLast},
                     {.output_index = 1U,
                      .direction = query::PhysicalSortDirection::kAscending,
                      .null_placement = query::ScalarNullPlacement::kFirst}};
  plan.limit = 3U;
  auto input = execution_result(std::move(plan),
                                {message(2U, 1U, true, encode_rows({{5, "b"}, {3, std::nullopt}})),
                                 message(3U, 1U, true, encode_rows({{5, "a"}, {7, "z"}}))});
  auto finalized =
      finalize_distributed_vector_rows_v2(std::move(input), {.output_batch = {.maximum_rows = 2U}});
  ASSERT_TRUE(finalized.has_value()) << finalized.error().to_string();
  EXPECT_EQ(finalized->row_count, 3U);
  EXPECT_EQ(finalized->encoded_batches.size(), 2U);
  EXPECT_EQ(finalized->result_schema, result_schema());
  const std::vector<TestRow> rows = decode_rows(*finalized);
  ASSERT_EQ(rows.size(), 3U);
  EXPECT_EQ(rows[0].score, 7);
  EXPECT_EQ(rows[0].label, "z");
  EXPECT_EQ(rows[1].score, 5);
  EXPECT_EQ(rows[1].label, "a");
  EXPECT_EQ(rows[2].score, 5);
  EXPECT_EQ(rows[2].label, "b");
}

TEST(DistributedVectorRowFinalizationV2Test, PreservesPlanOrderForTiesAndEmitsSchemaForZeroLimit) {
  auto stable_plan = row_plan();
  stable_plan.order_keys = {{.output_index = 0U}};
  stable_plan.limit = 2U;
  auto stable_input = execution_result(
      std::move(stable_plan), {message(2U, 1U, true, encode_rows({{5, "first"}})),
                               message(3U, 1U, true, encode_rows({{5, "second"}, {6, "third"}}))});
  auto stable = finalize_distributed_vector_rows_v2(std::move(stable_input));
  ASSERT_TRUE(stable.has_value()) << stable.error().to_string();
  const std::vector<TestRow> stable_rows = decode_rows(*stable);
  ASSERT_EQ(stable_rows.size(), 2U);
  EXPECT_EQ(stable_rows[0].label, "first");
  EXPECT_EQ(stable_rows[1].label, "second");

  auto zero_plan = row_plan();
  zero_plan.limit = 0U;
  auto zero_input = execution_result(std::move(zero_plan),
                                     {message(2U, 1U, true, encode_rows({{5, "discarded"}}))});
  auto zero = finalize_distributed_vector_rows_v2(std::move(zero_input));
  ASSERT_TRUE(zero.has_value()) << zero.error().to_string();
  ASSERT_EQ(zero->encoded_batches.size(), 1U);
  EXPECT_EQ(zero->row_count, 0U);
  const auto zero_batch = network::decode_query_result_batch(zero->encoded_batches.front());
  ASSERT_TRUE(zero_batch.has_value());
  EXPECT_EQ(zero_batch->row_count(), 0U);
  EXPECT_EQ(zero_batch->columns().size(), 2U);
}

TEST(DistributedVectorRowFinalizationV2Test, RejectsDamagedStreamsSchemasAndResourceExcess) {
  auto gap = execution_result(row_plan(), {message(2U, 2U, true, encode_rows({{1, "x"}}))});
  EXPECT_EQ(finalize_distributed_vector_rows_v2(std::move(gap)).error().code(),
            common::StatusCode::kInvalidArgument);

  auto open = execution_result(row_plan(), {message(2U, 1U, false, encode_rows({{1, "x"}}))});
  EXPECT_EQ(finalize_distributed_vector_rows_v2(std::move(open)).error().code(),
            common::StatusCode::kInvalidArgument);

  auto duplicate = execution_result(row_plan(), {message(2U, 1U, true, encode_rows({{1, "x"}})),
                                                 message(3U, 1U, true, encode_rows({{2, "y"}})),
                                                 message(2U, 1U, true, encode_rows({{3, "z"}}))});
  EXPECT_EQ(finalize_distributed_vector_rows_v2(std::move(duplicate)).error().code(),
            common::StatusCode::kInvalidArgument);

  auto wrong_schema =
      execution_result(row_plan(), {message(2U, 1U, true, encode_rows({{1, "x"}}, "wrong"))});
  EXPECT_EQ(finalize_distributed_vector_rows_v2(std::move(wrong_schema)).error().code(),
            common::StatusCode::kCorruption);

  auto row_bounded =
      execution_result(row_plan(), {message(2U, 1U, true, encode_rows({{1, "x"}, {2, "y"}}))});
  EXPECT_EQ(finalize_distributed_vector_rows_v2(std::move(row_bounded), {.maximum_input_rows = 1U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);

  auto memory_bounded =
      execution_result(row_plan(), {message(2U, 1U, true, encode_rows({{1, "x"}}))});
  EXPECT_EQ(
      finalize_distributed_vector_rows_v2(std::move(memory_bounded), {.maximum_working_bytes = 1U})
          .error()
          .code(),
      common::StatusCode::kResourceExhausted);

  auto aggregate_plan = row_plan();
  aggregate_plan.mode = query::DistributedVectorPlanMode::kUngroupedAggregate;
  aggregate_plan.row_output_indices.clear();
  aggregate_plan.aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}};
  auto aggregate =
      execution_result(std::move(aggregate_plan), {message(2U, 1U, true, encode_rows({{1, "x"}}))});
  EXPECT_EQ(finalize_distributed_vector_rows_v2(std::move(aggregate)).error().code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cluster
