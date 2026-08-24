#include "chronos/cluster/distributed_vector_row_finalization_v2.hpp"
#include "chronos/common/byte_reader.hpp"
#include "chronos/query/distributed_sql_lowering.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
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

[[nodiscard]] query::VectorExpression add_score_expression(const std::int64_t value) {
  std::vector<query::VectorExpressionInstruction> instructions;
  instructions.emplace_back(
      query::VectorInputExpression{.input_column_ordinal = 0U,
                                   .type = type(schema::LogicalTypeKind::kInt64),
                                   .nullable = false});
  instructions.emplace_back(query::VectorConstantExpression{
      .value =
          query::ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), value).value()});
  instructions.emplace_back(
      query::VectorBinaryExpression{.operation = query::VectorBinaryOperation::kAdd,
                                    .left_instruction = 0U,
                                    .right_instruction = 1U});
  return query::VectorExpression::create(std::move(instructions)).value();
}

[[nodiscard]] query::VectorExpression lower_label_expression() {
  std::vector<query::VectorExpressionInstruction> instructions;
  instructions.emplace_back(
      query::VectorInputExpression{.input_column_ordinal = 1U,
                                   .type = type(schema::LogicalTypeKind::kString),
                                   .nullable = true});
  instructions.emplace_back(query::VectorUnaryExpression{
      .operation = query::VectorUnaryOperation::kLowerAscii, .operand_instruction = 0U});
  return query::VectorExpression::create(std::move(instructions)).value();
}

[[nodiscard]] query::VectorExpression nonnull_label_predicate() {
  std::vector<query::VectorExpressionInstruction> instructions;
  instructions.emplace_back(
      query::VectorInputExpression{.input_column_ordinal = 1U,
                                   .type = type(schema::LogicalTypeKind::kString),
                                   .nullable = true});
  instructions.emplace_back(query::VectorUnaryExpression{
      .operation = query::VectorUnaryOperation::kIsNotNull, .operand_instruction = 0U});
  return query::VectorExpression::create(std::move(instructions)).value();
}

[[nodiscard]] query::VectorExpression divide_score_expression(const std::int64_t divisor) {
  std::vector<query::VectorExpressionInstruction> instructions;
  instructions.emplace_back(
      query::VectorInputExpression{.input_column_ordinal = 0U,
                                   .type = type(schema::LogicalTypeKind::kInt64),
                                   .nullable = false});
  instructions.emplace_back(query::VectorConstantExpression{
      .value = query::ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), divisor)
                   .value()});
  instructions.emplace_back(
      query::VectorBinaryExpression{.operation = query::VectorBinaryOperation::kDivide,
                                    .left_instruction = 0U,
                                    .right_instruction = 1U});
  return query::VectorExpression::create(std::move(instructions)).value();
}

[[nodiscard]] query::VectorExpression divide_score_predicate(const std::int64_t divisor) {
  std::vector<query::VectorExpressionInstruction> instructions;
  instructions.emplace_back(
      query::VectorInputExpression{.input_column_ordinal = 0U,
                                   .type = type(schema::LogicalTypeKind::kInt64),
                                   .nullable = false});
  instructions.emplace_back(query::VectorConstantExpression{
      .value = query::ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), divisor)
                   .value()});
  instructions.emplace_back(
      query::VectorBinaryExpression{.operation = query::VectorBinaryOperation::kDivide,
                                    .left_instruction = 0U,
                                    .right_instruction = 1U});
  instructions.emplace_back(query::VectorConstantExpression{
      .value = query::ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), 0).value()});
  instructions.emplace_back(
      query::VectorBinaryExpression{.operation = query::VectorBinaryOperation::kGreater,
                                    .left_instruction = 2U,
                                    .right_instruction = 3U});
  return query::VectorExpression::create(std::move(instructions)).value();
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

TEST(DistributedVectorRowFinalizationV2Test, SortsByHiddenOutputAndPublishesOnlyVisibleColumns) {
  auto plan = row_plan();
  plan.visible_row_output_indices = {1U};
  plan.order_keys = {{.output_index = 0U,
                      .direction = query::PhysicalSortDirection::kDescending,
                      .null_placement = query::ScalarNullPlacement::kLast}};
  auto input = execution_result(std::move(plan),
                                {message(2U, 1U, true, encode_rows({{5, "middle"}, {3, "last"}})),
                                 message(3U, 1U, true, encode_rows({{7, "first"}}))});
  auto finalized = finalize_distributed_vector_rows_v2(std::move(input));
  ASSERT_TRUE(finalized.has_value()) << finalized.error().to_string();
  ASSERT_EQ(finalized->result_schema.columns.size(), 1U);
  EXPECT_EQ(finalized->result_schema.columns.front().name, "label");
  ASSERT_EQ(finalized->encoded_batches.size(), 1U);
  const auto batch = network::decode_query_result_batch(finalized->encoded_batches.front());
  ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
  ASSERT_EQ(batch->columns().size(), 1U);
  ASSERT_EQ(batch->row_count(), 3U);
  const std::array<std::string_view, 3U> expected{"first", "middle", "last"};
  for (std::uint32_t row = 0U; row < batch->row_count(); ++row) {
    const network::QueryResultCell* cell = batch->cell(row, 0U);
    ASSERT_NE(cell, nullptr);
    EXPECT_TRUE(std::ranges::equal(cell->value, std::as_bytes(std::span{expected[row]})));
  }
}

TEST(DistributedVectorRowFinalizationV2Test,
     ProjectsSourceAndCanonicalConstantsAfterGlobalOrderAndLimit) {
  auto plan = row_plan();
  plan.order_keys = {{.output_index = 0U,
                      .direction = query::PhysicalSortDirection::kDescending,
                      .null_placement = query::ScalarNullPlacement::kLast}};
  plan.limit = 2U;
  auto input = execution_result(std::move(plan),
                                {message(2U, 1U, true, encode_rows({{5, "middle"}, {3, "last"}})),
                                 message(3U, 1U, true, encode_rows({{7, "first"}}))});
  const auto nine_bytes = signed_bytes(9);
  query::DistributedVectorRowCoordinatorProjection projection{
      .outputs = {query::DistributedVectorRowSourceOutput{.worker_output_index = 1U},
                  query::DistributedVectorRowConstantOutput{
                      .is_null = false, .canonical_value = {nine_bytes.begin(), nine_bytes.end()}},
                  query::DistributedVectorRowConstantOutput{.is_null = true}},
      .result_schema = {.columns = {{"name", type(schema::LogicalTypeKind::kString), true},
                                    {"nine", type(schema::LogicalTypeKind::kInt64), false},
                                    {"missing", type(schema::LogicalTypeKind::kString), true}}}};
  auto finalized =
      finalize_distributed_vector_rows_with_projection_v2(std::move(input), projection);
  ASSERT_TRUE(finalized.has_value()) << finalized.error().to_string();
  ASSERT_EQ(finalized->encoded_batches.size(), 1U);
  const auto batch = network::decode_query_result_batch(finalized->encoded_batches.front());
  ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
  ASSERT_EQ(batch->row_count(), 2U);
  ASSERT_EQ(batch->columns().size(), 3U);
  const std::array<std::string_view, 2U> expected{"first", "middle"};
  for (std::uint32_t row = 0U; row < batch->row_count(); ++row) {
    const network::QueryResultCell* name = batch->cell(row, 0U);
    const network::QueryResultCell* nine = batch->cell(row, 1U);
    const network::QueryResultCell* missing = batch->cell(row, 2U);
    ASSERT_NE(name, nullptr);
    ASSERT_NE(nine, nullptr);
    ASSERT_NE(missing, nullptr);
    EXPECT_TRUE(std::ranges::equal(name->value, std::as_bytes(std::span{expected[row]})));
    common::ByteReader reader{nine->value};
    EXPECT_EQ(reader.read_i64_le(), 9);
    EXPECT_TRUE(missing->is_null);
  }

  auto malformed_input =
      execution_result(row_plan(), {message(2U, 1U, true, encode_rows({{1, "x"}}))});
  auto malformed = projection;
  std::get<query::DistributedVectorRowSourceOutput>(malformed.outputs[0]).worker_output_index = 9U;
  EXPECT_EQ(
      finalize_distributed_vector_rows_with_projection_v2(std::move(malformed_input), malformed)
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorRowFinalizationV2Test, AppliesCoordinatorPredicateBeforeGlobalOrderAndLimit) {
  auto plan = row_plan();
  plan.order_keys = {{.output_index = 0U,
                      .direction = query::PhysicalSortDirection::kAscending,
                      .null_placement = query::ScalarNullPlacement::kLast}};
  plan.limit = 2U;
  auto input = execution_result(
      std::move(plan), {message(2U, 1U, true, encode_rows({{5, "middle"}, {3, std::nullopt}})),
                        message(3U, 1U, true, encode_rows({{7, "first"}}))});
  const query::DistributedVectorRowCoordinatorProjection projection{
      .outputs = {query::DistributedVectorRowSourceOutput{.worker_output_index = 0U},
                  query::DistributedVectorRowSourceOutput{.worker_output_index = 1U}},
      .result_schema = result_schema(),
      .predicate = nonnull_label_predicate()};
  auto finalized =
      finalize_distributed_vector_rows_with_projection_v2(std::move(input), projection);
  ASSERT_TRUE(finalized.has_value()) << finalized.error().to_string();
  EXPECT_EQ(finalized->row_count, 2U);
  const std::vector<TestRow> rows = decode_rows(*finalized);
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0].score, 5);
  EXPECT_EQ(rows[0].label, "middle");
  EXPECT_EQ(rows[1].score, 7);
  EXPECT_EQ(rows[1].label, "first");
}

TEST(DistributedVectorRowFinalizationV2Test,
     EvaluatesCheckedFixedAndTextExpressionsAfterGlobalOrderAndLimit) {
  auto plan = row_plan();
  plan.order_keys = {{.output_index = 0U,
                      .direction = query::PhysicalSortDirection::kDescending,
                      .null_placement = query::ScalarNullPlacement::kLast}};
  plan.limit = 3U;
  auto input = execution_result(
      std::move(plan), {message(2U, 1U, true, encode_rows({{5, "MiDdLe"}, {3, std::nullopt}})),
                        message(3U, 1U, true, encode_rows({{7, "FIRST"}}))});
  const query::DistributedVectorRowCoordinatorProjection projection{
      .outputs = {query::DistributedVectorRowExpressionOutput{.expression =
                                                                  add_score_expression(2)},
                  query::DistributedVectorRowExpressionOutput{.expression =
                                                                  lower_label_expression()}},
      .result_schema = {.columns = {{"shifted", type(schema::LogicalTypeKind::kInt64), false},
                                    {"folded", type(schema::LogicalTypeKind::kString), true}}}};
  auto finalized = finalize_distributed_vector_rows_with_projection_v2(
      std::move(input), projection, {.output_batch = {.maximum_rows = 2U}});
  ASSERT_TRUE(finalized.has_value()) << finalized.error().to_string();
  EXPECT_EQ(finalized->row_count, 3U);
  EXPECT_EQ(finalized->encoded_batches.size(), 2U);
  const std::array<std::int64_t, 3U> expected_scores{9, 7, 5};
  const std::array<std::optional<std::string_view>, 3U> expected_labels{"first", "middle",
                                                                        std::nullopt};
  std::size_t output_row{};
  for (const auto& encoded : finalized->encoded_batches) {
    auto batch = network::decode_query_result_batch(encoded);
    ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
    for (std::uint32_t row = 0U; row < batch->row_count(); ++row) {
      const network::QueryResultCell* shifted = batch->cell(row, 0U);
      const network::QueryResultCell* folded = batch->cell(row, 1U);
      ASSERT_NE(shifted, nullptr);
      ASSERT_NE(folded, nullptr);
      common::ByteReader reader{shifted->value};
      EXPECT_EQ(reader.read_i64_le(), expected_scores[output_row]);
      EXPECT_EQ(folded->is_null, !expected_labels[output_row].has_value());
      if (expected_labels[output_row].has_value()) {
        EXPECT_TRUE(std::ranges::equal(folded->value,
                                       std::as_bytes(std::span{*expected_labels[output_row]})));
      }
      ++output_row;
    }
  }
  EXPECT_EQ(output_row, 3U);
}

TEST(DistributedVectorRowFinalizationV2Test,
     RetainsOneExpressionSizePerRowAcrossPayloadDrivenBatchSplits) {
  const auto make_input = [] {
    auto plan = row_plan();
    plan.order_keys = {{.output_index = 0U,
                        .direction = query::PhysicalSortDirection::kDescending,
                        .null_placement = query::ScalarNullPlacement::kLast}};
    plan.limit = 3U;
    return execution_result(std::move(plan),
                            {message(2U, 1U, true, encode_rows({{5, "MiDdLe"}, {3, std::nullopt}})),
                             message(3U, 1U, true, encode_rows({{7, "FIRST"}}))});
  };
  const query::DistributedVectorRowCoordinatorProjection projection{
      .outputs = {query::DistributedVectorRowExpressionOutput{.expression =
                                                                  add_score_expression(2)},
                  query::DistributedVectorRowExpressionOutput{.expression =
                                                                  lower_label_expression()}},
      .result_schema = {.columns = {{"shifted", type(schema::LogicalTypeKind::kInt64), false},
                                    {"folded", type(schema::LogicalTypeKind::kString), true}}}};

  auto one_row_batches = finalize_distributed_vector_rows_with_projection_v2(
      make_input(), projection, {.output_batch = {.maximum_rows = 1U}});
  ASSERT_TRUE(one_row_batches.has_value()) << one_row_batches.error().to_string();
  ASSERT_EQ(one_row_batches->encoded_batches.size(), 3U);
  std::size_t maximum_payload{};
  for (const auto& batch : one_row_batches->encoded_batches)
    maximum_payload = std::max(maximum_payload, batch.size());
  ASSERT_LE(maximum_payload, std::numeric_limits<std::uint32_t>::max());

  auto byte_split_batches = finalize_distributed_vector_rows_with_projection_v2(
      make_input(), projection,
      {.output_batch = {
           .protocol = {.maximum_payload_size = static_cast<std::uint32_t>(maximum_payload)},
           .maximum_rows = 3U}});
  ASSERT_TRUE(byte_split_batches.has_value()) << byte_split_batches.error().to_string();
  EXPECT_EQ(byte_split_batches->encoded_batches, one_row_batches->encoded_batches);
}

TEST(DistributedVectorRowFinalizationV2Test,
     RejectsStaleExpressionShapesAndPropagatesCheckedEvaluationErrors) {
  std::vector<query::VectorExpressionInstruction> stale_instructions;
  stale_instructions.emplace_back(
      query::VectorInputExpression{.input_column_ordinal = 0U,
                                   .type = type(schema::LogicalTypeKind::kUInt64),
                                   .nullable = false});
  const query::DistributedVectorRowCoordinatorProjection stale_projection{
      .outputs = {query::DistributedVectorRowExpressionOutput{
          .expression = query::VectorExpression::create(std::move(stale_instructions)).value()}},
      .result_schema = {.columns = {{"stale", type(schema::LogicalTypeKind::kUInt64), false}}}};
  auto stale_input = execution_result(row_plan(), {message(2U, 1U, true, encode_rows({{1, "x"}}))});
  EXPECT_EQ(
      finalize_distributed_vector_rows_with_projection_v2(std::move(stale_input), stale_projection)
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);

  const query::DistributedVectorRowCoordinatorProjection failing_projection{
      .outputs = {query::DistributedVectorRowExpressionOutput{.expression =
                                                                  divide_score_expression(0)}},
      .result_schema = {.columns = {{"failure", type(schema::LogicalTypeKind::kInt64), false}}}};
  auto failing_input =
      execution_result(row_plan(), {message(2U, 1U, true, encode_rows({{1, "x"}}))});
  EXPECT_EQ(finalize_distributed_vector_rows_with_projection_v2(std::move(failing_input),
                                                                failing_projection)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  query::DistributedVectorRowCoordinatorProjection invalid_predicate{
      .outputs = {query::DistributedVectorRowSourceOutput{.worker_output_index = 0U}},
      .result_schema = {.columns = {{"score", type(schema::LogicalTypeKind::kInt64), false}}},
      .predicate = add_score_expression(1)};
  auto invalid_predicate_input =
      execution_result(row_plan(), {message(2U, 1U, true, encode_rows({{1, "x"}}))});
  EXPECT_EQ(finalize_distributed_vector_rows_with_projection_v2(std::move(invalid_predicate_input),
                                                                invalid_predicate)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  invalid_predicate.predicate = divide_score_predicate(0);
  auto failing_predicate_input =
      execution_result(row_plan(), {message(2U, 1U, true, encode_rows({{1, "x"}}))});
  EXPECT_EQ(finalize_distributed_vector_rows_with_projection_v2(std::move(failing_predicate_input),
                                                                invalid_predicate)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
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
