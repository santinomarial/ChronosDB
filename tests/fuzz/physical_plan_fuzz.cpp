#include "chronos/query/physical_plan.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace {

[[nodiscard]] chronos::schema::LogicalType bool_type() {
  return chronos::schema::LogicalType::create(chronos::schema::LogicalTypeKind::kBool).value();
}

[[nodiscard]] chronos::schema::LogicalType int64_type() {
  return chronos::schema::LogicalType::create(chronos::schema::LogicalTypeKind::kInt64).value();
}

[[nodiscard]] chronos::columnar::OwnedPhysicalColumn
make_bool_column(const std::uint32_t rows, const std::span<const std::uint8_t> input) {
  chronos::columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(chronos::columnar::bitmap_size(rows));
  for (std::uint32_t row = 0U; row < rows; ++row) {
    if (!input.empty() && (input[row % input.size()] & 1U) != 0U)
      buffers.values[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
  }
  return chronos::columnar::OwnedPhysicalColumn::create(
             {.type = bool_type(), .nullable = false, .row_count = rows, .null_count = 0U},
             std::move(buffers))
      .value();
}

class OneChunkSource final : public chronos::query::PhysicalOperator {
public:
  explicit OneChunkSource(chronos::query::AccountedVectorChunk chunk) : chunk_(std::move(chunk)) {}

  [[nodiscard]] chronos::common::Result<chronos::query::PhysicalOperatorStep>
  next(const chronos::query::QueryResourceContext& resources) override {
    const auto active = resources.check_cancelled();
    if (!active.has_value())
      return chronos::common::make_unexpected(active.error());
    if (!chunk_.has_value())
      return chronos::query::PhysicalOperatorStep::end();
    chronos::query::AccountedVectorChunk result = std::move(*chunk_);
    chunk_.reset();
    return chronos::query::PhysicalOperatorStep::chunk(std::move(result));
  }

private:
  std::optional<chronos::query::AccountedVectorChunk> chunk_;
};

[[nodiscard]] chronos::query::VectorExpression bool_not_expression(const std::size_t ordinal,
                                                                   const bool nullable) {
  std::vector<chronos::query::VectorExpressionInstruction> instructions;
  instructions.emplace_back(chronos::query::VectorInputExpression{
      .input_column_ordinal = ordinal, .type = bool_type(), .nullable = nullable});
  instructions.emplace_back(chronos::query::VectorUnaryExpression{
      .operation = chronos::query::VectorUnaryOperation::kNot, .operand_instruction = 0U});
  return chronos::query::VectorExpression::create(std::move(instructions)).value();
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const std::span<const std::uint8_t> input{data, size};
  const std::vector<chronos::query::PhysicalColumnShape> shape{
      {.type = bool_type(), .nullable = false}};

  std::vector<chronos::query::PhysicalPipelineStage> hostile_stages;
  const std::size_t hostile_count = size > 32U ? 32U : size;
  hostile_stages.reserve(hostile_count);
  for (std::size_t index = 0U; index < hostile_count; ++index) {
    switch (data[index] % 8U) {
    case 0U:
      hostile_stages.emplace_back(
          chronos::query::BooleanFilterStage{static_cast<std::size_t>(data[index] >> 2U)});
      break;
    case 1U:
      hostile_stages.emplace_back(chronos::query::TimestampRangeFilterStage{
          .timestamp_column = static_cast<std::size_t>((data[index] >> 2U) & 3U),
          .predicate = {
              .lower =
                  chronos::query::TimestampRangeBound{
                      .value = static_cast<std::int64_t>(static_cast<std::int8_t>(data[index])),
                      .inclusive = (data[index] & 16U) != 0U},
              .upper = chronos::query::TimestampRangeBound{
                  .value = static_cast<std::int64_t>(data[index]),
                  .inclusive = (data[index] & 32U) != 0U}}});
      break;
    case 2U: {
      std::vector<std::size_t> ordinals;
      ordinals.push_back(static_cast<std::size_t>(data[index] & 3U));
      if ((data[index] & 4U) != 0U)
        ordinals.push_back(static_cast<std::size_t>((data[index] >> 3U) & 3U));
      hostile_stages.emplace_back(chronos::query::ColumnSubsetStage{std::move(ordinals)});
      break;
    }
    case 3U: {
      std::vector<std::size_t> ordinals;
      ordinals.push_back(static_cast<std::size_t>(data[index] & 3U));
      if ((data[index] & 4U) != 0U)
        ordinals.push_back(static_cast<std::size_t>((data[index] >> 3U) & 3U));
      hostile_stages.emplace_back(chronos::query::SourceColumnOutputStage{
          .input_column_ordinals = std::move(ordinals),
          .output_limits = {.maximum_rows = static_cast<std::uint32_t>(data[index]),
                            .maximum_columns = static_cast<std::size_t>(data[index] >> 4U),
                            .maximum_buffer_bytes = static_cast<std::size_t>(data[index]) * 32U,
                            .maximum_retained_buffer_bytes =
                                static_cast<std::size_t>(data[index]) * 64U}});
      break;
    }
    case 4U:
      hostile_stages.emplace_back(chronos::query::LimitStage{data[index]});
      break;
    case 5U: {
      std::vector<chronos::query::ColumnOutputPosition> positions;
      if ((data[index] & 3U) == 0U) {
        positions.emplace_back(chronos::query::SourceColumnOutputPosition{
            static_cast<std::size_t>((data[index] >> 1U) & 3U)});
      } else if ((data[index] & 3U) == 1U) {
        positions.emplace_back(chronos::query::ConstantColumnOutputPosition{
            chronos::query::ScalarValue::boolean((data[index] & 4U) != 0U).value()});
      } else if ((data[index] & 3U) == 2U) {
        positions.emplace_back(chronos::query::ConstantColumnOutputPosition{
            chronos::query::ScalarValue::untyped_null()});
      } else {
        std::vector<chronos::query::VectorExpressionInstruction> instructions;
        instructions.emplace_back(chronos::query::VectorInputExpression{
            .input_column_ordinal = static_cast<std::size_t>((data[index] >> 2U) & 3U),
            .type = bool_type(),
            .nullable = (data[index] & 16U) != 0U});
        instructions.emplace_back(chronos::query::VectorUnaryExpression{
            .operation = static_cast<chronos::query::VectorUnaryOperation>(data[index] >> 5U),
            .operand_instruction = static_cast<std::size_t>((data[index] >> 2U) & 3U)});
        auto expression = chronos::query::VectorExpression::create(std::move(instructions));
        if (expression.has_value()) {
          positions.emplace_back(
              chronos::query::ComputedColumnOutputPosition{std::move(*expression)});
        } else {
          positions.emplace_back(chronos::query::ConstantColumnOutputPosition{
              chronos::query::ScalarValue::untyped_null()});
        }
      }
      hostile_stages.emplace_back(chronos::query::ColumnOutputStage{
          .positions = std::move(positions),
          .output_limits = {.maximum_rows = static_cast<std::uint32_t>(data[index]),
                            .maximum_columns = static_cast<std::size_t>(data[index] >> 4U),
                            .maximum_buffer_bytes = static_cast<std::size_t>(data[index]) * 32U,
                            .maximum_retained_buffer_bytes =
                                static_cast<std::size_t>(data[index]) * 64U}});
      break;
    }
    case 6U: {
      std::vector<chronos::query::VectorAggregateDefinition> definitions;
      if ((data[index] & 1U) == 0U) {
        definitions.push_back({.operation = chronos::query::VectorAggregateOperation::kCountStar,
                               .input = std::nullopt});
      } else {
        definitions.push_back(
            {.operation =
                 static_cast<chronos::query::VectorAggregateOperation>((data[index] >> 1U) & 7U),
             .input = chronos::query::VectorAggregateInput{
                 .column_ordinal = static_cast<std::size_t>((data[index] >> 4U) & 3U),
                 .type = (data[index] & 8U) == 0U ? bool_type() : int64_type(),
                 .nullable = (data[index] & 128U) != 0U}});
      }
      hostile_stages.emplace_back(chronos::query::UngroupedAggregateStage{
          .definitions = std::move(definitions),
          .limits = {
              .maximum_aggregates = static_cast<std::size_t>(data[index] >> 4U),
              .maximum_retained_configuration_bytes = static_cast<std::size_t>(data[index]) * 64U,
              .output_limits = {.maximum_rows = static_cast<std::uint32_t>(data[index]),
                                .maximum_columns = static_cast<std::size_t>(data[index] >> 4U),
                                .maximum_buffer_bytes = static_cast<std::size_t>(data[index]) * 32U,
                                .maximum_retained_buffer_bytes =
                                    static_cast<std::size_t>(data[index]) * 64U}}});
      break;
    }
    case 7U: {
      std::vector<chronos::query::VectorGroupKeyDefinition> keys;
      keys.push_back({.column_ordinal = static_cast<std::size_t>((data[index] >> 1U) & 3U),
                      .type = (data[index] & 8U) == 0U ? bool_type() : int64_type(),
                      .nullable = (data[index] & 128U) != 0U});
      hostile_stages.emplace_back(chronos::query::GroupedAggregateStage{
          .keys = std::move(keys),
          .definitions = {{.operation = chronos::query::VectorAggregateOperation::kCountStar,
                           .input = std::nullopt}},
          .limits = {
              .maximum_groups = static_cast<std::size_t>(data[index] >> 4U),
              .maximum_group_keys = static_cast<std::size_t>((data[index] >> 5U) & 3U),
              .maximum_aggregates = static_cast<std::size_t>((data[index] >> 6U) & 3U),
              .maximum_key_bytes_per_group = static_cast<std::size_t>(data[index]),
              .maximum_retained_configuration_bytes = static_cast<std::size_t>(data[index]) * 64U,
              .output_limits = {.maximum_rows = static_cast<std::uint32_t>(data[index]),
                                .maximum_columns = static_cast<std::size_t>(data[index] >> 4U),
                                .maximum_buffer_bytes = static_cast<std::size_t>(data[index]) * 32U,
                                .maximum_retained_buffer_bytes =
                                    static_cast<std::size_t>(data[index]) * 64U}}});
      break;
    }
    default:
      break;
    }
  }
  const auto hostile_plan = chronos::query::PhysicalPipelinePlan::create(
      shape, std::move(hostile_stages),
      {.maximum_input_columns = 4U,
       .maximum_stages = 32U,
       .maximum_retained_configuration_bytes = std::size_t{8U} * 1'024U});
  if (hostile_plan.has_value())
    static_cast<void>(hostile_plan->stages().size());

  const std::uint32_t rows = size == 0U ? 1U : static_cast<std::uint32_t>(data[0]) + 1U;
  const std::uint64_t maximum_rows = size < 2U ? 0U : data[1];
  auto resources = chronos::query::QueryResourceContext::create(32'768U).value();
  auto reservation = resources.reserve(4'096U).value();
  std::vector<chronos::columnar::OwnedPhysicalColumn> columns;
  columns.push_back(make_bool_column(rows, input));
  auto chunk = chronos::query::VectorChunk::create(
                   std::move(columns), chronos::query::VectorSelection::all(rows).value(),
                   {.maximum_rows = 256U,
                    .maximum_columns = 1U,
                    .maximum_buffer_bytes = 4'096U,
                    .maximum_retained_buffer_bytes = 4'096U})
                   .value();
  auto accounted = chronos::query::AccountedVectorChunk::create(std::move(chunk),
                                                                std::move(reservation), resources)
                       .value();
  std::vector<chronos::query::PhysicalPipelineStage> stages;
  stages.emplace_back(chronos::query::BooleanFilterStage{0U});
  const std::uint8_t aggregate_mode = size >= 3U ? data[2] % 3U : 0U;
  if (aggregate_mode == 1U) {
    stages.emplace_back(chronos::query::UngroupedAggregateStage{
        .definitions = {{.operation = chronos::query::VectorAggregateOperation::kCountStar,
                         .input = std::nullopt}}});
    stages.emplace_back(chronos::query::LimitStage{maximum_rows});
    stages.emplace_back(chronos::query::ColumnOutputStage{
        .positions = {chronos::query::SourceColumnOutputPosition{0U}},
        .output_limits = {.maximum_rows = 256U,
                          .maximum_columns = 1U,
                          .maximum_buffer_bytes = 4'096U,
                          .maximum_retained_buffer_bytes = 8'192U}});
  } else if (aggregate_mode == 2U) {
    stages.emplace_back(chronos::query::GroupedAggregateStage{
        .keys = {{.column_ordinal = 0U, .type = bool_type(), .nullable = false}},
        .definitions = {{.operation = chronos::query::VectorAggregateOperation::kCountStar,
                         .input = std::nullopt}},
        .limits = {.maximum_groups = 2U,
                   .maximum_group_keys = 1U,
                   .maximum_aggregates = 1U,
                   .maximum_key_bytes_per_group = 1U,
                   .maximum_retained_configuration_bytes = 4'096U,
                   .output_limits = {.maximum_rows = 1U,
                                     .maximum_columns = 2U,
                                     .maximum_buffer_bytes = 4'096U,
                                     .maximum_retained_buffer_bytes = 8'192U}}});
    stages.emplace_back(chronos::query::LimitStage{maximum_rows});
    stages.emplace_back(chronos::query::ColumnOutputStage{
        .positions = {chronos::query::SourceColumnOutputPosition{0U}},
        .output_limits = {.maximum_rows = 256U,
                          .maximum_columns = 1U,
                          .maximum_buffer_bytes = 4'096U,
                          .maximum_retained_buffer_bytes = 8'192U}});
  } else {
    stages.emplace_back(chronos::query::LimitStage{maximum_rows});
    stages.emplace_back(chronos::query::ColumnOutputStage{
        .positions = {chronos::query::SourceColumnOutputPosition{0U},
                      chronos::query::ConstantColumnOutputPosition{
                          chronos::query::ScalarValue::boolean(true).value()},
                      chronos::query::ComputedColumnOutputPosition{bool_not_expression(0U, false)}},
        .output_limits = {.maximum_rows = 256U,
                          .maximum_columns = 3U,
                          .maximum_buffer_bytes = 4'096U,
                          .maximum_retained_buffer_bytes = 8'192U}});
  }
  const auto plan = chronos::query::PhysicalPipelinePlan::create(shape, std::move(stages)).value();
  auto pipeline = plan.instantiate(std::make_unique<OneChunkSource>(std::move(accounted))).value();
  std::size_t actual_rows = 0U;
  for (;;) {
    auto step = pipeline->next(resources);
    if (!step.has_value())
      break;
    if (step->kind() == chronos::query::PhysicalOperatorStepKind::kEnd)
      break;
    actual_rows += step->chunk()->chunk().selected_row_count();
  }

  std::size_t expected_rows = aggregate_mode == 1U && maximum_rows != 0U ? 1U : 0U;
  if (aggregate_mode == 2U && maximum_rows != 0U) {
    for (std::uint32_t row = 0U; row < rows; ++row) {
      if (!input.empty() && (input[row % input.size()] & 1U) != 0U) {
        expected_rows = 1U;
        break;
      }
    }
  }
  if (aggregate_mode == 0U) {
    for (std::uint32_t row = 0U; row < rows && expected_rows < maximum_rows; ++row) {
      if (!input.empty() && (input[row % input.size()] & 1U) != 0U)
        ++expected_rows;
    }
  }
  if (actual_rows != expected_rows)
    std::abort();
  return 0;
}
