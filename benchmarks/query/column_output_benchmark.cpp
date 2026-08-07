#include "chronos/query/column_output.hpp"
#include "chronos/schema/logical_type.hpp"
#include "support/counting_allocator.hpp"

#include <benchmark/benchmark.h>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

inline constexpr std::size_t kBenchmarkMemoryLimit = std::size_t{256U} * 1024U * 1024U;

[[nodiscard]] columnar::OwnedPhysicalColumn make_column(const std::uint32_t rows,
                                                        const std::uint64_t salt) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(static_cast<std::size_t>(rows) * sizeof(std::int64_t));
  for (std::uint32_t row = 0U; row < rows; ++row) {
    const std::uint64_t bits = std::bit_cast<std::uint64_t>(
        static_cast<std::int64_t>(static_cast<std::uint64_t>(row) * 17U + salt));
    for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
      buffers.values[static_cast<std::size_t>(row) * sizeof(bits) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
              .nullable = false,
              .row_count = rows,
              .null_count = 0U},
             std::move(buffers))
      .value();
}

void append_u32(std::vector<std::byte>& bytes, const std::uint32_t value) {
  for (std::size_t byte = 0U; byte < sizeof(value); ++byte)
    bytes.push_back(static_cast<std::byte>((value >> (byte * 8U)) & 0xffU));
}

[[nodiscard]] columnar::OwnedPhysicalColumn make_text_column(const std::uint32_t rows) {
  static constexpr std::string_view kValue{"AbCdEfGhIjKlMnOp"};
  columnar::ColumnVectorBuffers buffers;
  buffers.offsets.reserve((static_cast<std::size_t>(rows) + 1U) * sizeof(std::uint32_t));
  buffers.values.reserve(static_cast<std::size_t>(rows) * kValue.size());
  append_u32(buffers.offsets, 0U);
  for (std::uint32_t row = 0U; row < rows; ++row) {
    for (const char value : kValue)
      buffers.values.push_back(static_cast<std::byte>(value));
    append_u32(buffers.offsets,
               static_cast<std::uint32_t>((static_cast<std::size_t>(row) + 1U) * kValue.size()));
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value(),
              .nullable = false,
              .row_count = rows,
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] std::vector<std::uint32_t> selected_rows(const std::uint32_t rows,
                                                       const std::uint32_t stride) {
  std::vector<std::uint32_t> result;
  result.reserve(static_cast<std::size_t>(rows / stride) + 1U);
  for (std::uint32_t row = 0U; row < rows; row += stride)
    result.push_back(row);
  return result;
}

class OneChunkSource final : public PhysicalOperator {
public:
  explicit OneChunkSource(AccountedVectorChunk chunk) : chunk_(std::move(chunk)) {}

  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    if (!chunk_.has_value())
      return PhysicalOperatorStep::end();
    AccountedVectorChunk result = std::move(*chunk_);
    chunk_.reset();
    return PhysicalOperatorStep::chunk(std::move(result));
  }

private:
  std::optional<AccountedVectorChunk> chunk_;
};

[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
source(const QueryResourceContext& resources, const std::uint32_t rows,
       const std::size_t input_columns, const std::uint32_t selection_stride,
       const std::vector<std::size_t>& outputs) {
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.reserve(input_columns);
  for (std::size_t ordinal = 0U; ordinal < input_columns; ++ordinal)
    columns.push_back(make_column(rows, ordinal));
  common::Result<VectorChunk> chunk = VectorChunk::create(
      std::move(columns),
      VectorSelection::from_indices(rows, selected_rows(rows, selection_stride)).value(),
      {.maximum_rows = rows,
       .maximum_columns = input_columns,
       .maximum_buffer_bytes = kBenchmarkMemoryLimit,
       .maximum_retained_buffer_bytes = kBenchmarkMemoryLimit});
  if (!chunk.has_value())
    return common::make_unexpected(chunk.error());
  const std::size_t charge = chunk->retained_buffer_bytes() + 4'096U;
  common::Result<QueryMemoryReservation> reservation = resources.reserve(charge);
  if (!reservation.has_value())
    return common::make_unexpected(reservation.error());
  common::Result<AccountedVectorChunk> accounted =
      AccountedVectorChunk::create(std::move(*chunk), std::move(*reservation), resources);
  if (!accounted.has_value())
    return common::make_unexpected(accounted.error());
  return SourceColumnOutputOperator::create(
      std::make_unique<OneChunkSource>(std::move(*accounted)), outputs,
      {.maximum_rows = rows,
       .maximum_columns = outputs.size(),
       .maximum_buffer_bytes = kBenchmarkMemoryLimit,
       .maximum_retained_buffer_bytes = kBenchmarkMemoryLimit});
}

[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
mixed_source(const QueryResourceContext& resources, const std::uint32_t rows,
             const std::uint32_t selection_stride) {
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(make_column(rows, 0U));
  common::Result<VectorChunk> chunk = VectorChunk::create(
      std::move(columns),
      VectorSelection::from_indices(rows, selected_rows(rows, selection_stride)).value(),
      {.maximum_rows = rows,
       .maximum_columns = 1U,
       .maximum_buffer_bytes = kBenchmarkMemoryLimit,
       .maximum_retained_buffer_bytes = kBenchmarkMemoryLimit});
  if (!chunk.has_value())
    return common::make_unexpected(chunk.error());
  common::Result<QueryMemoryReservation> reservation =
      resources.reserve(chunk->retained_buffer_bytes() + 4'096U);
  if (!reservation.has_value())
    return common::make_unexpected(reservation.error());
  common::Result<AccountedVectorChunk> accounted =
      AccountedVectorChunk::create(std::move(*chunk), std::move(*reservation), resources);
  if (!accounted.has_value())
    return common::make_unexpected(accounted.error());

  std::vector<ColumnOutputPosition> positions;
  positions.emplace_back(SourceColumnOutputPosition{0U});
  positions.emplace_back(ConstantColumnOutputPosition{
      ScalarValue::signed_value(
          schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(), 42)
          .value()});
  positions.emplace_back(ConstantColumnOutputPosition{
      ScalarValue::text(schema::LogicalType::create(schema::LogicalTypeKind::kString).value(),
                        "chronos-constant")
          .value()});
  positions.emplace_back(ConstantColumnOutputPosition{
      ScalarValue::null(schema::LogicalType::create(schema::LogicalTypeKind::kBinary).value())});
  return ColumnOutputOperator::create(std::make_unique<OneChunkSource>(std::move(*accounted)),
                                      std::move(positions),
                                      {.maximum_rows = rows,
                                       .maximum_columns = 4U,
                                       .maximum_buffer_bytes = kBenchmarkMemoryLimit,
                                       .maximum_retained_buffer_bytes = kBenchmarkMemoryLimit});
}

[[nodiscard]] VectorExpression benchmark_expression() {
  const schema::LogicalType int64_type =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  std::vector<VectorExpressionInstruction> instructions;
  instructions.emplace_back(
      VectorInputExpression{.input_column_ordinal = 0U, .type = int64_type, .nullable = false});
  instructions.emplace_back(
      VectorConstantExpression{ScalarValue::signed_value(int64_type, 42).value()});
  instructions.emplace_back(VectorBinaryExpression{
      .operation = VectorBinaryOperation::kAdd, .left_instruction = 0U, .right_instruction = 1U});
  instructions.emplace_back(
      VectorConstantExpression{ScalarValue::signed_value(int64_type, 3).value()});
  instructions.emplace_back(VectorBinaryExpression{.operation = VectorBinaryOperation::kMultiply,
                                                   .left_instruction = 2U,
                                                   .right_instruction = 3U});
  instructions.emplace_back(VectorBinaryExpression{.operation = VectorBinaryOperation::kGreater,
                                                   .left_instruction = 4U,
                                                   .right_instruction = 0U});
  return VectorExpression::create(std::move(instructions)).value();
}

[[nodiscard]] VectorExpression fixed_scalar_expression() {
  const schema::LogicalType int64_type =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  const schema::LogicalType float64_type =
      schema::LogicalType::create(schema::LogicalTypeKind::kFloat64).value();
  std::vector<VectorExpressionInstruction> instructions;
  instructions.emplace_back(VectorConstantExpression{ScalarValue::null(float64_type)});
  instructions.emplace_back(
      VectorInputExpression{.input_column_ordinal = 0U, .type = int64_type, .nullable = false});
  instructions.emplace_back(
      VectorCastExpression{.operand_instruction = 1U, .target_type = float64_type});
  instructions.emplace_back(VectorBinaryExpression{.operation = VectorBinaryOperation::kCoalesce,
                                                   .left_instruction = 0U,
                                                   .right_instruction = 2U});
  return VectorExpression::create(std::move(instructions)).value();
}

[[nodiscard]] VectorExpression variable_scalar_expression() {
  const schema::LogicalType string_type =
      schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const schema::LogicalType symbol_type =
      schema::LogicalType::create(schema::LogicalTypeKind::kSymbol).value();
  std::vector<VectorExpressionInstruction> instructions;
  instructions.emplace_back(
      VectorInputExpression{.input_column_ordinal = 0U, .type = string_type, .nullable = false});
  instructions.emplace_back(VectorUnaryExpression{.operation = VectorUnaryOperation::kLowerAscii,
                                                  .operand_instruction = 0U});
  instructions.emplace_back(
      VectorCastExpression{.operand_instruction = 1U, .target_type = symbol_type});
  return VectorExpression::create(std::move(instructions)).value();
}

[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
text_expression_source(const QueryResourceContext& resources, const std::uint32_t rows,
                       const std::uint32_t selection_stride) {
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(make_text_column(rows));
  common::Result<VectorChunk> chunk = VectorChunk::create(
      std::move(columns),
      VectorSelection::from_indices(rows, selected_rows(rows, selection_stride)).value(),
      {.maximum_rows = rows,
       .maximum_columns = 1U,
       .maximum_buffer_bytes = kBenchmarkMemoryLimit,
       .maximum_retained_buffer_bytes = kBenchmarkMemoryLimit});
  if (!chunk.has_value())
    return common::make_unexpected(chunk.error());
  common::Result<QueryMemoryReservation> reservation =
      resources.reserve(chunk->retained_buffer_bytes() + 4'096U);
  if (!reservation.has_value())
    return common::make_unexpected(reservation.error());
  common::Result<AccountedVectorChunk> accounted =
      AccountedVectorChunk::create(std::move(*chunk), std::move(*reservation), resources);
  if (!accounted.has_value())
    return common::make_unexpected(accounted.error());
  std::vector<ColumnOutputPosition> positions;
  positions.emplace_back(ComputedColumnOutputPosition{variable_scalar_expression()});
  return ColumnOutputOperator::create(std::make_unique<OneChunkSource>(std::move(*accounted)),
                                      std::move(positions),
                                      {.maximum_rows = rows,
                                       .maximum_columns = 1U,
                                       .maximum_buffer_bytes = kBenchmarkMemoryLimit,
                                       .maximum_retained_buffer_bytes = kBenchmarkMemoryLimit});
}

[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
expression_source(const QueryResourceContext& resources, const std::uint32_t rows,
                  const std::uint32_t selection_stride, VectorExpression expression) {
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(make_column(rows, 0U));
  common::Result<VectorChunk> chunk = VectorChunk::create(
      std::move(columns),
      VectorSelection::from_indices(rows, selected_rows(rows, selection_stride)).value(),
      {.maximum_rows = rows,
       .maximum_columns = 1U,
       .maximum_buffer_bytes = kBenchmarkMemoryLimit,
       .maximum_retained_buffer_bytes = kBenchmarkMemoryLimit});
  if (!chunk.has_value())
    return common::make_unexpected(chunk.error());
  common::Result<QueryMemoryReservation> reservation =
      resources.reserve(chunk->retained_buffer_bytes() + 4'096U);
  if (!reservation.has_value())
    return common::make_unexpected(reservation.error());
  common::Result<AccountedVectorChunk> accounted =
      AccountedVectorChunk::create(std::move(*chunk), std::move(*reservation), resources);
  if (!accounted.has_value())
    return common::make_unexpected(accounted.error());
  std::vector<ColumnOutputPosition> positions;
  positions.emplace_back(ComputedColumnOutputPosition{std::move(expression)});
  return ColumnOutputOperator::create(std::make_unique<OneChunkSource>(std::move(*accounted)),
                                      std::move(positions),
                                      {.maximum_rows = rows,
                                       .maximum_columns = 1U,
                                       .maximum_buffer_bytes = kBenchmarkMemoryLimit,
                                       .maximum_retained_buffer_bytes = kBenchmarkMemoryLimit});
}

void materialize_reordered_duplicate_source_columns(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto input_columns = static_cast<std::size_t>(state.range(1));
  const auto selection_stride = static_cast<std::uint32_t>(state.range(2));
  std::vector<std::size_t> outputs;
  outputs.reserve(input_columns * 2U);
  for (std::size_t ordinal = input_columns; ordinal > 0U; --ordinal) {
    outputs.push_back(ordinal - 1U);
    outputs.push_back(ordinal - 1U);
  }
  QueryResourceContext resources = QueryResourceContext::create(kBenchmarkMemoryLimit).value();

  std::size_t measured_allocations = 0U;
  std::size_t measured_bytes = 0U;
  {
    auto pipeline = source(resources, rows, input_columns, selection_stride, outputs);
    if (!pipeline.has_value()) {
      const std::string message = pipeline.error().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark_support::ScopedAllocationCounting counting;
    auto step = (*pipeline)->next(resources);
    measured_allocations = counting.stop().allocations;
    if (!step.has_value() || step->chunk() == nullptr ||
        step->chunk()->chunk().column_count() != outputs.size()) {
      const std::string message = step.has_value()
                                      ? "source-column output benchmark returned the wrong shape"
                                      : step.error().to_string();
      state.SkipWithError(message);
      return;
    }
    measured_bytes = step->chunk()->chunk().buffer_bytes();
  }

  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto pipeline = source(resources, rows, input_columns, selection_stride, outputs);
    if (!pipeline.has_value()) {
      const std::string message = pipeline.error().to_string();
      state.SkipWithError(message);
      return;
    }
    state.ResumeTiming();
    auto step = (*pipeline)->next(resources);
    benchmark::DoNotOptimize(step);
    state.PauseTiming();
    if (!step.has_value() || step->chunk() == nullptr ||
        step->chunk()->chunk().column_count() != outputs.size()) {
      const std::string message = step.has_value()
                                      ? "source-column output benchmark returned the wrong shape"
                                      : step.error().to_string();
      state.SkipWithError(message);
      return;
    }
    state.ResumeTiming();
  }
  const std::size_t selected = (static_cast<std::size_t>(rows) + selection_stride - 1U) /
                               static_cast<std::size_t>(selection_stride);
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(selected) *
                          static_cast<std::int64_t>(outputs.size()));
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(measured_bytes));
  state.counters["input_columns"] = static_cast<double>(input_columns);
  state.counters["output_columns"] = static_cast<double>(outputs.size());
  state.counters["physical_rows"] = static_cast<double>(rows);
  state.counters["pull_allocations"] = static_cast<double>(measured_allocations);
  state.counters["selection_density"] = 1.0 / static_cast<double>(selection_stride);
  state.SetLabel("reverse order; every source column duplicated; source construction excluded");
}

BENCHMARK(materialize_reordered_duplicate_source_columns)
    ->Args({64, 4, 1})
    ->Args({1'024, 4, 1})
    ->Args({4'096, 4, 1})
    ->Args({1'024, 4, 4})
    ->Args({4'096, 8, 4});

void materialize_mixed_source_and_typed_constants(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto selection_stride = static_cast<std::uint32_t>(state.range(1));
  QueryResourceContext resources = QueryResourceContext::create(kBenchmarkMemoryLimit).value();

  std::size_t measured_allocations = 0U;
  std::size_t measured_bytes = 0U;
  {
    auto pipeline = mixed_source(resources, rows, selection_stride);
    if (!pipeline.has_value()) {
      const std::string message = pipeline.error().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark_support::ScopedAllocationCounting counting;
    auto step = (*pipeline)->next(resources);
    measured_allocations = counting.stop().allocations;
    if (!step.has_value() || step->chunk() == nullptr ||
        step->chunk()->chunk().column_count() != 4U) {
      state.SkipWithError(step.has_value() ? "mixed column output returned the wrong shape"
                                           : step.error().to_string());
      return;
    }
    measured_bytes = step->chunk()->chunk().buffer_bytes();
  }

  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto pipeline = mixed_source(resources, rows, selection_stride);
    if (!pipeline.has_value()) {
      const std::string message = pipeline.error().to_string();
      state.SkipWithError(message);
      return;
    }
    state.ResumeTiming();
    auto step = (*pipeline)->next(resources);
    benchmark::DoNotOptimize(step);
    state.PauseTiming();
    if (!step.has_value() || step->chunk() == nullptr ||
        step->chunk()->chunk().column_count() != 4U) {
      state.SkipWithError(step.has_value() ? "mixed column output returned the wrong shape"
                                           : step.error().to_string());
      return;
    }
    state.ResumeTiming();
  }
  const std::size_t selected = (static_cast<std::size_t>(rows) + selection_stride - 1U) /
                               static_cast<std::size_t>(selection_stride);
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(selected) * 4);
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(measured_bytes));
  state.counters["output_columns"] = 4.0;
  state.counters["physical_rows"] = static_cast<double>(rows);
  state.counters["pull_allocations"] = static_cast<double>(measured_allocations);
  state.counters["selection_density"] = 1.0 / static_cast<double>(selection_stride);
  state.SetLabel("source + fixed/string/typed-NULL constants; source construction excluded");
}

BENCHMARK(materialize_mixed_source_and_typed_constants)
    ->Args({64, 1})
    ->Args({1'024, 1})
    ->Args({4'096, 1})
    ->Args({1'024, 4})
    ->Args({4'096, 4});

void materialize_checked_numeric_expression(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto selection_stride = static_cast<std::uint32_t>(state.range(1));
  QueryResourceContext resources = QueryResourceContext::create(kBenchmarkMemoryLimit).value();
  std::size_t measured_allocations = 0U;
  std::size_t measured_bytes = 0U;
  {
    auto pipeline = expression_source(resources, rows, selection_stride, benchmark_expression());
    if (!pipeline.has_value()) {
      const std::string message = pipeline.error().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark_support::ScopedAllocationCounting counting;
    auto step = (*pipeline)->next(resources);
    measured_allocations = counting.stop().allocations;
    if (!step.has_value() || step->chunk() == nullptr ||
        step->chunk()->chunk().column_count() != 1U) {
      state.SkipWithError(step.has_value() ? "computed output returned the wrong shape"
                                           : step.error().to_string());
      return;
    }
    measured_bytes = step->chunk()->chunk().buffer_bytes();
  }

  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto pipeline = expression_source(resources, rows, selection_stride, benchmark_expression());
    if (!pipeline.has_value()) {
      const std::string message = pipeline.error().to_string();
      state.SkipWithError(message);
      return;
    }
    state.ResumeTiming();
    auto step = (*pipeline)->next(resources);
    benchmark::DoNotOptimize(step);
    state.PauseTiming();
    if (!step.has_value() || step->chunk() == nullptr ||
        step->chunk()->chunk().column_count() != 1U) {
      state.SkipWithError(step.has_value() ? "computed output returned the wrong shape"
                                           : step.error().to_string());
      return;
    }
    state.ResumeTiming();
  }
  const std::size_t selected = (static_cast<std::size_t>(rows) + selection_stride - 1U) /
                               static_cast<std::size_t>(selection_stride);
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(selected));
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(measured_bytes));
  state.counters["instructions"] = 6.0;
  state.counters["physical_rows"] = static_cast<double>(rows);
  state.counters["pull_allocations"] = static_cast<double>(measured_allocations);
  state.counters["selection_density"] = 1.0 / static_cast<double>(selection_stride);
  state.SetLabel("(source + 42) * 3 > source; source construction excluded");
}

BENCHMARK(materialize_checked_numeric_expression)
    ->Args({64, 1})
    ->Args({1'024, 1})
    ->Args({4'096, 1})
    ->Args({1'024, 4})
    ->Args({4'096, 4});

void materialize_fixed_width_cast_and_coalesce(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto selection_stride = static_cast<std::uint32_t>(state.range(1));
  QueryResourceContext resources = QueryResourceContext::create(kBenchmarkMemoryLimit).value();
  std::size_t measured_allocations = 0U;
  std::size_t measured_bytes = 0U;
  {
    auto pipeline = expression_source(resources, rows, selection_stride, fixed_scalar_expression());
    if (!pipeline.has_value()) {
      const std::string message = pipeline.error().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark_support::ScopedAllocationCounting counting;
    auto step = (*pipeline)->next(resources);
    measured_allocations = counting.stop().allocations;
    if (!step.has_value() || step->chunk() == nullptr ||
        step->chunk()->chunk().column_count() != 1U) {
      state.SkipWithError(step.has_value() ? "fixed scalar output returned the wrong shape"
                                           : step.error().to_string());
      return;
    }
    measured_bytes = step->chunk()->chunk().buffer_bytes();
  }

  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto pipeline = expression_source(resources, rows, selection_stride, fixed_scalar_expression());
    if (!pipeline.has_value()) {
      const std::string message = pipeline.error().to_string();
      state.SkipWithError(message);
      return;
    }
    state.ResumeTiming();
    auto step = (*pipeline)->next(resources);
    benchmark::DoNotOptimize(step);
    state.PauseTiming();
    if (!step.has_value() || step->chunk() == nullptr ||
        step->chunk()->chunk().column_count() != 1U) {
      state.SkipWithError(step.has_value() ? "fixed scalar output returned the wrong shape"
                                           : step.error().to_string());
      return;
    }
    state.ResumeTiming();
  }
  const std::size_t selected = (static_cast<std::size_t>(rows) + selection_stride - 1U) /
                               static_cast<std::size_t>(selection_stride);
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(selected));
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(measured_bytes));
  state.counters["instructions"] = 4.0;
  state.counters["physical_rows"] = static_cast<double>(rows);
  state.counters["pull_allocations"] = static_cast<double>(measured_allocations);
  state.counters["selection_density"] = 1.0 / static_cast<double>(selection_stride);
  state.SetLabel("coalesce(NULL, CAST(INT64 AS FLOAT64)); source construction excluded");
}

BENCHMARK(materialize_fixed_width_cast_and_coalesce)
    ->Args({64, 1})
    ->Args({1'024, 1})
    ->Args({4'096, 1})
    ->Args({1'024, 4})
    ->Args({4'096, 4});

void materialize_variable_width_case_and_cast(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto selection_stride = static_cast<std::uint32_t>(state.range(1));
  QueryResourceContext resources = QueryResourceContext::create(kBenchmarkMemoryLimit).value();
  std::size_t measured_allocations = 0U;
  std::size_t measured_bytes = 0U;
  {
    auto pipeline = text_expression_source(resources, rows, selection_stride);
    if (!pipeline.has_value()) {
      state.SkipWithError(pipeline.error().to_string());
      return;
    }
    benchmark_support::ScopedAllocationCounting counting;
    auto step = (*pipeline)->next(resources);
    measured_allocations = counting.stop().allocations;
    if (!step.has_value() || step->chunk() == nullptr) {
      state.SkipWithError(step.has_value() ? "variable scalar output returned no chunk"
                                           : step.error().to_string());
      return;
    }
    measured_bytes = step->chunk()->chunk().buffer_bytes();
  }

  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    auto pipeline = text_expression_source(resources, rows, selection_stride);
    if (!pipeline.has_value()) {
      state.SkipWithError(pipeline.error().to_string());
      return;
    }
    state.ResumeTiming();
    auto step = (*pipeline)->next(resources);
    benchmark::DoNotOptimize(step);
    if (!step.has_value()) {
      state.SkipWithError(step.error().to_string());
      return;
    }
  }
  const std::size_t selected = (static_cast<std::size_t>(rows) + selection_stride - 1U) /
                               static_cast<std::size_t>(selection_stride);
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(selected));
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(measured_bytes));
  state.counters["instructions"] = 3.0;
  state.counters["physical_rows"] = static_cast<double>(rows);
  state.counters["pull_allocations"] = static_cast<double>(measured_allocations);
  state.counters["selection_density"] = 1.0 / static_cast<double>(selection_stride);
  state.SetLabel("LOWER(STRING) then CAST to SYMBOL; 16-byte values; source construction excluded");
}

BENCHMARK(materialize_variable_width_case_and_cast)
    ->Args({64, 1})
    ->Args({1'024, 1})
    ->Args({4'096, 1})
    ->Args({1'024, 4})
    ->Args({4'096, 4});

} // namespace
} // namespace chronos::query
