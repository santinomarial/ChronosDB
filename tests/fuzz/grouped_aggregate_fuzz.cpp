#include "chronos/query/aggregate.hpp"
#include "chronos/query/value.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <utility>
#include <variant>
#include <vector>

namespace {

class OneChunkSource final : public chronos::query::PhysicalOperator {
public:
  explicit OneChunkSource(chronos::query::AccountedVectorChunk chunk) : chunk_(std::move(chunk)) {}

  [[nodiscard]] chronos::common::Result<chronos::query::PhysicalOperatorStep>
  next(const chronos::query::QueryResourceContext&) override {
    if (!chunk_.has_value())
      return chronos::query::PhysicalOperatorStep::end();
    chronos::query::AccountedVectorChunk chunk = std::move(*chunk_);
    chunk_.reset();
    return chronos::query::PhysicalOperatorStep::chunk(std::move(chunk));
  }

private:
  std::optional<chronos::query::AccountedVectorChunk> chunk_;
};

struct ExpectedGroup {
  bool is_null;
  double key;
  std::int64_t count;
};

[[nodiscard]] bool same_key(const ExpectedGroup& group, const bool is_null,
                            const double key) noexcept {
  if (group.is_null || is_null)
    return group.is_null == is_null;
  if (std::isnan(group.key) || std::isnan(key))
    return std::isnan(group.key) == std::isnan(key);
  return group.key == key;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  if (size == 0U)
    return 0;
  const std::size_t row_count = std::min<std::size_t>(size, 64U);
  chronos::columnar::ColumnVectorBuffers buffers;
  buffers.validity.resize(chronos::columnar::bitmap_size(static_cast<std::uint32_t>(row_count)));
  buffers.values.resize(row_count * sizeof(std::uint64_t));
  std::vector<ExpectedGroup> expected;
  std::uint32_t null_count = 0U;
  for (std::size_t row = 0U; row < row_count; ++row) {
    std::uint64_t bits = 0U;
    for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
      const std::uint8_t input = data[(row + byte + 1U) % size];
      bits |= static_cast<std::uint64_t>(input) << (byte * 8U);
    }
    const bool is_null = (data[row % size] & 7U) == 0U;
    if (!is_null) {
      buffers.validity[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
      for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
        buffers.values[row * sizeof(bits) + byte] =
            static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
      }
    } else {
      ++null_count;
    }
    const double key = std::bit_cast<double>(bits);
    auto group = std::ranges::find_if(expected, [&](const ExpectedGroup& candidate) {
      return same_key(candidate, is_null, key);
    });
    if (group == expected.end()) {
      expected.push_back({.is_null = is_null, .key = key, .count = 1});
    } else {
      ++group->count;
    }
  }

  const chronos::schema::LogicalType float64 =
      chronos::schema::LogicalType::create(chronos::schema::LogicalTypeKind::kFloat64).value();
  std::vector<chronos::columnar::OwnedPhysicalColumn> columns;
  columns.push_back(chronos::columnar::OwnedPhysicalColumn::create(
                        {.type = float64,
                         .nullable = true,
                         .row_count = static_cast<std::uint32_t>(row_count),
                         .null_count = null_count},
                        std::move(buffers))
                        .value());
  chronos::query::VectorChunk chunk =
      chronos::query::VectorChunk::create(
          std::move(columns),
          chronos::query::VectorSelection::all(static_cast<std::uint32_t>(row_count)).value())
          .value();
  chronos::query::QueryResourceContext resources =
      chronos::query::QueryResourceContext::create(8U << 20U).value();
  const std::size_t charge = chunk.retained_buffer_bytes() + 1'024U;
  auto accounted = chronos::query::AccountedVectorChunk::create(
      std::move(chunk), resources.reserve(charge).value(), resources);
  if (!accounted.has_value())
    __builtin_trap();
  const std::vector<chronos::query::VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = float64, .nullable = true}};
  const std::vector<chronos::query::VectorAggregateDefinition> definitions{
      {.operation = chronos::query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
  auto grouped = chronos::query::GroupedAggregateOperator::create(
      std::make_unique<OneChunkSource>(std::move(*accounted)), keys, definitions,
      {.maximum_groups = 64U});
  if (!grouped.has_value())
    __builtin_trap();

  for (const ExpectedGroup& model : expected) {
    auto step = (*grouped)->next(resources);
    if (!step.has_value() || step->kind() != chronos::query::PhysicalOperatorStepKind::kChunk)
      __builtin_trap();
    const chronos::query::VectorChunk& output = step->chunk()->chunk();
    const auto key_cell = output.cell({.column_ordinal = 0U, .selected_row = 0U}).value();
    const auto count_cell = output.cell({.column_ordinal = 1U, .selected_row = 0U}).value();
    const chronos::query::ScalarValue key =
        chronos::query::ScalarValue::from_column_cell(float64, key_cell).value();
    const chronos::query::ScalarValue count =
        chronos::query::ScalarValue::from_column_cell(output.column(1U)->type(), count_cell)
            .value();
    const bool actual_null = key.is_null();
    const double actual_key = actual_null ? 0.0 : std::get<double>(key.storage());
    if (!same_key(model, actual_null, actual_key) ||
        std::get<std::int64_t>(count.storage()) != model.count) {
      __builtin_trap();
    }
  }
  const auto end = (*grouped)->next(resources);
  if (!end.has_value() || end->kind() != chronos::query::PhysicalOperatorStepKind::kEnd)
    __builtin_trap();
  return 0;
}
