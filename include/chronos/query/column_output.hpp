#ifndef CHRONOS_QUERY_COLUMN_OUTPUT_HPP_
#define CHRONOS_QUERY_COLUMN_OUTPUT_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/value.hpp"
#include "chronos/query/vector_expression.hpp"

#include <cstddef>
#include <memory>
#include <variant>
#include <vector>

namespace chronos::query {

inline constexpr std::size_t kMaximumSourceColumnOutputWidth = kDefaultVectorChunkColumnLimit;
inline constexpr std::size_t kMaximumColumnOutputWidth = kDefaultVectorChunkColumnLimit;

struct SourceColumnOutputPosition {
  std::size_t input_column_ordinal;
};

struct ConstantColumnOutputPosition {
  ScalarValue value;
  // Preserve a nullable physical shape even when this particular constant is present. This is
  // required when materializing a present result from an operation whose declared result remains
  // nullable (for example SUM over a nonempty input).
  bool force_nullable{};
};

struct ComputedColumnOutputPosition {
  VectorExpression expression;
};

using ColumnOutputPosition = std::variant<SourceColumnOutputPosition, ConstantColumnOutputPosition,
                                          ComputedColumnOutputPosition>;

// Materializes caller-ordered source columns into new canonical output positions. Reordering and
// duplication are supported; selected nonempty rows are compacted into an identity selection.
class SourceColumnOutputOperator final : public PhysicalOperator {
public:
  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(std::unique_ptr<PhysicalOperator> input, std::vector<std::size_t> input_column_ordinals,
         VectorChunkLimits output_limits = {});

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override;

private:
  SourceColumnOutputOperator(std::unique_ptr<PhysicalOperator> input,
                             std::vector<std::size_t> input_column_ordinals,
                             VectorChunkLimits output_limits) noexcept;

  std::unique_ptr<PhysicalOperator> input_;
  std::vector<std::size_t> input_column_ordinals_;
  VectorChunkLimits output_limits_;
  bool ended_{};
};

// Materializes caller-ordered source columns, typed constants, and validated computed expressions
// into canonical owned physical columns. Source positions may be reordered or duplicated.
// Constants and successful expression rows are expanded without heap allocation per row.
class ColumnOutputOperator final : public PhysicalOperator {
public:
  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(std::unique_ptr<PhysicalOperator> input, std::vector<ColumnOutputPosition> positions,
         VectorChunkLimits output_limits = {});

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override;

private:
  ColumnOutputOperator(std::unique_ptr<PhysicalOperator> input,
                       std::vector<ColumnOutputPosition> positions,
                       VectorChunkLimits output_limits) noexcept;

  std::unique_ptr<PhysicalOperator> input_;
  std::vector<ColumnOutputPosition> positions_;
  VectorChunkLimits output_limits_;
  bool ended_{};
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_COLUMN_OUTPUT_HPP_
