#ifndef CHRONOS_QUERY_COLUMN_OUTPUT_HPP_
#define CHRONOS_QUERY_COLUMN_OUTPUT_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/physical_operator.hpp"

#include <cstddef>
#include <memory>
#include <vector>

namespace chronos::query {

inline constexpr std::size_t kMaximumSourceColumnOutputWidth = kDefaultVectorChunkColumnLimit;

// Materializes caller-ordered source columns into new canonical output positions. Reordering and
// duplication are supported; selected nonempty rows are compacted into an identity selection.
// Computed expressions and constants belong to later typed-expression increments.
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

} // namespace chronos::query

#endif // CHRONOS_QUERY_COLUMN_OUTPUT_HPP_
