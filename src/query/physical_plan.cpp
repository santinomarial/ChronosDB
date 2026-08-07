#include "chronos/query/physical_plan.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"

#include <cstddef>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

// The same-type arguments describe the conventional accumulator/capacity/element-size operation.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] common::Result<std::size_t> add_capacity_bytes(const std::size_t current,
                                                             const std::size_t capacity,
                                                             const std::size_t element_size) {
  const std::optional<std::size_t> bytes = common::checked_multiply(capacity, element_size);
  if (!bytes.has_value())
    return common::make_unexpected(exhausted("physical plan configuration size overflowed"));
  const std::optional<std::size_t> total = common::checked_add(current, *bytes);
  if (!total.has_value())
    return common::make_unexpected(exhausted("physical plan configuration size overflowed"));
  return *total;
}

[[nodiscard]] common::Result<std::size_t>
retained_bytes_for_configuration(const std::vector<PhysicalColumnShape>& input_columns,
                                 const std::vector<PhysicalColumnShape>& output_columns,
                                 const std::vector<PhysicalPipelineStage>& stages) {
  common::Result<std::size_t> total =
      add_capacity_bytes(0U, input_columns.capacity(), sizeof(PhysicalColumnShape));
  if (!total.has_value())
    return total;
  total = add_capacity_bytes(*total, output_columns.capacity(), sizeof(PhysicalColumnShape));
  if (!total.has_value())
    return total;
  total = add_capacity_bytes(*total, stages.capacity(), sizeof(PhysicalPipelineStage));
  if (!total.has_value())
    return total;
  for (const PhysicalPipelineStage& stage : stages) {
    if (const auto* subset = std::get_if<ColumnSubsetStage>(&stage); subset != nullptr) {
      total = add_capacity_bytes(*total, subset->column_ordinals.capacity(), sizeof(std::size_t));
      if (!total.has_value())
        return total;
    }
  }
  return total;
}

class SourceShapeOperator final : public PhysicalOperator {
public:
  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(std::unique_ptr<PhysicalOperator> source, std::vector<PhysicalColumnShape> columns) {
    if (source == nullptr)
      return common::make_unexpected(invalid("physical pipeline source must be non-null"));
    try {
      return std::unique_ptr<PhysicalOperator>{
          new SourceShapeOperator{std::move(source), std::move(columns)}};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("physical source-shape operator allocation failed"));
    }
  }

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override {
    if (ended_)
      return PhysicalOperatorStep::end();
    const common::Result<void> active = resources.check_cancelled();
    if (!active.has_value())
      return common::make_unexpected(active.error());

    common::Result<PhysicalOperatorStep> input = source_->next(resources);
    if (!input.has_value()) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(input.error());
    }
    if (input->kind() == PhysicalOperatorStepKind::kEnd) {
      ended_ = true;
      source_.reset();
      return PhysicalOperatorStep::end();
    }
    common::Result<AccountedVectorChunk> chunk = std::move(*input).take_chunk();
    if (!chunk.has_value()) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(chunk.error());
    }
    if (!chunk->belongs_to(resources)) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(
          invalid("physical pipeline source returned a chunk charged to another query"));
    }

    const std::span<const columnar::OwnedPhysicalColumn> actual = chunk->chunk().columns();
    if (actual.size() != columns_.size()) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(invalid("physical pipeline source column count mismatch"));
    }
    for (std::size_t ordinal = 0U; ordinal < actual.size(); ++ordinal) {
      if (actual[ordinal].type() != columns_[ordinal].type ||
          actual[ordinal].nullable() != columns_[ordinal].nullable) {
        static_cast<void>(resources.request_cancel());
        return common::make_unexpected(invalid("physical pipeline source column shape mismatch"));
      }
    }
    return PhysicalOperatorStep::chunk(std::move(*chunk));
  }

private:
  SourceShapeOperator(std::unique_ptr<PhysicalOperator> source,
                      std::vector<PhysicalColumnShape> columns) noexcept
      : source_(std::move(source)), columns_(std::move(columns)) {}

  std::unique_ptr<PhysicalOperator> source_;
  std::vector<PhysicalColumnShape> columns_;
  bool ended_{};
};

} // namespace

PhysicalPipelinePlan::PhysicalPipelinePlan(std::vector<PhysicalColumnShape> input_columns,
                                           std::vector<PhysicalColumnShape> output_columns,
                                           std::vector<PhysicalPipelineStage> stages,
                                           const std::size_t retained_configuration_bytes) noexcept
    : input_columns_(std::move(input_columns)), output_columns_(std::move(output_columns)),
      stages_(std::move(stages)), retained_configuration_bytes_(retained_configuration_bytes) {}

common::Result<PhysicalPipelinePlan>
PhysicalPipelinePlan::create(std::vector<PhysicalColumnShape> input_columns,
                             std::vector<PhysicalPipelineStage> stages,
                             const PhysicalPipelinePlanLimits limits) {
  if (input_columns.size() > limits.maximum_input_columns) {
    return common::make_unexpected(
        exhausted("physical pipeline input width exceeds the supported limit"));
  }
  if (stages.size() > limits.maximum_stages) {
    return common::make_unexpected(
        exhausted("physical pipeline stage count exceeds the supported limit"));
  }

  try {
    std::vector<PhysicalColumnShape> output_columns = input_columns;
    for (const PhysicalPipelineStage& stage : stages) {
      if (const auto* filter = std::get_if<BooleanFilterStage>(&stage); filter != nullptr) {
        if (filter->predicate_column >= output_columns.size()) {
          return common::make_unexpected(
              invalid("physical pipeline Boolean predicate column is out of range"));
        }
        if (output_columns[filter->predicate_column].type.kind() !=
            schema::LogicalTypeKind::kBool) {
          return common::make_unexpected(
              invalid("physical pipeline Boolean predicate column must have BOOL type"));
        }
        continue;
      }
      if (const auto* subset = std::get_if<ColumnSubsetStage>(&stage); subset != nullptr) {
        if (subset->column_ordinals.size() > kMaximumColumnSubsetWidth) {
          return common::make_unexpected(
              exhausted("physical pipeline column subset exceeds the supported width"));
        }
        for (std::size_t index = 0U; index < subset->column_ordinals.size(); ++index) {
          const std::size_t ordinal = subset->column_ordinals[index];
          if (ordinal >= output_columns.size()) {
            return common::make_unexpected(
                invalid("physical pipeline column subset ordinal is out of range"));
          }
          if (index != 0U && subset->column_ordinals[index - 1U] >= ordinal) {
            return common::make_unexpected(
                invalid("physical pipeline column subset must be unique and strictly increasing"));
          }
          if (index != ordinal)
            output_columns[index] = output_columns[ordinal];
        }
        using Difference = std::vector<PhysicalColumnShape>::difference_type;
        output_columns.erase(output_columns.begin() +
                                 static_cast<Difference>(subset->column_ordinals.size()),
                             output_columns.end());
        continue;
      }
      if (std::get_if<LimitStage>(&stage) == nullptr)
        return common::make_unexpected(invalid("physical pipeline stage is not supported"));
    }

    common::Result<std::size_t> retained =
        retained_bytes_for_configuration(input_columns, output_columns, stages);
    if (!retained.has_value())
      return common::make_unexpected(retained.error());
    if (*retained > limits.maximum_retained_configuration_bytes) {
      return common::make_unexpected(
          exhausted("physical pipeline retained configuration exceeds the supported byte limit"));
    }
    return PhysicalPipelinePlan{std::move(input_columns), std::move(output_columns),
                                std::move(stages), *retained};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("physical pipeline plan allocation failed"));
  }
}

std::span<const PhysicalColumnShape> PhysicalPipelinePlan::input_columns() const noexcept {
  return input_columns_;
}

std::span<const PhysicalColumnShape> PhysicalPipelinePlan::output_columns() const noexcept {
  return output_columns_;
}

std::span<const PhysicalPipelineStage> PhysicalPipelinePlan::stages() const noexcept {
  return stages_;
}

std::size_t PhysicalPipelinePlan::retained_configuration_bytes() const noexcept {
  return retained_configuration_bytes_;
}

common::Result<std::unique_ptr<PhysicalOperator>>
PhysicalPipelinePlan::instantiate(std::unique_ptr<PhysicalOperator> source) const {
  if (source == nullptr)
    return common::make_unexpected(invalid("physical pipeline source must be non-null"));
  try {
    common::Result<std::unique_ptr<PhysicalOperator>> checked =
        SourceShapeOperator::create(std::move(source), input_columns_);
    if (!checked.has_value())
      return common::make_unexpected(checked.error());
    std::unique_ptr<PhysicalOperator> pipeline = std::move(*checked);
    for (const PhysicalPipelineStage& stage : stages_) {
      common::Result<std::unique_ptr<PhysicalOperator>> next =
          common::make_unexpected(invalid("physical pipeline stage is not supported"));
      if (const auto* filter = std::get_if<BooleanFilterStage>(&stage); filter != nullptr) {
        next = BooleanFilterOperator::create(std::move(pipeline), filter->predicate_column);
      } else if (const auto* subset = std::get_if<ColumnSubsetStage>(&stage); subset != nullptr) {
        next = ColumnSubsetOperator::create(std::move(pipeline), subset->column_ordinals);
      } else if (const auto* limit = std::get_if<LimitStage>(&stage); limit != nullptr) {
        next = LimitOperator::create(std::move(pipeline), limit->maximum_rows);
      }
      if (!next.has_value())
        return common::make_unexpected(next.error());
      pipeline = std::move(*next);
    }
    return pipeline;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("physical pipeline instantiation allocation failed"));
  }
}

} // namespace chronos::query
