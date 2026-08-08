#include "chronos/query/relational_plan.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] bool same_shape(const std::span<const PhysicalColumnShape> physical,
                              const std::span<const VectorAsofColumnShape> asof) noexcept {
  if (physical.size() != asof.size())
    return false;
  for (std::size_t ordinal = 0U; ordinal < physical.size(); ++ordinal) {
    if (physical[ordinal].type != asof[ordinal].type ||
        physical[ordinal].nullable != asof[ordinal].nullable) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] bool same_shape(const std::span<const PhysicalColumnShape> physical,
                              const std::span<const PhysicalColumnShape> other) noexcept {
  return std::ranges::equal(physical, other);
}

[[nodiscard]] common::Result<std::size_t>
retained_bytes(const std::vector<PhysicalAsofPlanJoin>& joins,
               const PhysicalPipelinePlan& final_pipeline) {
  std::optional<std::size_t> total = final_pipeline.retained_configuration_bytes();
  const auto add = [&total](const std::size_t value) {
    total = total.has_value() ? common::checked_add(*total, value) : std::nullopt;
    return total.has_value();
  };
  const auto add_capacity = [&add](const std::size_t capacity, const std::size_t width) {
    const std::optional<std::size_t> bytes = common::checked_multiply(capacity, width);
    return bytes.has_value() && add(*bytes);
  };
  if (!add_capacity(joins.capacity(), sizeof(PhysicalAsofPlanJoin)))
    return common::make_unexpected(exhausted("physical ASOF plan configuration overflowed"));
  for (const PhysicalAsofPlanJoin& join : joins) {
    if (!add(join.left_preparation.retained_configuration_bytes()) ||
        !add(join.right_preparation.retained_configuration_bytes()) ||
        !add_capacity(join.definition.left_input_columns.capacity(),
                      sizeof(VectorAsofColumnShape)) ||
        !add_capacity(join.definition.right_input_columns.capacity(),
                      sizeof(VectorAsofColumnShape)) ||
        !add_capacity(join.definition.equality_keys.capacity(), sizeof(VectorAsofEqualityKey)) ||
        !add_capacity(join.definition.right_physical_ordering_key_ordinals.capacity(),
                      sizeof(std::size_t)) ||
        !add_capacity(join.definition.left_output_column_ordinals.capacity(),
                      sizeof(std::size_t)) ||
        !add_capacity(join.definition.right_output_column_ordinals.capacity(),
                      sizeof(std::size_t))) {
      return common::make_unexpected(exhausted("physical ASOF plan configuration overflowed"));
    }
  }
  return *total;
}

} // namespace

PhysicalAsofPlan::PhysicalAsofPlan(std::vector<PhysicalAsofPlanJoin> joins,
                                   PhysicalPipelinePlan final_pipeline,
                                   const std::size_t retained_configuration_bytes) noexcept
    : joins_(std::move(joins)), final_pipeline_(std::move(final_pipeline)),
      retained_configuration_bytes_(retained_configuration_bytes) {}

common::Result<PhysicalAsofPlan> PhysicalAsofPlan::create(std::vector<PhysicalAsofPlanJoin> joins,
                                                          PhysicalPipelinePlan final_pipeline,
                                                          const PhysicalAsofPlanLimits limits) {
  if (limits.maximum_joins == 0U || limits.maximum_retained_configuration_bytes == 0U)
    return common::make_unexpected(invalid("physical ASOF plan limits must be nonzero"));
  if (joins.empty())
    return common::make_unexpected(invalid("physical ASOF plan requires at least one join"));
  if (joins.size() > limits.maximum_joins || joins.capacity() > limits.maximum_joins)
    return common::make_unexpected(exhausted("physical ASOF plan join count exceeds its limit"));
  try {
    std::vector<PhysicalColumnShape> prior_output;
    for (std::size_t ordinal = 0U; ordinal < joins.size(); ++ordinal) {
      PhysicalAsofPlanJoin& join = joins[ordinal];
      if (ordinal != 0U && !same_shape(join.left_preparation.input_columns(), prior_output)) {
        return common::make_unexpected(
            invalid("physical ASOF left preparation input disagrees with the prior join"));
      }
      if (!same_shape(join.left_preparation.output_columns(), join.definition.left_input_columns) ||
          !same_shape(join.right_preparation.output_columns(),
                      join.definition.right_input_columns)) {
        return common::make_unexpected(
            invalid("physical ASOF preparation output disagrees with its join definition"));
      }
      common::Result<std::vector<VectorAsofColumnShape>> output =
          vector_asof_join_output_shape(join.definition, join.limits);
      if (!output.has_value())
        return common::make_unexpected(output.error());
      prior_output.clear();
      prior_output.reserve(output->size());
      for (const VectorAsofColumnShape& column : *output)
        prior_output.push_back({.type = column.type, .nullable = column.nullable});
    }
    if (!same_shape(final_pipeline.input_columns(), prior_output)) {
      return common::make_unexpected(
          invalid("physical ASOF final pipeline input disagrees with the last join"));
    }
    common::Result<std::size_t> retained = retained_bytes(joins, final_pipeline);
    if (!retained.has_value())
      return common::make_unexpected(retained.error());
    if (*retained > limits.maximum_retained_configuration_bytes) {
      return common::make_unexpected(
          exhausted("physical ASOF plan retained configuration exceeds its limit"));
    }
    return PhysicalAsofPlan{std::move(joins), std::move(final_pipeline), *retained};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("physical ASOF plan allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("physical ASOF plan exceeds container limits"));
  }
}

std::span<const PhysicalAsofPlanJoin> PhysicalAsofPlan::joins() const noexcept {
  return joins_;
}

const PhysicalPipelinePlan& PhysicalAsofPlan::final_pipeline() const noexcept {
  return final_pipeline_;
}

std::size_t PhysicalAsofPlan::source_count() const noexcept {
  return joins_.size() + 1U;
}

std::size_t PhysicalAsofPlan::retained_configuration_bytes() const noexcept {
  return retained_configuration_bytes_;
}

common::Result<std::unique_ptr<PhysicalOperator>>
PhysicalAsofPlan::instantiate(std::vector<std::unique_ptr<PhysicalOperator>> sources) const {
  if (sources.size() != source_count())
    return common::make_unexpected(invalid("physical ASOF plan source count mismatch"));
  for (const std::unique_ptr<PhysicalOperator>& source : sources) {
    if (source == nullptr)
      return common::make_unexpected(invalid("physical ASOF plan sources must be non-null"));
  }
  try {
    std::unique_ptr<PhysicalOperator> left = std::move(sources.front());
    for (std::size_t ordinal = 0U; ordinal < joins_.size(); ++ordinal) {
      const PhysicalAsofPlanJoin& join = joins_[ordinal];
      common::Result<std::unique_ptr<PhysicalOperator>> prepared_left =
          join.left_preparation.instantiate(std::move(left));
      if (!prepared_left.has_value())
        return common::make_unexpected(prepared_left.error());
      common::Result<std::unique_ptr<PhysicalOperator>> prepared_right =
          join.right_preparation.instantiate(std::move(sources[ordinal + 1U]));
      if (!prepared_right.has_value())
        return common::make_unexpected(prepared_right.error());
      common::Result<std::unique_ptr<PhysicalOperator>> joined = AsofJoinOperator::create(
          std::move(*prepared_left), std::move(*prepared_right), join.definition, join.limits);
      if (!joined.has_value())
        return common::make_unexpected(joined.error());
      left = std::move(*joined);
    }
    return final_pipeline_.instantiate(std::move(left));
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("physical ASOF plan instantiation allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("physical ASOF plan instantiation exceeds container limits"));
  }
}

} // namespace chronos::query
