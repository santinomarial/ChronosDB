#ifndef CHRONOS_QUERY_RELATIONAL_PLAN_HPP_
#define CHRONOS_QUERY_RELATIONAL_PLAN_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/asof_join.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/physical_plan.hpp"

#include <cstddef>
#include <memory>
#include <span>
#include <vector>

namespace chronos::query {

inline constexpr std::size_t kDefaultPhysicalAsofPlanJoinLimit = 63U;
inline constexpr std::size_t kDefaultPhysicalAsofPlanConfigurationByteLimit =
    std::size_t{8U} * 1024U * 1024U;

struct PhysicalAsofPlanLimits {
  std::size_t maximum_joins{kDefaultPhysicalAsofPlanJoinLimit};
  std::size_t maximum_retained_configuration_bytes{kDefaultPhysicalAsofPlanConfigurationByteLimit};
};

struct PhysicalAsofPlanJoin {
  PhysicalPipelinePlan left_preparation;
  PhysicalPipelinePlan right_preparation;
  VectorAsofJoinDefinition definition;
  AsofJoinLimits limits{};
};

// Immutable reusable left-deep ASOF plan. Each join owns checked unary preparation plans for the
// accumulated left input and the next right source. The final pipeline consumes the last join's
// exact output. instantiate() consumes exactly joins + 1 independent sources in SQL source order.
class PhysicalAsofPlan {
public:
  PhysicalAsofPlan() = delete;
  PhysicalAsofPlan(const PhysicalAsofPlan&) = delete;
  PhysicalAsofPlan& operator=(const PhysicalAsofPlan&) = delete;
  PhysicalAsofPlan(PhysicalAsofPlan&&) noexcept = default;
  PhysicalAsofPlan& operator=(PhysicalAsofPlan&&) noexcept = default;

  [[nodiscard]] static common::Result<PhysicalAsofPlan>
  create(std::vector<PhysicalAsofPlanJoin> joins, PhysicalPipelinePlan final_pipeline,
         PhysicalAsofPlanLimits limits = {});

  [[nodiscard]] std::span<const PhysicalAsofPlanJoin> joins() const noexcept;
  [[nodiscard]] const PhysicalPipelinePlan& final_pipeline() const noexcept;
  [[nodiscard]] std::size_t source_count() const noexcept;
  [[nodiscard]] std::size_t retained_configuration_bytes() const noexcept;

  [[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
  instantiate(std::vector<std::unique_ptr<PhysicalOperator>> sources) const;

private:
  PhysicalAsofPlan(std::vector<PhysicalAsofPlanJoin> joins, PhysicalPipelinePlan final_pipeline,
                   std::size_t retained_configuration_bytes) noexcept;

  std::vector<PhysicalAsofPlanJoin> joins_;
  PhysicalPipelinePlan final_pipeline_;
  std::size_t retained_configuration_bytes_;
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_RELATIONAL_PLAN_HPP_
