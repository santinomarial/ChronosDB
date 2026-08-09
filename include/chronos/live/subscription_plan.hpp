#ifndef CHRONOS_LIVE_SUBSCRIPTION_PLAN_HPP_
#define CHRONOS_LIVE_SUBSCRIPTION_PLAN_HPP_

#include "chronos/common/uuid.hpp"
#include "chronos/live/snapshot_subscription.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/query/physical_plan.hpp"

#include <memory>
#include <string_view>
#include <vector>

namespace chronos::live {

struct SubscriptionPlanLimits {
  query::SqlParserLimits parser{};
  query::SqlBinderLimits binder{};
  query::PhysicalSelectLoweringLimits lowering{};
};

// Exact executable identity for the current single-source SUBSCRIBE SELECT surface. The raw SQL
// bytes and every bound source table/schema/version are SHA-256 fingerprinted under a versioned
// domain. Textually different SQL intentionally has a different identity even if semantics happen
// to be equivalent. The retained schema owner and physical plan are immutable and thread-safe for
// concurrent const use; each instantiated pipeline remains thread-affine.
class PreparedSubscriptionPlan {
public:
  PreparedSubscriptionPlan() = delete;
  PreparedSubscriptionPlan(const PreparedSubscriptionPlan&) = delete;
  PreparedSubscriptionPlan& operator=(const PreparedSubscriptionPlan&) = delete;
  PreparedSubscriptionPlan(PreparedSubscriptionPlan&&) noexcept = default;
  PreparedSubscriptionPlan& operator=(PreparedSubscriptionPlan&&) noexcept = default;

  [[nodiscard]] const PlanFingerprint& fingerprint() const noexcept;
  [[nodiscard]] const std::shared_ptr<const schema::TableSchema>& schema_ptr() const noexcept;
  [[nodiscard]] const query::PhysicalPipelinePlan& physical_plan() const noexcept;
  [[nodiscard]] const std::vector<SnapshotSubscriptionColumn>& columns() const noexcept;
  [[nodiscard]] SubscriptionSource source(common::Uuid database_id, schema::TabletId tablet_id,
                                          wal::WalId wal_id,
                                          ResumeTokenMacKey token_key) const noexcept;
  [[nodiscard]] SubscriptionRequest request(common::Uuid subscription_id) const noexcept;

private:
  PreparedSubscriptionPlan(PlanFingerprint fingerprint,
                           std::shared_ptr<const schema::TableSchema> schema,
                           query::PhysicalPipelinePlan physical_plan,
                           std::vector<SnapshotSubscriptionColumn> columns) noexcept;

  PlanFingerprint fingerprint_{};
  std::shared_ptr<const schema::TableSchema> schema_;
  query::PhysicalPipelinePlan physical_plan_;
  std::vector<SnapshotSubscriptionColumn> columns_;

  friend query::SqlResult<PreparedSubscriptionPlan>
  prepare_subscription_plan(std::string_view, std::shared_ptr<const query::QueryCatalogSnapshot>,
                            const SubscriptionPlanLimits&);
};

[[nodiscard]] query::SqlResult<PreparedSubscriptionPlan>
prepare_subscription_plan(std::string_view sql,
                          std::shared_ptr<const query::QueryCatalogSnapshot> catalog,
                          const SubscriptionPlanLimits& limits = {});

} // namespace chronos::live

#endif // CHRONOS_LIVE_SUBSCRIPTION_PLAN_HPP_
