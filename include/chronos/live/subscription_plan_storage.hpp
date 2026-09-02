#ifndef CHRONOS_LIVE_SUBSCRIPTION_PLAN_STORAGE_HPP_
#define CHRONOS_LIVE_SUBSCRIPTION_PLAN_STORAGE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/live/subscription_plan.hpp"
#include "chronos/live/subscription_plan_definition.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace chronos::live {

struct SubscriptionPlanStorageConfig {
  std::string directory_path;
  common::Uuid database_id;
  SubscriptionPlanDefinitionLimits definition_limits{};
  SubscriptionPlanLimits plan_limits{};
  std::uint16_t file_permissions{0600U};
};

struct InstalledSubscriptionPlan {
  PlanFingerprint plan_fingerprint{};
  std::string file_name;
  bool already_present{};
};

[[nodiscard]] std::string subscription_plan_file_name(const PlanFingerprint& fingerprint);

// Locked database-scoped immutable plan-definition store. Exact SQL is recovered and prepared
// against the supplied catalog on every load; stored and recomputed table/schema/fingerprint
// bindings must agree before an executable plan is returned.
class SubscriptionPlanStorage {
public:
  SubscriptionPlanStorage() = delete;
  ~SubscriptionPlanStorage();
  SubscriptionPlanStorage(const SubscriptionPlanStorage&) = delete;
  SubscriptionPlanStorage& operator=(const SubscriptionPlanStorage&) = delete;
  SubscriptionPlanStorage(SubscriptionPlanStorage&&) noexcept;
  SubscriptionPlanStorage& operator=(SubscriptionPlanStorage&&) noexcept;

  [[nodiscard]] static common::Result<SubscriptionPlanStorage>
  create(SubscriptionPlanStorageConfig config);
  [[nodiscard]] static common::Result<SubscriptionPlanStorage>
  open_existing(SubscriptionPlanStorageConfig config);

  [[nodiscard]] common::Result<InstalledSubscriptionPlan>
  install(std::string_view sql, std::shared_ptr<const query::QueryCatalogSnapshot> catalog);
  [[nodiscard]] common::Result<PreparedSubscriptionPlan>
  load(const PlanFingerprint& fingerprint,
       std::shared_ptr<const query::QueryCatalogSnapshot> catalog) const;

  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;

private:
  class Impl;
  [[nodiscard]] static common::Result<SubscriptionPlanStorage>
  open(SubscriptionPlanStorageConfig config, bool create_lock);
  explicit SubscriptionPlanStorage(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::live

#endif // CHRONOS_LIVE_SUBSCRIPTION_PLAN_STORAGE_HPP_
