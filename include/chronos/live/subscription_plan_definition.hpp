#ifndef CHRONOS_LIVE_SUBSCRIPTION_PLAN_DEFINITION_HPP_
#define CHRONOS_LIVE_SUBSCRIPTION_PLAN_DEFINITION_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/live/subscription.hpp"

#include <cstddef>
#include <string>
#include <vector>

namespace chronos::live {

inline constexpr std::size_t kSubscriptionPlanDefinitionHeaderSize = 128U;
inline constexpr std::size_t kSubscriptionPlanDefinitionTrailerSize = 4U;
inline constexpr std::size_t kMaximumSubscriptionPlanDefinitionSize = 16U * 1024U * 1024U;

struct SubscriptionPlanDefinitionLimits {
  std::size_t maximum_definition_bytes{1024U * 1024U};
  std::size_t maximum_sql_bytes{256U * 1024U};
};

struct SubscriptionPlanDefinition {
  common::Uuid database_id;
  schema::TableId table_id;
  schema::SchemaId schema_id;
  schema::SchemaVersion schema_version;
  PlanFingerprint plan_fingerprint{};
  std::string sql;

  friend bool operator==(const SubscriptionPlanDefinition&,
                         const SubscriptionPlanDefinition&) = default;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_subscription_plan_definition_v1(const SubscriptionPlanDefinition& definition,
                                       SubscriptionPlanDefinitionLimits limits = {});

[[nodiscard]] common::Result<SubscriptionPlanDefinition>
decode_subscription_plan_definition_v1(common::ByteView bytes,
                                       SubscriptionPlanDefinitionLimits limits = {});

} // namespace chronos::live

#endif // CHRONOS_LIVE_SUBSCRIPTION_PLAN_DEFINITION_HPP_
