#include "chronos/live/subscription_protocol.hpp"

#include <string>
#include <utility>

namespace chronos::live {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Result<network::SubscriptionChangeOperation>
wire_operation(const LogicalChangeOperation operation) {
  switch (operation) {
  case LogicalChangeOperation::kUpsert:
    return network::SubscriptionChangeOperation::kUpsert;
  case LogicalChangeOperation::kDelete:
    return network::SubscriptionChangeOperation::kDelete;
  }
  return common::make_unexpected(invalid("logical subscription operation is unassigned"));
}

[[nodiscard]] common::Status validate_terminal_reason(
    const SubscriptionPhase phase, const network::SubscriptionEndReason reason) {
  if (phase == SubscriptionPhase::kSchemaChanged &&
      reason != network::SubscriptionEndReason::kSchemaChanged)
    return invalid("schema-changed subscription requires SCHEMA_CHANGED termination");
  if (phase != SubscriptionPhase::kSchemaChanged &&
      reason == network::SubscriptionEndReason::kSchemaChanged)
    return invalid("SCHEMA_CHANGED termination requires schema-changed subscription state");
  if (phase == SubscriptionPhase::kOverflowed &&
      reason != network::SubscriptionEndReason::kOverflowed)
    return invalid("overflowed subscription requires OVERFLOWED termination");
  if (phase != SubscriptionPhase::kOverflowed &&
      reason == network::SubscriptionEndReason::kOverflowed)
    return invalid("OVERFLOWED termination requires overflowed subscription state");
  return common::Status::ok();
}

} // namespace

common::Result<std::vector<std::byte>>
encode_subscription_registration(const SubscriptionRegistration& registration,
                                 const network::SubscriptionMessageLimits& limits) {
  return network::encode_subscription_ready(registration.initial_resume_token, limits);
}

common::Result<std::vector<std::byte>>
encode_subscription_registration(const MultiTabletSubscriptionRegistration& registration,
                                 const network::SubscriptionMessageLimits& limits) {
  return network::encode_subscription_ready(registration.initial_resume_token, limits);
}

common::Result<std::vector<std::byte>>
encode_subscription_delivery(const DeliveryRecord& delivery,
                             const network::SubscriptionMessageLimits& limits) {
  if (delivery.delivery_sequence == 0U || !delivery.change)
    return common::make_unexpected(invalid("subscription delivery owner is invalid"));
  const auto operation = wire_operation(delivery.change->operation);
  if (!operation.has_value())
    return common::make_unexpected(operation.error());
  return network::encode_subscription_change(
      {*operation, delivery.delivery_sequence, delivery.change->position.tablet_id,
       delivery.change->position.wal_id.bytes, delivery.change->position.record_sequence,
       delivery.change->schema_id, delivery.change->schema_version, delivery.change->result_key,
       delivery.change->payload},
      limits);
}

common::Result<std::vector<std::byte>>
acknowledge_subscription_delivery(SubscriptionManager& manager, const common::Uuid& subscription_id,
                                  const std::uint64_t delivery_sequence,
                                  const network::SubscriptionMessageLimits& limits) {
  auto token = manager.acknowledge(subscription_id, delivery_sequence);
  if (!token.has_value())
    return common::make_unexpected(token.error());
  return network::encode_subscription_checkpoint(
      {.acknowledged_delivery_sequence = delivery_sequence, .resume_token = *token}, limits);
}

common::Result<std::vector<std::byte>> acknowledge_subscription_delivery(
    MultiTabletSubscriptionManager& manager, const common::Uuid& subscription_id,
    const std::uint64_t delivery_sequence, const network::SubscriptionMessageLimits& limits) {
  auto token = manager.acknowledge(subscription_id, delivery_sequence);
  if (!token.has_value())
    return common::make_unexpected(token.error());
  return network::encode_subscription_checkpoint(
      {.acknowledged_delivery_sequence = delivery_sequence, .resume_token = *token}, limits);
}

common::Result<std::vector<std::byte>>
terminate_subscription(SubscriptionManager& manager, const common::Uuid& subscription_id,
                       const network::SubscriptionEndReason reason,
                       const network::SubscriptionMessageLimits& limits) {
  const auto current = manager.status(subscription_id);
  if (!current.has_value())
    return common::make_unexpected(current.error());
  if (const common::Status status = validate_terminal_reason(current->phase, reason);
      !status.is_ok())
    return common::make_unexpected(status);
  auto token = manager.cancel(subscription_id);
  if (!token.has_value())
    return common::make_unexpected(token.error());
  return network::encode_subscription_end(
      {.reason = reason,
       .safe_delivery_sequence = current->last_acknowledged_sequence,
       .resume_token = *token},
      limits);
}

common::Result<std::vector<std::byte>>
terminate_subscription(MultiTabletSubscriptionManager& manager, const common::Uuid& subscription_id,
                       const network::SubscriptionEndReason reason,
                       const network::SubscriptionMessageLimits& limits) {
  const auto current = manager.status(subscription_id);
  if (!current.has_value())
    return common::make_unexpected(current.error());
  if (const common::Status status = validate_terminal_reason(current->phase, reason);
      !status.is_ok())
    return common::make_unexpected(status);
  auto token = manager.cancel(subscription_id);
  if (!token.has_value())
    return common::make_unexpected(token.error());
  return network::encode_subscription_end(
      {.reason = reason,
       .safe_delivery_sequence = current->last_acknowledged_sequence,
       .resume_token = *token},
      limits);
}

} // namespace chronos::live
