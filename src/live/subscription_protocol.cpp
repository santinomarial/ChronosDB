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
