#include "chronos/live/subscription.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] bool key_is_zero(const ResumeTokenMacKey& key) noexcept {
  return std::ranges::all_of(key, [](const std::byte value) { return value == std::byte{0}; });
}

[[nodiscard]] std::size_t retained_bytes(const CommittedChange& change) noexcept {
  return change.result_key.size() + change.payload.size();
}

} // namespace

class SubscriptionManager::Impl {
public:
  struct State {
    SubscriptionRequest request;
    SubscriptionPhase phase{SubscriptionPhase::kSnapshot};
    SourcePosition snapshot_boundary;
    SourcePosition safe_position;
    std::uint64_t last_assigned_sequence{};
    std::uint64_t last_polled_sequence{};
    std::uint64_t last_acknowledged_sequence{};
    std::size_t buffered_bytes{};
    std::deque<DeliveryRecord> buffered;
  };

  Impl(SubscriptionSource source_value, SubscriptionLimits limits_value)
      : source(std::move(source_value)), limits(limits_value),
        latest_position{source.tablet_id, source.wal_id, 0U} {}

  [[nodiscard]] ResumeToken token_for(const State& state) const {
    return ResumeToken{source.database_id,
                       state.request.subscription_id,
                       state.request.schema_id,
                       state.request.schema_version,
                       state.last_acknowledged_sequence,
                       state.request.plan_fingerprint,
                       {state.safe_position}};
  }

  [[nodiscard]] common::Result<std::vector<std::byte>> encode_token(const State& state) const {
    return encode_resume_token_v1(token_for(state), source.token_key);
  }

  [[nodiscard]] bool can_buffer(const State& state, const std::size_t bytes) const noexcept {
    return state.buffered.size() < limits.maximum_buffered_changes_per_subscription &&
           bytes <= limits.maximum_buffered_bytes_per_subscription - state.buffered_bytes;
  }

  void overflow(State& state) const noexcept {
    state.phase = SubscriptionPhase::kOverflowed;
    state.buffered.clear();
    state.buffered_bytes = 0U;
  }

  void append(State& state, const std::shared_ptr<const CommittedChange>& change) const {
    const std::size_t bytes = retained_bytes(*change);
    if (!can_buffer(state, bytes)) {
      overflow(state);
      return;
    }
    ++state.last_assigned_sequence;
    state.buffered.push_back(DeliveryRecord{state.last_assigned_sequence, change});
    state.buffered_bytes += bytes;
  }

  SubscriptionSource source;
  SubscriptionLimits limits;
  SourcePosition latest_position;
  std::size_t retained_change_bytes{};
  std::deque<std::shared_ptr<const CommittedChange>> retained_changes;
  mutable std::map<common::Uuid, State> subscriptions;
};

SubscriptionManager::SubscriptionManager(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

SubscriptionManager::~SubscriptionManager() = default;
SubscriptionManager::SubscriptionManager(SubscriptionManager&&) noexcept = default;
SubscriptionManager& SubscriptionManager::operator=(SubscriptionManager&&) noexcept = default;

common::Result<SubscriptionManager>
SubscriptionManager::create(SubscriptionSource source, const SubscriptionLimits limits) {
  if (source.database_id.is_nil() || source.table_id.uuid().is_nil() ||
      source.tablet_id.uuid().is_nil() || !source.wal_id.is_valid() ||
      key_is_zero(source.token_key)) {
    return common::make_unexpected(invalid("subscription source identities and MAC key must be valid"));
  }
  if (limits.maximum_subscriptions == 0U || limits.maximum_retained_changes == 0U ||
      limits.maximum_retained_bytes == 0U ||
      limits.maximum_buffered_changes_per_subscription == 0U ||
      limits.maximum_buffered_bytes_per_subscription == 0U || limits.maximum_change_bytes == 0U ||
      limits.maximum_buffered_bytes_per_subscription > limits.maximum_retained_bytes ||
      limits.maximum_change_bytes > limits.maximum_retained_bytes) {
    return common::make_unexpected(invalid("subscription limits are inconsistent or zero"));
  }
  return SubscriptionManager{std::make_unique<Impl>(std::move(source), limits)};
}

common::Result<SubscriptionRegistration>
SubscriptionManager::register_subscription(const SubscriptionRequest& request) {
  if (request.subscription_id.is_nil() || request.schema_id.uuid().is_nil()) {
    return common::make_unexpected(invalid("subscription and schema identities must be nonzero"));
  }
  if (impl_->subscriptions.contains(request.subscription_id)) {
    return common::make_unexpected(common::Status{common::StatusCode::kAlreadyExists,
                                                   "subscription identity is already registered"});
  }
  if (impl_->subscriptions.size() >= impl_->limits.maximum_subscriptions) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                   "subscription capacity is exhausted"});
  }

  Impl::State state{request, SubscriptionPhase::kSnapshot, impl_->latest_position,
                    impl_->latest_position, 0U, 0U, 0U, 0U, {}};
  auto token = impl_->encode_token(state);
  if (!token.has_value()) {
    return common::make_unexpected(token.error());
  }
  impl_->subscriptions.emplace(request.subscription_id, std::move(state));
  return SubscriptionRegistration{impl_->latest_position, std::move(*token)};
}

common::Result<SubscriptionRegistration>
SubscriptionManager::resume_subscription(const common::ByteView encoded_token) {
  auto token = decode_resume_token_v1(encoded_token, impl_->source.token_key, 1U);
  if (!token.has_value()) {
    return common::make_unexpected(token.error());
  }
  if (token->database_id != impl_->source.database_id || token->source_positions.size() != 1U ||
      token->source_positions.front().tablet_id != impl_->source.tablet_id ||
      token->source_positions.front().wal_id != impl_->source.wal_id) {
    return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                   "resume token belongs to another source lineage"});
  }
  const auto existing_subscription = impl_->subscriptions.find(token->subscription_id);
  if (existing_subscription != impl_->subscriptions.end()) {
    if (existing_subscription->second.phase == SubscriptionPhase::kSnapshot ||
        existing_subscription->second.phase == SubscriptionPhase::kLive) {
      return common::make_unexpected(common::Status{common::StatusCode::kAlreadyExists,
                                                     "subscription identity is already active"});
    }
    impl_->subscriptions.erase(existing_subscription);
  }
  if (impl_->subscriptions.size() >= impl_->limits.maximum_subscriptions) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                   "subscription capacity is exhausted"});
  }
  const SourcePosition safe = token->source_positions.front();
  if (safe.record_sequence > impl_->latest_position.record_sequence) {
    return common::make_unexpected(common::Status{common::StatusCode::kOutOfRange,
                                                   "resume token is ahead of committed source state"});
  }
  if (safe.record_sequence < impl_->latest_position.record_sequence &&
      (impl_->retained_changes.empty() ||
       impl_->retained_changes.front()->position.record_sequence > safe.record_sequence + 1U)) {
    return common::make_unexpected(common::Status{common::StatusCode::kNotFound,
                                                   "resume token committed suffix has expired"});
  }

  SubscriptionRequest request{token->subscription_id, token->plan_fingerprint, token->schema_id,
                              token->schema_version};
  Impl::State state{request,
                    SubscriptionPhase::kLive,
                    safe,
                    safe,
                    token->safe_delivery_sequence,
                    token->safe_delivery_sequence,
                    token->safe_delivery_sequence,
                    0U,
                    {}};
  for (const auto& change : impl_->retained_changes) {
    if (change->position.record_sequence <= safe.record_sequence) {
      continue;
    }
    if (change->schema_id != state.request.schema_id ||
        change->schema_version != state.request.schema_version) {
      return common::make_unexpected(common::Status{common::StatusCode::kNotSupported,
                                                     "resume token plan schema is incompatible"});
    }
    if (!impl_->can_buffer(state, retained_bytes(*change))) {
      return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                     "retained suffix exceeds subscriber buffer"});
    }
    impl_->append(state, change);
  }
  auto refreshed = impl_->encode_token(state);
  if (!refreshed.has_value()) {
    return common::make_unexpected(refreshed.error());
  }
  impl_->subscriptions.emplace(request.subscription_id, std::move(state));
  return SubscriptionRegistration{safe, std::move(*refreshed)};
}

common::Status SubscriptionManager::complete_snapshot(const common::Uuid& subscription_id) {
  const auto iterator = impl_->subscriptions.find(subscription_id);
  if (iterator == impl_->subscriptions.end()) {
    return common::Status{common::StatusCode::kNotFound, "subscription is not registered"};
  }
  if (iterator->second.phase != SubscriptionPhase::kSnapshot) {
    return iterator->second.phase == SubscriptionPhase::kLive
               ? common::Status::ok()
               : common::Status{common::StatusCode::kUnavailable,
                                "subscription cannot complete an overflowed or cancelled snapshot"};
  }
  iterator->second.phase = SubscriptionPhase::kLive;
  return common::Status::ok();
}

common::Status SubscriptionManager::publish_committed(CommittedChange change) {
  if (change.position.tablet_id != impl_->source.tablet_id ||
      change.position.wal_id != impl_->source.wal_id ||
      change.position.record_sequence != impl_->latest_position.record_sequence + 1U) {
    return invalid("committed changes must follow the exact source lineage and sequence");
  }
  const std::size_t bytes = retained_bytes(change);
  if (bytes > impl_->limits.maximum_change_bytes || bytes > impl_->limits.maximum_retained_bytes) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "committed change exceeds configured retention bound"};
  }

  auto owned = std::make_shared<const CommittedChange>(std::move(change));
  impl_->latest_position = owned->position;
  impl_->retained_changes.push_back(owned);
  impl_->retained_change_bytes += bytes;
  while (impl_->retained_changes.size() > impl_->limits.maximum_retained_changes ||
         impl_->retained_change_bytes > impl_->limits.maximum_retained_bytes) {
    impl_->retained_change_bytes -= retained_bytes(*impl_->retained_changes.front());
    impl_->retained_changes.pop_front();
  }

  for (auto& [identity, state] : impl_->subscriptions) {
    static_cast<void>(identity);
    if (state.phase != SubscriptionPhase::kSnapshot && state.phase != SubscriptionPhase::kLive) {
      continue;
    }
    if (owned->schema_id != state.request.schema_id ||
        owned->schema_version != state.request.schema_version) {
      impl_->overflow(state);
      continue;
    }
    impl_->append(state, owned);
  }
  return common::Status::ok();
}

common::Result<std::vector<DeliveryRecord>>
SubscriptionManager::poll(const common::Uuid& subscription_id,
                          const std::size_t maximum_records) const {
  if (maximum_records == 0U) {
    return common::make_unexpected(invalid("poll record bound must be nonzero"));
  }
  const auto iterator = impl_->subscriptions.find(subscription_id);
  if (iterator == impl_->subscriptions.end()) {
    return common::make_unexpected(common::Status{common::StatusCode::kNotFound,
                                                   "subscription is not registered"});
  }
  Impl::State& state = iterator->second;
  if (state.phase == SubscriptionPhase::kSnapshot) {
    return common::make_unexpected(common::Status{common::StatusCode::kUnavailable,
                                                   "historical snapshot is not complete"});
  }
  if (state.phase == SubscriptionPhase::kOverflowed) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                   "subscription buffer overflowed; resume required"});
  }
  if (state.phase == SubscriptionPhase::kCancelled) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCancelled, "subscription is cancelled"});
  }

  const std::size_t count = std::min(maximum_records, state.buffered.size());
  std::vector<DeliveryRecord> output;
  output.reserve(count);
  for (std::size_t index = 0U; index < count; ++index) {
    output.push_back(state.buffered[index]);
  }
  if (!output.empty()) {
    state.last_polled_sequence = std::max(state.last_polled_sequence,
                                          output.back().delivery_sequence);
  }
  return output;
}

common::Result<std::vector<std::byte>>
SubscriptionManager::acknowledge(const common::Uuid& subscription_id,
                                 const std::uint64_t delivery_sequence) {
  const auto iterator = impl_->subscriptions.find(subscription_id);
  if (iterator == impl_->subscriptions.end()) {
    return common::make_unexpected(common::Status{common::StatusCode::kNotFound,
                                                   "subscription is not registered"});
  }
  Impl::State& state = iterator->second;
  if (delivery_sequence < state.last_acknowledged_sequence ||
      delivery_sequence > state.last_polled_sequence) {
    return common::make_unexpected(invalid("acknowledgment is outside the delivered sequence"));
  }
  while (!state.buffered.empty() &&
         state.buffered.front().delivery_sequence <= delivery_sequence) {
    state.safe_position = state.buffered.front().change->position;
    state.buffered_bytes -= retained_bytes(*state.buffered.front().change);
    state.buffered.pop_front();
  }
  state.last_acknowledged_sequence = delivery_sequence;
  return impl_->encode_token(state);
}

common::Result<std::vector<std::byte>>
SubscriptionManager::cancel(const common::Uuid& subscription_id) {
  const auto iterator = impl_->subscriptions.find(subscription_id);
  if (iterator == impl_->subscriptions.end()) {
    return common::make_unexpected(common::Status{common::StatusCode::kNotFound,
                                                   "subscription is not registered"});
  }
  Impl::State& state = iterator->second;
  auto token = impl_->encode_token(state);
  if (!token.has_value()) {
    return common::make_unexpected(token.error());
  }
  state.phase = SubscriptionPhase::kCancelled;
  state.buffered.clear();
  state.buffered_bytes = 0U;
  return token;
}

common::Result<SubscriptionStatus>
SubscriptionManager::status(const common::Uuid& subscription_id) const {
  const auto iterator = impl_->subscriptions.find(subscription_id);
  if (iterator == impl_->subscriptions.end()) {
    return common::make_unexpected(common::Status{common::StatusCode::kNotFound,
                                                   "subscription is not registered"});
  }
  const Impl::State& state = iterator->second;
  return SubscriptionStatus{state.phase,
                            state.snapshot_boundary,
                            state.last_assigned_sequence,
                            state.last_acknowledged_sequence,
                            state.buffered.size(),
                            state.buffered_bytes};
}

} // namespace chronos::live
