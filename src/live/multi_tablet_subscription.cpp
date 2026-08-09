#include "chronos/live/multi_tablet_subscription.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <map>
#include <memory>
#include <new>
#include <ranges>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] bool key_is_zero(const ResumeTokenMacKey& key) noexcept {
  return std::ranges::all_of(key, [](const std::byte value) { return value == std::byte{0}; });
}

[[nodiscard]] common::Result<std::size_t> change_bytes(const CommittedChange& change) {
  if (change.result_key.size() > std::numeric_limits<std::size_t>::max() - change.payload.size())
    return common::make_unexpected(invalid("committed change byte size overflows"));
  return change.result_key.size() + change.payload.size();
}

[[nodiscard]] bool valid_limits(const SubscriptionLimits& limits) noexcept {
  return limits.maximum_subscriptions != 0U && limits.maximum_retained_changes != 0U &&
         limits.maximum_retained_bytes != 0U &&
         limits.maximum_buffered_changes_per_subscription != 0U &&
         limits.maximum_buffered_bytes_per_subscription != 0U &&
         limits.maximum_change_bytes != 0U &&
         limits.maximum_buffered_bytes_per_subscription <= limits.maximum_retained_bytes &&
         limits.maximum_change_bytes <= limits.maximum_retained_bytes;
}

} // namespace

class MultiTabletSubscriptionManager::Impl {
public:
  struct SourceState {
    schema::TabletId tablet_id;
    wal::WalId wal_id;
    std::uint64_t latest_sequence{};
    std::uint64_t expired_through_sequence{};
  };

  struct State {
    common::Uuid subscription_id;
    SubscriptionPhase phase{SubscriptionPhase::kSnapshot};
    std::vector<SourcePosition> snapshot_boundaries;
    std::vector<SourcePosition> safe_positions;
    std::uint64_t last_assigned_sequence{};
    std::uint64_t last_polled_sequence{};
    std::uint64_t last_acknowledged_sequence{};
    std::size_t buffered_bytes{};
    std::deque<DeliveryRecord> buffered;
  };

  Impl(MultiTabletSubscriptionSource configured_source, SubscriptionLimits configured_limits,
       std::vector<SourceState> configured_sources,
       std::map<schema::TabletId, std::size_t> configured_indexes)
      : source(std::move(configured_source)), limits(configured_limits),
        sources(std::move(configured_sources)), source_indexes(std::move(configured_indexes)) {}

  [[nodiscard]] std::vector<SourcePosition> positions() const {
    std::vector<SourcePosition> result;
    result.reserve(sources.size());
    for (const SourceState& state : sources)
      result.push_back({state.tablet_id, state.wal_id, state.latest_sequence});
    return result;
  }

  [[nodiscard]] ResumeToken token_for(const State& state) const {
    return ResumeToken{source.database_id,
                       state.subscription_id,
                       source.schema_id,
                       source.schema_version,
                       state.last_acknowledged_sequence,
                       source.plan_fingerprint,
                       state.safe_positions};
  }

  [[nodiscard]] common::Result<std::vector<std::byte>> encode_token(const State& state) const {
    return encode_resume_token_v1(token_for(state), source.token_key);
  }

  [[nodiscard]] bool can_buffer(const State& state, const std::size_t bytes) const noexcept {
    return state.buffered.size() < limits.maximum_buffered_changes_per_subscription &&
           bytes <= limits.maximum_buffered_bytes_per_subscription - state.buffered_bytes;
  }

  static void overflow(State& state) noexcept {
    state.phase = SubscriptionPhase::kOverflowed;
    state.buffered.clear();
    state.buffered_bytes = 0U;
  }

  [[nodiscard]] bool append(State& state, const std::shared_ptr<const CommittedChange>& change,
                            const std::size_t bytes) const noexcept {
    if (state.last_assigned_sequence == std::numeric_limits<std::uint64_t>::max() ||
        !can_buffer(state, bytes)) {
      overflow(state);
      return false;
    }
    try {
      const std::uint64_t sequence = state.last_assigned_sequence + 1U;
      state.buffered.push_back(DeliveryRecord{sequence, change});
      state.last_assigned_sequence = sequence;
      state.buffered_bytes += bytes;
      return true;
    } catch (const std::bad_alloc&) {
      overflow(state);
      return false;
    }
  }

  [[nodiscard]] bool token_sources_match(const std::vector<SourcePosition>& positions) const {
    if (positions.size() != sources.size())
      return false;
    for (std::size_t index = 0U; index < sources.size(); ++index) {
      if (positions[index].tablet_id != sources[index].tablet_id ||
          positions[index].wal_id != sources[index].wal_id)
        return false;
    }
    return true;
  }

  MultiTabletSubscriptionSource source;
  SubscriptionLimits limits;
  std::vector<SourceState> sources;
  std::map<schema::TabletId, std::size_t> source_indexes;
  std::size_t retained_change_bytes{};
  std::deque<std::shared_ptr<const CommittedChange>> retained_changes;
  mutable std::map<common::Uuid, State> subscriptions;
};

MultiTabletSubscriptionManager::MultiTabletSubscriptionManager(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
MultiTabletSubscriptionManager::~MultiTabletSubscriptionManager() = default;
MultiTabletSubscriptionManager::MultiTabletSubscriptionManager(
    MultiTabletSubscriptionManager&&) noexcept = default;
MultiTabletSubscriptionManager&
MultiTabletSubscriptionManager::operator=(MultiTabletSubscriptionManager&&) noexcept = default;

common::Result<MultiTabletSubscriptionManager>
MultiTabletSubscriptionManager::create(MultiTabletSubscriptionSource source,
                                       const SubscriptionLimits limits) {
  if (source.database_id.is_nil() || source.table_id.uuid().is_nil() ||
      source.schema_id.uuid().is_nil() || source.schema_version.value() == 0U ||
      source.members.empty() || source.members.size() > kMaximumResumeTokenSources ||
      key_is_zero(source.token_key) || !valid_limits(limits)) {
    return common::make_unexpected(
        invalid("multi-tablet subscription source identities or limits are invalid"));
  }
  try {
    std::ranges::sort(source.members, {}, &MultiTabletSubscriptionMember::tablet_id);
    std::vector<Impl::SourceState> states;
    std::map<schema::TabletId, std::size_t> indexes;
    states.reserve(source.members.size());
    for (const MultiTabletSubscriptionMember& member : source.members) {
      if (member.tablet_id.uuid().is_nil() || !member.wal_id.is_valid() ||
          indexes.contains(member.tablet_id)) {
        return common::make_unexpected(
            invalid("multi-tablet subscription member is invalid or duplicated"));
      }
      const std::size_t index = states.size();
      indexes.emplace(member.tablet_id, index);
      states.push_back({member.tablet_id, member.wal_id, member.committed_record_sequence,
                        member.committed_record_sequence});
    }
    return MultiTabletSubscriptionManager{
        std::make_unique<Impl>(std::move(source), limits, std::move(states), std::move(indexes))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("multi-tablet subscription allocation failed"));
  }
}

common::Result<MultiTabletSubscriptionManager>
MultiTabletSubscriptionManager::restore(MultiTabletSubscriptionSource source,
                                        const MultiTabletSubscriptionCheckpoint& checkpoint,
                                        const SubscriptionLimits limits) {
  auto restored = create(std::move(source), limits);
  if (!restored.has_value())
    return common::make_unexpected(restored.error());
  Impl& impl = *restored->impl_;
  if (checkpoint.database_id != impl.source.database_id ||
      checkpoint.table_id != impl.source.table_id ||
      checkpoint.plan_fingerprint != impl.source.plan_fingerprint ||
      checkpoint.schema_id != impl.source.schema_id ||
      checkpoint.schema_version != impl.source.schema_version ||
      checkpoint.sources.size() != impl.sources.size() ||
      checkpoint.retained_changes.size() > limits.maximum_retained_changes)
    return common::make_unexpected(
        invalid("subscription checkpoint identity, source count, or retention count is invalid"));

  try {
    std::vector<std::uint64_t> expected_sequences;
    expected_sequences.reserve(impl.sources.size());
    for (std::size_t index = 0U; index < impl.sources.size(); ++index) {
      const MultiTabletSubscriptionCheckpointSource& saved = checkpoint.sources[index];
      const Impl::SourceState& current = impl.sources[index];
      if (saved.latest_position.tablet_id != current.tablet_id ||
          saved.latest_position.wal_id != current.wal_id ||
          saved.latest_position.record_sequence != current.latest_sequence ||
          saved.expired_through_sequence > current.latest_sequence)
        return common::make_unexpected(
            invalid("subscription checkpoint source lineage or frontier is invalid"));
      expected_sequences.push_back(saved.expired_through_sequence);
    }

    std::size_t retained_bytes = 0U;
    std::deque<std::shared_ptr<const CommittedChange>> retained;
    for (const CommittedChange& change : checkpoint.retained_changes) {
      const auto source_index = impl.source_indexes.find(change.position.tablet_id);
      if (source_index == impl.source_indexes.end())
        return common::make_unexpected(
            invalid("subscription checkpoint change has an unknown source"));
      const std::size_t index = source_index->second;
      if (change.position.wal_id != impl.sources[index].wal_id ||
          expected_sequences[index] == std::numeric_limits<std::uint64_t>::max() ||
          change.position.record_sequence != expected_sequences[index] + 1U ||
          change.position.record_sequence > impl.sources[index].latest_sequence ||
          change.schema_id != impl.source.schema_id ||
          change.schema_version != impl.source.schema_version ||
          (change.operation != LogicalChangeOperation::kUpsert &&
           change.operation != LogicalChangeOperation::kDelete) ||
          change.result_key.empty() ||
          (change.operation == LogicalChangeOperation::kDelete && !change.payload.empty()))
        return common::make_unexpected(
            invalid("subscription checkpoint retained change is invalid or discontinuous"));
      const auto bytes = change_bytes(change);
      if (!bytes.has_value() || *bytes > limits.maximum_change_bytes ||
          *bytes > limits.maximum_retained_bytes - retained_bytes)
        return common::make_unexpected(
            exhausted("subscription checkpoint retained bytes exceed configured limits"));
      retained.push_back(std::make_shared<const CommittedChange>(change));
      retained_bytes += *bytes;
      expected_sequences[index] = change.position.record_sequence;
    }
    for (std::size_t index = 0U; index < impl.sources.size(); ++index) {
      if (expected_sequences[index] != impl.sources[index].latest_sequence)
        return common::make_unexpected(
            invalid("subscription checkpoint omits a required retained source suffix"));
      impl.sources[index].expired_through_sequence =
          checkpoint.sources[index].expired_through_sequence;
    }
    impl.retained_changes = std::move(retained);
    impl.retained_change_bytes = retained_bytes;
    return restored;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription checkpoint restore allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("subscription checkpoint exceeds container limits"));
  }
}

common::Result<MultiTabletSubscriptionRegistration>
MultiTabletSubscriptionManager::register_subscription(const SubscriptionRequest& request) {
  if (request.subscription_id.is_nil() ||
      request.plan_fingerprint != impl_->source.plan_fingerprint ||
      request.schema_id != impl_->source.schema_id ||
      request.schema_version != impl_->source.schema_version) {
    return common::make_unexpected(
        invalid("subscription request does not match the coordinator plan and schema"));
  }
  if (impl_->subscriptions.contains(request.subscription_id))
    return common::make_unexpected(common::Status{common::StatusCode::kAlreadyExists,
                                                  "subscription identity is already registered"});
  if (impl_->subscriptions.size() >= impl_->limits.maximum_subscriptions)
    return common::make_unexpected(exhausted("subscription capacity is exhausted"));
  try {
    std::vector<SourcePosition> boundary = impl_->positions();
    Impl::State state{request.subscription_id,
                      SubscriptionPhase::kSnapshot,
                      boundary,
                      boundary,
                      0U,
                      0U,
                      0U,
                      0U,
                      {}};
    auto token = impl_->encode_token(state);
    if (!token.has_value())
      return common::make_unexpected(token.error());
    impl_->subscriptions.emplace(request.subscription_id, std::move(state));
    return MultiTabletSubscriptionRegistration{std::move(boundary), std::move(*token)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription registration allocation failed"));
  }
}

common::Result<MultiTabletSubscriptionRegistration>
MultiTabletSubscriptionManager::resume_subscription(const common::ByteView encoded_token) {
  auto token =
      decode_resume_token_v1(encoded_token, impl_->source.token_key, impl_->sources.size());
  if (!token.has_value())
    return common::make_unexpected(token.error());
  if (token->database_id != impl_->source.database_id ||
      token->schema_id != impl_->source.schema_id ||
      token->schema_version != impl_->source.schema_version ||
      token->plan_fingerprint != impl_->source.plan_fingerprint ||
      !impl_->token_sources_match(token->source_positions)) {
    return common::make_unexpected(
        invalid("resume token does not match the coordinator plan, schema, or source set"));
  }
  const auto existing = impl_->subscriptions.find(token->subscription_id);
  if (existing != impl_->subscriptions.end() &&
      (existing->second.phase == SubscriptionPhase::kSnapshot ||
       existing->second.phase == SubscriptionPhase::kLive)) {
    return common::make_unexpected(common::Status{common::StatusCode::kAlreadyExists,
                                                  "subscription identity is already active"});
  }
  if (impl_->subscriptions.size() - (existing != impl_->subscriptions.end() ? 1U : 0U) >=
      impl_->limits.maximum_subscriptions)
    return common::make_unexpected(exhausted("subscription capacity is exhausted"));
  for (std::size_t index = 0U; index < impl_->sources.size(); ++index) {
    const std::uint64_t safe = token->source_positions[index].record_sequence;
    if (safe > impl_->sources[index].latest_sequence)
      return common::make_unexpected(common::Status{
          common::StatusCode::kOutOfRange, "resume token is ahead of committed source state"});
    if (safe < impl_->sources[index].expired_through_sequence)
      return common::make_unexpected(common::Status{
          common::StatusCode::kNotFound, "resume token committed source suffix has expired"});
  }

  try {
    Impl::State state{token->subscription_id,
                      SubscriptionPhase::kLive,
                      token->source_positions,
                      token->source_positions,
                      token->safe_delivery_sequence,
                      token->safe_delivery_sequence,
                      token->safe_delivery_sequence,
                      0U,
                      {}};
    for (const auto& change : impl_->retained_changes) {
      const std::size_t source_index = impl_->source_indexes.at(change->position.tablet_id);
      if (change->position.record_sequence <= state.safe_positions[source_index].record_sequence)
        continue;
      if (change->schema_id != impl_->source.schema_id ||
          change->schema_version != impl_->source.schema_version) {
        return common::make_unexpected(common::Status{common::StatusCode::kNotSupported,
                                                      "resume token plan schema is incompatible"});
      }
      const auto bytes = change_bytes(*change);
      if (!bytes.has_value() || !impl_->append(state, change, *bytes))
        return common::make_unexpected(
            exhausted("retained suffix exceeds subscriber buffer or delivery sequence"));
    }
    auto refreshed = impl_->encode_token(state);
    if (!refreshed.has_value())
      return common::make_unexpected(refreshed.error());
    if (existing != impl_->subscriptions.end())
      impl_->subscriptions.erase(existing);
    impl_->subscriptions.emplace(token->subscription_id, std::move(state));
    return MultiTabletSubscriptionRegistration{token->source_positions, std::move(*refreshed)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription resume allocation failed"));
  }
}

common::Status
MultiTabletSubscriptionManager::complete_snapshot(const common::Uuid& subscription_id) {
  const auto iterator = impl_->subscriptions.find(subscription_id);
  if (iterator == impl_->subscriptions.end())
    return common::Status{common::StatusCode::kNotFound, "subscription is not registered"};
  if (iterator->second.phase == SubscriptionPhase::kLive)
    return common::Status::ok();
  if (iterator->second.phase != SubscriptionPhase::kSnapshot)
    return common::Status{common::StatusCode::kUnavailable,
                          "subscription cannot complete after overflow or cancellation"};
  iterator->second.phase = SubscriptionPhase::kLive;
  return common::Status::ok();
}

common::Status MultiTabletSubscriptionManager::publish_committed(CommittedChange change) {
  const auto source = impl_->source_indexes.find(change.position.tablet_id);
  if (source == impl_->source_indexes.end())
    return invalid("committed change belongs to an unconfigured tablet");
  Impl::SourceState& source_state = impl_->sources[source->second];
  if (change.position.wal_id != source_state.wal_id ||
      source_state.latest_sequence == std::numeric_limits<std::uint64_t>::max() ||
      change.position.record_sequence != source_state.latest_sequence + 1U)
    return invalid("committed change does not follow its exact source lineage and sequence");
  if ((change.operation != LogicalChangeOperation::kUpsert &&
       change.operation != LogicalChangeOperation::kDelete) ||
      change.result_key.empty() ||
      (change.operation == LogicalChangeOperation::kDelete && !change.payload.empty()))
    return invalid("committed logical change is not canonical");
  const auto bytes = change_bytes(change);
  if (!bytes.has_value())
    return bytes.error();
  if (*bytes > impl_->limits.maximum_change_bytes || *bytes > impl_->limits.maximum_retained_bytes)
    return exhausted("committed change exceeds configured retention bound");

  std::shared_ptr<const CommittedChange> owned;
  try {
    owned = std::make_shared<const CommittedChange>(std::move(change));
    impl_->retained_changes.push_back(owned);
  } catch (const std::bad_alloc&) {
    return exhausted("committed change retention allocation failed");
  }
  source_state.latest_sequence = owned->position.record_sequence;
  while (impl_->retained_changes.size() > impl_->limits.maximum_retained_changes ||
         *bytes > impl_->limits.maximum_retained_bytes - impl_->retained_change_bytes) {
    const auto& expired = impl_->retained_changes.front();
    const std::size_t expired_source = impl_->source_indexes.at(expired->position.tablet_id);
    impl_->sources[expired_source].expired_through_sequence = std::max(
        impl_->sources[expired_source].expired_through_sequence, expired->position.record_sequence);
    impl_->retained_change_bytes -= *change_bytes(*expired);
    impl_->retained_changes.pop_front();
  }
  impl_->retained_change_bytes += *bytes;

  for (auto& [identity, state] : impl_->subscriptions) {
    static_cast<void>(identity);
    if (state.phase != SubscriptionPhase::kSnapshot && state.phase != SubscriptionPhase::kLive)
      continue;
    if (owned->schema_id != impl_->source.schema_id ||
        owned->schema_version != impl_->source.schema_version) {
      Impl::overflow(state);
      continue;
    }
    static_cast<void>(impl_->append(state, owned, *bytes));
  }
  return common::Status::ok();
}

common::Result<std::vector<DeliveryRecord>>
MultiTabletSubscriptionManager::poll(const common::Uuid& subscription_id,
                                     const std::size_t maximum_records) const {
  if (maximum_records == 0U)
    return common::make_unexpected(invalid("poll record bound must be nonzero"));
  const auto iterator = impl_->subscriptions.find(subscription_id);
  if (iterator == impl_->subscriptions.end())
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "subscription is not registered"});
  Impl::State& state = iterator->second;
  if (state.phase == SubscriptionPhase::kSnapshot)
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "historical snapshot is not complete"});
  if (state.phase == SubscriptionPhase::kOverflowed)
    return common::make_unexpected(common::Status{
        common::StatusCode::kResourceExhausted, "subscription buffer overflowed; resume required"});
  if (state.phase == SubscriptionPhase::kCancelled)
    return common::make_unexpected(
        common::Status{common::StatusCode::kCancelled, "subscription is cancelled"});
  try {
    const std::size_t count = std::min(maximum_records, state.buffered.size());
    std::vector<DeliveryRecord> output;
    output.reserve(count);
    for (std::size_t index = 0U; index < count; ++index)
      output.push_back(state.buffered[index]);
    if (!output.empty())
      state.last_polled_sequence =
          std::max(state.last_polled_sequence, output.back().delivery_sequence);
    return output;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription poll allocation failed"));
  }
}

common::Result<std::vector<std::byte>>
MultiTabletSubscriptionManager::acknowledge(const common::Uuid& subscription_id,
                                            const std::uint64_t delivery_sequence) {
  const auto iterator = impl_->subscriptions.find(subscription_id);
  if (iterator == impl_->subscriptions.end())
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "subscription is not registered"});
  Impl::State& state = iterator->second;
  if (state.phase != SubscriptionPhase::kLive ||
      delivery_sequence < state.last_acknowledged_sequence ||
      delivery_sequence > state.last_polled_sequence)
    return common::make_unexpected(invalid("acknowledgement is outside live delivered state"));
  while (!state.buffered.empty() && state.buffered.front().delivery_sequence <= delivery_sequence) {
    const auto& change = state.buffered.front().change;
    const std::size_t source_index = impl_->source_indexes.at(change->position.tablet_id);
    state.safe_positions[source_index] = change->position;
    state.buffered_bytes -= *change_bytes(*change);
    state.buffered.pop_front();
  }
  state.last_acknowledged_sequence = delivery_sequence;
  return impl_->encode_token(state);
}

common::Result<std::vector<std::byte>>
MultiTabletSubscriptionManager::cancel(const common::Uuid& subscription_id) {
  const auto iterator = impl_->subscriptions.find(subscription_id);
  if (iterator == impl_->subscriptions.end())
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "subscription is not registered"});
  Impl::State& state = iterator->second;
  auto token = impl_->encode_token(state);
  if (!token.has_value())
    return common::make_unexpected(token.error());
  state.phase = SubscriptionPhase::kCancelled;
  state.buffered.clear();
  state.buffered_bytes = 0U;
  return token;
}

common::Result<MultiTabletSubscriptionStatus>
MultiTabletSubscriptionManager::status(const common::Uuid& subscription_id) const {
  const auto iterator = impl_->subscriptions.find(subscription_id);
  if (iterator == impl_->subscriptions.end())
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "subscription is not registered"});
  const Impl::State& state = iterator->second;
  try {
    return MultiTabletSubscriptionStatus{state.phase,
                                         state.snapshot_boundaries,
                                         state.last_assigned_sequence,
                                         state.last_acknowledged_sequence,
                                         state.buffered.size(),
                                         state.buffered_bytes};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription status allocation failed"));
  }
}

common::Result<std::vector<SourcePosition>>
MultiTabletSubscriptionManager::latest_positions() const {
  try {
    return impl_->positions();
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription position allocation failed"));
  }
}

common::Result<MultiTabletSubscriptionCheckpoint>
MultiTabletSubscriptionManager::checkpoint() const {
  try {
    MultiTabletSubscriptionCheckpoint checkpoint{impl_->source.database_id,
                                                 impl_->source.table_id,
                                                 impl_->source.plan_fingerprint,
                                                 impl_->source.schema_id,
                                                 impl_->source.schema_version,
                                                 {},
                                                 {}};
    checkpoint.sources.reserve(impl_->sources.size());
    for (const Impl::SourceState& source : impl_->sources) {
      checkpoint.sources.push_back({{source.tablet_id, source.wal_id, source.latest_sequence},
                                    source.expired_through_sequence});
    }
    checkpoint.retained_changes.reserve(impl_->retained_changes.size());
    for (const auto& change : impl_->retained_changes)
      checkpoint.retained_changes.push_back(*change);
    return checkpoint;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription checkpoint allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("subscription checkpoint exceeds container limits"));
  }
}

const MultiTabletSubscriptionSource& MultiTabletSubscriptionManager::source() const noexcept {
  return impl_->source;
}

} // namespace chronos::live
