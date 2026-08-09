#include "chronos/query/distributed.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <limits>
#include <optional>
#include <set>
#include <utility>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] bool intersects(const DistributedTablet& tablet,
                              const DistributedEventTimePredicate& predicate) noexcept {
  if (predicate.lower_inclusive.has_value() &&
      tablet.maximum_event_time < *predicate.lower_inclusive) {
    return false;
  }
  if (predicate.upper_exclusive.has_value() &&
      tablet.minimum_event_time >= *predicate.upper_exclusive) {
    return false;
  }
  return true;
}

inline constexpr std::size_t kExchangeMessageCharge = sizeof(ExchangeMessage);

} // namespace

common::Result<DistributedAggregatePlan> plan_distributed_aggregation(
    const common::Uuid query_id, const std::vector<DistributedTablet>& tablets,
    const DistributedEventTimePredicate& predicate, const DistributedReadConsistency consistency,
    const DistributedPlanLimits limits) {
  if (query_id.is_nil() || limits.maximum_tablets == 0U || limits.maximum_fragments == 0U ||
      tablets.size() > limits.maximum_tablets ||
      (predicate.lower_inclusive.has_value() && predicate.upper_exclusive.has_value() &&
       *predicate.lower_inclusive >= *predicate.upper_exclusive)) {
    return common::make_unexpected(invalid("distributed query identity, limits, or predicate invalid"));
  }
  switch (consistency) {
  case DistributedReadConsistency::kLeaderLinearizable:
  case DistributedReadConsistency::kFollowerBoundedStale:
  case DistributedReadConsistency::kLocalEventual:
    break;
  default:
    return common::make_unexpected(invalid("distributed read consistency mode is invalid"));
  }
  std::set<schema::TabletId> identities;
  DistributedAggregatePlan plan{query_id, consistency, {}};
  for (const DistributedTablet& tablet : tablets) {
    if (tablet.tablet_id.uuid().is_nil() || tablet.minimum_event_time > tablet.maximum_event_time ||
        tablet.leader_node == 0U || !identities.insert(tablet.tablet_id).second) {
      return common::make_unexpected(invalid("distributed tablet metadata is invalid"));
    }
    if (intersects(tablet, predicate)) {
      if (plan.fragments.size() >= limits.maximum_fragments) {
        return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                       "distributed fragment bound is exhausted"});
      }
      plan.fragments.push_back(tablet);
    }
  }
  return plan;
}

common::Status MergeableAggregateState::add(const double value) {
  if (count == std::numeric_limits<std::uint64_t>::max()) {
    return common::Status{common::StatusCode::kOutOfRange, "distributed aggregate count overflows"};
  }
  ++count;
  sum += value;
  minimum = minimum.has_value() ? std::min(*minimum, value) : value;
  maximum = maximum.has_value() ? std::max(*maximum, value) : value;
  const double delta = value - mean;
  mean += delta / static_cast<double>(count);
  m2 += delta * (value - mean);
  return common::Status::ok();
}

common::Status MergeableAggregateState::merge(const MergeableAggregateState& other) {
  if (other.count == 0U) return common::Status::ok();
  if (!other.minimum.has_value() || !other.maximum.has_value() ||
      (count != 0U && (!minimum.has_value() || !maximum.has_value()))) {
    return invalid("distributed partial aggregate state is inconsistent");
  }
  if (count > std::numeric_limits<std::uint64_t>::max() - other.count) {
    return common::Status{common::StatusCode::kOutOfRange, "distributed aggregate count overflows"};
  }
  if (count == 0U) {
    *this = other;
    return common::Status::ok();
  }
  const std::uint64_t combined = count + other.count;
  const double delta = other.mean - mean;
  m2 += other.m2 + delta * delta * static_cast<double>(count) *
                       static_cast<double>(other.count) / static_cast<double>(combined);
  mean += delta * static_cast<double>(other.count) / static_cast<double>(combined);
  count = combined;
  sum += other.sum;
  minimum = std::min(*minimum, *other.minimum);
  maximum = std::max(*maximum, *other.maximum);
  return common::Status::ok();
}

std::optional<double> MergeableAggregateState::variance_population() const noexcept {
  return count == 0U ? std::nullopt : std::optional<double>{m2 / static_cast<double>(count)};
}

class BoundedExchange::Impl {
public:
  Impl(common::Uuid id, ExchangeLimits configured) : query_id(id), limits(configured) {}
  common::Uuid query_id;
  ExchangeLimits limits;
  mutable std::mutex mutex;
  std::deque<ExchangeMessage> messages;
  std::size_t charged_bytes{};
  bool is_cancelled{};
};

BoundedExchange::BoundedExchange(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}
BoundedExchange::~BoundedExchange() = default;
BoundedExchange::BoundedExchange(BoundedExchange&&) noexcept = default;
BoundedExchange& BoundedExchange::operator=(BoundedExchange&&) noexcept = default;

common::Result<BoundedExchange> BoundedExchange::create(const common::Uuid query_id,
                                                        const ExchangeLimits limits) {
  if (query_id.is_nil() || limits.maximum_messages == 0U ||
      limits.maximum_bytes < kExchangeMessageCharge) {
    return common::make_unexpected(invalid("exchange identity or bounds are invalid"));
  }
  return BoundedExchange{std::make_unique<Impl>(query_id, limits)};
}

common::Status BoundedExchange::push(ExchangeMessage message) {
  std::scoped_lock lock{impl_->mutex};
  if (impl_->is_cancelled) {
    return common::Status{common::StatusCode::kCancelled, "exchange is cancelled"};
  }
  if (message.query_id != impl_->query_id || message.tablet_id.uuid().is_nil() ||
      message.sequence == 0U) {
    return invalid("exchange message identity or sequence is invalid");
  }
  if (impl_->messages.size() >= impl_->limits.maximum_messages ||
      kExchangeMessageCharge > impl_->limits.maximum_bytes - impl_->charged_bytes) {
    return common::Status{common::StatusCode::kResourceExhausted, "exchange is backpressured"};
  }
  impl_->charged_bytes += kExchangeMessageCharge;
  impl_->messages.push_back(std::move(message));
  return common::Status::ok();
}

common::Result<std::optional<ExchangeMessage>> BoundedExchange::try_pop() {
  std::scoped_lock lock{impl_->mutex};
  if (impl_->is_cancelled) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCancelled, "exchange is cancelled"});
  }
  if (impl_->messages.empty()) return std::optional<ExchangeMessage>{};
  ExchangeMessage message = std::move(impl_->messages.front());
  impl_->messages.pop_front();
  impl_->charged_bytes -= kExchangeMessageCharge;
  return std::optional<ExchangeMessage>{std::move(message)};
}

common::Status BoundedExchange::cancel() {
  std::scoped_lock lock{impl_->mutex};
  impl_->is_cancelled = true;
  impl_->messages.clear();
  impl_->charged_bytes = 0U;
  return common::Status::ok();
}

bool BoundedExchange::cancelled() const noexcept {
  std::scoped_lock lock{impl_->mutex};
  return impl_->is_cancelled;
}
std::size_t BoundedExchange::queued_messages() const noexcept {
  std::scoped_lock lock{impl_->mutex};
  return impl_->messages.size();
}

class DistributedAggregateCoordinator::Impl {
public:
  explicit Impl(DistributedAggregatePlan value) : plan(std::move(value)) {
    for (const auto& fragment : plan.fragments) expected.insert(fragment.tablet_id);
  }
  DistributedAggregatePlan plan;
  std::set<schema::TabletId> expected;
  std::map<schema::TabletId, MergeableAggregateState> completed;
  std::optional<common::Status> failure;
};

DistributedAggregateCoordinator::DistributedAggregateCoordinator(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
DistributedAggregateCoordinator::~DistributedAggregateCoordinator() = default;
DistributedAggregateCoordinator::DistributedAggregateCoordinator(
    DistributedAggregateCoordinator&&) noexcept = default;
DistributedAggregateCoordinator& DistributedAggregateCoordinator::operator=(
    DistributedAggregateCoordinator&&) noexcept = default;

common::Result<DistributedAggregateCoordinator>
DistributedAggregateCoordinator::create(DistributedAggregatePlan plan) {
  if (plan.query_id.is_nil()) {
    return common::make_unexpected(invalid("distributed coordinator query identity is invalid"));
  }
  return DistributedAggregateCoordinator{std::make_unique<Impl>(std::move(plan))};
}

common::Status DistributedAggregateCoordinator::accept(const ExchangeMessage& message) {
  if (impl_->failure.has_value()) return *impl_->failure;
  if (message.query_id != impl_->plan.query_id || !message.terminal ||
      !impl_->expected.contains(message.tablet_id)) {
    return invalid("distributed fragment result is not an expected terminal message");
  }
  if (!impl_->completed.emplace(message.tablet_id, message.partial).second) {
    return common::Status{common::StatusCode::kAlreadyExists,
                          "distributed fragment result is duplicated"};
  }
  return common::Status::ok();
}

common::Status DistributedAggregateCoordinator::worker_failed(
    const schema::TabletId& tablet_id, common::Status failure) {
  if (!impl_->expected.contains(tablet_id) || failure.is_ok()) {
    return invalid("distributed worker failure is invalid or belongs to another plan");
  }
  impl_->failure = std::move(failure);
  return common::Status::ok();
}

common::Result<MergeableAggregateState> DistributedAggregateCoordinator::finish() const {
  if (impl_->failure.has_value()) return common::make_unexpected(*impl_->failure);
  if (impl_->completed.size() != impl_->expected.size()) {
    return common::make_unexpected(common::Status{common::StatusCode::kUnavailable,
                                                   "distributed query has incomplete fragments"});
  }
  MergeableAggregateState merged;
  for (const auto& [tablet, partial] : impl_->completed) {
    static_cast<void>(tablet);
    const common::Status status = merged.merge(partial);
    if (!status.is_ok()) return common::make_unexpected(status);
  }
  return merged;
}

} // namespace chronos::query
