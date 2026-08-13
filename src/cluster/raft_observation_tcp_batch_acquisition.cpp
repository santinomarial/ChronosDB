#include "chronos/cluster/raft_observation_tcp_batch_acquisition.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <limits>
#include <new>
#include <optional>
#include <poll.h>
#include <ranges>
#include <set>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status status(common::StatusCode code, const char* message) {
  return {code, message};
}

using TimePoint = RaftObservationTcpClient::TimePoint;

[[nodiscard]] std::chrono::milliseconds bounded_wait(std::chrono::milliseconds maximum_wait,
                                                     const TimePoint now,
                                                     const TimePoint deadline) noexcept {
  if (deadline <= now)
    return std::chrono::milliseconds{0};
  return std::min(maximum_wait,
                  std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now));
}

[[nodiscard]] bool is_running(const RaftObservationTcpPairAcquisition& pair) noexcept {
  return pair.state() == RaftObservationTcpPairAcquisitionState::kRunning;
}

} // namespace

namespace {

template <typename Plan>
common::Result<RaftObservationTcpBatchAcquisitionConfig>
construct_raft_observation_tcp_batch_impl(const Plan& plan,
                                          const raft::MetadataCatalogSnapshot& catalog,
                                          const RaftObservationTcpBatchConstructionConfig& config) {
  if (plan.read_policy.consistency != query::DistributedReadConsistency::kFollowerBoundedStale ||
      !plan.read_policy.maximum_staleness_positions.has_value() || plan.query_id.is_nil() ||
      plan.fragments.empty() ||
      plan.fragments.size() > query::DistributedPlanLimits{}.maximum_fragments ||
      !std::ranges::is_sorted(plan.fragments, {}, &query::DistributedTablet::tablet_id) ||
      std::ranges::adjacent_find(plan.fragments, {}, &query::DistributedTablet::tablet_id) !=
          plan.fragments.end() ||
      config.source_node_id == 0U || config.first_correlation_id == 0U ||
      config.authenticator == nullptr || config.node_authorizer == nullptr ||
      config.maximum_pairs == 0U ||
      config.maximum_pairs > query::DistributedPlanLimits{}.maximum_fragments) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft observation batch construction is invalid"));
  }
  if (catalog.applied_index == 0U ||
      !std::ranges::is_sorted(catalog.tablet_placements, {},
                              &raft::TabletPlacementMetadata::tablet_id) ||
      std::ranges::adjacent_find(catalog.tablet_placements, {},
                                 &raft::TabletPlacementMetadata::tablet_id) !=
          catalog.tablet_placements.end() ||
      !std::ranges::is_sorted(catalog.tablet_group_bindings, {},
                              &raft::TabletGroupBindingMetadata::tablet_id) ||
      std::ranges::adjacent_find(catalog.tablet_group_bindings, {},
                                 &raft::TabletGroupBindingMetadata::tablet_id) !=
          catalog.tablet_group_bindings.end()) {
    return common::make_unexpected(
        status(common::StatusCode::kCorruption,
               "Raft observation placement metadata is not a canonical committed snapshot"));
  }
  struct Selection {
    raft::GroupId group_id;
    raft::NodeId leader{};
    raft::NodeId follower{};
  };
  try {
    std::vector<Selection> selections;
    selections.reserve(plan.fragments.size());
    for (const auto& fragment : plan.fragments) {
      const auto placement =
          std::ranges::lower_bound(catalog.tablet_placements, fragment.tablet_id, {},
                                   &raft::TabletPlacementMetadata::tablet_id);
      const auto group = std::ranges::lower_bound(catalog.tablet_group_bindings, fragment.tablet_id,
                                                  {}, &raft::TabletGroupBindingMetadata::tablet_id);
      if (placement == catalog.tablet_placements.end() ||
          placement->tablet_id != fragment.tablet_id ||
          group == catalog.tablet_group_bindings.end() || group->tablet_id != fragment.tablet_id ||
          group->group_id.is_nil() || placement->replicas.size() < 2U ||
          placement->replicas.size() > raft::MetadataLimits{}.maximum_replicas_per_tablet ||
          placement->replicas.front() == 0U || !std::ranges::is_sorted(placement->replicas) ||
          std::ranges::adjacent_find(placement->replicas) != placement->replicas.end() ||
          placement->leader_hint.value_or(0U) != fragment.leader_node ||
          !std::ranges::binary_search(placement->replicas, fragment.leader_node)) {
        return common::make_unexpected(
            status(common::StatusCode::kUnavailable,
                   "planned tablet lacks committed leader/follower observation placement"));
      }
      raft::NodeId follower{};
      if (config.source_node_id != fragment.leader_node &&
          std::ranges::binary_search(placement->replicas, config.source_node_id)) {
        follower = config.source_node_id;
      } else {
        const auto candidate =
            std::ranges::find_if(placement->replicas, [&](const raft::NodeId node) {
              return node != fragment.leader_node;
            });
        if (candidate != placement->replicas.end())
          follower = *candidate;
      }
      if (follower == 0U)
        return common::make_unexpected(status(common::StatusCode::kUnavailable,
                                              "planned tablet has no follower observation target"));
      selections.push_back({group->group_id, fragment.leader_node, follower});
    }
    std::ranges::sort(selections, {}, &Selection::group_id);
    for (std::size_t index = 1U; index < selections.size(); ++index) {
      if (selections[index - 1U].group_id == selections[index].group_id &&
          (selections[index - 1U].leader != selections[index].leader ||
           selections[index - 1U].follower != selections[index].follower)) {
        return common::make_unexpected(
            status(common::StatusCode::kCorruption,
                   "one Raft group has inconsistent committed observation selections"));
      }
    }
    selections.erase(std::unique(selections.begin(), selections.end(),
                                 [](const Selection& left, const Selection& right) {
                                   return left.group_id == right.group_id;
                                 }),
                     selections.end());
    if (selections.size() > config.maximum_pairs)
      return common::make_unexpected(
          status(common::StatusCode::kResourceExhausted,
                 "Raft observation constructed pair limit is exhausted"));
    if (selections.size() >
        (std::numeric_limits<std::uint64_t>::max() - config.first_correlation_id + 1U) / 2U) {
      return common::make_unexpected(
          status(common::StatusCode::kOutOfRange, "Raft observation correlation range overflows"));
    }
    std::set<raft::NodeId> target_set;
    for (const auto& selection : selections) {
      target_set.insert(selection.leader);
      target_set.insert(selection.follower);
    }
    std::vector<raft::NodeId> targets(target_set.begin(), target_set.end());
    auto routes = resolve_raft_observation_tcp_routes(catalog, targets, config.tls_contexts,
                                                      config.route_limits);
    if (!routes.has_value())
      return common::make_unexpected(routes.error());
    std::vector<RaftObservationTcpPairAcquisitionConfig> pairs;
    pairs.reserve(selections.size());
    for (std::size_t index = 0U; index < selections.size(); ++index) {
      const Selection& selection = selections[index];
      const auto leader_route = std::ranges::lower_bound(*routes, selection.leader, {},
                                                         &RaftObservationTcpRoute::node_id);
      const auto follower_route = std::ranges::lower_bound(*routes, selection.follower, {},
                                                           &RaftObservationTcpRoute::node_id);
      const std::uint64_t leader_correlation = config.first_correlation_id + index * 2U;
      pairs.push_back({.leader = {.route = *leader_route,
                                  .authenticator = config.authenticator,
                                  .node_authorizer = config.node_authorizer,
                                  .request = {config.source_node_id, selection.leader,
                                              selection.group_id, leader_correlation},
                                  .carrier_limits = config.carrier_limits,
                                  .connect_timeout = config.connect_timeout,
                                  .retry = config.retry},
                       .follower = {.route = *follower_route,
                                    .authenticator = config.authenticator,
                                    .node_authorizer = config.node_authorizer,
                                    .request = {config.source_node_id, selection.follower,
                                                selection.group_id, leader_correlation + 1U},
                                    .carrier_limits = config.carrier_limits,
                                    .connect_timeout = config.connect_timeout,
                                    .retry = config.retry}});
    }
    return RaftObservationTcpBatchAcquisitionConfig{.pairs = std::move(pairs),
                                                    .maximum_pairs = config.maximum_pairs};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft observation batch construction allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft observation batch construction is too large"));
  }
}

} // namespace

common::Result<RaftObservationTcpBatchAcquisitionConfig>
construct_raft_observation_tcp_batch(const query::DistributedAggregatePlan& plan,
                                     const raft::MetadataCatalogSnapshot& catalog,
                                     const RaftObservationTcpBatchConstructionConfig& config) {
  return construct_raft_observation_tcp_batch_impl(plan, catalog, config);
}

common::Result<RaftObservationTcpBatchAcquisitionConfig>
construct_raft_observation_tcp_batch(const query::DistributedVectorQueryPlan& plan,
                                     const raft::MetadataCatalogSnapshot& catalog,
                                     const RaftObservationTcpBatchConstructionConfig& config) {
  return construct_raft_observation_tcp_batch_impl(plan, catalog, config);
}

class RaftObservationTcpBatchAcquisition::Impl {
public:
  Impl(std::vector<RaftObservationTcpPairAcquisition> owned_pairs,
       std::vector<pollfd> descriptors) noexcept
      : pairs(std::move(owned_pairs)), poll_descriptors(std::move(descriptors)) {
    batch_metrics.total_pairs = pairs.size();
    batch_metrics.active_pairs = pairs.size();
  }

  void cancel_running() noexcept {
    std::size_t completed{};
    for (auto& pair : pairs) {
      if (is_running(pair))
        static_cast<void>(pair.cancel());
      completed += pair.state() == RaftObservationTcpPairAcquisitionState::kComplete ? 1U : 0U;
    }
    batch_metrics.completed_pairs = completed;
    batch_metrics.active_pairs = 0U;
  }

  [[nodiscard]] common::Status fail(common::Status failure) {
    cancel_running();
    batch_failure = std::move(failure);
    batch_state = RaftObservationTcpBatchAcquisitionState::kFailed;
    return batch_failure;
  }

  [[nodiscard]] common::Status drive_pairs() {
    std::size_t completed{};
    std::size_t active{};
    for (auto& pair : pairs) {
      if (is_running(pair)) {
        const common::Status progress = pair.poll_once(std::chrono::milliseconds{0});
        if (!progress.is_ok() && pair.state() != RaftObservationTcpPairAcquisitionState::kFailed)
          return fail(progress);
      }
      if (pair.state() == RaftObservationTcpPairAcquisitionState::kFailed)
        return fail(pair.failure());
      completed += pair.state() == RaftObservationTcpPairAcquisitionState::kComplete ? 1U : 0U;
      active += is_running(pair) ? 1U : 0U;
    }
    batch_metrics.completed_pairs = completed;
    batch_metrics.active_pairs = active;
    if (completed != pairs.size())
      return common::Status::ok();
    try {
      std::vector<query::DistributedAggregateFollowerReadAuthority> authorities;
      authorities.reserve(pairs.size());
      for (const auto& pair : pairs) {
        auto authority = pair.result();
        if (!authority.has_value())
          return fail(authority.error());
        authorities.push_back(std::move(*authority));
      }
      batch_result.emplace(std::move(authorities));
      batch_state = RaftObservationTcpBatchAcquisitionState::kComplete;
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return fail(status(common::StatusCode::kResourceExhausted,
                         "Raft observation batch result allocation failed"));
    } catch (const std::length_error&) {
      return fail(status(common::StatusCode::kResourceExhausted,
                         "Raft observation batch result is too large"));
    }
  }

  std::vector<RaftObservationTcpPairAcquisition> pairs;
  std::vector<pollfd> poll_descriptors;
  RaftObservationTcpBatchAcquisitionMetrics batch_metrics;
  RaftObservationTcpBatchAcquisitionState batch_state{
      RaftObservationTcpBatchAcquisitionState::kRunning};
  std::optional<std::vector<query::DistributedAggregateFollowerReadAuthority>> batch_result;
  common::Status batch_failure{common::StatusCode::kInternal,
                               "Raft observation batch acquisition has not failed"};
};

RaftObservationTcpBatchAcquisition::RaftObservationTcpBatchAcquisition() noexcept = default;
RaftObservationTcpBatchAcquisition::RaftObservationTcpBatchAcquisition(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftObservationTcpBatchAcquisition::~RaftObservationTcpBatchAcquisition() = default;
RaftObservationTcpBatchAcquisition::RaftObservationTcpBatchAcquisition(
    RaftObservationTcpBatchAcquisition&&) noexcept = default;
RaftObservationTcpBatchAcquisition& RaftObservationTcpBatchAcquisition::operator=(
    RaftObservationTcpBatchAcquisition&&) noexcept = default;

common::Result<RaftObservationTcpBatchAcquisition>
RaftObservationTcpBatchAcquisition::create(RaftObservationTcpBatchAcquisitionConfig config) {
  if (config.maximum_pairs == 0U ||
      config.maximum_pairs > query::DistributedPlanLimits{}.maximum_fragments ||
      config.pairs.empty()) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft observation batch configuration is invalid"));
  }
  if (config.pairs.size() > config.maximum_pairs) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted, "Raft observation pair limit is exhausted"));
  }
  if (!std::ranges::is_sorted(config.pairs, {},
                              [](const auto& pair) { return pair.leader.request.group_id; }) ||
      std::ranges::adjacent_find(config.pairs, {}, [](const auto& pair) {
        return pair.leader.request.group_id;
      }) != config.pairs.end()) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft observation pair batch is not canonical"));
  }
  try {
    std::vector<RaftObservationTcpPairAcquisition> pairs;
    pairs.reserve(config.pairs.size());
    for (auto& pair_config : config.pairs) {
      auto pair = RaftObservationTcpPairAcquisition::create(std::move(pair_config));
      if (!pair.has_value())
        return common::make_unexpected(pair.error());
      pairs.push_back(std::move(*pair));
    }
    std::vector<pollfd> descriptors(pairs.size() * 2U);
    return RaftObservationTcpBatchAcquisition{
        std::make_unique<Impl>(std::move(pairs), std::move(descriptors))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted, "Raft observation batch allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft observation batch exceeds container limits"));
  }
}

common::Status
RaftObservationTcpBatchAcquisition::poll_once(const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument,
                  "Raft observation batch acquisition is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return status(common::StatusCode::kInvalidArgument,
                  "Raft observation batch poll timeout is invalid");
  Impl& impl = *implementation_;
  if (impl.batch_state == RaftObservationTcpBatchAcquisitionState::kFailed ||
      impl.batch_state == RaftObservationTcpBatchAcquisitionState::kCancelled) {
    return impl.batch_failure;
  }
  if (impl.batch_state == RaftObservationTcpBatchAcquisitionState::kComplete)
    return common::Status::ok();
  common::Status driven = impl.drive_pairs();
  if (!driven.is_ok() || impl.batch_state != RaftObservationTcpBatchAcquisitionState::kRunning)
    return driven;

  std::size_t count{};
  auto wait = maximum_wait;
  const auto now = TimePoint::clock::now();
  for (const auto& pair : impl.pairs) {
    if (!is_running(pair))
      continue;
    if (const auto deadline = pair.wake_deadline(); deadline.has_value())
      wait = bounded_wait(wait, now, *deadline);
    const auto targets = pair.poll_targets();
    for (std::size_t index = 0U; index < targets.size; ++index) {
      const auto& target = targets.targets[index];
      impl.poll_descriptors[count++] = {
          .fd = target.descriptor,
          .events = static_cast<short>((target.interest.want_read ? POLLIN : 0) |
                                       (target.interest.want_write ? POLLOUT : 0))};
    }
  }
  const int ready = ::poll(impl.poll_descriptors.data(), static_cast<nfds_t>(count),
                           static_cast<int>(wait.count()));
  if (ready < 0 && errno != EINTR)
    return impl.fail(
        status(common::StatusCode::kIoError, "polling Raft observation batch acquisition failed"));
  return impl.drive_pairs();
}

common::Status RaftObservationTcpBatchAcquisition::cancel() {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument,
                  "Raft observation batch acquisition is empty");
  Impl& impl = *implementation_;
  if (impl.batch_state == RaftObservationTcpBatchAcquisitionState::kFailed ||
      impl.batch_state == RaftObservationTcpBatchAcquisitionState::kCancelled) {
    return impl.batch_failure;
  }
  if (impl.batch_state == RaftObservationTcpBatchAcquisitionState::kComplete) {
    return status(common::StatusCode::kInvalidArgument,
                  "completed Raft observation batch cannot be cancelled");
  }
  impl.cancel_running();
  impl.batch_failure =
      status(common::StatusCode::kCancelled, "Raft observation batch acquisition was cancelled");
  impl.batch_state = RaftObservationTcpBatchAcquisitionState::kCancelled;
  return impl.batch_failure;
}

RaftObservationTcpBatchAcquisitionState RaftObservationTcpBatchAcquisition::state() const noexcept {
  return implementation_ ? implementation_->batch_state
                         : RaftObservationTcpBatchAcquisitionState::kFailed;
}

RaftObservationTcpBatchAcquisitionMetrics
RaftObservationTcpBatchAcquisition::metrics() const noexcept {
  return implementation_ ? implementation_->batch_metrics
                         : RaftObservationTcpBatchAcquisitionMetrics{};
}

common::Result<std::vector<query::DistributedAggregateFollowerReadAuthority>>
RaftObservationTcpBatchAcquisition::result() const {
  if (!implementation_)
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft observation batch acquisition is empty"));
  if (implementation_->batch_state == RaftObservationTcpBatchAcquisitionState::kFailed ||
      implementation_->batch_state == RaftObservationTcpBatchAcquisitionState::kCancelled) {
    return common::make_unexpected(implementation_->batch_failure);
  }
  if (implementation_->batch_state != RaftObservationTcpBatchAcquisitionState::kComplete ||
      !implementation_->batch_result.has_value()) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft observation batch result is unavailable"));
  }
  try {
    return *implementation_->batch_result;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft observation batch result allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft observation batch result is too large"));
  }
}

const common::Status& RaftObservationTcpBatchAcquisition::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "Raft observation batch acquisition is empty"};
  return implementation_ ? implementation_->batch_failure : empty;
}

} // namespace chronos::cluster
