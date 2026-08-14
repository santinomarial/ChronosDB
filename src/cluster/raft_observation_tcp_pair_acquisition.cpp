#include "chronos/cluster/raft_observation_tcp_pair_acquisition.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <new>
#include <optional>
#include <poll.h>
#include <stdexcept>
#include <utility>

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

[[nodiscard]] bool is_running(const RaftObservationTcpAcquisition& acquisition) noexcept {
  return acquisition.state() == RaftObservationTcpAcquisitionState::kRunning;
}

} // namespace

class RaftObservationTcpPairAcquisition::Impl {
public:
  Impl(RaftObservationTcpAcquisition leader_owner, RaftObservationTcpAcquisition follower_owner)
      : leader(std::move(leader_owner)), follower(std::move(follower_owner)) {}

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (is_running(leader))
      static_cast<void>(leader.cancel());
    if (is_running(follower))
      static_cast<void>(follower.cancel());
    pair_failure = std::move(failure);
    pair_state = RaftObservationTcpPairAcquisitionState::kFailed;
    return pair_failure;
  }

  [[nodiscard]] common::Status drive_children() {
    for (RaftObservationTcpAcquisition* acquisition : {&leader, &follower}) {
      if (!is_running(*acquisition))
        continue;
      const common::Status progress = acquisition->poll_once(std::chrono::milliseconds{0});
      if (!progress.is_ok() &&
          acquisition->state() != RaftObservationTcpAcquisitionState::kFailed) {
        return fail(progress);
      }
    }
    if (leader.state() == RaftObservationTcpAcquisitionState::kFailed)
      return fail(leader.failure());
    if (follower.state() == RaftObservationTcpAcquisitionState::kFailed)
      return fail(follower.failure());
    if (leader.state() != RaftObservationTcpAcquisitionState::kComplete ||
        follower.state() != RaftObservationTcpAcquisitionState::kComplete) {
      return common::Status::ok();
    }
    auto leader_result = leader.result();
    if (!leader_result.has_value())
      return fail(leader_result.error());
    auto follower_result = follower.result();
    if (!follower_result.has_value())
      return fail(follower_result.error());
    query::DistributedAggregateFollowerReadAuthority authority{std::move(*leader_result),
                                                               std::move(*follower_result)};
    if (!query::is_valid_distributed_aggregate_follower_read_authority(authority)) {
      return fail(status(common::StatusCode::kUnavailable,
                         "remote Raft observation pair is not stable and correlated"));
    }
    pair_result.emplace(std::move(authority));
    pair_state = RaftObservationTcpPairAcquisitionState::kComplete;
    return common::Status::ok();
  }

  RaftObservationTcpAcquisition leader;
  RaftObservationTcpAcquisition follower;
  RaftObservationTcpPairAcquisitionState pair_state{
      RaftObservationTcpPairAcquisitionState::kRunning};
  std::optional<query::DistributedAggregateFollowerReadAuthority> pair_result;
  common::Status pair_failure{common::StatusCode::kInternal,
                              "Raft observation pair acquisition has not failed"};
};

RaftObservationTcpPairAcquisition::RaftObservationTcpPairAcquisition() noexcept = default;
RaftObservationTcpPairAcquisition::RaftObservationTcpPairAcquisition(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftObservationTcpPairAcquisition::~RaftObservationTcpPairAcquisition() = default;
RaftObservationTcpPairAcquisition::RaftObservationTcpPairAcquisition(
    RaftObservationTcpPairAcquisition&&) noexcept = default;
RaftObservationTcpPairAcquisition& RaftObservationTcpPairAcquisition::operator=(
    RaftObservationTcpPairAcquisition&&) noexcept = default;

common::Result<RaftObservationTcpPairAcquisition>
RaftObservationTcpPairAcquisition::create(RaftObservationTcpPairAcquisitionConfig config) {
  if (config.leader.request.source_node_id != config.follower.request.source_node_id ||
      config.leader.request.target_node_id == config.follower.request.target_node_id ||
      config.leader.request.group_id != config.follower.request.group_id ||
      config.leader.request.correlation_id == config.follower.request.correlation_id) {
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument,
               "Raft observation pair requests are not independent and correlated"));
  }
  auto leader = RaftObservationTcpAcquisition::create(std::move(config.leader));
  if (!leader.has_value())
    return common::make_unexpected(leader.error());
  auto follower = RaftObservationTcpAcquisition::create(std::move(config.follower));
  if (!follower.has_value())
    return common::make_unexpected(follower.error());
  try {
    return RaftObservationTcpPairAcquisition{
        std::make_unique<Impl>(std::move(*leader), std::move(*follower))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft observation pair acquisition allocation failed"));
  }
}

common::Status
RaftObservationTcpPairAcquisition::poll_once(const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument,
                  "Raft observation pair acquisition is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return status(common::StatusCode::kInvalidArgument,
                  "Raft observation pair poll timeout is invalid");
  Impl& impl = *implementation_;
  if (impl.pair_state == RaftObservationTcpPairAcquisitionState::kFailed ||
      impl.pair_state == RaftObservationTcpPairAcquisitionState::kCancelled) {
    return impl.pair_failure;
  }
  if (impl.pair_state == RaftObservationTcpPairAcquisitionState::kComplete)
    return common::Status::ok();
  common::Status driven = impl.drive_children();
  if (!driven.is_ok() || impl.pair_state != RaftObservationTcpPairAcquisitionState::kRunning)
    return driven;

  std::array<pollfd, 2U> descriptors{};
  std::size_t count{};
  auto wait = maximum_wait;
  const auto now = TimePoint::clock::now();
  for (RaftObservationTcpAcquisition* acquisition : {&impl.leader, &impl.follower}) {
    if (!is_running(*acquisition))
      continue;
    if (const auto deadline = acquisition->wake_deadline(); deadline.has_value())
      wait = bounded_wait(wait, now, *deadline);
    if (acquisition->descriptor() < 0)
      continue;
    const auto interest = acquisition->interest();
    descriptors[count++] = {.fd = acquisition->descriptor(),
                            .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                         (interest.want_write ? POLLOUT : 0))};
  }
  const int ready =
      ::poll(descriptors.data(), static_cast<nfds_t>(count), static_cast<int>(wait.count()));
  if (ready < 0 && errno != EINTR)
    return impl.fail(
        status(common::StatusCode::kIoError, "polling Raft observation pair acquisition failed"));
  driven = impl.drive_children();
  return driven;
}

common::Status RaftObservationTcpPairAcquisition::cancel() {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument,
                  "Raft observation pair acquisition is empty");
  Impl& impl = *implementation_;
  if (impl.pair_state == RaftObservationTcpPairAcquisitionState::kFailed ||
      impl.pair_state == RaftObservationTcpPairAcquisitionState::kCancelled) {
    return impl.pair_failure;
  }
  if (impl.pair_state == RaftObservationTcpPairAcquisitionState::kComplete) {
    return status(common::StatusCode::kInvalidArgument,
                  "completed Raft observation pair acquisition cannot be cancelled");
  }
  if (is_running(impl.leader))
    static_cast<void>(impl.leader.cancel());
  if (is_running(impl.follower))
    static_cast<void>(impl.follower.cancel());
  impl.pair_failure =
      status(common::StatusCode::kCancelled, "Raft observation pair acquisition was cancelled");
  impl.pair_state = RaftObservationTcpPairAcquisitionState::kCancelled;
  return impl.pair_failure;
}

RaftObservationTcpPairAcquisitionState RaftObservationTcpPairAcquisition::state() const noexcept {
  return implementation_ ? implementation_->pair_state
                         : RaftObservationTcpPairAcquisitionState::kFailed;
}

RaftObservationTcpPairAcquisitionMetrics
RaftObservationTcpPairAcquisition::metrics() const noexcept {
  return implementation_
             ? RaftObservationTcpPairAcquisitionMetrics{implementation_->leader.metrics(),
                                                        implementation_->follower.metrics()}
             : RaftObservationTcpPairAcquisitionMetrics{};
}

RaftObservationTcpPairPollTargets RaftObservationTcpPairAcquisition::poll_targets() const noexcept {
  RaftObservationTcpPairPollTargets result;
  if (!implementation_ ||
      implementation_->pair_state != RaftObservationTcpPairAcquisitionState::kRunning) {
    return result;
  }
  for (const RaftObservationTcpAcquisition* acquisition :
       {&implementation_->leader, &implementation_->follower}) {
    if (!is_running(*acquisition) || acquisition->descriptor() < 0)
      continue;
    result.targets[result.size++] = {acquisition->descriptor(), acquisition->interest()};
  }
  return result;
}

std::optional<RaftObservationTcpClient::TimePoint>
RaftObservationTcpPairAcquisition::wake_deadline() const noexcept {
  if (!implementation_ ||
      implementation_->pair_state != RaftObservationTcpPairAcquisitionState::kRunning) {
    return std::nullopt;
  }
  const auto leader = implementation_->leader.wake_deadline();
  const auto follower = implementation_->follower.wake_deadline();
  if (!leader.has_value())
    return follower;
  if (!follower.has_value())
    return leader;
  return std::min(*leader, *follower);
}

common::Result<query::DistributedAggregateFollowerReadAuthority>
RaftObservationTcpPairAcquisition::result() const {
  if (!implementation_)
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "Raft observation pair acquisition is empty"));
  if (implementation_->pair_state == RaftObservationTcpPairAcquisitionState::kFailed ||
      implementation_->pair_state == RaftObservationTcpPairAcquisitionState::kCancelled) {
    return common::make_unexpected(implementation_->pair_failure);
  }
  if (implementation_->pair_state != RaftObservationTcpPairAcquisitionState::kComplete ||
      !implementation_->pair_result.has_value()) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft observation pair result is unavailable"));
  }
  try {
    return *implementation_->pair_result;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft observation pair result allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft observation pair result is too large"));
  }
}

const common::Status& RaftObservationTcpPairAcquisition::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "Raft observation pair acquisition is empty"};
  return implementation_ ? implementation_->pair_failure : empty;
}

} // namespace chronos::cluster
