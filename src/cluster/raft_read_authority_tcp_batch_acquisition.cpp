#include "chronos/cluster/raft_read_authority_tcp_batch_acquisition.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
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

[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}

using TimePoint = RaftReadAuthorityTcpClient::TimePoint;

[[nodiscard]] std::chrono::milliseconds bounded_wait(const std::chrono::milliseconds maximum_wait,
                                                     const TimePoint now,
                                                     const TimePoint deadline) noexcept {
  if (deadline <= now)
    return std::chrono::milliseconds{0};
  return std::min(maximum_wait,
                  std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now));
}

[[nodiscard]] bool is_running(const RaftReadAuthorityTcpAcquisition& acquisition) noexcept {
  return acquisition.state() == RaftReadAuthorityTcpAcquisitionState::kRunning;
}

} // namespace

class RaftReadAuthorityTcpBatchAcquisition::Impl {
public:
  Impl(std::vector<RaftReadAuthorityTcpAcquisition> owned_acquisitions,
       std::vector<pollfd> descriptors)
      : acquisitions(std::move(owned_acquisitions)), poll_descriptors(std::move(descriptors)) {
    batch_metrics.total_groups = acquisitions.size();
    batch_metrics.active_groups = acquisitions.size();
  }

  void cancel_running() noexcept {
    std::size_t completed{};
    for (auto& acquisition : acquisitions) {
      if (is_running(acquisition))
        static_cast<void>(acquisition.cancel());
      completed += acquisition.state() == RaftReadAuthorityTcpAcquisitionState::kComplete ? 1U : 0U;
    }
    batch_metrics.completed_groups = completed;
    batch_metrics.active_groups = 0U;
  }

  [[nodiscard]] common::Status fail(common::Status failure) {
    cancel_running();
    batch_failure = std::move(failure);
    batch_state = RaftReadAuthorityTcpBatchAcquisitionState::kFailed;
    return batch_failure;
  }

  [[nodiscard]] common::Status drive_acquisitions() {
    std::size_t completed{};
    std::size_t active{};
    for (auto& acquisition : acquisitions) {
      if (is_running(acquisition)) {
        const common::Status progress = acquisition.poll_once(std::chrono::milliseconds{0});
        if (!progress.is_ok() &&
            acquisition.state() != RaftReadAuthorityTcpAcquisitionState::kFailed) {
          return fail(progress);
        }
      }
      if (acquisition.state() == RaftReadAuthorityTcpAcquisitionState::kFailed)
        return fail(acquisition.failure());
      completed += acquisition.state() == RaftReadAuthorityTcpAcquisitionState::kComplete ? 1U : 0U;
      active += is_running(acquisition) ? 1U : 0U;
    }
    batch_metrics.completed_groups = completed;
    batch_metrics.active_groups = active;
    if (completed != acquisitions.size())
      return common::Status::ok();

    try {
      std::vector<query::DistributedVectorGroupReadAuthority> authorities;
      authorities.reserve(acquisitions.size());
      for (const auto& acquisition : acquisitions) {
        auto authority = acquisition.result();
        if (!authority.has_value())
          return fail(authority.error());
        authorities.push_back(
            {.barrier = authority->barrier, .observation = std::move(authority->observation)});
      }
      batch_result.emplace(std::move(authorities));
      batch_state = RaftReadAuthorityTcpBatchAcquisitionState::kComplete;
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return fail(status(common::StatusCode::kResourceExhausted,
                         "Raft read-authority batch result allocation failed"));
    } catch (const std::length_error&) {
      return fail(status(common::StatusCode::kResourceExhausted,
                         "Raft read-authority batch result is too large"));
    }
  }

  std::vector<RaftReadAuthorityTcpAcquisition> acquisitions;
  std::vector<pollfd> poll_descriptors;
  RaftReadAuthorityTcpBatchAcquisitionMetrics batch_metrics;
  RaftReadAuthorityTcpBatchAcquisitionState batch_state{
      RaftReadAuthorityTcpBatchAcquisitionState::kRunning};
  std::optional<std::vector<query::DistributedVectorGroupReadAuthority>> batch_result;
  common::Status batch_failure{common::StatusCode::kInternal,
                               "Raft read-authority batch acquisition has not failed"};
};

RaftReadAuthorityTcpBatchAcquisition::RaftReadAuthorityTcpBatchAcquisition() noexcept = default;
RaftReadAuthorityTcpBatchAcquisition::RaftReadAuthorityTcpBatchAcquisition(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftReadAuthorityTcpBatchAcquisition::~RaftReadAuthorityTcpBatchAcquisition() = default;
RaftReadAuthorityTcpBatchAcquisition::RaftReadAuthorityTcpBatchAcquisition(
    RaftReadAuthorityTcpBatchAcquisition&&) noexcept = default;
RaftReadAuthorityTcpBatchAcquisition& RaftReadAuthorityTcpBatchAcquisition::operator=(
    RaftReadAuthorityTcpBatchAcquisition&&) noexcept = default;

common::Result<RaftReadAuthorityTcpBatchAcquisition>
RaftReadAuthorityTcpBatchAcquisition::create(RaftReadAuthorityTcpBatchAcquisitionConfig config) {
  if (config.maximum_groups == 0U ||
      config.maximum_groups > query::DistributedPlanLimits{}.maximum_fragments ||
      config.groups.empty()) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft read-authority batch configuration is invalid"));
  }
  if (config.groups.size() > config.maximum_groups) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft read-authority group limit is exhausted"));
  }
  if (!std::ranges::is_sorted(config.groups, {},
                              [](const auto& group) { return group.request.group_id; }) ||
      std::ranges::adjacent_find(config.groups, {}, [](const auto& group) {
        return group.request.group_id;
      }) != config.groups.end()) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft read-authority group batch is not canonical"));
  }

  const raft::NodeId source_node_id = config.groups.front().request.source_node_id;
  try {
    std::set<std::uint64_t> correlation_ids;
    for (const auto& group : config.groups) {
      if (group.request.source_node_id != source_node_id ||
          !correlation_ids.insert(group.request.correlation_id).second) {
        return common::make_unexpected(
            status(common::StatusCode::kInvalidArgument,
                   "Raft read-authority batch source or correlation is ambiguous"));
      }
    }

    std::vector<RaftReadAuthorityTcpAcquisition> acquisitions;
    acquisitions.reserve(config.groups.size());
    for (auto& group_config : config.groups) {
      auto acquisition = RaftReadAuthorityTcpAcquisition::create(std::move(group_config));
      if (!acquisition.has_value())
        return common::make_unexpected(acquisition.error());
      acquisitions.push_back(std::move(*acquisition));
    }
    std::vector<pollfd> descriptors(acquisitions.size());
    return RaftReadAuthorityTcpBatchAcquisition{
        std::make_unique<Impl>(std::move(acquisitions), std::move(descriptors))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft read-authority batch allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft read-authority batch exceeds container limits"));
  }
}

common::Status
RaftReadAuthorityTcpBatchAcquisition::poll_once(const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument,
                  "Raft read-authority batch acquisition is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return status(common::StatusCode::kInvalidArgument,
                  "Raft read-authority batch poll timeout is invalid");
  Impl& impl = *implementation_;
  if (impl.batch_state == RaftReadAuthorityTcpBatchAcquisitionState::kFailed ||
      impl.batch_state == RaftReadAuthorityTcpBatchAcquisitionState::kCancelled) {
    return impl.batch_failure;
  }
  if (impl.batch_state == RaftReadAuthorityTcpBatchAcquisitionState::kComplete)
    return common::Status::ok();

  common::Status driven = impl.drive_acquisitions();
  if (!driven.is_ok() || impl.batch_state != RaftReadAuthorityTcpBatchAcquisitionState::kRunning)
    return driven;

  std::size_t count{};
  auto wait = maximum_wait;
  const auto now = TimePoint::clock::now();
  for (const auto& acquisition : impl.acquisitions) {
    if (!is_running(acquisition))
      continue;
    if (const auto deadline = acquisition.wake_deadline(); deadline.has_value())
      wait = bounded_wait(wait, now, *deadline);
    const int descriptor = acquisition.descriptor();
    if (descriptor < 0)
      continue;
    const auto interest = acquisition.interest();
    impl.poll_descriptors[count++] = {.fd = descriptor,
                                      .events =
                                          static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                             (interest.want_write ? POLLOUT : 0)),
                                      .revents = 0};
  }
  const int ready = ::poll(impl.poll_descriptors.data(), static_cast<nfds_t>(count),
                           static_cast<int>(wait.count()));
  if (ready < 0 && errno != EINTR) {
    return impl.fail(status(common::StatusCode::kIoError,
                            "polling Raft read-authority batch acquisition failed"));
  }
  return impl.drive_acquisitions();
}

common::Status RaftReadAuthorityTcpBatchAcquisition::cancel() {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument,
                  "Raft read-authority batch acquisition is empty");
  Impl& impl = *implementation_;
  if (impl.batch_state == RaftReadAuthorityTcpBatchAcquisitionState::kFailed ||
      impl.batch_state == RaftReadAuthorityTcpBatchAcquisitionState::kCancelled) {
    return impl.batch_failure;
  }
  if (impl.batch_state == RaftReadAuthorityTcpBatchAcquisitionState::kComplete) {
    return status(common::StatusCode::kInvalidArgument,
                  "completed Raft read-authority batch cannot be cancelled");
  }
  impl.cancel_running();
  impl.batch_failure =
      status(common::StatusCode::kCancelled, "Raft read-authority batch acquisition was cancelled");
  impl.batch_state = RaftReadAuthorityTcpBatchAcquisitionState::kCancelled;
  return impl.batch_failure;
}

RaftReadAuthorityTcpBatchAcquisitionState
RaftReadAuthorityTcpBatchAcquisition::state() const noexcept {
  return implementation_ ? implementation_->batch_state
                         : RaftReadAuthorityTcpBatchAcquisitionState::kFailed;
}

RaftReadAuthorityTcpBatchAcquisitionMetrics
RaftReadAuthorityTcpBatchAcquisition::metrics() const noexcept {
  return implementation_ ? implementation_->batch_metrics
                         : RaftReadAuthorityTcpBatchAcquisitionMetrics{};
}

common::Result<std::vector<query::DistributedVectorGroupReadAuthority>>
RaftReadAuthorityTcpBatchAcquisition::result() const {
  if (!implementation_)
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft read-authority batch acquisition is empty"));
  const Impl& impl = *implementation_;
  if (impl.batch_state == RaftReadAuthorityTcpBatchAcquisitionState::kFailed ||
      impl.batch_state == RaftReadAuthorityTcpBatchAcquisitionState::kCancelled) {
    return common::make_unexpected(impl.batch_failure);
  }
  const std::optional<std::vector<query::DistributedVectorGroupReadAuthority>>& batch_result =
      impl.batch_result;
  if (impl.batch_state != RaftReadAuthorityTcpBatchAcquisitionState::kComplete ||
      !batch_result.has_value()) {
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft read-authority batch result is unavailable"));
  }
  try {
    return batch_result.value();
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft read-authority batch result allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft read-authority batch result is too large"));
  }
}

const common::Status& RaftReadAuthorityTcpBatchAcquisition::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "Raft read-authority batch acquisition is empty"};
  return implementation_ ? implementation_->batch_failure : empty;
}

} // namespace chronos::cluster
