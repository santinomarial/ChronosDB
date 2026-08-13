#include "chronos/service/replicated_read_barrier.hpp"

#include "chronos/service/replicated_raft_transport_runtime.hpp"

#include <algorithm>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <new>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <variant>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}

[[nodiscard]] common::Status validate_groups(std::vector<raft::GroupId>& groups,
                                             const ReplicatedReadBarrierLimits& limits) {
  constexpr std::chrono::milliseconds maximum_timeout{60'000};
  if (groups.empty() || groups.size() > limits.maximum_groups || limits.maximum_groups == 0U ||
      limits.request_timeout.count() <= 0 || limits.request_timeout > maximum_timeout)
    return status(common::StatusCode::kInvalidArgument,
                  "replicated read-barrier configuration is invalid");
  if (std::ranges::any_of(groups, &raft::GroupId::is_nil))
    return status(common::StatusCode::kInvalidArgument, "replicated read-barrier group is nil");
  std::ranges::sort(groups);
  if (std::ranges::adjacent_find(groups) != groups.end())
    return status(common::StatusCode::kAlreadyExists,
                  "replicated read-barrier group is duplicated");
  return common::Status::ok();
}

[[nodiscard]] common::Status
validate_ready(const raft::GroupReadBarrier& ready, const raft::GroupId& expected_group,
               const std::optional<raft::RaftGroupObservation>& observation = std::nullopt) {
  if (ready.group_id != expected_group || ready.barrier.term == 0U || ready.barrier.context == 0U ||
      ready.barrier.read_index == 0U)
    return status(common::StatusCode::kCorruption,
                  "replicated read-barrier completion identity is invalid");
  if (observation.has_value() &&
      (observation->group_id != expected_group || observation->node_id == 0U ||
       (observation->role != raft::Role::kFollower && observation->role != raft::Role::kCandidate &&
        observation->role != raft::Role::kLeader) ||
       observation->last_log_index < observation->commit_index ||
       observation->commit_index < observation->applied_index)) {
    return status(common::StatusCode::kCorruption,
                  "replicated read-barrier leader observation is invalid");
  }
  if (observation.has_value() &&
      (observation->role != raft::Role::kLeader || observation->leader_id != observation->node_id ||
       observation->current_term != ready.barrier.term ||
       observation->commit_index < ready.barrier.read_index))
    return status(common::StatusCode::kUnavailable,
                  "replicated read-barrier leader observation changed");
  return common::Status::ok();
}

} // namespace

class ReplicatedReadBarrier::Impl {
public:
  enum class Mode : std::uint8_t { kLocal = 1, kTransported = 2 };
  enum class Stage : std::uint8_t {
    kCommitPending = 1,
    kCommitInFlight = 2,
    kBarrierPending = 3,
    kBarrierInFlight = 4,
    kWaitingForQuorum = 5,
    kComplete = 6
  };
  struct GroupState {
    raft::GroupId group_id;
    Stage stage{Stage::kCommitPending};
    std::uint64_t submission_sequence{};
    raft::Term barrier_term{};
    std::uint64_t barrier_context{};
    std::optional<raft::GroupReadBarrier> ready;
    std::optional<raft::RaftGroupObservation> observation;
  };
  struct Request {
    std::chrono::steady_clock::time_point deadline;
    std::vector<GroupState> groups;
    common::Status result;
    bool capture_observations{};
    bool completed{};
  };

  Impl(Mode configured_mode, raft::AsyncDurableMultiRaftRuntime* configured_runtime,
       std::vector<raft::GroupId> configured_groups,
       const ReplicatedReadBarrierLimits configured_limits) noexcept
      : mode(configured_mode), runtime(configured_runtime), group_ids(std::move(configured_groups)),
        limits(configured_limits) {}

  [[nodiscard]] common::Result<std::vector<raft::GroupReadBarrier>>
  await_local(std::vector<raft::RaftGroupObservation>* const observations) {
    std::scoped_lock lock(mutex);
    if (!accepting)
      return common::make_unexpected(
          status(common::StatusCode::kUnavailable, "replicated read-barrier owner is stopped"));
    if (waiter_active)
      return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                            "replicated read-barrier waiter is already active"));
    waiter_active = true;
    struct WaiterGuard {
      bool& active;
      ~WaiterGuard() {
        active = false;
      }
    } guard{waiter_active};
    std::vector<raft::GroupReadBarrier> barriers;
    try {
      barriers.reserve(group_ids.size());
      if (observations != nullptr) {
        observations->clear();
        observations->reserve(group_ids.size());
      }
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                            "replicated read-barrier allocation failed"));
    }
    for (const raft::GroupId& group_id : group_ids) {
      std::vector<raft::DurableRaftRequest> requests;
      try {
        requests.reserve(observations == nullptr ? 2U : 3U);
        requests.emplace_back(group_id, raft::CommitCurrentTermOperation{});
        requests.emplace_back(group_id, raft::BeginReadBarrierOperation{});
        if (observations != nullptr)
          requests.emplace_back(group_id, raft::ObserveGroupOperation{});
      } catch (const std::bad_alloc&) {
        return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                              "replicated read-barrier allocation failed"));
      }
      auto completion = runtime->try_submit(std::move(requests));
      if (!completion.has_value())
        return common::make_unexpected(completion.error());
      auto results = completion->wait();
      if (!results.has_value())
        return common::make_unexpected(results.error());
      const std::size_t expected_results = observations == nullptr ? 2U : 3U;
      if (results->size() != expected_results)
        return common::make_unexpected(
            status(common::StatusCode::kCorruption, "local read-barrier result count is invalid"));
      if (!(*results)[0].status.is_ok())
        return common::make_unexpected((*results)[0].status);
      if (!(*results)[1].status.is_ok())
        return common::make_unexpected((*results)[1].status);
      if (!(*results)[1].transition.has_value() ||
          !(*results)[1].transition->read_barrier_ready.has_value())
        return common::make_unexpected(
            status(common::StatusCode::kUnavailable,
                   "local read barrier requires an immediately confirmed group quorum"));
      const raft::GroupReadBarrier& ready = *(*results)[1].transition->read_barrier_ready;
      if (observations != nullptr &&
          (!(*results)[2].status.is_ok() || (*results)[2].transition.has_value() ||
           !(*results)[2].observation.has_value())) {
        return common::make_unexpected(
            status(common::StatusCode::kCorruption,
                   "local read-barrier batch lacks its ordered leader observation"));
      }
      const common::Status valid = observations == nullptr
                                       ? validate_ready(ready, group_id)
                                       : validate_ready(ready, group_id, (*results)[2].observation);
      if (!valid.is_ok())
        return common::make_unexpected(valid);
      barriers.push_back(ready);
      if (observations != nullptr)
        observations->push_back(std::move(*(*results)[2].observation));
    }
    return barriers;
  }

  [[nodiscard]] common::Result<std::vector<raft::GroupReadBarrier>>
  await_transported(std::vector<raft::RaftGroupObservation>* const observations) {
    std::unique_lock lock(mutex);
    if (!accepting)
      return common::make_unexpected(
          status(common::StatusCode::kUnavailable, "replicated read-barrier owner is stopped"));
    if (request.has_value())
      return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                            "replicated read-barrier waiter is already active"));
    waiter_active = true;
    struct WaiterGuard {
      Impl& owner;
      ~WaiterGuard() {
        owner.waiter_active = false;
        owner.condition.notify_all();
      }
    } guard{*this};
    try {
      Request next{.deadline = std::chrono::steady_clock::now() + limits.request_timeout,
                   .capture_observations = observations != nullptr};
      next.groups.reserve(group_ids.size());
      for (const raft::GroupId& group_id : group_ids)
        next.groups.push_back({.group_id = group_id});
      request.emplace(std::move(next));
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                            "replicated read-barrier allocation failed"));
    }
    const auto deadline = request->deadline;
    condition.wait_until(lock, deadline,
                         [&] { return !accepting || (request && request->completed); });
    if (!accepting) {
      request.reset();
      return common::make_unexpected(
          status(common::StatusCode::kUnavailable, "replicated read-barrier owner stopped"));
    }
    if (!request->completed) {
      request.reset();
      return common::make_unexpected(
          status(common::StatusCode::kUnavailable, "replicated read-barrier request timed out"));
    }
    if (!request->result.is_ok()) {
      common::Status failure = request->result;
      request.reset();
      return common::make_unexpected(std::move(failure));
    }
    std::vector<raft::GroupReadBarrier> barriers;
    try {
      barriers.reserve(request->groups.size());
      if (observations != nullptr) {
        observations->clear();
        observations->reserve(request->groups.size());
      }
      for (GroupState& group : request->groups) {
        if (!group.ready.has_value() ||
            (observations != nullptr && !group.observation.has_value())) {
          request.reset();
          return common::make_unexpected(status(common::StatusCode::kCorruption,
                                                "replicated read-barrier vector is incomplete"));
        }
        barriers.push_back(*group.ready);
        if (observations != nullptr)
          observations->push_back(std::move(*group.observation));
      }
    } catch (const std::bad_alloc&) {
      request.reset();
      return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                            "replicated read-barrier allocation failed"));
    }
    request.reset();
    return barriers;
  }

  void finish(common::Status result) {
    if (!request.has_value() || request->completed)
      return;
    request->result = std::move(result);
    request->completed = true;
    condition.notify_one();
  }

  void finish_if_complete() {
    if (request.has_value() && !request->completed &&
        std::ranges::all_of(request->groups,
                            [](const GroupState& group) { return group.ready.has_value(); }))
      finish(common::Status::ok());
  }

  void accept_ready(GroupState& group, const raft::GroupReadBarrier& ready,
                    const raft::RaftGroupObservation& observation) {
    if (!request.has_value() || request->completed)
      return;
    if (request->capture_observations) {
      try {
        group.observation.emplace(observation);
      } catch (const std::bad_alloc&) {
        finish(status(common::StatusCode::kResourceExhausted,
                      "replicated read-barrier observation allocation failed"));
        return;
      } catch (const std::length_error&) {
        finish(status(common::StatusCode::kResourceExhausted,
                      "replicated read-barrier observation exceeds limits"));
        return;
      }
    }
    group.ready = ready;
    group.stage = Stage::kComplete;
    finish_if_complete();
  }

  Mode mode;
  raft::AsyncDurableMultiRaftRuntime* runtime{};
  std::vector<raft::GroupId> group_ids;
  ReplicatedReadBarrierLimits limits;
  mutable std::mutex mutex;
  std::condition_variable condition;
  std::optional<Request> request;
  bool accepting{true};
  bool waiter_active{};
};

ReplicatedReadBarrier::ReplicatedReadBarrier(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
ReplicatedReadBarrier::~ReplicatedReadBarrier() {
  if (impl_ != nullptr)
    static_cast<void>(shutdown());
}
ReplicatedReadBarrier::ReplicatedReadBarrier(ReplicatedReadBarrier&&) noexcept = default;
ReplicatedReadBarrier& ReplicatedReadBarrier::operator=(ReplicatedReadBarrier&&) noexcept = default;

common::Result<ReplicatedReadBarrier>
ReplicatedReadBarrier::create_local(raft::AsyncDurableMultiRaftRuntime* const runtime,
                                    std::vector<raft::GroupId> groups,
                                    const ReplicatedReadBarrierLimits limits) {
  const common::Status valid = validate_groups(groups, limits);
  if (!valid.is_ok() || runtime == nullptr)
    return common::make_unexpected(valid.is_ok() ? status(common::StatusCode::kInvalidArgument,
                                                          "local read-barrier runtime is null")
                                                 : valid);
  try {
    return ReplicatedReadBarrier{
        std::make_unique<Impl>(Impl::Mode::kLocal, runtime, std::move(groups), limits)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "replicated read-barrier owner allocation failed"));
  }
}

common::Result<ReplicatedReadBarrier>
ReplicatedReadBarrier::create_transported(std::vector<raft::GroupId> groups,
                                          const ReplicatedReadBarrierLimits limits) {
  const common::Status valid = validate_groups(groups, limits);
  if (!valid.is_ok())
    return common::make_unexpected(valid);
  try {
    return ReplicatedReadBarrier{
        std::make_unique<Impl>(Impl::Mode::kTransported, nullptr, std::move(groups), limits)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "replicated read-barrier owner allocation failed"));
  }
}

common::Result<std::vector<raft::GroupReadBarrier>> ReplicatedReadBarrier::await() {
  if (impl_ == nullptr)
    return common::make_unexpected(
        status(common::StatusCode::kUnavailable, "replicated read-barrier owner was moved from"));
  return impl_->mode == Impl::Mode::kLocal ? impl_->await_local(nullptr)
                                           : impl_->await_transported(nullptr);
}

common::Result<std::vector<ReplicatedReadAuthority>> ReplicatedReadBarrier::await_authority() {
  if (impl_ == nullptr)
    return common::make_unexpected(
        status(common::StatusCode::kUnavailable, "replicated read-barrier owner was moved from"));
  std::vector<raft::RaftGroupObservation> observations;
  auto barriers = impl_->mode == Impl::Mode::kLocal ? impl_->await_local(&observations)
                                                    : impl_->await_transported(&observations);
  if (!barriers.has_value())
    return common::make_unexpected(barriers.error());
  if (barriers->size() != observations.size())
    return common::make_unexpected(
        status(common::StatusCode::kCorruption, "replicated read authority vector is incomplete"));
  try {
    std::vector<ReplicatedReadAuthority> authority;
    authority.reserve(barriers->size());
    for (std::size_t index = 0U; index < barriers->size(); ++index) {
      if ((*barriers)[index].group_id != observations[index].group_id)
        return common::make_unexpected(status(common::StatusCode::kCorruption,
                                              "replicated read authority group order differs"));
      authority.push_back({std::move((*barriers)[index]), std::move(observations[index])});
    }
    return authority;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "replicated read authority allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted, "replicated read authority exceeds limits"));
  }
}

common::Status ReplicatedReadBarrier::poll_owner_drive(ReplicatedRaftTransportRuntime& transport) {
  if (impl_ == nullptr || impl_->mode != Impl::Mode::kTransported)
    return status(common::StatusCode::kInvalidArgument,
                  "replicated read-barrier transport drive is unavailable");
  std::scoped_lock lock(impl_->mutex);
  if (!impl_->accepting || !impl_->request.has_value() || impl_->request->completed)
    return common::Status::ok();
  if (std::chrono::steady_clock::now() >= impl_->request->deadline) {
    impl_->finish(
        status(common::StatusCode::kUnavailable, "replicated read-barrier request timed out"));
    return common::Status::ok();
  }
  for (Impl::GroupState& group : impl_->request->groups) {
    raft::DurableRaftOperation operation;
    if (group.stage == Impl::Stage::kCommitPending)
      operation = raft::CommitCurrentTermOperation{};
    else if (group.stage == Impl::Stage::kBarrierPending)
      operation = raft::BeginReadBarrierOperation{};
    else
      continue;
    auto submitted = transport.try_submit_application({group.group_id, std::move(operation)});
    if (!submitted.has_value()) {
      if (submitted.error().code() == common::StatusCode::kResourceExhausted)
        return common::Status::ok();
      impl_->finish(submitted.error());
      return common::Status::ok();
    }
    group.submission_sequence = *submitted;
    group.stage = group.stage == Impl::Stage::kCommitPending ? Impl::Stage::kCommitInFlight
                                                             : Impl::Stage::kBarrierInFlight;
  }
  return common::Status::ok();
}

common::Status
ReplicatedReadBarrier::poll_owner_observe(const cluster::RaftTransportRuntimeResult& result) {
  if (impl_ == nullptr || impl_->mode != Impl::Mode::kTransported)
    return status(common::StatusCode::kInvalidArgument,
                  "replicated read-barrier transport observation is unavailable");
  std::scoped_lock lock(impl_->mutex);
  if (!impl_->accepting || !impl_->request.has_value() || impl_->request->completed)
    return common::Status::ok();
  auto found =
      std::ranges::find(impl_->request->groups, result.group_id, &Impl::GroupState::group_id);
  if (found == impl_->request->groups.end())
    return common::Status::ok();

  if (result.origin == cluster::RaftTransportRuntimeResultOrigin::kApplication &&
      result.submission_sequence == found->submission_sequence &&
      (found->stage == Impl::Stage::kCommitInFlight ||
       found->stage == Impl::Stage::kBarrierInFlight)) {
    const bool committing = found->stage == Impl::Stage::kCommitInFlight;
    found->submission_sequence = 0U;
    if (!result.result.status.is_ok()) {
      if (result.result.status.code() == common::StatusCode::kUnavailable) {
        found->stage = committing ? Impl::Stage::kCommitPending : Impl::Stage::kBarrierPending;
        return common::Status::ok();
      }
      impl_->finish(result.result.status);
      return common::Status::ok();
    }
    if (!result.result.transition.has_value()) {
      impl_->finish(status(common::StatusCode::kCorruption,
                           "replicated read-barrier application transition is absent"));
      return common::Status::ok();
    }
    if (committing) {
      found->stage = Impl::Stage::kBarrierPending;
      return common::Status::ok();
    }
    const raft::MultiRaftTransition& transition = *result.result.transition;
    if (transition.read_barrier_ready.has_value()) {
      if (!result.observation.has_value()) {
        impl_->finish(status(common::StatusCode::kCorruption,
                             "replicated read-barrier leader observation is absent"));
        return common::Status::ok();
      }
      const common::Status valid =
          validate_ready(*transition.read_barrier_ready, found->group_id, result.observation);
      if (!valid.is_ok()) {
        impl_->finish(valid);
        return common::Status::ok();
      }
      impl_->accept_ready(*found, *transition.read_barrier_ready, *result.observation);
      return common::Status::ok();
    }
    std::optional<raft::ReadBarrierRequest> identity;
    for (const raft::GroupOutboundMessage& outbound : transition.outbound) {
      const auto* request = std::get_if<raft::ReadBarrierRequest>(&outbound.outbound.message);
      if (request == nullptr)
        continue;
      if (!identity.has_value())
        identity = *request;
      else if (request->term != identity->term || request->context != identity->context) {
        impl_->finish(status(common::StatusCode::kCorruption,
                             "replicated read-barrier outbound identity is inconsistent"));
        return common::Status::ok();
      }
    }
    if (!identity.has_value() || identity->term == 0U || identity->context == 0U ||
        !result.observation.has_value() || result.observation->role != raft::Role::kLeader ||
        result.observation->current_term != identity->term) {
      impl_->finish(status(common::StatusCode::kCorruption,
                           "replicated read-barrier outbound identity is absent"));
      return common::Status::ok();
    }
    found->barrier_term = identity->term;
    found->barrier_context = identity->context;
    found->stage = Impl::Stage::kWaitingForQuorum;
    return common::Status::ok();
  }

  if (!result.result.status.is_ok() || !result.result.transition.has_value() ||
      !result.result.transition->read_barrier_ready.has_value() ||
      found->stage != Impl::Stage::kWaitingForQuorum)
    return common::Status::ok();
  const raft::GroupReadBarrier& ready = *result.result.transition->read_barrier_ready;
  if (ready.barrier.term != found->barrier_term || ready.barrier.context != found->barrier_context)
    return common::Status::ok();
  if (!result.observation.has_value()) {
    impl_->finish(status(common::StatusCode::kCorruption,
                         "replicated read-barrier leader observation is absent"));
    return common::Status::ok();
  }
  const common::Status valid = validate_ready(ready, found->group_id, result.observation);
  if (!valid.is_ok()) {
    impl_->finish(valid);
    return common::Status::ok();
  }
  impl_->accept_ready(*found, ready, *result.observation);
  return common::Status::ok();
}

common::Status ReplicatedReadBarrier::shutdown() {
  if (impl_ == nullptr)
    return common::Status::ok();
  std::unique_lock lock(impl_->mutex);
  impl_->accepting = false;
  impl_->condition.notify_all();
  impl_->condition.wait(lock, [&] { return !impl_->waiter_active; });
  return common::Status::ok();
}

std::span<const raft::GroupId> ReplicatedReadBarrier::groups() const noexcept {
  return impl_ == nullptr ? std::span<const raft::GroupId>{} : impl_->group_ids;
}

} // namespace chronos::service
