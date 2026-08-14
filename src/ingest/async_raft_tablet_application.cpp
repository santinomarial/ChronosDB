#include "chronos/ingest/async_raft_tablet_application.hpp"

#include <algorithm>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

using QuorumResult = common::Result<raft::QuorumSyncReceipt>;

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status unavailable(std::string message) {
  return {common::StatusCode::kUnavailable, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}

class TabletApplicationBatchContext final : public raft::AsyncDurableRaftWorkerBatchContext {
public:
  TabletApplicationBatchContext(const std::size_t requests,
                                std::vector<raft::GroupId> configured_groups) noexcept
      : group_ids(std::move(configured_groups)), request_count(requests) {}

  std::vector<raft::GroupId> group_ids;
  std::size_t request_count;
};

} // namespace

namespace detail {

class AsyncRaftTabletQuorumCompletionState {
public:
  void complete(QuorumResult result) {
    {
      const std::lock_guard lock{mutex_};
      if (result_.has_value())
        return;
      result_.emplace(std::move(result));
    }
    condition_.notify_all();
  }

  [[nodiscard]] bool is_ready() const {
    const std::lock_guard lock{mutex_};
    return result_.has_value() && !consumed_;
  }

  [[nodiscard]] QuorumResult wait() {
    std::unique_lock lock{mutex_};
    condition_.wait(lock, [this] { return result_.has_value(); });
    if (consumed_) {
      return common::make_unexpected(invalid("Raft tablet quorum completion was already consumed"));
    }
    std::optional<QuorumResult> result = std::move(result_);
    if (!result.has_value()) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "ready Raft tablet quorum completion has no result"});
    }
    consumed_ = true;
    return std::move(result).value();
  }

private:
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::optional<QuorumResult> result_;
  bool consumed_{};
};

} // namespace detail

class AsyncRaftTabletApplication::Impl {
public:
  struct PendingQuorumCompletion {
    raft::Term leader_term{};
    raft::LogIndex log_index{};
    std::weak_ptr<detail::AsyncRaftTabletQuorumCompletionState> state;
    bool resolved{};
  };

  struct ReadyQuorumCompletion {
    std::shared_ptr<detail::AsyncRaftTabletQuorumCompletionState> state;
    QuorumResult result;
  };

  struct OwnedTablet {
    raft::GroupId group_id;
    RaftTabletStateMachine machine;
    std::optional<raft::QuorumSyncReceipt> latest_receipt;
    std::vector<PendingQuorumCompletion> pending_receipts;
  };

  Impl(std::vector<AsyncRaftTabletApplicationConfig> configured,
       const AsyncRaftTabletApplicationLimits configured_limits) noexcept
      : pending(std::move(configured)), limits(configured_limits) {}

  void complete_pending(const common::Status& status) {
    for (OwnedTablet& owned : tablets) {
      for (const PendingQuorumCompletion& pending_receipt : owned.pending_receipts)
        if (auto state = pending_receipt.state.lock(); state != nullptr)
          state->complete(common::make_unexpected(status));
      pending_quorum_count -= owned.pending_receipts.size();
      owned.pending_receipts.clear();
    }
  }

  [[nodiscard]] common::Status fail(common::Status status) {
    if (failure.is_ok()) {
      failure = std::move(status);
      complete_pending(failure);
    }
    return failure;
  }

  void prune_expired_receipts() {
    for (OwnedTablet& owned : tablets) {
      const std::size_t prior = owned.pending_receipts.size();
      std::erase_if(owned.pending_receipts, [](const PendingQuorumCompletion& pending_receipt) {
        return pending_receipt.state.expired();
      });
      pending_quorum_count -= prior - owned.pending_receipts.size();
    }
  }

  [[nodiscard]] common::Status initialize(raft::DurableMultiRaftRuntime& runtime) {
    std::lock_guard lock{mutex};
    if (initialized || shutdown)
      return fail(invalid("Raft tablet application initialization state is invalid"));
    try {
      tablets.reserve(pending.size());
      for (AsyncRaftTabletApplicationConfig& config : pending) {
        common::Result<RaftTabletStateMachine> recovered =
            config.snapshot_storage.has_value()
                ? RaftTabletStateMachine::recover(
                      config.group_id, runtime, std::move(*config.snapshot_storage),
                      std::move(config.retry_directory), std::move(config.tablet),
                      std::move(config.retained_schemas), config.decode_limits)
                : RaftTabletStateMachine::recover(
                      config.group_id, runtime, std::move(config.retry_directory),
                      std::move(config.tablet), std::move(config.retained_schemas),
                      config.decode_limits);
        if (!recovered.has_value())
          return fail(recovered.error());
        tablets.push_back(OwnedTablet{config.group_id, std::move(*recovered), std::nullopt, {}});
      }
      pending.clear();
      initialized = true;
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return fail(exhausted("Raft tablet application recovery allocation failed"));
    } catch (const std::length_error&) {
      return fail(exhausted("Raft tablet application recovery exceeds container limits"));
    }
  }

  [[nodiscard]] common::Status apply_committed(raft::DurableMultiRaftRuntime& runtime,
                                               const std::span<const raft::GroupId> group_ids) {
    std::lock_guard lock{mutex};
    if (!failure.is_ok())
      return failure;
    if (!initialized || shutdown)
      return fail(unavailable("Raft tablet application owner is not active"));
    try {
      std::vector<ReadyQuorumCompletion> ready;
      for (const raft::GroupId& group_id : group_ids) {
        const auto found = std::ranges::lower_bound(tablets, group_id, {}, &OwnedTablet::group_id);
        if (found == tablets.end() || found->group_id != group_id)
          continue;
        OwnedTablet& owned = *found;
        auto applied = owned.machine.apply_committed();
        if (!applied.has_value())
          return fail(applied.error());
        const raft::RaftNode* const node = runtime.find_group(owned.group_id);
        if (node == nullptr)
          return fail(unavailable("Raft tablet application group disappeared"));
        for (PendingQuorumCompletion& pending_receipt : owned.pending_receipts) {
          auto state = pending_receipt.state.lock();
          if (state == nullptr)
            continue;
          if (node->role() != raft::Role::kLeader ||
              node->current_term() != pending_receipt.leader_term) {
            ready.push_back({std::move(state),
                             common::make_unexpected(
                                 unavailable("Raft tablet quorum request lost its leader term"))});
            pending_receipt.resolved = true;
            continue;
          }
          if (pending_receipt.log_index > node->applied_index())
            continue;
          auto exact = owned.machine.prove_applied_quorum_sync(pending_receipt.log_index);
          if (!exact.has_value())
            return fail(exact.error());
          ready.push_back({std::move(state), exact.value()});
          pending_receipt.resolved = true;
        }
        if (node->role() != raft::Role::kLeader || node->applied_index() == 0U)
          continue;
        auto receipt = owned.machine.prove_applied_quorum_sync(node->applied_index());
        if (!receipt.has_value())
          return fail(receipt.error());
        owned.latest_receipt = *receipt;
      }
      for (OwnedTablet& owned : tablets) {
        const std::size_t prior = owned.pending_receipts.size();
        std::erase_if(owned.pending_receipts, [](const PendingQuorumCompletion& pending_receipt) {
          return pending_receipt.resolved || pending_receipt.state.expired();
        });
        pending_quorum_count -= prior - owned.pending_receipts.size();
      }
      for (ReadyQuorumCompletion& completion : ready)
        completion.state->complete(std::move(completion.result));
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return fail(exhausted("Raft tablet quorum resolution allocation failed"));
    } catch (const std::length_error&) {
      return fail(exhausted("Raft tablet quorum resolution exceeds container limits"));
    }
  }

  mutable std::mutex mutex;
  std::vector<AsyncRaftTabletApplicationConfig> pending;
  std::vector<OwnedTablet> tablets;
  AsyncRaftTabletApplicationLimits limits;
  std::size_t pending_quorum_count{};
  common::Status failure;
  bool initialized{};
  bool shutdown{};
};

AsyncRaftTabletApplication::AsyncRaftTabletApplication(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
AsyncRaftTabletApplication::~AsyncRaftTabletApplication() = default;

common::Result<std::shared_ptr<AsyncRaftTabletApplication>>
AsyncRaftTabletApplication::create(std::vector<AsyncRaftTabletApplicationConfig> tablets,
                                   const AsyncRaftTabletApplicationLimits limits) {
  if (limits.maximum_tablets == 0U || limits.maximum_tablets > 65'536U ||
      limits.maximum_pending_quorum_completions == 0U ||
      limits.maximum_pending_quorum_completions > 1'048'576U || tablets.empty() ||
      tablets.size() > limits.maximum_tablets)
    return common::make_unexpected(invalid("Raft tablet application limits are invalid"));
  for (const AsyncRaftTabletApplicationConfig& tablet : tablets)
    if (tablet.group_id.is_nil())
      return common::make_unexpected(invalid("Raft tablet application group is nil"));
  try {
    std::ranges::sort(tablets, {}, &AsyncRaftTabletApplicationConfig::group_id);
    if (std::ranges::adjacent_find(tablets, {}, &AsyncRaftTabletApplicationConfig::group_id) !=
        tablets.end())
      return common::make_unexpected(invalid("Raft tablet application group is duplicated"));
    return std::shared_ptr<AsyncRaftTabletApplication>{
        new AsyncRaftTabletApplication{std::make_unique<Impl>(std::move(tablets), limits)}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft tablet application allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("Raft tablet application configuration exceeds container limits"));
  }
}

common::Result<TabletSnapshot>
AsyncRaftTabletApplication::snapshot(const raft::GroupId& group_id) const {
  std::lock_guard lock{impl_->mutex};
  if (!impl_->failure.is_ok())
    return common::make_unexpected(impl_->failure);
  const auto found = std::ranges::find(impl_->tablets, group_id, &Impl::OwnedTablet::group_id);
  if (!impl_->initialized || impl_->shutdown || found == impl_->tablets.end())
    return common::make_unexpected(unavailable("Raft tablet application snapshot is unavailable"));
  return found->machine.tablet().snapshot();
}

std::optional<raft::QuorumSyncReceipt>
AsyncRaftTabletApplication::latest_quorum_sync_receipt(const raft::GroupId& group_id) const {
  std::lock_guard lock{impl_->mutex};
  const auto found = std::ranges::find(impl_->tablets, group_id, &Impl::OwnedTablet::group_id);
  return !impl_->failure.is_ok() || !impl_->initialized || impl_->shutdown ||
                 found == impl_->tablets.end()
             ? std::nullopt
             : found->latest_receipt;
}

common::Result<AsyncRaftTabletQuorumCompletion> AsyncRaftTabletApplication::request_quorum_sync(
    raft::AsyncDurableMultiRaftRuntime& runtime, const raft::GroupId& group_id,
    const raft::Term required_leader_term, const raft::LogIndex log_index) {
  if (required_leader_term == 0U || log_index == 0U)
    return common::make_unexpected(invalid("Raft tablet quorum term or index is zero"));
  if (!runtime.owns_worker_extension(*this)) {
    return common::make_unexpected(
        invalid("Raft tablet quorum request uses a different asynchronous owner"));
  }
  std::shared_ptr<detail::AsyncRaftTabletQuorumCompletionState> state;
  {
    std::lock_guard lock{impl_->mutex};
    if (!impl_->failure.is_ok())
      return common::make_unexpected(impl_->failure);
    if (!impl_->initialized || impl_->shutdown)
      return common::make_unexpected(unavailable("Raft tablet application owner is not active"));
    impl_->prune_expired_receipts();
    const auto found =
        std::ranges::lower_bound(impl_->tablets, group_id, {}, &Impl::OwnedTablet::group_id);
    if (found == impl_->tablets.end() || found->group_id != group_id)
      return common::make_unexpected(invalid("Raft tablet quorum group is not configured"));
    if (impl_->pending_quorum_count >= impl_->limits.maximum_pending_quorum_completions)
      return common::make_unexpected(exhausted("Raft tablet quorum completion capacity is full"));
    try {
      state = std::make_shared<detail::AsyncRaftTabletQuorumCompletionState>();
      found->pending_receipts.push_back({required_leader_term, log_index, state, false});
      ++impl_->pending_quorum_count;
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("Raft tablet quorum completion allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(
          exhausted("Raft tablet quorum completion exceeds container limits"));
    }
  }
  auto observation = runtime.try_observe_group(group_id);
  if (!observation.has_value()) {
    state->complete(common::make_unexpected(observation.error()));
    return common::make_unexpected(observation.error());
  }
  return AsyncRaftTabletQuorumCompletion{
      std::move(state),
      AsyncRaftTabletQuorumCompletion::Identity{
          .group_id = group_id, .leader_term = required_leader_term, .log_index = log_index}};
}

std::size_t AsyncRaftTabletApplication::tablet_count() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->initialized && !impl_->shutdown ? impl_->tablets.size() : 0U;
}

std::size_t AsyncRaftTabletApplication::pending_quorum_completions() const {
  std::lock_guard lock{impl_->mutex};
  impl_->prune_expired_receipts();
  return impl_->pending_quorum_count;
}

bool AsyncRaftTabletApplication::initialized() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->initialized;
}

bool AsyncRaftTabletApplication::failed() const {
  std::lock_guard lock{impl_->mutex};
  return !impl_->failure.is_ok();
}

common::Status AsyncRaftTabletApplication::failure_status() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->failure;
}

common::Status AsyncRaftTabletApplication::initialize(raft::DurableMultiRaftRuntime& runtime) {
  return impl_->initialize(runtime);
}

common::Result<std::unique_ptr<raft::AsyncDurableRaftWorkerBatchContext>>
AsyncRaftTabletApplication::prepare_batch(
    raft::DurableMultiRaftRuntime&, const std::span<const raft::DurableRaftRequest> requests) {
  std::lock_guard lock{impl_->mutex};
  if (!impl_->failure.is_ok())
    return common::make_unexpected(impl_->failure);
  if (!impl_->initialized || impl_->shutdown)
    return common::make_unexpected(unavailable("Raft tablet application owner is not active"));
  try {
    std::vector<raft::GroupId> group_ids;
    group_ids.reserve(requests.size());
    for (const raft::DurableRaftRequest& request : requests)
      group_ids.push_back(request.group_id);
    std::ranges::sort(group_ids);
    group_ids.erase(std::ranges::unique(group_ids).begin(), group_ids.end());
    return std::unique_ptr<raft::AsyncDurableRaftWorkerBatchContext>{
        std::make_unique<TabletApplicationBatchContext>(requests.size(), std::move(group_ids))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft tablet application batch allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("Raft tablet application batch exceeds container limits"));
  }
}

common::Status AsyncRaftTabletApplication::complete_batch(
    raft::DurableMultiRaftRuntime& runtime,
    std::unique_ptr<raft::AsyncDurableRaftWorkerBatchContext> context,
    const std::span<const raft::DurableRaftResult> results) {
  const auto* const batch = dynamic_cast<const TabletApplicationBatchContext*>(context.get());
  if (batch == nullptr || batch->request_count != results.size()) {
    std::lock_guard lock{impl_->mutex};
    return impl_->fail(common::Status{common::StatusCode::kCorruption,
                                      "Raft tablet application batch context changed"});
  }
  return impl_->apply_committed(runtime, batch->group_ids);
}

common::Status AsyncRaftTabletApplication::shutdown(raft::DurableMultiRaftRuntime&) {
  std::lock_guard lock{impl_->mutex};
  impl_->shutdown = true;
  impl_->complete_pending(impl_->failure.is_ok()
                              ? unavailable("Raft tablet application owner shut down")
                              : impl_->failure);
  impl_->tablets.clear();
  impl_->pending.clear();
  return impl_->failure;
}

AsyncRaftTabletQuorumCompletion::AsyncRaftTabletQuorumCompletion() noexcept = default;
AsyncRaftTabletQuorumCompletion::~AsyncRaftTabletQuorumCompletion() = default;
AsyncRaftTabletQuorumCompletion::AsyncRaftTabletQuorumCompletion(
    AsyncRaftTabletQuorumCompletion&&) noexcept = default;
AsyncRaftTabletQuorumCompletion&
AsyncRaftTabletQuorumCompletion::operator=(AsyncRaftTabletQuorumCompletion&&) noexcept = default;

AsyncRaftTabletQuorumCompletion::AsyncRaftTabletQuorumCompletion(
    std::shared_ptr<detail::AsyncRaftTabletQuorumCompletionState> state,
    const Identity identity) noexcept
    : state_(std::move(state)), group_id_(identity.group_id), leader_term_(identity.leader_term),
      log_index_(identity.log_index) {}

bool AsyncRaftTabletQuorumCompletion::is_valid() const noexcept {
  return state_ != nullptr;
}

bool AsyncRaftTabletQuorumCompletion::is_ready() const {
  return state_ != nullptr && state_->is_ready();
}

const raft::GroupId& AsyncRaftTabletQuorumCompletion::group_id() const noexcept {
  return group_id_;
}

raft::Term AsyncRaftTabletQuorumCompletion::leader_term() const noexcept {
  return leader_term_;
}

raft::LogIndex AsyncRaftTabletQuorumCompletion::log_index() const noexcept {
  return log_index_;
}

QuorumResult AsyncRaftTabletQuorumCompletion::wait() {
  if (state_ == nullptr)
    return common::make_unexpected(invalid("Raft tablet quorum completion is invalid"));
  return state_->wait();
}

} // namespace chronos::ingest
