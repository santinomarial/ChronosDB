#include "chronos/ingest/async_raft_tablet_application.hpp"

#include <algorithm>
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

class AsyncRaftTabletApplication::Impl {
public:
  struct OwnedTablet {
    raft::GroupId group_id;
    RaftTabletStateMachine machine;
    std::optional<raft::QuorumSyncReceipt> latest_receipt;
  };

  explicit Impl(std::vector<AsyncRaftTabletApplicationConfig> configured) noexcept
      : pending(std::move(configured)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (failure.is_ok())
      failure = std::move(status);
    return failure;
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
        tablets.push_back(OwnedTablet{config.group_id, std::move(*recovered), std::nullopt});
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
      if (node->role() != raft::Role::kLeader || node->applied_index() == 0U)
        continue;
      auto receipt = owned.machine.prove_applied_quorum_sync(node->applied_index());
      if (!receipt.has_value())
        return fail(receipt.error());
      owned.latest_receipt = *receipt;
    }
    return common::Status::ok();
  }

  mutable std::mutex mutex;
  std::vector<AsyncRaftTabletApplicationConfig> pending;
  std::vector<OwnedTablet> tablets;
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
  if (limits.maximum_tablets == 0U || limits.maximum_tablets > 65'536U || tablets.empty() ||
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
        new AsyncRaftTabletApplication{std::make_unique<Impl>(std::move(tablets))}};
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

std::size_t AsyncRaftTabletApplication::tablet_count() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->initialized && !impl_->shutdown ? impl_->tablets.size() : 0U;
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
  impl_->tablets.clear();
  impl_->pending.clear();
  return impl_->failure;
}

} // namespace chronos::ingest
