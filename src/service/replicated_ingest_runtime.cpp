#include "chronos/service/replicated_ingest_runtime.hpp"

#include "chronos/raft/async_durable_worker_extension_set.hpp"

#include <memory>
#include <new>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

class IngestAsyncRuntimeShutdownObserver final
    : public raft::AsyncDurableMultiRaftShutdownObserver {
public:
  explicit IngestAsyncRuntimeShutdownObserver(
      ReplicatedIngestRuntimeShutdownObserver& configured) noexcept
      : observer_(configured) {}

  void on_shutdown_stage(const raft::AsyncDurableMultiRaftShutdownStage stage) noexcept override {
    switch (stage) {
    case raft::AsyncDurableMultiRaftShutdownStage::kAcceptedWorkDrained:
      observer_.on_shutdown_stage(ReplicatedIngestRuntimeShutdownStage::kAcceptedWorkDrained);
      return;
    case raft::AsyncDurableMultiRaftShutdownStage::kExtensionStopped:
      observer_.on_shutdown_stage(ReplicatedIngestRuntimeShutdownStage::kApplicationsStopped);
      return;
    case raft::AsyncDurableMultiRaftShutdownStage::kLogClosed:
      observer_.on_shutdown_stage(ReplicatedIngestRuntimeShutdownStage::kLogClosed);
      return;
    }
  }

private:
  ReplicatedIngestRuntimeShutdownObserver& observer_;
};

[[nodiscard]] common::Status validate_config(const ReplicatedIngestRuntimeConfig& config) {
  if (config.local_node_id == 0U || config.metadata.group_id.is_nil() ||
      config.groups.size() != config.tablets.size() + 1U)
    return invalid("replicated ingest runtime identity or group count is invalid");
  std::size_t metadata_matches = 0U;
  for (std::size_t index = 0U; index < config.groups.size(); ++index) {
    if (config.groups[index].group_id.is_nil())
      return invalid("replicated ingest runtime contains a nil group");
    for (std::size_t previous = 0U; previous < index; ++previous) {
      if (config.groups[index].group_id == config.groups[previous].group_id)
        return invalid("replicated ingest runtime contains duplicate groups");
    }
    if (config.groups[index].group_id == config.metadata.group_id)
      ++metadata_matches;
  }
  if (metadata_matches != 1U)
    return invalid("replicated ingest metadata group is not configured exactly once");
  for (std::size_t index = 0U; index < config.tablets.size(); ++index) {
    const raft::GroupId& group_id = config.tablets[index].group_id;
    if (group_id.is_nil() || group_id == config.metadata.group_id)
      return invalid("replicated ingest tablet group identity is invalid");
    std::size_t group_matches = 0U;
    for (const raft::RaftGroupConfiguration& group : config.groups) {
      if (group.group_id == group_id)
        ++group_matches;
    }
    if (group_matches != 1U)
      return invalid("replicated ingest tablet group is not configured exactly once");
    for (std::size_t previous = 0U; previous < index; ++previous) {
      if (config.tablets[previous].group_id == group_id)
        return invalid("replicated ingest runtime contains duplicate tablet groups");
    }
  }
  return common::Status::ok();
}

} // namespace

class ReplicatedIngestRuntime::Impl {
public:
  Impl(std::shared_ptr<ingest::AsyncRaftTabletApplication> configured_tablets,
       std::shared_ptr<raft::AsyncRaftMetadataApplication> configured_metadata,
       std::shared_ptr<raft::AsyncDurableRaftWorkerExtensionSet> configured_extensions,
       raft::AsyncDurableMultiRaftRuntime configured_runtime) noexcept
      : tablets(std::move(configured_tablets)), metadata(std::move(configured_metadata)),
        extensions(std::move(configured_extensions)), runtime(std::move(configured_runtime)),
        coordinator(std::nullopt) {}

  std::shared_ptr<ingest::AsyncRaftTabletApplication> tablets;
  std::shared_ptr<raft::AsyncRaftMetadataApplication> metadata;
  std::shared_ptr<raft::AsyncDurableRaftWorkerExtensionSet> extensions;
  std::optional<raft::AsyncDurableMultiRaftRuntime> runtime;
  std::optional<ReplicatedIngestCoordinator> coordinator;
  bool shutdown_complete{};
  common::Status shutdown_status;
};

ReplicatedIngestRuntime::ReplicatedIngestRuntime(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

ReplicatedIngestRuntime::~ReplicatedIngestRuntime() {
  try {
    if (impl_ != nullptr)
      static_cast<void>(shutdown());
  } catch (...) { // NOLINT(bugprone-empty-catch)
    // Explicit shutdown reports failures; destruction is necessarily best-effort.
  }
}

ReplicatedIngestRuntime::ReplicatedIngestRuntime(ReplicatedIngestRuntime&&) noexcept = default;
ReplicatedIngestRuntime&
ReplicatedIngestRuntime::operator=(ReplicatedIngestRuntime&&) noexcept = default;

common::Result<ReplicatedIngestRuntime>
ReplicatedIngestRuntime::create_new(ReplicatedIngestRuntimeConfig config) {
  return start(std::move(config), std::nullopt);
}

common::Result<ReplicatedIngestRuntime>
ReplicatedIngestRuntime::open_existing(ReplicatedIngestRuntimeConfig config,
                                       const raft::RaftPersistentLogOpenOptions open_options) {
  return start(std::move(config), open_options);
}

common::Result<ReplicatedIngestRuntime>
ReplicatedIngestRuntime::start(ReplicatedIngestRuntimeConfig config,
                               std::optional<raft::RaftPersistentLogOpenOptions> open_options) {
  const common::Status validation = validate_config(config);
  if (!validation.is_ok())
    return common::make_unexpected(validation);
  auto tablets = ingest::AsyncRaftTabletApplication::create(std::move(config.tablets),
                                                            config.application_limits);
  if (!tablets.has_value())
    return common::make_unexpected(tablets.error());
  auto metadata = raft::AsyncRaftMetadataApplication::create(std::move(config.metadata));
  if (!metadata.has_value())
    return common::make_unexpected(metadata.error());
  std::vector<std::shared_ptr<raft::AsyncDurableRaftWorkerExtension>> children;
  try {
    children.reserve(2U);
    children.push_back(*tablets);
    children.push_back(*metadata);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("replicated ingest extension allocation failed"));
  }
  auto extensions = raft::AsyncDurableRaftWorkerExtensionSet::create(std::move(children));
  if (!extensions.has_value())
    return common::make_unexpected(extensions.error());
  common::Result<raft::AsyncDurableMultiRaftRuntime> runtime =
      open_options.has_value()
          ? raft::AsyncDurableMultiRaftRuntime::open_existing(
                config.local_node_id, config.log, *open_options, std::move(config.groups),
                config.runtime_limits, *extensions)
          : raft::AsyncDurableMultiRaftRuntime::create_new(config.local_node_id, config.log,
                                                           std::move(config.groups),
                                                           config.runtime_limits, *extensions);
  if (!runtime.has_value())
    return common::make_unexpected(runtime.error());
  std::unique_ptr<Impl> impl;
  try {
    impl = std::make_unique<Impl>(std::move(*tablets), std::move(*metadata), std::move(*extensions),
                                  std::move(*runtime));
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("replicated ingest runtime allocation failed"));
  }
  auto& installed_runtime = impl->runtime;
  if (!installed_runtime.has_value())
    return common::make_unexpected(invalid("replicated ingest runtime owner is absent"));
  auto coordinator = ReplicatedIngestCoordinator::create(
      *installed_runtime, *impl->tablets, *impl->metadata, config.coordinator_limits);
  if (!coordinator.has_value()) {
    const common::Status stopped = installed_runtime->shutdown();
    return common::make_unexpected(stopped.is_ok() ? coordinator.error() : stopped);
  }
  impl->coordinator.emplace(std::move(*coordinator));
  return ReplicatedIngestRuntime{std::move(impl)};
}

raft::AsyncDurableMultiRaftRuntime* ReplicatedIngestRuntime::runtime() noexcept {
  if (!is_running())
    return nullptr;
  auto& runtime = impl_->runtime;
  return runtime.has_value() ? std::addressof(*runtime) : nullptr;
}

ingest::AsyncRaftTabletApplication* ReplicatedIngestRuntime::tablet_application() noexcept {
  return is_running() ? impl_->tablets.get() : nullptr;
}

raft::AsyncRaftMetadataApplication* ReplicatedIngestRuntime::metadata_application() noexcept {
  return is_running() ? impl_->metadata.get() : nullptr;
}

ReplicatedIngestCoordinator* ReplicatedIngestRuntime::coordinator() noexcept {
  if (!is_running())
    return nullptr;
  auto& coordinator = impl_->coordinator;
  return coordinator.has_value() ? std::addressof(*coordinator) : nullptr;
}

bool ReplicatedIngestRuntime::is_running() const noexcept {
  return impl_ != nullptr && !impl_->shutdown_complete && impl_->runtime.has_value() &&
         impl_->coordinator.has_value();
}

common::Status ReplicatedIngestRuntime::shutdown() {
  return shutdown_with(nullptr);
}

common::Status
ReplicatedIngestRuntime::shutdown(ReplicatedIngestRuntimeShutdownObserver& observer) {
  return shutdown_with(std::addressof(observer));
}

common::Status
ReplicatedIngestRuntime::shutdown_with(ReplicatedIngestRuntimeShutdownObserver* const observer) {
  if (impl_ == nullptr)
    return invalid("replicated ingest runtime was moved from");
  if (impl_->shutdown_complete)
    return impl_->shutdown_status;
  impl_->coordinator.reset();
  if (observer != nullptr)
    observer->on_shutdown_stage(ReplicatedIngestRuntimeShutdownStage::kCoordinatorReleased);
  auto& runtime = impl_->runtime;
  if (runtime.has_value()) {
    if (observer != nullptr) {
      IngestAsyncRuntimeShutdownObserver runtime_observer{*observer};
      impl_->shutdown_status = runtime->shutdown(runtime_observer);
    } else {
      impl_->shutdown_status = runtime->shutdown();
    }
    runtime.reset();
  } else {
    impl_->shutdown_status = invalid("replicated ingest runtime owner is absent");
  }
  if (observer != nullptr)
    observer->on_shutdown_stage(ReplicatedIngestRuntimeShutdownStage::kWorkerStopped);
  impl_->shutdown_complete = true;
  return impl_->shutdown_status;
}

} // namespace chronos::service
