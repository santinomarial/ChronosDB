#include "chronos/raft/async_metadata_application.hpp"

#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <span>
#include <utility>

namespace chronos::raft {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

class MetadataApplicationBatchContext final : public AsyncDurableRaftWorkerBatchContext {
public:
  MetadataApplicationBatchContext(const std::size_t requests,
                                  const bool metadata_group_touched) noexcept
      : request_count(requests), touched(metadata_group_touched) {}

  std::size_t request_count{};
  bool touched{};
};

} // namespace

class AsyncRaftMetadataApplication::Impl {
public:
  explicit Impl(AsyncRaftMetadataApplicationConfig configured) noexcept
      : config(std::move(configured)) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (failure.is_ok())
      failure = std::move(status);
    return failure;
  }

  [[nodiscard]] common::Status publish_catalog() {
    auto projected = machine->state().catalog_snapshot();
    if (!projected.has_value())
      return fail(projected.error());
    try {
      published = std::make_shared<const MetadataCatalogSnapshot>(std::move(*projected));
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return fail(exhausted("metadata catalog snapshot publication allocation failed"));
    }
  }

  [[nodiscard]] common::Status initialize(DurableMultiRaftRuntime& runtime) {
    std::lock_guard lock{mutex};
    if (initialized || shutdown)
      return fail(invalid("metadata application initialization state is invalid"));
    common::Result<DurableMetadataStateMachine> recovered =
        config.snapshot_storage.has_value()
            ? DurableMetadataStateMachine::recover(
                  config.group_id, runtime, std::move(*config.snapshot_storage),
                  config.state_limits, config.codec_limits, config.schema_codec_limits)
            : DurableMetadataStateMachine::recover(config.group_id, runtime, config.state_limits,
                                                   config.codec_limits, config.schema_codec_limits);
    if (!recovered.has_value())
      return fail(recovered.error());
    machine.emplace(std::move(*recovered));
    const common::Status published_status = publish_catalog();
    if (!published_status.is_ok())
      return published_status;
    config.snapshot_storage.reset();
    initialized = true;
    return common::Status::ok();
  }

  [[nodiscard]] common::Status apply_and_publish() {
    std::lock_guard lock{mutex};
    if (!failure.is_ok())
      return failure;
    if (!initialized || shutdown || !machine.has_value())
      return fail(unavailable("metadata application owner is not active"));
    auto applied = machine->apply_committed();
    if (!applied.has_value())
      return fail(applied.error());
    if (applied->last_applied_index == 0U)
      return common::Status::ok();
    return publish_catalog();
  }

  mutable std::mutex mutex;
  AsyncRaftMetadataApplicationConfig config;
  std::optional<DurableMetadataStateMachine> machine;
  std::shared_ptr<const MetadataCatalogSnapshot> published;
  common::Status failure;
  bool initialized{};
  bool shutdown{};
};

AsyncRaftMetadataApplication::AsyncRaftMetadataApplication(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

AsyncRaftMetadataApplication::~AsyncRaftMetadataApplication() = default;

common::Result<std::shared_ptr<AsyncRaftMetadataApplication>>
AsyncRaftMetadataApplication::create(AsyncRaftMetadataApplicationConfig config) {
  if (config.group_id.is_nil())
    return common::make_unexpected(invalid("metadata application group identity is nil"));
  try {
    return std::shared_ptr<AsyncRaftMetadataApplication>{
        new AsyncRaftMetadataApplication{std::make_unique<Impl>(std::move(config))}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("metadata application allocation failed"));
  }
}

const GroupId& AsyncRaftMetadataApplication::group_id() const noexcept {
  return impl_->config.group_id;
}

common::Result<std::shared_ptr<const MetadataCatalogSnapshot>>
AsyncRaftMetadataApplication::catalog_snapshot() const {
  std::lock_guard lock{impl_->mutex};
  if (!impl_->failure.is_ok())
    return common::make_unexpected(impl_->failure);
  if (!impl_->initialized || impl_->shutdown || impl_->published == nullptr)
    return common::make_unexpected(unavailable("metadata catalog snapshot is unavailable"));
  return impl_->published;
}

bool AsyncRaftMetadataApplication::initialized() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->initialized && !impl_->shutdown;
}

bool AsyncRaftMetadataApplication::failed() const {
  std::lock_guard lock{impl_->mutex};
  return !impl_->failure.is_ok();
}

common::Status AsyncRaftMetadataApplication::failure_status() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->failure;
}

common::Status AsyncRaftMetadataApplication::initialize(DurableMultiRaftRuntime& runtime) {
  return impl_->initialize(runtime);
}

common::Result<std::unique_ptr<AsyncDurableRaftWorkerBatchContext>>
AsyncRaftMetadataApplication::prepare_batch(DurableMultiRaftRuntime&,
                                            const std::span<const DurableRaftRequest> requests) {
  std::lock_guard lock{impl_->mutex};
  if (!impl_->failure.is_ok())
    return common::make_unexpected(impl_->failure);
  if (!impl_->initialized || impl_->shutdown)
    return common::make_unexpected(unavailable("metadata application owner is not active"));
  bool touched = false;
  for (const DurableRaftRequest& request : requests)
    touched = touched || request.group_id == impl_->config.group_id;
  try {
    return std::unique_ptr<AsyncDurableRaftWorkerBatchContext>{
        std::make_unique<MetadataApplicationBatchContext>(requests.size(), touched)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("metadata application batch allocation failed"));
  }
}

common::Status AsyncRaftMetadataApplication::complete_batch(
    DurableMultiRaftRuntime&, std::unique_ptr<AsyncDurableRaftWorkerBatchContext> context,
    const std::span<const DurableRaftResult> results) {
  const auto* const batch = dynamic_cast<const MetadataApplicationBatchContext*>(context.get());
  if (batch == nullptr || batch->request_count != results.size()) {
    std::lock_guard lock{impl_->mutex};
    return impl_->fail(common::Status{common::StatusCode::kCorruption,
                                      "metadata application batch context changed"});
  }
  return batch->touched ? impl_->apply_and_publish() : common::Status::ok();
}

common::Status AsyncRaftMetadataApplication::shutdown(DurableMultiRaftRuntime&) {
  std::lock_guard lock{impl_->mutex};
  impl_->shutdown = true;
  impl_->published.reset();
  impl_->machine.reset();
  impl_->config.snapshot_storage.reset();
  return impl_->failure;
}

} // namespace chronos::raft
