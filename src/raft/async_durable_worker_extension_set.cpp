#include "chronos/raft/async_durable_worker_extension_set.hpp"

#include <exception>
#include <memory>
#include <new>
#include <utility>

namespace chronos::raft {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status extension_exception(const char* operation) {
  return {common::StatusCode::kInternal, operation};
}

[[nodiscard]] common::Status extension_allocation_failure(const char* operation) {
  return {common::StatusCode::kResourceExhausted, operation};
}

class ExtensionSetBatchContext final : public AsyncDurableRaftWorkerBatchContext {
public:
  explicit ExtensionSetBatchContext(
      std::vector<std::unique_ptr<AsyncDurableRaftWorkerBatchContext>> child_contexts) noexcept
      : child_contexts_(std::move(child_contexts)) {}

  [[nodiscard]] std::vector<std::unique_ptr<AsyncDurableRaftWorkerBatchContext>>&
  child_contexts() noexcept {
    return child_contexts_;
  }

private:
  std::vector<std::unique_ptr<AsyncDurableRaftWorkerBatchContext>> child_contexts_;
};

} // namespace

common::Result<std::shared_ptr<AsyncDurableRaftWorkerExtensionSet>>
AsyncDurableRaftWorkerExtensionSet::create(
    std::vector<std::shared_ptr<AsyncDurableRaftWorkerExtension>> extensions) {
  if (extensions.empty())
    return common::make_unexpected(invalid("durable Raft worker extension set cannot be empty"));
  if (extensions.size() > kMaximumExtensions) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "durable Raft worker extension set exceeds its bounded capacity"});
  }
  for (std::size_t index = 0; index < extensions.size(); ++index) {
    if (extensions[index] == nullptr) {
      return common::make_unexpected(
          invalid("durable Raft worker extension set contains a null child"));
    }
    if (dynamic_cast<AsyncDurableRaftWorkerExtensionSet*>(extensions[index].get()) != nullptr) {
      return common::make_unexpected(
          invalid("durable Raft worker extension sets cannot be nested"));
    }
    for (std::size_t prior = 0; prior < index; ++prior) {
      if (extensions[prior].get() == extensions[index].get()) {
        return common::make_unexpected(
            invalid("durable Raft worker extension set contains a duplicate child"));
      }
    }
  }
  try {
    return std::shared_ptr<AsyncDurableRaftWorkerExtensionSet>{
        new AsyncDurableRaftWorkerExtensionSet{std::move(extensions)}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "cannot allocate durable Raft worker extension set"});
  }
}

AsyncDurableRaftWorkerExtensionSet::AsyncDurableRaftWorkerExtensionSet(
    std::vector<std::shared_ptr<AsyncDurableRaftWorkerExtension>> extensions) noexcept
    : extensions_(std::move(extensions)) {}

std::size_t AsyncDurableRaftWorkerExtensionSet::size() const noexcept {
  return extensions_.size();
}

bool AsyncDurableRaftWorkerExtensionSet::contains_worker_extension(
    const AsyncDurableRaftWorkerExtension& candidate) const noexcept {
  if (this == &candidate)
    return true;
  for (const auto& extension : extensions_) {
    if (extension.get() == &candidate)
      return true;
  }
  return false;
}

common::Status AsyncDurableRaftWorkerExtensionSet::initialize(DurableMultiRaftRuntime& runtime) {
  if (attempted_initializations_ != 0U || initialized_ || shutdown_complete_) {
    return {common::StatusCode::kInternal,
            "durable Raft worker extension set was initialized more than once"};
  }
  for (auto& extension : extensions_) {
    ++attempted_initializations_;
    common::Status status;
    try {
      status = extension->initialize(runtime);
    } catch (const std::bad_alloc&) {
      return extension_allocation_failure(
          "durable Raft child extension initialization exhausted memory");
    } catch (...) {
      return extension_exception("durable Raft child extension initialization threw");
    }
    if (!status.is_ok())
      return status;
  }
  initialized_ = true;
  return common::Status::ok();
}

common::Result<std::unique_ptr<AsyncDurableRaftWorkerBatchContext>>
AsyncDurableRaftWorkerExtensionSet::prepare_batch(
    DurableMultiRaftRuntime& runtime, const std::span<const DurableRaftRequest> requests) {
  if (!initialized_ || shutdown_complete_) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInternal, "durable Raft worker extension set is not initialized"});
  }
  try {
    std::vector<std::unique_ptr<AsyncDurableRaftWorkerBatchContext>> contexts;
    contexts.reserve(extensions_.size());
    for (auto& extension : extensions_) {
      auto context = extension->prepare_batch(runtime, requests);
      if (!context.has_value())
        return common::make_unexpected(context.error());
      if (*context == nullptr) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kInternal,
                           "durable Raft child extension returned a missing batch context"});
      }
      contexts.push_back(std::move(*context));
    }
    return std::unique_ptr<AsyncDurableRaftWorkerBatchContext>{
        std::make_unique<ExtensionSetBatchContext>(std::move(contexts))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kResourceExhausted,
        "durable Raft worker extension set exhausted memory while preparing a batch"});
  } catch (...) {
    return common::make_unexpected(
        extension_exception("durable Raft child extension batch preparation threw"));
  }
}

common::Status AsyncDurableRaftWorkerExtensionSet::complete_batch(
    DurableMultiRaftRuntime& runtime, std::unique_ptr<AsyncDurableRaftWorkerBatchContext> context,
    const std::span<const DurableRaftResult> results) {
  if (!initialized_ || shutdown_complete_) {
    return {common::StatusCode::kInternal, "durable Raft worker extension set is not initialized"};
  }
  auto* const composed = dynamic_cast<ExtensionSetBatchContext*>(context.get());
  if (composed == nullptr || composed->child_contexts().size() != extensions_.size()) {
    return {common::StatusCode::kCorruption,
            "durable Raft worker extension set received an incompatible batch context"};
  }
  for (std::size_t index = 0; index < extensions_.size(); ++index) {
    common::Status status;
    try {
      status = extensions_[index]->complete_batch(
          runtime, std::move(composed->child_contexts()[index]), results);
    } catch (const std::bad_alloc&) {
      return extension_allocation_failure(
          "durable Raft child extension batch completion exhausted memory");
    } catch (...) {
      return extension_exception("durable Raft child extension batch completion threw");
    }
    if (!status.is_ok())
      return status;
  }
  return common::Status::ok();
}

common::Status AsyncDurableRaftWorkerExtensionSet::shutdown(DurableMultiRaftRuntime& runtime) {
  if (shutdown_complete_)
    return common::Status::ok();
  common::Status first_failure = common::Status::ok();
  while (attempted_initializations_ > 0U) {
    --attempted_initializations_;
    common::Status status;
    try {
      status = extensions_[attempted_initializations_]->shutdown(runtime);
    } catch (const std::bad_alloc&) {
      status =
          extension_allocation_failure("durable Raft child extension shutdown exhausted memory");
    } catch (...) {
      status = extension_exception("durable Raft child extension shutdown threw");
    }
    if (first_failure.is_ok() && !status.is_ok())
      first_failure = std::move(status);
  }
  initialized_ = false;
  shutdown_complete_ = true;
  return first_failure;
}

} // namespace chronos::raft
