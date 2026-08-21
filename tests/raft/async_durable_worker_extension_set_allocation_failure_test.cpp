#include "chronos/raft/async_durable_worker_extension_set.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-extension-set-allocation-XXXXXX")
            .string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }

  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  TemporaryDirectory(const TemporaryDirectory&) = delete;
  TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] GroupId group_id() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{0x5a};
  return GroupId{bytes};
}

[[nodiscard]] common::Result<DurableMultiRaftRuntime>
create_runtime(const TemporaryDirectory& directory) {
  return DurableMultiRaftRuntime::create_new(1U, {.directory_path = directory.path().string()},
                                             {{group_id(), {1U}}}, {});
}

class EmptyContext final : public AsyncDurableRaftWorkerBatchContext {};

class NoopExtension final : public AsyncDurableRaftWorkerExtension {
public:
  common::Status initialize(DurableMultiRaftRuntime&) override {
    ++initialize_calls;
    return common::Status::ok();
  }

  common::Result<std::unique_ptr<AsyncDurableRaftWorkerBatchContext>>
  prepare_batch(DurableMultiRaftRuntime&, std::span<const DurableRaftRequest>) override {
    ++prepare_calls;
    return std::unique_ptr<AsyncDurableRaftWorkerBatchContext>{std::make_unique<EmptyContext>()};
  }

  common::Status complete_batch(DurableMultiRaftRuntime&,
                                std::unique_ptr<AsyncDurableRaftWorkerBatchContext>,
                                std::span<const DurableRaftResult>) override {
    ++complete_calls;
    return common::Status::ok();
  }

  common::Status shutdown(DurableMultiRaftRuntime&) override {
    ++shutdown_calls;
    return common::Status::ok();
  }

  std::size_t initialize_calls{};
  std::size_t prepare_calls{};
  std::size_t complete_calls{};
  std::size_t shutdown_calls{};
};

struct ContextDestruction {
  std::thread::id owner;
  std::size_t destroyed{};
  bool wrong_thread{};
};

class TrackedContext final : public AsyncDurableRaftWorkerBatchContext {
public:
  explicit TrackedContext(ContextDestruction& destruction) noexcept : destruction_(&destruction) {}

  ~TrackedContext() override {
    ++destruction_->destroyed;
    destruction_->wrong_thread =
        destruction_->wrong_thread || std::this_thread::get_id() != destruction_->owner;
  }

private:
  ContextDestruction* destruction_;
};

class PreloadedContextExtension final : public AsyncDurableRaftWorkerExtension {
public:
  explicit PreloadedContextExtension(ContextDestruction& destruction) : destruction_(&destruction) {
    reset_context();
  }

  void reset_context() {
    context_ = std::make_unique<TrackedContext>(*destruction_);
  }

  common::Status initialize(DurableMultiRaftRuntime&) override {
    ++initialize_calls;
    return common::Status::ok();
  }

  common::Result<std::unique_ptr<AsyncDurableRaftWorkerBatchContext>>
  prepare_batch(DurableMultiRaftRuntime&, std::span<const DurableRaftRequest>) override {
    ++prepare_calls;
    return std::unique_ptr<AsyncDurableRaftWorkerBatchContext>{std::move(context_)};
  }

  common::Status complete_batch(DurableMultiRaftRuntime&,
                                std::unique_ptr<AsyncDurableRaftWorkerBatchContext> context,
                                std::span<const DurableRaftResult>) override {
    ++complete_calls;
    if (dynamic_cast<TrackedContext*>(context.get()) == nullptr) {
      return {common::StatusCode::kCorruption, "composed child context changed"};
    }
    return common::Status::ok();
  }

  common::Status shutdown(DurableMultiRaftRuntime&) override {
    ++shutdown_calls;
    return common::Status::ok();
  }

  ContextDestruction* destruction_;
  std::unique_ptr<TrackedContext> context_;
  std::size_t initialize_calls{};
  std::size_t prepare_calls{};
  std::size_t complete_calls{};
  std::size_t shutdown_calls{};
};

enum class AllocationHook : std::uint8_t { kNone, kInitialize, kPrepare, kComplete, kShutdown };

void inject_allocation_failure() {
  test::ScopedAllocationFailure failure{0U};
  void* const memory = ::operator new(1U);
  ::operator delete(memory);
}

class AllocationFaultExtension final : public AsyncDurableRaftWorkerExtension {
public:
  explicit AllocationFaultExtension(const AllocationHook hook) noexcept : hook_(hook) {}

  void set_hook(const AllocationHook hook) noexcept {
    hook_ = hook;
  }

  common::Status initialize(DurableMultiRaftRuntime&) override {
    ++initialize_calls;
    fail_if(AllocationHook::kInitialize);
    return common::Status::ok();
  }

  common::Result<std::unique_ptr<AsyncDurableRaftWorkerBatchContext>>
  prepare_batch(DurableMultiRaftRuntime&, std::span<const DurableRaftRequest>) override {
    ++prepare_calls;
    fail_if(AllocationHook::kPrepare);
    return std::unique_ptr<AsyncDurableRaftWorkerBatchContext>{std::make_unique<EmptyContext>()};
  }

  common::Status complete_batch(DurableMultiRaftRuntime&,
                                std::unique_ptr<AsyncDurableRaftWorkerBatchContext>,
                                std::span<const DurableRaftResult>) override {
    ++complete_calls;
    fail_if(AllocationHook::kComplete);
    return common::Status::ok();
  }

  common::Status shutdown(DurableMultiRaftRuntime&) override {
    ++shutdown_calls;
    fail_if(AllocationHook::kShutdown);
    return common::Status::ok();
  }

  std::size_t initialize_calls{};
  std::size_t prepare_calls{};
  std::size_t complete_calls{};
  std::size_t shutdown_calls{};

private:
  void fail_if(const AllocationHook hook) const {
    if (hook_ == hook)
      inject_allocation_failure();
  }

  AllocationHook hook_;
};

template <typename Operation>
[[nodiscard]] auto run_with_allocation_failure(const std::size_t fail_after, std::size_t& observed,
                                               Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    observed = failure.observed_allocations();
    failure.disable();
  }
  return std::move(*result);
}

TEST(AsyncDurableRaftWorkerExtensionSetAllocationFailureTest,
     CreationClassifiesEveryOwnedAllocationAndReleasesTransferredChildren) {
  auto child = std::make_shared<NoopExtension>();
  std::size_t failure_count{};
  bool reached_success{};

  for (std::size_t fail_after = 0U; fail_after < 8U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::vector<std::shared_ptr<AsyncDurableRaftWorkerExtension>> children{child};
    std::size_t observed{};
    auto set = run_with_allocation_failure(fail_after, observed, [&] {
      return AsyncDurableRaftWorkerExtensionSet::create(std::move(children));
    });

    EXPECT_GT(observed, 0U);
    if (set.has_value()) {
      reached_success = true;
      EXPECT_EQ((*set)->size(), 1U);
      EXPECT_EQ(child.use_count(), 2U);
      break;
    }
    ++failure_count;
    EXPECT_EQ(set.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(child.use_count(), 1U);
  }

  EXPECT_TRUE(reached_success);
  EXPECT_EQ(failure_count, 2U);
}

TEST(AsyncDurableRaftWorkerExtensionSetAllocationFailureTest,
     BatchCompositionClassifiesEveryOwnedAllocationAndRemainsReusable) {
  std::size_t failure_count{};
  bool reached_success{};

  for (std::size_t fail_after = 0U; fail_after < 8U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    TemporaryDirectory directory;
    auto runtime = create_runtime(directory);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    ContextDestruction destruction{.owner = std::this_thread::get_id()};
    auto first = std::make_shared<PreloadedContextExtension>(destruction);
    auto second = std::make_shared<PreloadedContextExtension>(destruction);
    auto third = std::make_shared<PreloadedContextExtension>(destruction);
    auto set = AsyncDurableRaftWorkerExtensionSet::create({first, second, third});
    ASSERT_TRUE(set.has_value()) << set.error().to_string();
    ASSERT_TRUE((*set)->initialize(*runtime).is_ok());

    std::size_t observed{};
    auto context = run_with_allocation_failure(fail_after, observed,
                                               [&] { return (*set)->prepare_batch(*runtime, {}); });
    EXPECT_GT(observed, 0U);
    if (context.has_value()) {
      reached_success = true;
      EXPECT_EQ(first->prepare_calls, 1U);
      EXPECT_EQ(second->prepare_calls, 1U);
      EXPECT_EQ(third->prepare_calls, 1U);
      EXPECT_TRUE((*set)->complete_batch(*runtime, std::move(*context), {}).is_ok());
      EXPECT_EQ(destruction.destroyed, 3U);
    } else {
      ++failure_count;
      EXPECT_EQ(context.error().code(), common::StatusCode::kResourceExhausted);
      const std::size_t prepared =
          first->prepare_calls + second->prepare_calls + third->prepare_calls;
      EXPECT_TRUE(prepared == 0U || prepared == 3U);
      EXPECT_EQ(destruction.destroyed, prepared);

      first->reset_context();
      second->reset_context();
      third->reset_context();
      auto retry = (*set)->prepare_batch(*runtime, {});
      ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
      EXPECT_TRUE((*set)->complete_batch(*runtime, std::move(*retry), {}).is_ok());
      EXPECT_EQ(first->complete_calls, 1U);
      EXPECT_EQ(second->complete_calls, 1U);
      EXPECT_EQ(third->complete_calls, 1U);
    }
    EXPECT_FALSE(destruction.wrong_thread);
    EXPECT_TRUE((*set)->shutdown(*runtime).is_ok());
    EXPECT_TRUE(runtime->close().is_ok());
    if (reached_success)
      break;
  }

  EXPECT_TRUE(reached_success);
  EXPECT_EQ(failure_count, 2U);
}

TEST(AsyncDurableRaftWorkerExtensionSetAllocationFailureTest,
     ChildInitializationAllocationFailureIsResourceExhaustedAndCleansAttemptedChildren) {
  TemporaryDirectory directory;
  auto runtime = create_runtime(directory);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto first = std::make_shared<AllocationFaultExtension>(AllocationHook::kNone);
  auto failed = std::make_shared<AllocationFaultExtension>(AllocationHook::kInitialize);
  auto skipped = std::make_shared<AllocationFaultExtension>(AllocationHook::kNone);
  auto set = AsyncDurableRaftWorkerExtensionSet::create({first, failed, skipped});
  ASSERT_TRUE(set.has_value()) << set.error().to_string();

  const common::Status initialized = (*set)->initialize(*runtime);
  EXPECT_EQ(initialized.code(), common::StatusCode::kResourceExhausted);
  EXPECT_TRUE((*set)->shutdown(*runtime).is_ok());
  EXPECT_EQ(first->initialize_calls, 1U);
  EXPECT_EQ(failed->initialize_calls, 1U);
  EXPECT_EQ(skipped->initialize_calls, 0U);
  EXPECT_EQ(first->shutdown_calls, 1U);
  EXPECT_EQ(failed->shutdown_calls, 1U);
  EXPECT_EQ(skipped->shutdown_calls, 0U);
  EXPECT_TRUE(runtime->close().is_ok());
}

TEST(AsyncDurableRaftWorkerExtensionSetAllocationFailureTest,
     ChildBatchAllocationFailuresAreResourceExhaustedAndPreparationCanRetry) {
  TemporaryDirectory directory;
  auto runtime = create_runtime(directory);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto first = std::make_shared<AllocationFaultExtension>(AllocationHook::kPrepare);
  auto second = std::make_shared<AllocationFaultExtension>(AllocationHook::kNone);
  auto set = AsyncDurableRaftWorkerExtensionSet::create({first, second});
  ASSERT_TRUE(set.has_value()) << set.error().to_string();
  ASSERT_TRUE((*set)->initialize(*runtime).is_ok());

  auto prepared = (*set)->prepare_batch(*runtime, {});
  ASSERT_FALSE(prepared.has_value());
  EXPECT_EQ(prepared.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(first->prepare_calls, 1U);
  EXPECT_EQ(second->prepare_calls, 0U);

  first->set_hook(AllocationHook::kNone);
  prepared = (*set)->prepare_batch(*runtime, {});
  ASSERT_TRUE(prepared.has_value()) << prepared.error().to_string();
  first->set_hook(AllocationHook::kComplete);
  const common::Status completed = (*set)->complete_batch(*runtime, std::move(*prepared), {});
  EXPECT_EQ(completed.code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(first->complete_calls, 1U);
  EXPECT_EQ(second->complete_calls, 0U);
  EXPECT_TRUE((*set)->shutdown(*runtime).is_ok());
  EXPECT_TRUE(runtime->close().is_ok());
}

TEST(AsyncDurableRaftWorkerExtensionSetAllocationFailureTest,
     ChildShutdownAllocationFailureRetainsFirstFailureAndContinuesReverseCleanup) {
  TemporaryDirectory directory;
  auto runtime = create_runtime(directory);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto first = std::make_shared<AllocationFaultExtension>(AllocationHook::kNone);
  auto second = std::make_shared<AllocationFaultExtension>(AllocationHook::kShutdown);
  auto set = AsyncDurableRaftWorkerExtensionSet::create({first, second});
  ASSERT_TRUE(set.has_value()) << set.error().to_string();
  ASSERT_TRUE((*set)->initialize(*runtime).is_ok());

  const common::Status shutdown = (*set)->shutdown(*runtime);
  EXPECT_EQ(shutdown.code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(second->shutdown_calls, 1U);
  EXPECT_EQ(first->shutdown_calls, 1U);
  EXPECT_TRUE((*set)->shutdown(*runtime).is_ok());
  EXPECT_EQ(second->shutdown_calls, 1U);
  EXPECT_EQ(first->shutdown_calls, 1U);
  EXPECT_TRUE(runtime->close().is_ok());
}

} // namespace
} // namespace chronos::raft
