#include "chronos/raft/async_durable_worker_extension_set.hpp"

#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <mutex>
#include <span>
#include <stdexcept>
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
        (std::filesystem::temp_directory_path() / "chronos-extension-set-XXXXXX").string();
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

[[nodiscard]] GroupId group_id(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return GroupId{bytes};
}

class Trace {
public:
  void append(std::string event) {
    const std::lock_guard lock{mutex_};
    events_.push_back(std::move(event));
  }

  [[nodiscard]] std::vector<std::string> copy() const {
    const std::lock_guard lock{mutex_};
    return events_;
  }

private:
  mutable std::mutex mutex_;
  std::vector<std::string> events_;
};

class ChildContext final : public AsyncDurableRaftWorkerBatchContext {};

class RecordingExtension final : public AsyncDurableRaftWorkerExtension {
public:
  RecordingExtension(std::string name, std::shared_ptr<Trace> trace,
                     common::Status initialization_status = common::Status::ok(),
                     common::Status completion_status = common::Status::ok(),
                     common::Status shutdown_status = common::Status::ok(),
                     const bool throw_during_shutdown = false)
      : name_(std::move(name)), trace_(std::move(trace)),
        initialization_status_(std::move(initialization_status)),
        completion_status_(std::move(completion_status)),
        shutdown_status_(std::move(shutdown_status)),
        throw_during_shutdown_(throw_during_shutdown) {}

  common::Status initialize(DurableMultiRaftRuntime&) override {
    worker_thread_ = std::this_thread::get_id();
    trace_->append(name_ + ":initialize");
    return initialization_status_;
  }

  common::Result<std::unique_ptr<AsyncDurableRaftWorkerBatchContext>>
  prepare_batch(DurableMultiRaftRuntime&, std::span<const DurableRaftRequest>) override {
    if (std::this_thread::get_id() != worker_thread_) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "child worker thread changed"});
    }
    trace_->append(name_ + ":prepare");
    return std::unique_ptr<AsyncDurableRaftWorkerBatchContext>{std::make_unique<ChildContext>()};
  }

  common::Status complete_batch(DurableMultiRaftRuntime&,
                                std::unique_ptr<AsyncDurableRaftWorkerBatchContext> context,
                                std::span<const DurableRaftResult>) override {
    if (std::this_thread::get_id() != worker_thread_)
      return {common::StatusCode::kInternal, "child worker thread changed"};
    if (dynamic_cast<ChildContext*>(context.get()) == nullptr)
      return {common::StatusCode::kCorruption, "child context changed"};
    trace_->append(name_ + ":complete");
    return completion_status_;
  }

  common::Status shutdown(DurableMultiRaftRuntime&) override {
    trace_->append(name_ + ":shutdown");
    if (std::this_thread::get_id() != worker_thread_)
      return {common::StatusCode::kInternal, "child shutdown worker thread changed"};
    if (throw_during_shutdown_)
      throw std::runtime_error{"injected child shutdown failure"};
    return shutdown_status_;
  }

private:
  std::string name_;
  std::shared_ptr<Trace> trace_;
  common::Status initialization_status_;
  common::Status completion_status_;
  common::Status shutdown_status_;
  bool throw_during_shutdown_{};
  std::thread::id worker_thread_;
};

TEST(AsyncDurableRaftWorkerExtensionSetTest, RejectsInvalidAndUnboundedDefinitions) {
  auto trace = std::make_shared<Trace>();
  auto child = std::make_shared<RecordingExtension>("A", trace);

  EXPECT_EQ(AsyncDurableRaftWorkerExtensionSet::create({}).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(AsyncDurableRaftWorkerExtensionSet::create({nullptr}).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(AsyncDurableRaftWorkerExtensionSet::create({child, child}).error().code(),
            common::StatusCode::kInvalidArgument);

  auto inner = AsyncDurableRaftWorkerExtensionSet::create({child});
  ASSERT_TRUE(inner.has_value()) << inner.error().to_string();
  EXPECT_EQ(AsyncDurableRaftWorkerExtensionSet::create({*inner}).error().code(),
            common::StatusCode::kInvalidArgument);

  std::vector<std::shared_ptr<AsyncDurableRaftWorkerExtension>> too_many;
  too_many.reserve(AsyncDurableRaftWorkerExtensionSet::kMaximumExtensions + 1U);
  for (std::size_t index = 0; index < AsyncDurableRaftWorkerExtensionSet::kMaximumExtensions + 1U;
       ++index) {
    too_many.push_back(
        std::make_shared<RecordingExtension>("child-" + std::to_string(index), trace));
  }
  EXPECT_EQ(AsyncDurableRaftWorkerExtensionSet::create(std::move(too_many)).error().code(),
            common::StatusCode::kResourceExhausted);
}

TEST(AsyncDurableRaftWorkerExtensionSetTest, RunsBatchCallbacksInOrderAndShutdownInReverseOrder) {
  TemporaryDirectory directory;
  auto trace = std::make_shared<Trace>();
  auto first = std::make_shared<RecordingExtension>("A", trace);
  auto second = std::make_shared<RecordingExtension>("B", trace);
  auto set = AsyncDurableRaftWorkerExtensionSet::create({first, second});
  ASSERT_TRUE(set.has_value()) << set.error().to_string();
  EXPECT_EQ((*set)->size(), 2U);

  const GroupId group = group_id(std::byte{1U});
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U}}}, {}, *set);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  EXPECT_TRUE(runtime->owns_worker_extension(**set));
  EXPECT_TRUE(runtime->owns_worker_extension(*first));
  EXPECT_TRUE(runtime->owns_worker_extension(*second));
  auto unrelated = std::make_shared<RecordingExtension>("C", trace);
  EXPECT_FALSE(runtime->owns_worker_extension(*unrelated));

  auto election = runtime->try_submit({{group, StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  const auto result = election->wait();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_TRUE(runtime->shutdown().is_ok());

  EXPECT_EQ(trace->copy(),
            (std::vector<std::string>{"A:initialize", "B:initialize", "A:prepare", "B:prepare",
                                      "A:complete", "B:complete", "B:shutdown", "A:shutdown"}));
}

TEST(AsyncDurableRaftWorkerExtensionSetTest, CleansUpAttemptedChildrenAfterInitializationFailure) {
  TemporaryDirectory directory;
  auto trace = std::make_shared<Trace>();
  auto first = std::make_shared<RecordingExtension>("A", trace);
  auto failed = std::make_shared<RecordingExtension>(
      "B", trace,
      common::Status{common::StatusCode::kUnavailable, "injected initialization failure"});
  auto skipped = std::make_shared<RecordingExtension>("C", trace);
  auto set = AsyncDurableRaftWorkerExtensionSet::create({first, failed, skipped});
  ASSERT_TRUE(set.has_value()) << set.error().to_string();

  const GroupId group = group_id(std::byte{2U});
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U}}}, {}, *set);
  ASSERT_FALSE(runtime.has_value());
  EXPECT_EQ(runtime.error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(trace->copy(),
            (std::vector<std::string>{"A:initialize", "B:initialize", "B:shutdown", "A:shutdown"}));
}

TEST(AsyncDurableRaftWorkerExtensionSetTest, ContinuesReverseShutdownAfterAChildThrows) {
  TemporaryDirectory directory;
  auto trace = std::make_shared<Trace>();
  auto first = std::make_shared<RecordingExtension>("A", trace);
  auto second = std::make_shared<RecordingExtension>(
      "B", trace, common::Status::ok(), common::Status::ok(), common::Status::ok(), true);
  auto set = AsyncDurableRaftWorkerExtensionSet::create({first, second});
  ASSERT_TRUE(set.has_value()) << set.error().to_string();

  const GroupId group = group_id(std::byte{3U});
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U}}}, {}, *set);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  const common::Status shutdown = runtime->shutdown();
  EXPECT_EQ(shutdown.code(), common::StatusCode::kInternal);
  EXPECT_EQ(trace->copy(),
            (std::vector<std::string>{"A:initialize", "B:initialize", "B:shutdown", "A:shutdown"}));
  EXPECT_EQ(runtime->shutdown(), shutdown);
}

TEST(AsyncDurableRaftWorkerExtensionSetTest, StopsBatchCompletionAtTheFirstChildFailure) {
  TemporaryDirectory directory;
  auto trace = std::make_shared<Trace>();
  auto failed = std::make_shared<RecordingExtension>(
      "A", trace, common::Status::ok(),
      common::Status{common::StatusCode::kCorruption, "injected completion failure"});
  auto skipped = std::make_shared<RecordingExtension>("B", trace);
  auto set = AsyncDurableRaftWorkerExtensionSet::create({failed, skipped});
  ASSERT_TRUE(set.has_value()) << set.error().to_string();

  const GroupId group = group_id(std::byte{4U});
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U}}}, {}, *set);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto election = runtime->try_submit({{group, StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  const auto result = election->wait();
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(runtime->shutdown(), result.error());
  EXPECT_EQ(trace->copy(),
            (std::vector<std::string>{"A:initialize", "B:initialize", "A:prepare", "B:prepare",
                                      "A:complete", "B:shutdown", "A:shutdown"}));
}

} // namespace
} // namespace chronos::raft
