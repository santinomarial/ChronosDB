#include "chronos/raft/async_durable_runtime.hpp"
#include "raft/async_durable_runtime_internal.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

constexpr std::size_t kMaximumAllocationSweep = 512U;

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-async-owner-allocation-XXXXXX").string();
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

[[nodiscard]] GroupId group_id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(static_cast<std::byte>(seed));
  return GroupId{bytes};
}

template <typename Operation>
[[nodiscard]] auto run_with_allocation_failure(const std::size_t fail_after, std::size_t& observed,
                                               Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    test::ScopedAllocationFailure failure{fail_after};
    try {
      result.emplace(operation());
    } catch (const std::bad_alloc&) {
      failure.disable();
      throw std::runtime_error{"allocation failure escaped at index " + std::to_string(fail_after)};
    }
    observed = failure.observed_allocations();
    failure.disable();
  }
  return std::move(*result);
}

TEST(AsyncDurableMultiRaftRuntimeAllocationFailureTest,
     OwnerConstructionClassifiesEveryAllocationAndReleasesDurableStorage) {
  for (std::uint8_t persisted_term = 0U; persisted_term < 2U; ++persisted_term) {
    SCOPED_TRACE(static_cast<std::uint32_t>(persisted_term));
    bool reached_success{};
    std::size_t failure_count{};
    std::size_t successful_allocations{};

    for (std::size_t fail_after = 0U; fail_after < kMaximumAllocationSweep; ++fail_after) {
      SCOPED_TRACE(fail_after);
      TemporaryDirectory directory;
      const GroupId group = group_id(static_cast<std::uint8_t>(0x60U + persisted_term));
      const RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
      auto durable = DurableMultiRaftRuntime::create_new(1U, log_config, {{group, {1U}}});
      ASSERT_TRUE(durable.has_value()) << durable.error().to_string();
      if (persisted_term != 0U) {
        auto elected = durable->execute_batch({{group, StartElectionOperation{}}});
        ASSERT_TRUE(elected.has_value()) << elected.error().to_string();
      }

      std::size_t observed{};
      auto owner = run_with_allocation_failure(fail_after, observed, [&] {
        return detail::AsyncDurableMultiRaftRuntimeTestAccess::start_with(std::move(*durable));
      });
      EXPECT_GT(observed, 0U);
      if (owner.has_value()) {
        reached_success = true;
        successful_allocations = observed;
        EXPECT_TRUE(owner->shutdown().is_ok());
        auto reopened = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, {{group, {1U}}});
        ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
        ASSERT_NE(reopened->find_group(group), nullptr);
        EXPECT_EQ(reopened->find_group(group)->current_term(), persisted_term);
        EXPECT_TRUE(reopened->close().is_ok());
        break;
      }
      ++failure_count;
      EXPECT_EQ(owner.error().code(), common::StatusCode::kResourceExhausted);

      auto reopened = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, {{group, {1U}}});
      ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
      ASSERT_NE(reopened->find_group(group), nullptr);
      EXPECT_EQ(reopened->find_group(group)->current_term(), persisted_term);
      EXPECT_TRUE(reopened->close().is_ok());
    }

    EXPECT_TRUE(reached_success);
    EXPECT_EQ(failure_count, successful_allocations);
  }
}

TEST(AsyncDurableMultiRaftRuntimeAllocationFailureTest,
     DelegatedCreateAndReopenAllocationFailuresRemainInsideTheResultBoundary) {
  for (std::uint8_t reopen = 0U; reopen < 2U; ++reopen) {
    SCOPED_TRACE(static_cast<std::uint32_t>(reopen));
    TemporaryDirectory directory;
    const GroupId group = group_id(static_cast<std::uint8_t>(0x65U + reopen));
    const RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
    if (reopen != 0U) {
      auto durable = DurableMultiRaftRuntime::create_new(1U, log_config, {{group, {1U}}});
      ASSERT_TRUE(durable.has_value()) << durable.error().to_string();
      auto elected = durable->execute_batch({{group, StartElectionOperation{}}});
      ASSERT_TRUE(elected.has_value()) << elected.error().to_string();
      ASSERT_TRUE(durable->close().is_ok());
    }

    std::vector<RaftGroupConfiguration> groups{{group, {1U}}};
    std::size_t observed{};
    auto owner = run_with_allocation_failure(0U, observed, [&] {
      return reopen == 0U
                 ? AsyncDurableMultiRaftRuntime::create_new(1U, log_config, std::move(groups))
                 : AsyncDurableMultiRaftRuntime::open_existing(1U, log_config, {},
                                                               std::move(groups));
    });
    EXPECT_EQ(observed, 1U);
    ASSERT_FALSE(owner.has_value());
    EXPECT_EQ(owner.error().code(), common::StatusCode::kResourceExhausted);

    if (reopen != 0U) {
      auto recovered = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, {{group, {1U}}});
      ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
      ASSERT_NE(recovered->find_group(group), nullptr);
      EXPECT_EQ(recovered->find_group(group)->current_term(), 1U);
      EXPECT_TRUE(recovered->close().is_ok());
    }
  }
}

TEST(AsyncDurableMultiRaftRuntimeAllocationFailureTest,
     BatchAdmissionClassifiesEveryAllocationAndRemainsReusable) {
  TemporaryDirectory directory;
  const GroupId group = group_id(0x62U);
  auto owner = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U}}});
  ASSERT_TRUE(owner.has_value()) << owner.error().to_string();
  bool reached_success{};
  std::size_t failure_count{};
  std::size_t successful_allocations{};

  for (std::size_t fail_after = 0U; fail_after < kMaximumAllocationSweep; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::vector<DurableRaftRequest> requests{{group, StartElectionOperation{}}};
    std::size_t observed{};
    auto admitted = run_with_allocation_failure(
        fail_after, observed, [&] { return owner->try_submit(std::move(requests)); });
    EXPECT_GT(observed, 0U);
    if (admitted.has_value()) {
      reached_success = true;
      successful_allocations = observed;
      auto completed = admitted->wait();
      ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
      ASSERT_EQ(completed->size(), 1U);
      EXPECT_TRUE(completed->front().status.is_ok());
      break;
    }
    ++failure_count;
    EXPECT_EQ(admitted.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(owner->is_accepting());
    EXPECT_EQ(owner->metrics().pending_batches, 0U);
  }

  EXPECT_TRUE(reached_success);
  EXPECT_EQ(failure_count, successful_allocations);
  EXPECT_EQ(owner->metrics().rejected_batches, failure_count);
  EXPECT_EQ(owner->metrics().admitted_batches, 1U);
  EXPECT_TRUE(owner->shutdown().is_ok());
}

TEST(AsyncDurableMultiRaftRuntimeAllocationFailureTest,
     ObservationAdmissionClassifiesEveryAllocationAndRemainsReusable) {
  TemporaryDirectory directory;
  const GroupId group = group_id(0x63U);
  auto owner = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U}}});
  ASSERT_TRUE(owner.has_value()) << owner.error().to_string();
  bool reached_success{};
  std::size_t failure_count{};
  std::size_t successful_allocations{};

  for (std::size_t fail_after = 0U; fail_after < kMaximumAllocationSweep; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::size_t observed{};
    auto admitted = run_with_allocation_failure(fail_after, observed,
                                                [&] { return owner->try_observe_group(group); });
    EXPECT_GT(observed, 0U);
    if (admitted.has_value()) {
      reached_success = true;
      successful_allocations = observed;
      auto completed = admitted->wait();
      ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
      ASSERT_EQ(completed->size(), 1U);
      EXPECT_TRUE(completed->front().observation.has_value());
      break;
    }
    ++failure_count;
    EXPECT_EQ(admitted.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(owner->is_accepting());
    EXPECT_EQ(owner->metrics().pending_batches, 0U);
  }

  EXPECT_TRUE(reached_success);
  EXPECT_EQ(failure_count, successful_allocations);
  EXPECT_EQ(owner->metrics().rejected_batches, failure_count);
  EXPECT_EQ(owner->metrics().admitted_batches, 1U);
  EXPECT_TRUE(owner->shutdown().is_ok());
}

TEST(AsyncDurableMultiRaftRuntimeAllocationFailureTest,
     ReclamationAdmissionClassifiesEveryAllocationAndRemainsReusable) {
  TemporaryDirectory directory;
  const GroupId group = group_id(0x64U);
  auto owner = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U}}});
  ASSERT_TRUE(owner.has_value()) << owner.error().to_string();
  bool reached_success{};
  std::size_t failure_count{};
  std::size_t successful_allocations{};

  for (std::size_t fail_after = 0U; fail_after < kMaximumAllocationSweep; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::size_t observed{};
    auto admitted = run_with_allocation_failure(
        fail_after, observed, [&] { return owner->try_checkpoint_and_reclaim(); });
    EXPECT_GT(observed, 0U);
    if (admitted.has_value()) {
      reached_success = true;
      successful_allocations = observed;
      auto completed = admitted->wait();
      ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
      break;
    }
    ++failure_count;
    EXPECT_EQ(admitted.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(owner->is_accepting());
    EXPECT_EQ(owner->metrics().pending_batches, 0U);
  }

  EXPECT_TRUE(reached_success);
  EXPECT_EQ(failure_count, successful_allocations);
  EXPECT_EQ(owner->metrics().rejected_reclamations, failure_count);
  EXPECT_EQ(owner->metrics().admitted_reclamations, 1U);
  EXPECT_TRUE(owner->shutdown().is_ok());
}

} // namespace
} // namespace chronos::raft
