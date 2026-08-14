#include "chronos/raft/runtime_timer_driver.hpp"

#include <chrono>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <vector>

namespace chronos::raft {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-raft-timer-driver-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] GroupId group(const std::byte seed = std::byte{1U}) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return GroupId{bytes};
}

class DeadlineSource final : public RaftElectionDeadlineSource {
public:
  common::Result<RaftTimerRuntime::TimePoint>
  next_election_deadline(const GroupId& group_id, const Term current_term,
                         const RaftTimerRuntime::TimePoint now) override {
    ++calls;
    last_group = group_id;
    last_term = current_term;
    return now + std::chrono::milliseconds{5};
  }

  std::size_t calls{};
  GroupId last_group;
  Term last_term{};
};

class ThrowingDeadlineSource final : public RaftElectionDeadlineSource {
public:
  common::Result<RaftTimerRuntime::TimePoint>
  next_election_deadline(const GroupId&, Term, RaftTimerRuntime::TimePoint) override {
    throw std::runtime_error{"deadline source failure"};
  }
};

[[nodiscard]] RaftGroupObservation follower(const GroupId& id) {
  return {.group_id = id, .node_id = 1U, .role = Role::kFollower, .current_term = 0U};
}

void drive_until_completed(RaftTimerDriver& driver, const RaftTimerDriver::TimePoint now,
                           const std::size_t expected) {
  for (std::size_t attempt = 0U; attempt < 100'000U; ++attempt) {
    ASSERT_TRUE(driver.drive(now).is_ok()) << driver.failure().to_string();
    if (driver.completed_actions() == expected)
      return;
    std::this_thread::yield();
  }
  FAIL() << "Raft timer driver completion did not become ready";
}

TEST(RaftTimerDriverTest, DrivesBootstrapElectionAndHeartbeatThroughDurableOwner) {
  using namespace std::chrono_literals;
  TemporaryDirectory directory;
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group(), {1U}}});
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  DeadlineSource deadlines;
  auto driver = RaftTimerDriver::create({.runtime = &*runtime,
                                         .election_deadlines = &deadlines,
                                         .limits = {.maximum_inflight_actions = 1U,
                                                    .maximum_completed_actions = 1U,
                                                    .timers = {.maximum_groups = 1U,
                                                               .maximum_actions_per_poll = 1U,
                                                               .heartbeat_interval = 2ms}}});
  ASSERT_TRUE(driver.has_value()) << driver.error().to_string();
  const auto start = RaftTimerDriver::TimePoint{};
  ASSERT_TRUE(driver->add_group(follower(group()), start).is_ok());
  EXPECT_EQ(driver->next_deadline(), std::optional{start + 5ms});
  EXPECT_EQ(deadlines.last_term, 0U);
  EXPECT_TRUE(driver->drive(start + 4ms).is_ok());
  EXPECT_EQ(driver->inflight_actions(), 0U);

  drive_until_completed(*driver, start + 5ms, 1U);
  EXPECT_EQ(driver->next_completed_sequence(), std::optional<std::uint64_t>{1U});
  auto election = driver->take_completed();
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  EXPECT_EQ(election->submission_sequence, 1U);
  EXPECT_EQ(election->action.kind, RaftTimerActionKind::kStartElection);
  ASSERT_TRUE(election->result.status.is_ok()) << election->result.status.to_string();
  const auto& election_transition = election->result.transition;
  if (!election_transition.has_value()) {
    ADD_FAILURE() << "election completion lacks its transition";
  } else {
    EXPECT_TRUE(election_transition.value().persistence.has_value());
  }
  EXPECT_EQ(election->observation.role, Role::kLeader);
  EXPECT_EQ(election->observation.current_term, 1U);
  EXPECT_EQ(driver->next_completed_sequence(), std::nullopt);
  EXPECT_EQ(driver->next_deadline(), std::optional{start + 7ms});

  drive_until_completed(*driver, start + 7ms, 1U);
  auto heartbeat = driver->take_completed();
  ASSERT_TRUE(heartbeat.has_value());
  EXPECT_EQ(heartbeat->submission_sequence, 2U);
  EXPECT_EQ(heartbeat->action.kind, RaftTimerActionKind::kHeartbeat);
  EXPECT_TRUE(heartbeat->result.status.is_ok());
  EXPECT_EQ(heartbeat->observation.role, Role::kLeader);
  ASSERT_TRUE(runtime->shutdown().is_ok());
}

TEST(RaftTimerDriverTest, ContainsElectionDeadlineSourceExceptions) {
  TemporaryDirectory directory;
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group(), {1U}}});
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  ThrowingDeadlineSource deadlines;
  auto driver = RaftTimerDriver::create({.runtime = &runtime.value(),
                                         .election_deadlines = &deadlines,
                                         .limits = {.maximum_inflight_actions = 1U,
                                                    .maximum_completed_actions = 1U,
                                                    .timers = {.maximum_groups = 1U}}});
  ASSERT_TRUE(driver.has_value()) << driver.error().to_string();

  const common::Status status = driver->add_group(follower(group()), RaftTimerDriver::TimePoint{});
  EXPECT_EQ(status.code(), common::StatusCode::kInternal);
  EXPECT_FALSE(driver->failed());
  EXPECT_FALSE(driver->next_deadline().has_value());
  ASSERT_TRUE(runtime->shutdown().is_ok());
}

TEST(RaftTimerDriverTest, CompletedQueueBackpressuresOwningResults) {
  using namespace std::chrono_literals;
  TemporaryDirectory directory;
  const GroupId first = group(std::byte{1U});
  const GroupId second = group(std::byte{2U});
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{first, {1U}}, {second, {1U}}});
  ASSERT_TRUE(runtime.has_value());
  DeadlineSource deadlines;
  auto driver = RaftTimerDriver::create({.runtime = &*runtime,
                                         .election_deadlines = &deadlines,
                                         .limits = {.maximum_inflight_actions = 2U,
                                                    .maximum_completed_actions = 1U,
                                                    .timers = {.maximum_groups = 2U,
                                                               .maximum_actions_per_poll = 2U,
                                                               .heartbeat_interval = 2ms}}});
  ASSERT_TRUE(driver.has_value());
  const auto start = RaftTimerDriver::TimePoint{};
  ASSERT_TRUE(driver->add_group(follower(first), start).is_ok());
  ASSERT_TRUE(driver->add_group(follower(second), start).is_ok());
  drive_until_completed(*driver, start + 5ms, 1U);
  EXPECT_LE(driver->inflight_actions(), 1U);
  EXPECT_TRUE(driver->next_completed_sequence().has_value());
  auto one = driver->take_completed();
  ASSERT_TRUE(one.has_value());
  drive_until_completed(*driver, start + 5ms, 1U);
  EXPECT_TRUE(driver->next_completed_sequence().has_value());
  auto two = driver->take_completed();
  ASSERT_TRUE(two.has_value());
  EXPECT_LT(one->submission_sequence, two->submission_sequence);
  EXPECT_NE(one->action.group_id, two->action.group_id);
  ASSERT_TRUE(runtime->shutdown().is_ok());
}

} // namespace
} // namespace chronos::raft
