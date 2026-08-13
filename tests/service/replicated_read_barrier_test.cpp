#include "chronos/service/replicated_read_barrier.hpp"

#include <chrono>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace chronos::service {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-read-barrier-XXXXXX").string();
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

[[nodiscard]] raft::GroupId group(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(static_cast<std::byte>(seed));
  return common::Uuid{bytes};
}

TEST(ReplicatedReadBarrierTest, ConfirmsSortedSingleVoterGroupsAfterCurrentTermNoops) {
  TemporaryDirectory directory;
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()},
      {{group(0x22U), {1U}}, {group(0x11U), {1U}}});
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  for (const raft::GroupId group_id : {group(0x22U), group(0x11U)}) {
    auto election = runtime->try_submit({{group_id, raft::StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value());
    ASSERT_TRUE(election->wait().has_value());
  }
  auto barrier =
      ReplicatedReadBarrier::create_local(std::addressof(*runtime), {group(0x22U), group(0x11U)});
  ASSERT_TRUE(barrier.has_value()) << barrier.error().to_string();
  auto ready = barrier->await();
  ASSERT_TRUE(ready.has_value()) << ready.error().to_string();
  ASSERT_EQ(ready->size(), 2U);
  EXPECT_EQ((*ready)[0].group_id, group(0x11U));
  EXPECT_EQ((*ready)[1].group_id, group(0x22U));
  for (const raft::GroupReadBarrier& group_barrier : *ready) {
    EXPECT_EQ(group_barrier.barrier.term, 1U);
    EXPECT_EQ(group_barrier.barrier.read_index, 1U);
    EXPECT_NE(group_barrier.barrier.context, 0U);
  }
  auto authority = barrier->await_authority();
  ASSERT_TRUE(authority.has_value()) << authority.error().to_string();
  ASSERT_EQ(authority->size(), 2U);
  for (const ReplicatedReadAuthority& proof : *authority) {
    EXPECT_EQ(proof.observation.group_id, proof.barrier.group_id);
    EXPECT_EQ(proof.observation.role, raft::Role::kLeader);
    EXPECT_EQ(proof.observation.leader_id, proof.observation.node_id);
    EXPECT_EQ(proof.observation.current_term, proof.barrier.barrier.term);
    EXPECT_GE(proof.observation.commit_index, proof.barrier.barrier.read_index);
  }
  EXPECT_TRUE(barrier->shutdown().is_ok());
  EXPECT_FALSE(barrier->await().has_value());
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

TEST(ReplicatedReadBarrierTest, TransportedWaitIsBoundedWithoutAPollOwner) {
  auto barrier = ReplicatedReadBarrier::create_transported(
      {group(0x33U)}, {.maximum_groups = 1U, .request_timeout = std::chrono::milliseconds{5}});
  ASSERT_TRUE(barrier.has_value());
  const auto before = std::chrono::steady_clock::now();
  auto ready = barrier->await();
  const auto elapsed = std::chrono::steady_clock::now() - before;
  ASSERT_FALSE(ready.has_value());
  EXPECT_EQ(ready.error().code(), common::StatusCode::kUnavailable);
  EXPECT_GE(elapsed, std::chrono::milliseconds{5});
}

TEST(ReplicatedReadBarrierTest, RejectsDuplicateAndNilGroups) {
  EXPECT_EQ(ReplicatedReadBarrier::create_transported({group(0x44U), group(0x44U)}).error().code(),
            common::StatusCode::kAlreadyExists);
  EXPECT_EQ(ReplicatedReadBarrier::create_transported({raft::GroupId{}}).error().code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::service
