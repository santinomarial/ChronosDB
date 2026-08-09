#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/ingest/raft_tablet_state_machine.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-raft-tablet-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr) {
      path_ = created;
    }
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

[[nodiscard]] raft::GroupId group_id() {
  common::Uuid::Bytes bytes{};
  bytes.fill(std::byte{0x44U});
  return raft::GroupId{bytes};
}

[[nodiscard]] schema::TabletId tablet_id() {
  return columnar::test::id<schema::TabletId>(70U);
}

[[nodiscard]] TabletState tablet() {
  return TabletState::create(
             columnar::test::batch_schema(), tablet_id(),
             TabletStateConfig{
                 .head_capacity = {.row_capacity = 8U, .variable_value_bytes = {0U, 8U, 0U}},
                 .maximum_schema_versions = 1U,
                 .maximum_sealed_generations = 2U,
                 .maximum_retry_entries = 8U})
      .value();
}

[[nodiscard]] RetryDirectory retry_directory() {
  return RetryDirectory::create({.maximum_entries = 8U}).value();
}

[[nodiscard]] std::vector<std::shared_ptr<const schema::TableSchema>> schemas() {
  return {columnar::test::batch_schema()};
}

[[nodiscard]] std::vector<std::byte> command(const std::uint8_t seed = 1U) {
  auto batch = columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                                    columnar::test::batch_columns())
                   .value();
  const auto encoded_batch = columnar::encode_columnar_batch_v1(batch).value();
  const auto encoded =
      encode_columnar_append_v1({.client_id = test::request_id<ClientId>(seed),
                                 .client_batch_id = test::request_id<ClientBatchId>(seed + 32U),
                                 .tablet_id = tablet_id()},
                                encoded_batch)
          .value();
  return std::vector<std::byte>{encoded.bytes().begin(), encoded.bytes().end()};
}

[[nodiscard]] raft::DurableMultiRaftRuntime runtime(const raft::RaftPersistentLogConfig& log_config,
                                                    std::vector<raft::NodeId> voters) {
  return raft::DurableMultiRaftRuntime::create_new(
             1U, log_config, {{.group_id = group_id(), .voters = std::move(voters)}})
      .value();
}

TEST(RaftTabletStateMachineTest, AppliesCommittedEntriesOnceAndRebuildsFromRetainedLog) {
  TemporaryDirectory directory;
  const raft::RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  raft::DurableMultiRaftRuntime durable = runtime(log_config, {1U});
  ASSERT_TRUE(durable.execute_batch({{group_id(), raft::StartElectionOperation{}}}).has_value());

  {
    auto machine = RaftTabletStateMachine::recover(group_id(), durable, retry_directory(), tablet(),
                                                   schemas());
    ASSERT_TRUE(machine.has_value()) << machine.error().to_string();
    const std::vector<std::byte> payload = command();
    ASSERT_TRUE(
        durable
            .execute_batch(
                {{group_id(), raft::ProposeOperation{kRaftColumnarAppendEntryType, payload}}})
            .has_value());
    EXPECT_EQ(durable.find_group(group_id())->commit_index(), 1U);
    EXPECT_EQ(durable.find_group(group_id())->applied_index(), 0U);
    EXPECT_EQ(machine->tablet().snapshot()->visible_row_count(), 0U);
    EXPECT_EQ(machine->prove_applied_quorum_sync(1U).error().code(),
              common::StatusCode::kUnavailable);

    auto first = machine->apply_committed();
    ASSERT_TRUE(first.has_value()) << first.error().to_string();
    EXPECT_EQ(first->first_applied_index, 1U);
    EXPECT_EQ(first->last_applied_index, 1U);
    EXPECT_EQ(first->applied_entries, 1U);
    EXPECT_EQ(first->matching_retries, 0U);
    EXPECT_EQ(machine->tablet().snapshot()->visible_row_count(), 2U);
    EXPECT_EQ(machine->tablet().snapshot()->applied_position(),
              head::HeadCommitPosition::raft(group_id(), 1U));
    EXPECT_EQ(durable.find_group(group_id())->applied_index(), 1U);
    const auto acknowledged = machine->prove_applied_quorum_sync(1U);
    ASSERT_TRUE(acknowledged.has_value()) << acknowledged.error().to_string();
    EXPECT_EQ(acknowledged->log_index, 1U);
    EXPECT_EQ(acknowledged->group_id, group_id());

    ASSERT_TRUE(
        durable
            .execute_batch(
                {{group_id(), raft::ProposeOperation{kRaftColumnarAppendEntryType, payload}}})
            .has_value());
    auto duplicate = machine->apply_committed();
    ASSERT_TRUE(duplicate.has_value()) << duplicate.error().to_string();
    EXPECT_EQ(duplicate->applied_entries, 1U);
    EXPECT_EQ(duplicate->matching_retries, 1U);
    EXPECT_EQ(machine->tablet().snapshot()->visible_row_count(), 2U);
    EXPECT_EQ(machine->tablet().snapshot()->applied_position(),
              head::HeadCommitPosition::raft(group_id(), 2U));
    EXPECT_EQ(durable.find_group(group_id())->applied_index(), 2U);
  }

  const std::uint64_t durable_sequence = durable.durable_physical_sequence();
  ASSERT_TRUE(durable.close().is_ok());
  auto reopened = raft::DurableMultiRaftRuntime::open_existing(
      1U, log_config, {}, {{.group_id = group_id(), .voters = {1U}}});
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  auto rebuilt = RaftTabletStateMachine::recover(group_id(), *reopened, retry_directory(), tablet(),
                                                 schemas());
  ASSERT_TRUE(rebuilt.has_value()) << rebuilt.error().to_string();
  EXPECT_EQ(rebuilt->tablet().snapshot()->visible_row_count(), 2U);
  EXPECT_EQ(rebuilt->tablet().snapshot()->applied_position(),
            head::HeadCommitPosition::raft(group_id(), 2U));
  EXPECT_EQ(reopened->durable_physical_sequence(), durable_sequence);
}

TEST(RaftTabletStateMachineTest, KeepsAnAppendedButUncommittedEntryInvisible) {
  TemporaryDirectory directory;
  const raft::RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  raft::DurableMultiRaftRuntime durable = runtime(log_config, {1U, 2U, 3U});
  auto machine =
      RaftTabletStateMachine::recover(group_id(), durable, retry_directory(), tablet(), schemas());
  ASSERT_TRUE(machine.has_value()) << machine.error().to_string();
  ASSERT_TRUE(durable.execute_batch({{group_id(), raft::StartElectionOperation{}}}).has_value());
  ASSERT_TRUE(
      durable
          .execute_batch(
              {{group_id(), raft::ReceiveOperation{2U, raft::RequestVoteResponse{1U, true}}}})
          .has_value());
  ASSERT_EQ(durable.find_group(group_id())->role(), raft::Role::kLeader);
  ASSERT_TRUE(durable
                  .execute_batch({{group_id(), raft::ProposeOperation{kRaftColumnarAppendEntryType,
                                                                      command()}}})
                  .has_value());
  EXPECT_EQ(durable.find_group(group_id())->last_log_index(), 1U);
  EXPECT_EQ(durable.find_group(group_id())->commit_index(), 0U);
  const auto invisible = machine->apply_committed();
  ASSERT_TRUE(invisible.has_value());
  EXPECT_EQ(invisible->applied_entries, 0U);
  EXPECT_EQ(machine->tablet().snapshot()->visible_row_count(), 0U);

  ASSERT_TRUE(durable
                  .execute_batch({{group_id(),
                                   raft::ReceiveOperation{
                                       2U, raft::AppendEntriesResponse{.term = 1U,
                                                                       .success = true,
                                                                       .match_index = 1U}}}})
                  .has_value());
  ASSERT_EQ(durable.find_group(group_id())->commit_index(), 1U);
  const auto visible = machine->apply_committed();
  ASSERT_TRUE(visible.has_value()) << visible.error().to_string();
  EXPECT_EQ(visible->applied_entries, 1U);
  EXPECT_EQ(machine->tablet().snapshot()->visible_row_count(), 2U);
}

TEST(RaftTabletStateMachineTest, FailsClosedOnCorruptCommittedCommandBytes) {
  TemporaryDirectory directory;
  const raft::RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  raft::DurableMultiRaftRuntime durable = runtime(log_config, {1U});
  auto machine =
      RaftTabletStateMachine::recover(group_id(), durable, retry_directory(), tablet(), schemas());
  ASSERT_TRUE(machine.has_value());
  ASSERT_TRUE(durable.execute_batch({{group_id(), raft::StartElectionOperation{}}}).has_value());
  ASSERT_TRUE(durable
                  .execute_batch(
                      {{group_id(), raft::ProposeOperation{kRaftColumnarAppendEntryType,
                                                           {std::byte{0x01U}, std::byte{0x02U}}}}})
                  .has_value());
  const auto applied = machine->apply_committed();
  ASSERT_FALSE(applied.has_value());
  EXPECT_EQ(applied.error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(machine->failed());
  EXPECT_TRUE(machine->tablet().metrics().failed);
  EXPECT_EQ(durable.find_group(group_id())->applied_index(), 0U);
}

TEST(RaftTabletStateMachineTest, AdvancesAcrossCommittedMembershipEntries) {
  TemporaryDirectory directory;
  const raft::RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  raft::DurableMultiRaftRuntime durable = runtime(log_config, {1U, 2U});
  auto machine =
      RaftTabletStateMachine::recover(group_id(), durable, retry_directory(), tablet(), schemas());
  ASSERT_TRUE(machine.has_value()) << machine.error().to_string();
  ASSERT_TRUE(durable.execute_batch({{group_id(), raft::StartElectionOperation{}}}).has_value());
  ASSERT_TRUE(
      durable
          .execute_batch(
              {{group_id(), raft::ReceiveOperation{2U, raft::RequestVoteResponse{1U, true}}}})
          .has_value());
  ASSERT_TRUE(durable.execute_batch({{group_id(), raft::BeginMembershipChangeOperation{{1U}}}})
                  .has_value());
  ASSERT_TRUE(durable
                  .execute_batch({{group_id(),
                                   raft::ReceiveOperation{
                                       2U, raft::AppendEntriesResponse{.term = 1U,
                                                                       .success = true,
                                                                       .match_index = 1U}}}})
                  .has_value());
  ASSERT_TRUE(
      durable.execute_batch({{group_id(), raft::FinalizeMembershipChangeOperation{}}}).has_value());
  ASSERT_TRUE(durable
                  .execute_batch({{group_id(),
                                   raft::ReceiveOperation{
                                       2U, raft::AppendEntriesResponse{.term = 1U,
                                                                       .success = true,
                                                                       .match_index = 2U}}}})
                  .has_value());

  auto membership = machine->apply_committed();
  ASSERT_TRUE(membership.has_value()) << membership.error().to_string();
  EXPECT_EQ(membership->first_applied_index, 1U);
  EXPECT_EQ(membership->last_applied_index, 2U);
  EXPECT_EQ(membership->applied_entries, 0U);
  EXPECT_EQ(durable.find_group(group_id())->applied_index(), 2U);
  EXPECT_EQ(machine->tablet().snapshot()->visible_row_count(), 0U);

  ASSERT_TRUE(durable
                  .execute_batch({{group_id(), raft::ProposeOperation{kRaftColumnarAppendEntryType,
                                                                      command()}}})
                  .has_value());
  auto appended = machine->apply_committed();
  ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
  EXPECT_EQ(appended->first_applied_index, 3U);
  EXPECT_EQ(appended->last_applied_index, 3U);
  EXPECT_EQ(appended->applied_entries, 1U);
  EXPECT_EQ(machine->tablet().snapshot()->applied_position(),
            head::HeadCommitPosition::raft(group_id(), 3U));
}

} // namespace
} // namespace chronos::ingest
