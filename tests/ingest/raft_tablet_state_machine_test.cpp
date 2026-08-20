#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/ingest/raft_tablet_state_machine.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include <array>
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
    const RetryIdentity retry_identity{test::request_id<ClientId>(1U),
                                       test::request_id<ClientBatchId>(33U)};
    const auto first_outcome = machine->tablet().snapshot()->retry_outcome(retry_identity);
    ASSERT_NE(first_outcome, nullptr);
    EXPECT_EQ(durable.find_group(group_id())->applied_index(), 1U);
    EXPECT_EQ(machine->compact_applied_prefix(1U, 1U, {}).error().code(),
              common::StatusCode::kNotSupported);
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
    const TabletSnapshot duplicate_snapshot = machine->tablet().snapshot().value();
    EXPECT_EQ(duplicate_snapshot.visible_row_count(), 2U);
    EXPECT_EQ(duplicate_snapshot.retry_outcome(retry_identity).get(), first_outcome.get());
    EXPECT_EQ(duplicate_snapshot.active_generation().applied_position(),
              head::HeadCommitPosition::raft(group_id(), 1U));
    EXPECT_EQ(duplicate_snapshot.applied_position(),
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

TEST(RaftTabletStateMachineTest, RebuildsCompactedPrefixThenCommittedSuffixFromInstalledSnapshot) {
  TemporaryDirectory directory;
  const std::filesystem::path log_directory = directory.path() / "raft";
  const std::filesystem::path snapshot_directory = directory.path() / "snapshots";
  ASSERT_TRUE(std::filesystem::create_directories(log_directory));
  ASSERT_TRUE(std::filesystem::create_directories(snapshot_directory));
  const raft::RaftPersistentLogConfig log_config{.directory_path = log_directory.string()};
  const std::vector<std::byte> payload = command();
  const std::vector<std::byte> suffix_payload = command(2U);
  std::array<std::byte, 32U> first_part_set_checksum{};
  first_part_set_checksum.front() = std::byte{0xA5U};
  std::array<std::byte, 32U> second_part_set_checksum{};
  second_part_set_checksum.back() = std::byte{0x5AU};

  {
    raft::DurableMultiRaftRuntime durable = runtime(log_config, {1U});
    ASSERT_TRUE(durable.execute_batch({{group_id(), raft::StartElectionOperation{}}}).has_value());
    auto snapshot_storage = RaftTabletSnapshotStorage::create(
        {.directory_path = snapshot_directory.string(), .group_id = group_id()});
    ASSERT_TRUE(snapshot_storage.has_value()) << snapshot_storage.error().to_string();
    auto machine = RaftTabletStateMachine::recover(
        group_id(), durable, std::move(*snapshot_storage), retry_directory(), tablet(), schemas());
    ASSERT_TRUE(machine.has_value()) << machine.error().to_string();
    ASSERT_TRUE(
        durable
            .execute_batch(
                {{group_id(), raft::ProposeOperation{kRaftColumnarAppendEntryType, payload}}})
            .has_value());
    ASSERT_TRUE(machine->apply_committed().has_value());
    ASSERT_EQ(durable.find_group(group_id())->applied_index(), 1U);

    auto compacted = machine->compact_applied_prefix(1U, 1U, first_part_set_checksum);
    ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
    EXPECT_EQ(compacted->snapshot.last_included_index, 1U);
    EXPECT_EQ(compacted->snapshot.last_included_term, 1U);
    EXPECT_EQ(compacted->snapshot.manifest_generation, 1U);
    EXPECT_EQ(compacted->snapshot.part_set_checksum, first_part_set_checksum);
    EXPECT_EQ(compacted->snapshot.configuration_index, 0U);
    EXPECT_EQ(compacted->snapshot.voters, std::vector<raft::NodeId>{1U});
    EXPECT_EQ(compacted->application_entries, 1U);
    EXPECT_FALSE(compacted->application_snapshot_already_present);
    EXPECT_EQ(durable.find_group(group_id())->persistent_state().snapshot, compacted->snapshot);
    EXPECT_TRUE(durable.find_group(group_id())->persistent_state().log.empty());

    ASSERT_TRUE(
        durable
            .execute_batch(
                {{group_id(), raft::ProposeOperation{kRaftColumnarAppendEntryType, payload}}})
            .has_value());
    EXPECT_EQ(durable.find_group(group_id())->commit_index(), 2U);
    EXPECT_EQ(durable.find_group(group_id())->applied_index(), 1U);
    ASSERT_TRUE(machine->apply_committed().has_value());
    auto compacted_again = machine->compact_applied_prefix(2U, 2U, second_part_set_checksum);
    ASSERT_TRUE(compacted_again.has_value()) << compacted_again.error().to_string();
    EXPECT_EQ(compacted_again->snapshot.last_included_index, 2U);
    EXPECT_EQ(compacted_again->snapshot.part_set_checksum, second_part_set_checksum);
    EXPECT_EQ(compacted_again->application_entries, 2U);
    EXPECT_TRUE(durable.find_group(group_id())->persistent_state().log.empty());
    auto reclaimed = machine->reclaim_obsolete_snapshots();
    ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
    EXPECT_EQ(reclaimed->authoritative_index, 2U);
    EXPECT_EQ(reclaimed->reclaimed_files, 1U);
    EXPECT_FALSE(
        std::filesystem::exists(snapshot_directory / "snapshot-00000000000000000001.rtas"));
    EXPECT_TRUE(std::filesystem::exists(snapshot_directory / "snapshot-00000000000000000002.rtas"));

    ASSERT_TRUE(
        durable
            .execute_batch({{group_id(),
                             raft::ProposeOperation{kRaftColumnarAppendEntryType, suffix_payload}}})
            .has_value());
    EXPECT_EQ(durable.find_group(group_id())->commit_index(), 3U);
    EXPECT_EQ(durable.find_group(group_id())->applied_index(), 2U);
    ASSERT_TRUE(durable.close().is_ok());
  }

  auto reopened = raft::DurableMultiRaftRuntime::open_existing(
      1U, log_config, {}, {{.group_id = group_id(), .voters = {1U}}});
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->find_group(group_id())->persistent_state().snapshot.part_set_checksum,
            second_part_set_checksum);
  auto missing = RaftTabletStateMachine::recover(group_id(), *reopened, retry_directory(), tablet(),
                                                 schemas());
  ASSERT_FALSE(missing.has_value());
  EXPECT_EQ(missing.error().code(), common::StatusCode::kNotSupported);

  auto snapshot_storage = RaftTabletSnapshotStorage::open_existing(
      {.directory_path = snapshot_directory.string(), .group_id = group_id()});
  ASSERT_TRUE(snapshot_storage.has_value()) << snapshot_storage.error().to_string();
  auto installed = snapshot_storage->load(2U);
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  EXPECT_EQ(installed->snapshot.raft_snapshot,
            reopened->find_group(group_id())->persistent_state().snapshot);
  EXPECT_EQ(installed->snapshot.entries.size(), 2U);
  auto rebuilt = RaftTabletStateMachine::recover(
      group_id(), *reopened, std::move(*snapshot_storage), retry_directory(), tablet(), schemas());
  ASSERT_TRUE(rebuilt.has_value()) << rebuilt.error().to_string();
  auto rebuilt_snapshot = rebuilt->tablet().snapshot();
  ASSERT_TRUE(rebuilt_snapshot.has_value()) << rebuilt_snapshot.error().to_string();
  EXPECT_EQ(rebuilt_snapshot->visible_row_count(), 4U);
  EXPECT_EQ(rebuilt_snapshot->retry_entry_count(), 2U);
  EXPECT_EQ(rebuilt_snapshot->applied_position(), head::HeadCommitPosition::raft(group_id(), 3U));
  EXPECT_EQ(reopened->find_group(group_id())->applied_index(), 3U);
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

TEST(RaftTabletStateMachineTest, AdvancesAcrossCommittedInternalEntries) {
  TemporaryDirectory directory;
  const std::filesystem::path log_directory = directory.path() / "raft";
  const std::filesystem::path snapshot_directory = directory.path() / "snapshots";
  ASSERT_TRUE(std::filesystem::create_directories(log_directory));
  ASSERT_TRUE(std::filesystem::create_directories(snapshot_directory));
  const raft::RaftPersistentLogConfig log_config{.directory_path = log_directory.string()};
  raft::DurableMultiRaftRuntime durable = runtime(log_config, {1U, 2U});
  auto snapshot_storage = RaftTabletSnapshotStorage::create(
      {.directory_path = snapshot_directory.string(), .group_id = group_id()});
  ASSERT_TRUE(snapshot_storage.has_value()) << snapshot_storage.error().to_string();
  auto machine = RaftTabletStateMachine::recover(group_id(), durable, std::move(*snapshot_storage),
                                                 retry_directory(), tablet(), schemas());
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
  ASSERT_TRUE(durable.execute_batch({{group_id(), raft::StartElectionOperation{}}}).has_value());
  ASSERT_TRUE(
      durable.execute_batch({{group_id(), raft::CommitCurrentTermOperation{}}}).has_value());

  auto membership = machine->apply_committed();
  ASSERT_TRUE(membership.has_value()) << membership.error().to_string();
  EXPECT_EQ(membership->first_applied_index, 1U);
  EXPECT_EQ(membership->last_applied_index, 3U);
  EXPECT_EQ(membership->applied_entries, 0U);
  EXPECT_EQ(durable.find_group(group_id())->applied_index(), 3U);
  EXPECT_EQ(machine->tablet().snapshot()->visible_row_count(), 0U);
  EXPECT_EQ(machine->tablet().snapshot()->applied_position(),
            head::HeadCommitPosition::raft(group_id(), 3U));
  auto compacted = machine->compact_applied_prefix(3U, 1U, {});
  ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
  EXPECT_EQ(compacted->application_entries, 0U);
  EXPECT_EQ(compacted->snapshot.configuration_index, 2U);
  EXPECT_EQ(compacted->snapshot.voters, std::vector<raft::NodeId>{1U});
  EXPECT_TRUE(durable.find_group(group_id())->persistent_state().log.empty());

  ASSERT_TRUE(durable
                  .execute_batch({{group_id(), raft::ProposeOperation{kRaftColumnarAppendEntryType,
                                                                      command()}}})
                  .has_value());
  auto appended = machine->apply_committed();
  ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
  EXPECT_EQ(appended->first_applied_index, 4U);
  EXPECT_EQ(appended->last_applied_index, 4U);
  EXPECT_EQ(appended->applied_entries, 1U);
  EXPECT_EQ(machine->tablet().snapshot()->applied_position(),
            head::HeadCommitPosition::raft(group_id(), 4U));
}

} // namespace
} // namespace chronos::ingest
