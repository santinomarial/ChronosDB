#include "chronos/head/mutable_head.hpp"
#include "chronos/ingest/async_raft_tablet_application.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/raft/async_durable_runtime.hpp"
#include "chronos/service/replicated_ingest_database.hpp"
#include "service/replicated_ingest_database_crash_fixture.hpp"
#include "wal/wal_crash_protocol.hpp"

#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>

namespace chronos::service {
namespace {

class ReplicatedDatabaseCrashDirectory {
public:
  ReplicatedDatabaseCrashDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-replicated-database-crash-XXXXXX")
            .string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }

  ~ReplicatedDatabaseCrashDirectory() {
    std::error_code ignored;
    if (!path_.empty())
      std::filesystem::remove_all(path_, ignored);
  }

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

struct ExpectedPublication {
  std::size_t rows{};
  std::size_t retries{};
  raft::LogIndex applied_index{};
};

void expect_recovered_publication(ReplicatedIngestDatabase& database,
                                  const ExpectedPublication expected) {
  auto catalog = database.ingest_runtime()->metadata_application()->catalog_snapshot();
  ASSERT_TRUE(catalog.has_value()) << catalog.error().to_string();
  ASSERT_EQ((*catalog)->schema_definitions.size(), 1U);
  ASSERT_EQ((*catalog)->tablet_group_bindings.size(), 1U);
  EXPECT_EQ((*catalog)->tablet_group_bindings.front().tablet_id, test::crash_tablet_id());
  EXPECT_EQ((*catalog)->tablet_group_bindings.front().group_id, test::crash_tablet_group());
  auto publication =
      database.ingest_runtime()->tablet_application()->snapshot(test::crash_tablet_group());
  ASSERT_TRUE(publication.has_value()) << publication.error().to_string();
  EXPECT_EQ(publication->visible_row_count(), expected.rows);
  EXPECT_EQ(publication->retry_entry_count(), expected.retries);
  EXPECT_EQ(publication->applied_position(),
            head::HeadCommitPosition::raft(test::crash_tablet_group(), expected.applied_index));
}

[[nodiscard]] common::Result<network::NetworkTask>
await_crash_response(ReplicatedIngestRuntime& runtime) {
  for (std::size_t attempt = 0U; attempt < 10'000U; ++attempt) {
    auto response = runtime.coordinator()->poll();
    if (!response.has_value())
      return common::make_unexpected(response.error());
    auto& available_response = *response;
    if (available_response.has_value())
      return std::move(*available_response);
    std::this_thread::yield();
  }
  return common::make_unexpected(
      common::Status{common::StatusCode::kUnavailable, "replicated crash-test response timed out"});
}

TEST(ReplicatedIngestDatabaseCrashTest,
     ReclaimsProcessOwnershipAndReplaysCommittedStateAfterSigkill) {
  ReplicatedDatabaseCrashDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  auto spawned = wal::test::CrashChildProcess::spawn({.directory = directory.path()});
  ASSERT_TRUE(spawned.has_value()) << spawned.error().to_string();
  wal::test::CrashChildProcess child = std::move(*spawned);
  auto ready = child.wait_for("READY");
  ASSERT_TRUE(ready.has_value()) << ready.error().to_string();
  ASSERT_EQ(ready->fields.size(), 2U);
  EXPECT_EQ(ready->fields[0], "2");
  EXPECT_EQ(ready->fields[1], "1");
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  auto database =
      ReplicatedIngestDatabase::open_existing(test::crash_database_config(directory.path(), false));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  expect_recovered_publication(*database, {.rows = 2U, .retries = 1U, .applied_index = 1U});

  auto election = database->ingest_runtime()->runtime()->try_submit(
      {{test::crash_tablet_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_TRUE(election->wait().has_value());
  auto retry = database->ingest_runtime()->runtime()->try_submit(
      {{test::crash_tablet_group(),
        raft::ProposeOperation{ingest::kRaftColumnarAppendEntryType, test::crash_command()}}});
  ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
  ASSERT_TRUE(retry->wait().has_value());
  expect_recovered_publication(*database, {.rows = 2U, .retries = 1U, .applied_index = 2U});
  ASSERT_TRUE(database->shutdown().is_ok());

  auto repeated =
      ReplicatedIngestDatabase::open_existing(test::crash_database_config(directory.path(), false));
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  expect_recovered_publication(*repeated, {.rows = 2U, .retries = 1U, .applied_index = 2U});
  ASSERT_TRUE(repeated->shutdown().is_ok());
}

TEST(ReplicatedIngestDatabaseCrashTest, ReclaimsSnapshotOwnershipAndRetainedSuffixesAfterSigkill) {
  ReplicatedDatabaseCrashDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  auto spawned =
      wal::test::CrashChildProcess::spawn({.directory = directory.path(), .compaction = true});
  ASSERT_TRUE(spawned.has_value()) << spawned.error().to_string();
  wal::test::CrashChildProcess child = std::move(*spawned);
  auto ready = child.wait_for("READY");
  ASSERT_TRUE(ready.has_value()) << ready.error().to_string();
  ASSERT_EQ(ready->fields.size(), 2U);
  EXPECT_EQ(ready->fields[0], "4");
  EXPECT_EQ(ready->fields[1], "2");
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  auto database =
      ReplicatedIngestDatabase::open_existing(test::crash_database_config(directory.path(), true));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  expect_recovered_publication(*database, {.rows = 4U, .retries = 2U, .applied_index = 2U});
  auto catalog = database->ingest_runtime()->metadata_application()->catalog_snapshot();
  ASSERT_TRUE(catalog.has_value()) << catalog.error().to_string();
  ASSERT_EQ((*catalog)->cluster_nodes.size(), 1U);
  EXPECT_EQ((*catalog)->cluster_nodes.front(),
            (raft::ClusterNodeMetadata{1U, "node-1.example:7000"}));

  auto election = database->ingest_runtime()->runtime()->try_submit(
      {{test::crash_tablet_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_TRUE(election->wait().has_value());
  auto retry = database->ingest_runtime()->runtime()->try_submit(
      {{test::crash_tablet_group(), raft::ProposeOperation{ingest::kRaftColumnarAppendEntryType,
                                                           test::crash_suffix_command()}}});
  ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
  ASSERT_TRUE(retry->wait().has_value());
  expect_recovered_publication(*database, {.rows = 4U, .retries = 2U, .applied_index = 3U});
  ASSERT_TRUE(database->shutdown().is_ok());

  auto repeated =
      ReplicatedIngestDatabase::open_existing(test::crash_database_config(directory.path(), true));
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  expect_recovered_publication(*repeated, {.rows = 4U, .retries = 2U, .applied_index = 3U});
  ASSERT_TRUE(repeated->shutdown().is_ok());
}

TEST(ReplicatedIngestDatabaseCrashTest, RecoversAmbiguousQuorumWriteAfterAdmissionWithoutResponse) {
  ReplicatedDatabaseCrashDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  auto spawned = wal::test::CrashChildProcess::spawn(
      {.directory = directory.path(), .pause_after = "after_ingest_admission"});
  ASSERT_TRUE(spawned.has_value()) << spawned.error().to_string();
  wal::test::CrashChildProcess child = std::move(*spawned);
  auto admitted = child.wait_for("ADMITTED", 2U);
  ASSERT_TRUE(admitted.has_value()) << admitted.error().to_string();
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  auto database =
      ReplicatedIngestDatabase::open_existing(test::crash_database_config(directory.path(), false));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  auto publication =
      database->ingest_runtime()->tablet_application()->snapshot(test::crash_tablet_group());
  ASSERT_TRUE(publication.has_value()) << publication.error().to_string();
  const bool absent = publication->visible_row_count() == 2U &&
                      publication->retry_entry_count() == 1U &&
                      publication->applied_position() ==
                          head::HeadCommitPosition::raft(test::crash_tablet_group(), 1U);
  const bool committed = publication->visible_row_count() == 4U &&
                         publication->retry_entry_count() == 2U &&
                         publication->applied_position() ==
                             head::HeadCommitPosition::raft(test::crash_tablet_group(), 2U);
  ASSERT_TRUE(absent || committed);
  const raft::LogIndex recovered_index = committed ? 2U : 1U;

  auto election = database->ingest_runtime()->runtime()->try_submit(
      {{test::crash_tablet_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_TRUE(election->wait().has_value());
  ASSERT_TRUE(database->ingest_runtime()
                  ->coordinator()
                  ->admit(test::crash_request(test::crash_suffix_command(), 3U))
                  .is_ok());
  auto response = await_crash_response(*database->ingest_runtime());
  ASSERT_TRUE(response.has_value()) << response.error().to_string();
  ASSERT_EQ(response->frame.header.message_type,
            network::MessageType::kQuorumSyncIngestAcknowledgement);
  EXPECT_EQ(response->frame.header.request_id, 3U);
  auto acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(response->frame.payload);
  ASSERT_TRUE(acknowledgement.has_value()) << acknowledgement.error().to_string();
  EXPECT_EQ(acknowledgement->group_id, test::crash_tablet_group());
  EXPECT_EQ(acknowledgement->outcome,
            committed ? network::IngestOutcome::kMatchingRetry : network::IngestOutcome::kApplied);
  EXPECT_EQ(acknowledgement->log_index, recovered_index + 1U);
  expect_recovered_publication(*database,
                               {.rows = 4U, .retries = 2U, .applied_index = recovered_index + 1U});
  ASSERT_TRUE(database->shutdown().is_ok());

  auto repeated =
      ReplicatedIngestDatabase::open_existing(test::crash_database_config(directory.path(), false));
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  expect_recovered_publication(*repeated,
                               {.rows = 4U, .retries = 2U, .applied_index = recovered_index + 1U});
  ASSERT_TRUE(repeated->shutdown().is_ok());
}

} // namespace
} // namespace chronos::service
