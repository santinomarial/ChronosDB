#include "chronos/head/mutable_head.hpp"
#include "chronos/ingest/async_raft_tablet_application.hpp"
#include "chronos/raft/async_durable_runtime.hpp"
#include "chronos/service/replicated_ingest_database.hpp"
#include "service/replicated_ingest_database_crash_fixture.hpp"
#include "wal/wal_crash_protocol.hpp"

#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
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

void expect_recovered_publication(ReplicatedIngestDatabase& database,
                                  const raft::LogIndex applied_index) {
  auto catalog = database.ingest_runtime()->metadata_application()->catalog_snapshot();
  ASSERT_TRUE(catalog.has_value()) << catalog.error().to_string();
  ASSERT_EQ((*catalog)->schema_definitions.size(), 1U);
  ASSERT_EQ((*catalog)->tablet_group_bindings.size(), 1U);
  EXPECT_EQ((*catalog)->tablet_group_bindings.front().tablet_id, test::crash_tablet_id());
  EXPECT_EQ((*catalog)->tablet_group_bindings.front().group_id, test::crash_tablet_group());
  auto publication =
      database.ingest_runtime()->tablet_application()->snapshot(test::crash_tablet_group());
  ASSERT_TRUE(publication.has_value()) << publication.error().to_string();
  EXPECT_EQ(publication->visible_row_count(), 2U);
  EXPECT_EQ(publication->retry_entry_count(), 1U);
  EXPECT_EQ(publication->applied_position(),
            head::HeadCommitPosition::raft(test::crash_tablet_group(), applied_index));
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

  auto database = ReplicatedIngestDatabase::open_existing(
      {.bootstrap = test::existing_crash_bootstrap_config(directory.path()),
       .groups = test::crash_groups()});
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  expect_recovered_publication(*database, 1U);

  auto election = database->ingest_runtime()->runtime()->try_submit(
      {{test::crash_tablet_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_TRUE(election->wait().has_value());
  auto retry = database->ingest_runtime()->runtime()->try_submit(
      {{test::crash_tablet_group(),
        raft::ProposeOperation{ingest::kRaftColumnarAppendEntryType, test::crash_command()}}});
  ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
  ASSERT_TRUE(retry->wait().has_value());
  expect_recovered_publication(*database, 2U);
  ASSERT_TRUE(database->shutdown().is_ok());

  auto repeated = ReplicatedIngestDatabase::open_existing(
      {.bootstrap = test::existing_crash_bootstrap_config(directory.path()),
       .groups = test::crash_groups()});
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  expect_recovered_publication(*repeated, 2U);
  ASSERT_TRUE(repeated->shutdown().is_ok());
}

} // namespace
} // namespace chronos::service
