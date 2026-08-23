#include "chronos/common/status.hpp"
#include "chronos/ingest/raft_tablet_snapshot.hpp"
#include "chronos/ingest/raft_tablet_snapshot_storage.hpp"
#include "chronos/ingest/raft_tablet_state_machine.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/node.hpp"
#include "ingest/tablet_snapshot_install_crash_fixture.hpp"
#include "ingest/tablet_snapshot_install_crash_protocol.hpp"
#include "wal/wal_crash_protocol.hpp"

#include <array>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

class CompactionCrashDirectory {
public:
  CompactionCrashDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-tablet-compaction-crash-XXXXXX")
            .string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr) {
      path_ = created;
      std::error_code error;
      static_cast<void>(std::filesystem::create_directory(path_ / "raft", error));
      if (!error)
        static_cast<void>(std::filesystem::create_directory(path_ / "snapshots", error));
      if (error) {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
        path_.clear();
      }
    }
  }

  ~CompactionCrashDirectory() {
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

struct TabletCompactionCrashPoint {
  std::string_view failpoint;
  bool application_snapshot_present;
  bool raft_snapshot_authoritative;
};

constexpr std::array<TabletCompactionCrashPoint, 10U> kCompactionCrashPoints{
    TabletCompactionCrashPoint{test::kAfterApplicationCompactionTemporaryCreate, false, false},
    TabletCompactionCrashPoint{test::kAfterApplicationCompactionWrite, false, false},
    TabletCompactionCrashPoint{test::kAfterApplicationCompactionReadback, false, false},
    TabletCompactionCrashPoint{test::kAfterApplicationCompactionFileSync, false, false},
    TabletCompactionCrashPoint{test::kAfterApplicationCompactionTemporaryClose, false, false},
    TabletCompactionCrashPoint{test::kAfterApplicationCompactionRename, true, false},
    TabletCompactionCrashPoint{test::kAfterApplicationCompactionDirectorySync, true, false},
    TabletCompactionCrashPoint{test::kAfterApplicationCompactionRaftWrite, true, true},
    TabletCompactionCrashPoint{test::kAfterApplicationCompactionRaftSync, true, true},
    TabletCompactionCrashPoint{test::kAfterApplicationCompactionSuccess, true, true},
};

void expect_application_snapshot(const RaftTabletApplicationSnapshot& snapshot) {
  EXPECT_EQ(snapshot.group_id, test::crash_group_id());
  EXPECT_EQ(snapshot.table_id, test::crash_compaction_schemas().front()->table_id());
  EXPECT_EQ(snapshot.tablet_id, test::crash_compaction_tablet_id());
  EXPECT_EQ(snapshot.raft_snapshot.last_included_index, 1U);
  EXPECT_EQ(snapshot.raft_snapshot.last_included_term, 1U);
  EXPECT_EQ(snapshot.raft_snapshot.manifest_generation, 1U);
  EXPECT_EQ(snapshot.raft_snapshot.part_set_checksum, (std::array<std::byte, 32U>{}));
  EXPECT_EQ(snapshot.raft_snapshot.configuration_index, 0U);
  EXPECT_EQ(snapshot.raft_snapshot.voters, std::vector<raft::NodeId>{4U});
  ASSERT_EQ(snapshot.entries.size(), 1U);
  EXPECT_EQ(snapshot.entries.front().index, 1U);
  EXPECT_EQ(snapshot.entries.front().term, 1U);
  EXPECT_EQ(snapshot.entries.front().payload, test::crash_compaction_command());
}

void expect_tablet_recovered(const RaftTabletStateMachine& machine) {
  auto snapshot = machine.tablet().snapshot();
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  EXPECT_EQ(snapshot->visible_row_count(), 2U);
  EXPECT_EQ(snapshot->applied_position(),
            head::HeadCommitPosition::raft(test::crash_group_id(), 1U));
}

class TabletSnapshotCompactionCrashMatrixTest
    : public ::testing::TestWithParam<TabletCompactionCrashPoint> {};

TEST_P(TabletSnapshotCompactionCrashMatrixTest,
       RecoversOrphanOrExactAuthorityAndConvergesAfterSigkill) {
  CompactionCrashDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const TabletCompactionCrashPoint point = GetParam();
  auto spawned = wal::test::CrashChildProcess::spawn(
      {.directory = directory.path(), .pause_after = std::string{point.failpoint}});
  ASSERT_TRUE(spawned.has_value()) << spawned.error().to_string();
  wal::test::CrashChildProcess child = std::move(*spawned);
  auto reached = child.wait_for("FAILPOINT");
  ASSERT_TRUE(reached.has_value()) << reached.error().to_string();
  ASSERT_EQ(reached->fields.size(), 1U);
  EXPECT_EQ(reached->fields.front(), point.failpoint);
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  {
    auto storage =
        RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto loaded = storage->load(1U);
    if (point.application_snapshot_present) {
      ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
      expect_application_snapshot(loaded->snapshot);
    } else {
      ASSERT_FALSE(loaded.has_value());
      EXPECT_EQ(loaded.error().code(), common::StatusCode::kNotFound);
    }
  }

  auto runtime = raft::DurableMultiRaftRuntime::open_existing(
      4U, test::crash_log_config(directory.path()), {}, test::crash_compaction_groups());
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  const raft::RaftNode* raft_node = runtime->find_group(test::crash_group_id());
  ASSERT_NE(raft_node, nullptr);
  EXPECT_EQ(raft_node->persistent_state().snapshot.last_included_index,
            point.raft_snapshot_authoritative ? 1U : 0U);

  auto storage =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  auto recovered = RaftTabletStateMachine::recover(
      test::crash_group_id(), *runtime, std::move(*storage),
      test::crash_compaction_retry_directory(), test::crash_compaction_tablet(),
      test::crash_compaction_schemas());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  std::optional<RaftTabletStateMachine> machine{std::move(*recovered)};
  expect_tablet_recovered(*machine);

  if (!point.raft_snapshot_authoritative) {
    auto compacted = machine->compact_applied_prefix(1U, 1U, {});
    ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
    EXPECT_EQ(compacted->application_snapshot_already_present, point.application_snapshot_present);
    EXPECT_EQ(compacted->application_entries, 1U);
  }
  raft_node = runtime->find_group(test::crash_group_id());
  ASSERT_NE(raft_node, nullptr);
  EXPECT_EQ(raft_node->persistent_state().snapshot.last_included_index, 1U);
  auto reclaimed = machine->reclaim_obsolete_snapshots();
  ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
  EXPECT_EQ(reclaimed->authoritative_index, 1U);
  EXPECT_EQ(reclaimed->reclaimed_files, 0U);
  machine.reset();
  ASSERT_TRUE(runtime->close().is_ok());

  auto repeated_runtime = raft::DurableMultiRaftRuntime::open_existing(
      4U, test::crash_log_config(directory.path()), {}, test::crash_compaction_groups());
  ASSERT_TRUE(repeated_runtime.has_value()) << repeated_runtime.error().to_string();
  auto repeated_storage =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
  ASSERT_TRUE(repeated_storage.has_value()) << repeated_storage.error().to_string();
  auto final_snapshot = repeated_storage->load(1U);
  ASSERT_TRUE(final_snapshot.has_value()) << final_snapshot.error().to_string();
  expect_application_snapshot(final_snapshot->snapshot);
  const raft::RaftNode* repeated_node = repeated_runtime->find_group(test::crash_group_id());
  ASSERT_NE(repeated_node, nullptr);
  EXPECT_EQ(repeated_node->persistent_state().snapshot, final_snapshot->snapshot.raft_snapshot);
  {
    auto repeated = RaftTabletStateMachine::recover(
        test::crash_group_id(), *repeated_runtime, std::move(*repeated_storage),
        test::crash_compaction_retry_directory(), test::crash_compaction_tablet(),
        test::crash_compaction_schemas());
    ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
    expect_tablet_recovered(*repeated);
  }
  ASSERT_TRUE(repeated_runtime->close().is_ok());
}

INSTANTIATE_TEST_SUITE_P(EveryCrossDomainDurabilityTransition,
                         TabletSnapshotCompactionCrashMatrixTest,
                         ::testing::ValuesIn(kCompactionCrashPoints),
                         [](const ::testing::TestParamInfo<TabletCompactionCrashPoint>& parameter) {
                           return std::string{parameter.param.failpoint};
                         });

} // namespace
} // namespace chronos::ingest
