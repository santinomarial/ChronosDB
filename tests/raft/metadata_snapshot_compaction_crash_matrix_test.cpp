#include "chronos/common/status.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/metadata_runtime.hpp"
#include "chronos/raft/metadata_snapshot.hpp"
#include "chronos/raft/metadata_snapshot_storage.hpp"
#include "chronos/raft/node.hpp"
#include "raft/metadata_snapshot_install_crash_fixture.hpp"
#include "raft/metadata_snapshot_install_crash_protocol.hpp"
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

namespace chronos::raft {
namespace {

class CompactionCrashDirectory {
public:
  CompactionCrashDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-metadata-compaction-crash-XXXXXX")
            .string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
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

struct MetadataCompactionCrashPoint {
  std::string_view failpoint;
  bool application_snapshot_present;
  bool raft_snapshot_authoritative;
};

constexpr std::array<MetadataCompactionCrashPoint, 10U> kCompactionCrashPoints{
    MetadataCompactionCrashPoint{test::kAfterMetadataCompactionTemporaryCreate, false, false},
    MetadataCompactionCrashPoint{test::kAfterMetadataCompactionWrite, false, false},
    MetadataCompactionCrashPoint{test::kAfterMetadataCompactionReadback, false, false},
    MetadataCompactionCrashPoint{test::kAfterMetadataCompactionFileSync, false, false},
    MetadataCompactionCrashPoint{test::kAfterMetadataCompactionTemporaryClose, false, false},
    MetadataCompactionCrashPoint{test::kAfterMetadataCompactionRename, true, false},
    MetadataCompactionCrashPoint{test::kAfterMetadataCompactionDirectorySync, true, false},
    MetadataCompactionCrashPoint{test::kAfterMetadataCompactionRaftWrite, true, true},
    MetadataCompactionCrashPoint{test::kAfterMetadataCompactionRaftSync, true, true},
    MetadataCompactionCrashPoint{test::kAfterMetadataCompactionSuccess, true, true},
};

void expect_application_snapshot(const MetadataApplicationSnapshot& snapshot) {
  EXPECT_EQ(snapshot.group_id, test::metadata_crash_group_id());
  EXPECT_EQ(snapshot.raft_snapshot.last_included_index, 1U);
  EXPECT_EQ(snapshot.raft_snapshot.last_included_term, 1U);
  EXPECT_EQ(snapshot.raft_snapshot.manifest_generation, 1U);
  EXPECT_EQ(snapshot.raft_snapshot.configuration_index, 0U);
  EXPECT_EQ(snapshot.raft_snapshot.voters, std::vector<NodeId>{1U});
  ASSERT_EQ(snapshot.entries.size(), 1U);
  EXPECT_EQ(snapshot.entries.front().index, 1U);
  EXPECT_EQ(snapshot.entries.front().term, 1U);
  EXPECT_EQ(snapshot.entries.front().type, kRaftMetadataCommandEntryType);
  EXPECT_EQ(snapshot.entries.front().payload, test::metadata_compaction_proposal().payload);
}

void expect_catalog_recovered(const DurableMetadataStateMachine& metadata) {
  EXPECT_EQ(metadata.state().applied_index(), 1U);
  const ClusterNodeMetadata* const node = metadata.state().find_node(1U);
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->endpoint, "node-1");
}

class MetadataSnapshotCompactionCrashMatrixTest
    : public ::testing::TestWithParam<MetadataCompactionCrashPoint> {};

TEST_P(MetadataSnapshotCompactionCrashMatrixTest,
       RecoversOrphanOrExactAuthorityAndConvergesAfterSigkill) {
  CompactionCrashDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const MetadataCompactionCrashPoint point = GetParam();
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
    auto storage = MetadataSnapshotStorage::open_existing(
        test::metadata_compaction_storage_config(directory.path()));
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

  const RaftPersistentLogConfig log_config = test::metadata_compaction_log_config(directory.path());
  const std::vector<RaftGroupConfiguration> groups = test::metadata_crash_groups();
  auto runtime = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  const RaftNode* raft_node = runtime->find_group(test::metadata_crash_group_id());
  ASSERT_NE(raft_node, nullptr);
  EXPECT_EQ(raft_node->persistent_state().snapshot.last_included_index,
            point.raft_snapshot_authoritative ? 1U : 0U);

  auto storage = MetadataSnapshotStorage::open_existing(
      test::metadata_compaction_storage_config(directory.path()));
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  auto recovered = DurableMetadataStateMachine::recover(test::metadata_crash_group_id(), *runtime,
                                                        std::move(*storage));
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  std::optional<DurableMetadataStateMachine> metadata{std::move(*recovered)};
  expect_catalog_recovered(*metadata);

  if (!point.raft_snapshot_authoritative) {
    auto compacted = metadata->compact_applied_prefix(1U);
    ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
    EXPECT_EQ(compacted->application_snapshot_already_present, point.application_snapshot_present);
    EXPECT_EQ(compacted->application_entries, 1U);
  }
  raft_node = runtime->find_group(test::metadata_crash_group_id());
  ASSERT_NE(raft_node, nullptr);
  EXPECT_EQ(raft_node->persistent_state().snapshot.last_included_index, 1U);
  auto reclaimed = metadata->reclaim_obsolete_snapshots();
  ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
  EXPECT_EQ(reclaimed->authoritative_index, 1U);
  EXPECT_EQ(reclaimed->reclaimed_files, 0U);
  metadata.reset();
  ASSERT_TRUE(runtime->close().is_ok());

  auto repeated_runtime = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_TRUE(repeated_runtime.has_value()) << repeated_runtime.error().to_string();
  auto repeated_storage = MetadataSnapshotStorage::open_existing(
      test::metadata_compaction_storage_config(directory.path()));
  ASSERT_TRUE(repeated_storage.has_value()) << repeated_storage.error().to_string();
  auto final_snapshot = repeated_storage->load(1U);
  ASSERT_TRUE(final_snapshot.has_value()) << final_snapshot.error().to_string();
  expect_application_snapshot(final_snapshot->snapshot);
  const RaftNode* repeated_node = repeated_runtime->find_group(test::metadata_crash_group_id());
  ASSERT_NE(repeated_node, nullptr);
  EXPECT_EQ(repeated_node->persistent_state().snapshot, final_snapshot->snapshot.raft_snapshot);
  auto repeated = DurableMetadataStateMachine::recover(
      test::metadata_crash_group_id(), *repeated_runtime, std::move(*repeated_storage));
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  expect_catalog_recovered(*repeated);
  ASSERT_TRUE(repeated_runtime->close().is_ok());
}

INSTANTIATE_TEST_SUITE_P(
    EveryCrossDomainDurabilityTransition, MetadataSnapshotCompactionCrashMatrixTest,
    ::testing::ValuesIn(kCompactionCrashPoints),
    [](const ::testing::TestParamInfo<MetadataCompactionCrashPoint>& parameter) {
      return std::string{parameter.param.failpoint};
    });

} // namespace
} // namespace chronos::raft
