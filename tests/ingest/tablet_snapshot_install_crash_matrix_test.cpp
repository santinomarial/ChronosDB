#include "chronos/ingest/tablet_movement_raft_snapshot_completion.hpp"
#include "ingest/tablet_snapshot_install_crash_fixture.hpp"
#include "ingest/tablet_snapshot_install_crash_protocol.hpp"
#include "wal/wal_crash_protocol.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <variant>

namespace chronos::ingest {
namespace {

class CrashDirectory {
public:
  CrashDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-tablet-snapshot-crash-XXXXXX").string();
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

  ~CrashDirectory() {
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

struct SnapshotInstallCrashPoint {
  std::string_view failpoint;
  bool application_snapshot_present;
  bool raft_snapshot_authoritative;
};

constexpr std::array<SnapshotInstallCrashPoint, 10U> kCrashPoints{
    SnapshotInstallCrashPoint{test::kAfterApplicationTemporaryCreate, false, false},
    SnapshotInstallCrashPoint{test::kAfterApplicationWrite, false, false},
    SnapshotInstallCrashPoint{test::kAfterApplicationReadback, false, false},
    SnapshotInstallCrashPoint{test::kAfterApplicationFileSync, false, false},
    SnapshotInstallCrashPoint{test::kAfterApplicationTemporaryClose, false, false},
    SnapshotInstallCrashPoint{test::kAfterApplicationRename, true, false},
    SnapshotInstallCrashPoint{test::kAfterApplicationDirectorySync, true, false},
    SnapshotInstallCrashPoint{test::kAfterRaftStateWrite, true, true},
    SnapshotInstallCrashPoint{test::kAfterRaftStateSync, true, true},
    SnapshotInstallCrashPoint{test::kAfterSuccessRelease, true, true},
};

void expect_success_response(const raft::DurableRaftResult& result,
                             const raft::SnapshotMetadata& expected) {
  ASSERT_TRUE(result.status.is_ok()) << result.status.to_string();
  if (!result.transition.has_value()) {
    ADD_FAILURE() << "successful retry returned no transition";
    return;
  }
  const raft::MultiRaftTransition& transition = result.transition.value();
  EXPECT_FALSE(transition.snapshot_install.has_value());
  ASSERT_EQ(transition.outbound.size(), 1U);
  const raft::GroupOutboundMessage& outbound = transition.outbound.front();
  EXPECT_EQ(outbound.group_id, test::crash_group_id());
  EXPECT_EQ(outbound.source, 4U);
  EXPECT_EQ(outbound.outbound.destination, 1U);
  const auto* response = std::get_if<raft::InstallSnapshotResponse>(&outbound.outbound.message);
  ASSERT_NE(response, nullptr);
  EXPECT_TRUE(response->success);
  EXPECT_EQ(response->term, expected.last_included_term);
  EXPECT_EQ(response->last_included_index, expected.last_included_index);
}

class TabletSnapshotInstallCrashMatrixTest
    : public ::testing::TestWithParam<SnapshotInstallCrashPoint> {};

TEST_P(TabletSnapshotInstallCrashMatrixTest, RecoversOneAuthorityAndConvergesAfterSigkill) {
  CrashDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const SnapshotInstallCrashPoint point = GetParam();
  auto spawned = wal::test::CrashChildProcess::spawn(
      {.directory = directory.path(), .pause_after = std::string{point.failpoint}});
  ASSERT_TRUE(spawned.has_value()) << spawned.error().to_string();
  wal::test::CrashChildProcess child = std::move(*spawned);
  auto reached = child.wait_for("FAILPOINT");
  ASSERT_TRUE(reached.has_value()) << reached.error().to_string();
  ASSERT_EQ(reached->fields.size(), 1U);
  EXPECT_EQ(reached->fields.front(), point.failpoint);
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  const RaftTabletApplicationSnapshot expected = test::crash_application_snapshot();
  auto expected_bytes = encode_raft_tablet_application_snapshot_v1(expected);
  ASSERT_TRUE(expected_bytes.has_value()) << expected_bytes.error().to_string();
  {
    auto storage =
        RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto installed = storage->load(expected.raft_snapshot.last_included_index);
    if (point.application_snapshot_present) {
      ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
      EXPECT_EQ(installed->snapshot, expected);
      EXPECT_EQ(installed->bytes, *expected_bytes);
    } else {
      ASSERT_FALSE(installed.has_value());
      EXPECT_EQ(installed.error().code(), common::StatusCode::kNotFound);
    }
    EXPECT_FALSE(std::filesystem::exists(directory.path() / "snapshots" /
                                         "snapshot-00000000000000000009.rtas.tmp"));

    auto runtime = raft::DurableMultiRaftRuntime::open_existing(
        4U, test::crash_log_config(directory.path()), {}, test::crash_groups());
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    const raft::RaftNode* recovered_group = runtime->find_group(test::crash_group_id());
    ASSERT_NE(recovered_group, nullptr);
    if (point.raft_snapshot_authoritative) {
      EXPECT_EQ(recovered_group->persistent_state().snapshot, expected.raft_snapshot);
      EXPECT_GE(recovered_group->commit_index(), expected.raft_snapshot.last_included_index);
      EXPECT_GE(recovered_group->applied_index(), expected.raft_snapshot.last_included_index);
      auto repeated = runtime->execute_batch(
          {{test::crash_group_id(),
            raft::ReceiveOperation{1U,
                                   raft::InstallSnapshotRequest{4U, 1U, expected.raft_snapshot}}}});
      ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
      ASSERT_EQ(repeated->size(), 1U);
      expect_success_response(repeated->front(), expected.raft_snapshot);
    } else {
      EXPECT_EQ(recovered_group->persistent_state().snapshot.last_included_index, 0U);
      auto pending = test::request_crash_snapshot(*runtime, expected.raft_snapshot);
      ASSERT_TRUE(pending.has_value()) << pending.error().to_string();
      auto recovered_movement = test::crash_recovered_movement(*expected_bytes);
      ASSERT_TRUE(recovered_movement.has_value()) << recovered_movement.error().to_string();
      auto completed = complete_recovered_tablet_movement_raft_snapshot(
          *recovered_movement, test::crash_table_id(), *storage, *pending, *runtime);
      ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
      const auto* response =
          std::get_if<raft::InstallSnapshotResponse>(&completed->acknowledgement.outbound.message);
      ASSERT_NE(response, nullptr);
      EXPECT_TRUE(response->success);
      EXPECT_EQ(response->last_included_index, expected.raft_snapshot.last_included_index);
    }
    EXPECT_EQ(runtime->find_group(test::crash_group_id())->persistent_state().snapshot,
              expected.raft_snapshot);
    auto converged = storage->load(expected.raft_snapshot.last_included_index);
    ASSERT_TRUE(converged.has_value()) << converged.error().to_string();
    EXPECT_EQ(converged->bytes, *expected_bytes);
    EXPECT_TRUE(runtime->close().is_ok());
  }

  auto repeated_runtime = raft::DurableMultiRaftRuntime::open_existing(
      4U, test::crash_log_config(directory.path()), {}, test::crash_groups());
  ASSERT_TRUE(repeated_runtime.has_value()) << repeated_runtime.error().to_string();
  EXPECT_EQ(repeated_runtime->find_group(test::crash_group_id())->persistent_state().snapshot,
            expected.raft_snapshot);
  EXPECT_GE(repeated_runtime->durable_physical_sequence(), 2U);
  EXPECT_TRUE(repeated_runtime->close().is_ok());
  auto repeated_storage =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(directory.path()));
  ASSERT_TRUE(repeated_storage.has_value()) << repeated_storage.error().to_string();
  auto repeated_snapshot = repeated_storage->load(expected.raft_snapshot.last_included_index);
  ASSERT_TRUE(repeated_snapshot.has_value()) << repeated_snapshot.error().to_string();
  EXPECT_EQ(repeated_snapshot->bytes, *expected_bytes);
}

INSTANTIATE_TEST_SUITE_P(EveryAuthorityBoundary, TabletSnapshotInstallCrashMatrixTest,
                         ::testing::ValuesIn(kCrashPoints),
                         [](const ::testing::TestParamInfo<SnapshotInstallCrashPoint>& parameter) {
                           return std::string{parameter.param.failpoint};
                         });

} // namespace
} // namespace chronos::ingest
