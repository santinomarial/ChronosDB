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

namespace chronos::ingest {
namespace {

class TabletApplicationCrashDirectory {
public:
  TabletApplicationCrashDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-tablet-apply-crash-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr) {
      path_ = created;
      std::error_code error;
      static_cast<void>(std::filesystem::create_directory(path_ / "raft", error));
      if (error) {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
        path_.clear();
      }
    }
  }

  ~TabletApplicationCrashDirectory() {
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

struct TabletApplicationCrashPoint {
  std::string_view failpoint;
  bool applied_index_persisted;
};

constexpr std::array<TabletApplicationCrashPoint, 4U> kCrashPoints{
    TabletApplicationCrashPoint{test::kBeforeTabletAppliedIndexWrite, false},
    TabletApplicationCrashPoint{test::kAfterTabletAppliedIndexWrite, true},
    TabletApplicationCrashPoint{test::kAfterTabletAppliedIndexSync, true},
    TabletApplicationCrashPoint{test::kAfterTabletApplicationSuccess, true},
};

void expect_recovered_tablet(const RaftTabletStateMachine& machine) {
  auto publication = machine.tablet().snapshot();
  ASSERT_TRUE(publication.has_value()) << publication.error().to_string();
  EXPECT_EQ(publication->visible_row_count(), 2U);
  EXPECT_EQ(publication->retry_entry_count(), 1U);
  EXPECT_EQ(publication->applied_position(),
            head::HeadCommitPosition::raft(test::crash_group_id(), 1U));
}

class TabletApplicationCrashMatrixTest
    : public ::testing::TestWithParam<TabletApplicationCrashPoint> {};

TEST_P(TabletApplicationCrashMatrixTest,
       RebuildsPublishedRowsRegardlessOfPersistedAppliedIndexAfterSigkill) {
  TabletApplicationCrashDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const TabletApplicationCrashPoint point = GetParam();
  auto spawned = wal::test::CrashChildProcess::spawn(
      {.directory = directory.path(), .pause_after = std::string{point.failpoint}});
  ASSERT_TRUE(spawned.has_value()) << spawned.error().to_string();
  wal::test::CrashChildProcess child = std::move(*spawned);
  auto reached = child.wait_for("FAILPOINT");
  ASSERT_TRUE(reached.has_value()) << reached.error().to_string();
  ASSERT_EQ(reached->fields.size(), 1U);
  EXPECT_EQ(reached->fields.front(), point.failpoint);
  ASSERT_TRUE(child.kill_abruptly().is_ok());

  auto runtime = raft::DurableMultiRaftRuntime::open_existing(
      4U, test::crash_log_config(directory.path()), {}, test::crash_compaction_groups());
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  const raft::RaftNode* node = runtime->find_group(test::crash_group_id());
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->persistent_state().commit_index, 1U);
  EXPECT_EQ(node->persistent_state().applied_index, point.applied_index_persisted ? 1U : 0U);
  ASSERT_EQ(node->persistent_state().log.size(), 1U);
  EXPECT_EQ(node->persistent_state().log.front().index, 1U);

  auto recovered = RaftTabletStateMachine::recover(
      test::crash_group_id(), *runtime, test::crash_compaction_retry_directory(),
      test::crash_compaction_tablet(), test::crash_compaction_schemas());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  std::optional<RaftTabletStateMachine> machine{std::move(*recovered)};
  expect_recovered_tablet(*machine);
  node = runtime->find_group(test::crash_group_id());
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->persistent_state().applied_index, 1U);
  machine.reset();
  ASSERT_TRUE(runtime->close().is_ok());

  auto repeated_runtime = raft::DurableMultiRaftRuntime::open_existing(
      4U, test::crash_log_config(directory.path()), {}, test::crash_compaction_groups());
  ASSERT_TRUE(repeated_runtime.has_value()) << repeated_runtime.error().to_string();
  auto repeated = RaftTabletStateMachine::recover(
      test::crash_group_id(), *repeated_runtime, test::crash_compaction_retry_directory(),
      test::crash_compaction_tablet(), test::crash_compaction_schemas());
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  expect_recovered_tablet(*repeated);
  EXPECT_EQ(repeated_runtime->find_group(test::crash_group_id())->persistent_state().applied_index,
            1U);
  ASSERT_TRUE(repeated_runtime->close().is_ok());
}

INSTANTIATE_TEST_SUITE_P(
    EveryPublicationPersistenceBoundary, TabletApplicationCrashMatrixTest,
    ::testing::ValuesIn(kCrashPoints),
    [](const ::testing::TestParamInfo<TabletApplicationCrashPoint>& parameter) {
      return std::string{parameter.param.failpoint};
    });

} // namespace
} // namespace chronos::ingest
