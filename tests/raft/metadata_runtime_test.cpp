#include "chronos/raft/metadata_runtime.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-metadata-raft-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
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

[[nodiscard]] GroupId group_id() {
  common::Uuid::Bytes bytes{};
  bytes.fill(std::byte{0x55U});
  return GroupId{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] ProposeOperation proposal(MetadataCommand command) {
  return ProposeOperation{kRaftMetadataCommandEntryType,
                          encode_metadata_command_v1(std::move(command)).value()};
}

TEST(DurableMetadataStateMachineTest, AppliesAndRebuildsCommittedMetadataGroup) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  const std::vector<RaftGroupConfiguration> groups{{group_id(), {1U}}};
  auto runtime = DurableMultiRaftRuntime::create_new(1U, log_config, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  ASSERT_TRUE(runtime->execute_batch({{group_id(), StartElectionOperation{}}}).has_value());
  auto recovered = DurableMetadataStateMachine::recover(group_id(), *runtime);
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  std::optional<DurableMetadataStateMachine> metadata{std::move(*recovered)};

  const auto table = id<schema::TableId>(1U);
  const auto schema_id = id<schema::SchemaId>(2U);
  const auto tablet = id<schema::TabletId>(3U);
  std::vector<DurableRaftRequest> requests;
  requests.push_back({group_id(), proposal(ClusterNodeMetadata{1U, "node-1"})});
  requests.push_back(
      {group_id(), proposal(SchemaMetadata{table, schema_id, schema::SchemaVersion::initial()})});
  requests.push_back({group_id(), proposal(TabletPlacementMetadata{table, tablet, 1U, {1U}, 1U})});
  requests.push_back({group_id(), proposal(RetentionMetadata{table, 1000, 100U})});
  ASSERT_TRUE(runtime->execute_batch(std::move(requests)).has_value());
  EXPECT_EQ(runtime->find_group(group_id())->commit_index(), 4U);
  EXPECT_EQ(metadata->state().applied_index(), 0U);
  EXPECT_EQ(metadata->prove_applied_quorum_sync(4U).error().code(),
            common::StatusCode::kUnavailable);

  auto report = metadata->apply_committed();
  ASSERT_TRUE(report.has_value()) << report.error().to_string();
  EXPECT_EQ(report->first_applied_index, 1U);
  EXPECT_EQ(report->last_applied_index, 4U);
  EXPECT_EQ(report->applied_commands, 4U);
  EXPECT_EQ(metadata->state().find_node(1U)->endpoint, "node-1");
  EXPECT_EQ(metadata->state().find_schema(schema_id)->table_id, table);
  EXPECT_EQ(metadata->state().find_tablet(tablet)->placement_epoch, 1U);
  EXPECT_EQ(metadata->state().find_retention(table)->retry_retention_positions, 100U);
  EXPECT_TRUE(metadata->prove_applied_quorum_sync(4U).has_value());

  metadata.reset();
  const std::uint64_t durable_sequence = runtime->durable_physical_sequence();
  ASSERT_TRUE(runtime->close().is_ok());
  auto reopened = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  auto rebuilt = DurableMetadataStateMachine::recover(group_id(), *reopened);
  ASSERT_TRUE(rebuilt.has_value()) << rebuilt.error().to_string();
  EXPECT_EQ(rebuilt->state().applied_index(), 4U);
  EXPECT_EQ(rebuilt->state().find_node(1U)->endpoint, "node-1");
  EXPECT_EQ(rebuilt->state().find_tablet(tablet)->leader_hint, 1U);
  EXPECT_EQ(reopened->durable_physical_sequence(), durable_sequence);
}

} // namespace
} // namespace chronos::raft
