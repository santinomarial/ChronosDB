#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/ingest/async_raft_tablet_application.hpp"
#include "chronos/ingest/columnar_append.hpp"
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
        (std::filesystem::temp_directory_path() / "chronos-async-tablet-XXXXXX").string();
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

[[nodiscard]] raft::GroupId group_id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(static_cast<std::byte>(seed));
  return raft::GroupId{bytes};
}

[[nodiscard]] schema::TabletId tablet_id(const std::uint8_t seed) {
  return columnar::test::id<schema::TabletId>(seed);
}

[[nodiscard]] TabletState tablet(const std::uint8_t seed) {
  return TabletState::create(
             columnar::test::batch_schema(), tablet_id(seed),
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

[[nodiscard]] AsyncRaftTabletApplicationConfig application_config(const raft::GroupId& group,
                                                                  const std::uint8_t tablet_seed) {
  return {.group_id = group,
          .snapshot_storage = std::nullopt,
          .retry_directory = retry_directory(),
          .tablet = tablet(tablet_seed),
          .retained_schemas = schemas(),
          .decode_limits = {}};
}

[[nodiscard]] std::vector<std::byte> command(const std::uint8_t tablet_seed,
                                             const std::uint8_t request_seed = 1U) {
  auto batch = columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                                    columnar::test::batch_columns())
                   .value();
  const auto encoded_batch = columnar::encode_columnar_batch_v1(batch).value();
  const auto encoded = encode_columnar_append_v1(
                           {.client_id = test::request_id<ClientId>(request_seed),
                            .client_batch_id = test::request_id<ClientBatchId>(request_seed + 32U),
                            .tablet_id = tablet_id(tablet_seed)},
                           encoded_batch)
                           .value();
  return {encoded.bytes().begin(), encoded.bytes().end()};
}

[[nodiscard]] std::vector<raft::RaftGroupConfiguration> groups() {
  return {{.group_id = group_id(0x41U), .voters = {1U}},
          {.group_id = group_id(0x42U), .voters = {1U}}};
}

[[nodiscard]] common::Result<std::shared_ptr<AsyncRaftTabletApplication>> application() {
  std::vector<AsyncRaftTabletApplicationConfig> configured;
  configured.push_back(application_config(group_id(0x42U), 72U));
  configured.push_back(application_config(group_id(0x41U), 71U));
  return AsyncRaftTabletApplication::create(std::move(configured));
}

TEST(AsyncRaftTabletApplicationTest, AppliesOnlyTouchedGroupsBeforePublishingCompletion) {
  TemporaryDirectory directory;
  const raft::RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  auto extension = application();
  ASSERT_TRUE(extension.has_value()) << extension.error().to_string();
  auto runtime =
      raft::AsyncDurableMultiRaftRuntime::create_new(1U, log_config, groups(), {}, *extension);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  EXPECT_TRUE((*extension)->initialized());
  EXPECT_EQ((*extension)->tablet_count(), 2U);

  auto elections = runtime->try_submit({{group_id(0x41U), raft::StartElectionOperation{}},
                                        {group_id(0x42U), raft::StartElectionOperation{}}});
  ASSERT_TRUE(elections.has_value()) << elections.error().to_string();
  ASSERT_TRUE(elections->wait().has_value());

  auto proposal = runtime->try_submit(
      {{group_id(0x41U), raft::ProposeOperation{kRaftColumnarAppendEntryType, command(71U)}}});
  ASSERT_TRUE(proposal.has_value()) << proposal.error().to_string();
  auto proposal_result = proposal->wait();
  ASSERT_TRUE(proposal_result.has_value()) << proposal_result.error().to_string();
  ASSERT_EQ(proposal_result->size(), 1U);
  EXPECT_TRUE(proposal_result->front().status.is_ok());

  auto first = (*extension)->snapshot(group_id(0x41U));
  auto untouched = (*extension)->snapshot(group_id(0x42U));
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_TRUE(untouched.has_value()) << untouched.error().to_string();
  EXPECT_EQ(first->visible_row_count(), 2U);
  EXPECT_EQ(first->applied_position(), head::HeadCommitPosition::raft(group_id(0x41U), 1U));
  EXPECT_EQ(untouched->visible_row_count(), 0U);
  const auto receipt = (*extension)->latest_quorum_sync_receipt(group_id(0x41U));
  ASSERT_TRUE(receipt.has_value());
  EXPECT_EQ(receipt->group_id, group_id(0x41U));
  EXPECT_EQ(receipt->log_index, 1U);
  EXPECT_FALSE((*extension)->latest_quorum_sync_receipt(group_id(0x42U)).has_value());

  EXPECT_TRUE(runtime->shutdown().is_ok());
  EXPECT_EQ((*extension)->tablet_count(), 0U);
  EXPECT_EQ((*extension)->snapshot(group_id(0x41U)).error().code(),
            common::StatusCode::kUnavailable);
}

TEST(AsyncRaftTabletApplicationTest, RebuildsCommittedTabletStateBeforeReopenAdmission) {
  TemporaryDirectory directory;
  const raft::RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  {
    auto extension = application();
    ASSERT_TRUE(extension.has_value()) << extension.error().to_string();
    auto runtime =
        raft::AsyncDurableMultiRaftRuntime::create_new(1U, log_config, groups(), {}, *extension);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto election = runtime->try_submit({{group_id(0x41U), raft::StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value());
    ASSERT_TRUE(election->wait().has_value());
    auto proposal = runtime->try_submit(
        {{group_id(0x41U), raft::ProposeOperation{kRaftColumnarAppendEntryType, command(71U)}}});
    ASSERT_TRUE(proposal.has_value());
    ASSERT_TRUE(proposal->wait().has_value());
    ASSERT_TRUE(runtime->shutdown().is_ok());
  }

  auto rebuilt_extension = application();
  ASSERT_TRUE(rebuilt_extension.has_value()) << rebuilt_extension.error().to_string();
  auto reopened = raft::AsyncDurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups(),
                                                                    {}, *rebuilt_extension);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_TRUE(reopened->is_accepting());
  auto rebuilt = (*rebuilt_extension)->snapshot(group_id(0x41U));
  ASSERT_TRUE(rebuilt.has_value()) << rebuilt.error().to_string();
  EXPECT_EQ(rebuilt->visible_row_count(), 2U);
  EXPECT_EQ(rebuilt->applied_position(), head::HeadCommitPosition::raft(group_id(0x41U), 1U));
  EXPECT_TRUE(reopened->shutdown().is_ok());
}

TEST(AsyncRaftTabletApplicationTest, RejectsDuplicateGroupsBeforeStartingAWorker) {
  std::vector<AsyncRaftTabletApplicationConfig> configured;
  configured.push_back(application_config(group_id(0x41U), 71U));
  configured.push_back(application_config(group_id(0x41U), 72U));
  auto duplicate = AsyncRaftTabletApplication::create(std::move(configured));
  ASSERT_FALSE(duplicate.has_value());
  EXPECT_EQ(duplicate.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(AsyncRaftTabletApplicationTest, FailsOwnerClosedOnCorruptCommittedCommand) {
  TemporaryDirectory directory;
  const raft::RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  auto extension = application();
  ASSERT_TRUE(extension.has_value()) << extension.error().to_string();
  auto runtime =
      raft::AsyncDurableMultiRaftRuntime::create_new(1U, log_config, groups(), {}, *extension);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto election = runtime->try_submit({{group_id(0x41U), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());

  auto corrupt =
      runtime->try_submit({{group_id(0x41U), raft::ProposeOperation{kRaftColumnarAppendEntryType,
                                                                    {std::byte{0xFFU}}}}});
  ASSERT_TRUE(corrupt.has_value());
  auto result = corrupt->wait();
  ASSERT_FALSE(result.has_value());
  EXPECT_TRUE((*extension)->failed());
  EXPECT_FALSE(runtime->is_accepting());
  EXPECT_FALSE(runtime->terminal_status().is_ok());
  EXPECT_EQ((*extension)->snapshot(group_id(0x41U)).error(), (*extension)->failure_status());
  EXPECT_FALSE(runtime->shutdown().is_ok());
}

} // namespace
} // namespace chronos::ingest
