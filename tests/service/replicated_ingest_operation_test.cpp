#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/ingest/async_raft_tablet_application.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/service/replicated_ingest_operation.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-replicated-ingest-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] raft::GroupId group_id() {
  common::Uuid::Bytes bytes{};
  bytes.fill(std::byte{0x51U});
  return raft::GroupId{bytes};
}

[[nodiscard]] schema::TabletId tablet_id() {
  return columnar::test::id<schema::TabletId>(81U);
}

[[nodiscard]] std::vector<std::byte> command() {
  auto batch = columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                                    columnar::test::batch_columns())
                   .value();
  const auto encoded_batch = columnar::encode_columnar_batch_v1(batch).value();
  const auto encoded = ingest::encode_columnar_append_v1(
                           {.client_id = ingest::test::request_id<ingest::ClientId>(1U),
                            .client_batch_id = ingest::test::request_id<ingest::ClientBatchId>(33U),
                            .tablet_id = tablet_id()},
                           encoded_batch)
                           .value();
  return {encoded.bytes().begin(), encoded.bytes().end()};
}

[[nodiscard]] common::Result<std::shared_ptr<ingest::AsyncRaftTabletApplication>> application() {
  auto tablet = ingest::TabletState::create(
      columnar::test::batch_schema(), tablet_id(),
      {.head_capacity = {.row_capacity = 8U, .variable_value_bytes = {0U, 8U, 0U}},
       .maximum_schema_versions = 1U,
       .maximum_sealed_generations = 2U,
       .maximum_retry_entries = 8U});
  auto retries = ingest::RetryDirectory::create({.maximum_entries = 8U});
  if (!tablet.has_value())
    return common::make_unexpected(tablet.error());
  if (!retries.has_value())
    return common::make_unexpected(retries.error());
  std::vector<ingest::AsyncRaftTabletApplicationConfig> configured;
  configured.push_back({.group_id = group_id(),
                        .snapshot_storage = std::nullopt,
                        .retry_directory = std::move(*retries),
                        .tablet = std::move(*tablet),
                        .retained_schemas = {columnar::test::batch_schema()},
                        .decode_limits = {}});
  return ingest::AsyncRaftTabletApplication::create(std::move(configured));
}

[[nodiscard]] common::Result<ReplicatedIngestResult> await(ReplicatedIngestOperation& operation) {
  for (std::size_t attempt = 0U; attempt < 10'000U; ++attempt) {
    auto polled = operation.poll();
    if (!polled.has_value())
      return common::make_unexpected(polled.error());
    if (polled->has_value())
      return std::move(**polled);
    std::this_thread::yield();
  }
  return common::make_unexpected(
      common::Status{common::StatusCode::kUnavailable, "replicated ingest test timed out"});
}

TEST(ReplicatedIngestOperationTest, ProducesExactAppliedAndMatchingRetryAcknowledgements) {
  TemporaryDirectory directory;
  auto extension = application();
  ASSERT_TRUE(extension.has_value()) << extension.error().to_string();
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group_id(), {1U}}}, {}, *extension);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto election = runtime->try_submit({{group_id(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());

  auto first = ReplicatedIngestOperation::submit(group_id(), 1U, command(), *runtime, **extension);
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  auto applied = await(*first);
  ASSERT_TRUE(applied.has_value()) << applied.error().to_string();
  EXPECT_EQ(applied->outcome, network::IngestOutcome::kApplied);
  EXPECT_EQ(applied->applied_row_count, 2U);
  EXPECT_EQ(applied->receipt.log_index, 1U);
  auto encoded = encode_replicated_ingest_acknowledgement(*applied);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto decoded = network::decode_quorum_sync_ingest_acknowledgement(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->outcome, network::IngestOutcome::kApplied);
  EXPECT_EQ(decoded->log_index, 1U);

  auto retry = ReplicatedIngestOperation::submit(group_id(), 1U, command(), *runtime, **extension);
  ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
  auto matched = await(*retry);
  ASSERT_TRUE(matched.has_value()) << matched.error().to_string();
  EXPECT_EQ(matched->outcome, network::IngestOutcome::kMatchingRetry);
  EXPECT_EQ(matched->applied_row_count, 2U);
  EXPECT_EQ(matched->receipt.log_index, 2U);
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

TEST(ReplicatedIngestOperationTest, RejectsAProposalOutsideTheRequiredLeaderTerm) {
  TemporaryDirectory directory;
  auto extension = application();
  ASSERT_TRUE(extension.has_value()) << extension.error().to_string();
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group_id(), {1U}}}, {}, *extension);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto operation =
      ReplicatedIngestOperation::submit(group_id(), 2U, command(), *runtime, **extension);
  ASSERT_TRUE(operation.has_value()) << operation.error().to_string();
  auto rejected = await(*operation);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kUnavailable);
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

} // namespace
} // namespace chronos::service
