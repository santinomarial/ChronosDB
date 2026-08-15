#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/schema_definition_codec.hpp"
#include "chronos/raft/tablet_group_binding_codec.hpp"
#include "chronos/service/replicated_ingest_runtime.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include <chrono>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
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
        (std::filesystem::temp_directory_path() / "chronos-replicated-runtime-XXXXXX").string();
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

[[nodiscard]] raft::GroupId group_id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(static_cast<std::byte>(seed));
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

[[nodiscard]] network::NetworkTask request() {
  const network::IngestProtocolContext context{.protocol_major = network::kProtocolV2Major,
                                               .protocol_minor = network::kProtocolV2LatestMinor,
                                               .feature_bits =
                                                   network::kProtocolV2QuorumSyncFeature};
  auto payload =
      network::encode_ingest_request(network::DurabilityMode::kQuorumSync, command(), context)
          .value();
  return {.connection_id = 10U,
          .principal_id = 9U,
          .protocol = {.protocol_major = context.protocol_major,
                       .protocol_minor = context.protocol_minor,
                       .feature_bits = context.feature_bits,
                       .maximum_payload_size = network::kDefaultMaximumPayloadSize},
          .frame = {.header = {.protocol_major = context.protocol_major,
                               .protocol_minor = context.protocol_minor,
                               .message_type = network::MessageType::kIngestRequest,
                               .request_id = 1U,
                               .payload_size = static_cast<std::uint32_t>(payload.size())},
                    .payload = std::move(payload)}};
}

[[nodiscard]] common::Result<ReplicatedIngestRuntimeConfig>
config(const std::filesystem::path& directory) {
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
  std::vector<ingest::AsyncRaftTabletApplicationConfig> tablets;
  tablets.push_back({.group_id = group_id(0x51U),
                     .snapshot_storage = std::nullopt,
                     .retry_directory = std::move(*retries),
                     .tablet = std::move(*tablet),
                     .retained_schemas = {columnar::test::batch_schema()},
                     .decode_limits = {}});
  return ReplicatedIngestRuntimeConfig{.local_node_id = 1U,
                                       .log = {.directory_path = directory.string()},
                                       .groups = {{group_id(0x50U), {1U}}, {group_id(0x51U), {1U}}},
                                       .tablets = std::move(tablets),
                                       .metadata = {.group_id = group_id(0x50U)}};
}

[[nodiscard]] raft::ProposeOperation schema_proposal() {
  return {raft::kRaftSchemaDefinitionEntryType,
          raft::encode_schema_definition_v1(
              {.name = "events", .quoted = false, .schema = columnar::test::batch_schema()})
              .value()};
}

[[nodiscard]] raft::ProposeOperation placement_proposal() {
  return {raft::kRaftMetadataCommandEntryType,
          raft::encode_metadata_command_v1(
              raft::TabletPlacementMetadata{
                  columnar::test::batch_schema()->table_id(), tablet_id(), 1U, {1U}, 1U})
              .value()};
}

[[nodiscard]] raft::ProposeOperation binding_proposal() {
  return {raft::kRaftTabletGroupBindingEntryType,
          raft::encode_tablet_group_binding_v1({tablet_id(), group_id(0x51U)}).value()};
}

void elect_and_publish_route(ReplicatedIngestRuntime& owner) {
  auto metadata_election =
      owner.runtime()->try_submit({{group_id(0x50U), raft::StartElectionOperation{}}});
  ASSERT_TRUE(metadata_election.has_value());
  ASSERT_TRUE(metadata_election->wait().has_value());
  auto metadata = owner.runtime()->try_submit({{group_id(0x50U), schema_proposal()},
                                               {group_id(0x50U), placement_proposal()},
                                               {group_id(0x50U), binding_proposal()}});
  ASSERT_TRUE(metadata.has_value());
  ASSERT_TRUE(metadata->wait().has_value());
  auto tablet_election =
      owner.runtime()->try_submit({{group_id(0x51U), raft::StartElectionOperation{}}});
  ASSERT_TRUE(tablet_election.has_value());
  ASSERT_TRUE(tablet_election->wait().has_value());
}

[[nodiscard]] common::Result<network::NetworkTask> await_response(ReplicatedIngestRuntime& owner) {
  for (std::size_t attempt = 0U; attempt < 10'000U; ++attempt) {
    auto response = owner.coordinator()->poll();
    if (!response.has_value())
      return common::make_unexpected(response.error());
    auto& available = *response;
    if (available.has_value())
      return std::move(*available);
    std::this_thread::yield();
  }
  return common::make_unexpected(
      common::Status{common::StatusCode::kUnavailable, "replicated runtime test timed out"});
}

TEST(ReplicatedIngestRuntimeTest, OwnsCreateShutdownAndExactRecoveryComposition) {
  TemporaryDirectory directory;
  {
    auto configured = config(directory.path());
    ASSERT_TRUE(configured.has_value());
    auto owner = ReplicatedIngestRuntime::create_new(std::move(*configured));
    ASSERT_TRUE(owner.has_value()) << owner.error().to_string();
    auto moved_owner = std::move(*owner);
    EXPECT_FALSE(owner->is_running());
    EXPECT_EQ(owner->runtime(), nullptr);
    EXPECT_EQ(owner->coordinator(), nullptr);
    EXPECT_EQ(owner->shutdown().code(), common::StatusCode::kInvalidArgument);
    EXPECT_TRUE(moved_owner.is_running());
    ASSERT_NE(moved_owner.runtime(), nullptr);
    ASSERT_NE(moved_owner.tablet_application(), nullptr);
    ASSERT_NE(moved_owner.metadata_application(), nullptr);
    ASSERT_NE(moved_owner.coordinator(), nullptr);
    elect_and_publish_route(moved_owner);
    EXPECT_TRUE(moved_owner.coordinator()->admit(request()).is_ok());
    auto response = await_response(moved_owner);
    ASSERT_TRUE(response.has_value()) << response.error().to_string();
    ASSERT_EQ(response->frame.header.message_type,
              network::MessageType::kQuorumSyncIngestAcknowledgement);
    auto acknowledgement =
        network::decode_quorum_sync_ingest_acknowledgement(response->frame.payload);
    ASSERT_TRUE(acknowledgement.has_value());
    EXPECT_EQ(acknowledgement->outcome, network::IngestOutcome::kApplied);
    EXPECT_EQ(acknowledgement->log_index, 1U);
    EXPECT_TRUE(moved_owner.shutdown().is_ok());
    EXPECT_TRUE(moved_owner.shutdown().is_ok());
    EXPECT_FALSE(moved_owner.is_running());
    EXPECT_EQ(moved_owner.runtime(), nullptr);
    EXPECT_EQ(moved_owner.coordinator(), nullptr);
  }

  auto configured = config(directory.path());
  ASSERT_TRUE(configured.has_value());
  auto reopened = ReplicatedIngestRuntime::open_existing(std::move(*configured));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  auto catalog = reopened->metadata_application()->catalog_snapshot();
  ASSERT_TRUE(catalog.has_value());
  ASSERT_EQ((*catalog)->tablet_group_bindings.size(), 1U);
  EXPECT_EQ((*catalog)->tablet_group_bindings.front().group_id, group_id(0x51U));
  auto snapshot = reopened->tablet_application()->snapshot(group_id(0x51U));
  ASSERT_TRUE(snapshot.has_value());
  EXPECT_EQ(snapshot->visible_row_count(), 2U);
  auto election =
      reopened->runtime()->try_submit({{group_id(0x51U), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
  EXPECT_TRUE(reopened->coordinator()->admit(request()).is_ok());
  auto response = await_response(*reopened);
  ASSERT_TRUE(response.has_value()) << response.error().to_string();
  auto acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(response->frame.payload);
  ASSERT_TRUE(acknowledgement.has_value());
  EXPECT_EQ(acknowledgement->outcome, network::IngestOutcome::kMatchingRetry);
  EXPECT_EQ(acknowledgement->log_index, 2U);
  EXPECT_TRUE(reopened->shutdown().is_ok());
}

TEST(ReplicatedIngestRuntimeTest, RejectsTabletMetadataGroupAliasingBeforeWorkerStart) {
  TemporaryDirectory directory;
  auto configured = config(directory.path());
  ASSERT_TRUE(configured.has_value());
  configured->metadata.group_id = group_id(0x51U);
  auto owner = ReplicatedIngestRuntime::create_new(std::move(*configured));
  ASSERT_FALSE(owner.has_value());
  EXPECT_EQ(owner.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_FALSE(std::filesystem::exists(directory.path() / "raft-00000000000000000001.log"));
}

} // namespace
} // namespace chronos::service
