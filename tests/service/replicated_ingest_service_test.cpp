#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/schema_definition_codec.hpp"
#include "chronos/raft/tablet_group_binding_codec.hpp"
#include "chronos/service/replicated_ingest_runtime.hpp"
#include "chronos/service/replicated_ingest_service.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include <chrono>
#include <filesystem>
#include <gtest/gtest.h>
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
        (std::filesystem::temp_directory_path() / "chronos-replicated-service-XXXXXX").string();
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
  return columnar::test::id<schema::TabletId>(82U);
}

[[nodiscard]] std::vector<std::byte> command() {
  auto batch = columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                                    columnar::test::batch_columns())
                   .value();
  const auto encoded_batch = columnar::encode_columnar_batch_v1(batch).value();
  const auto encoded = ingest::encode_columnar_append_v1(
                           {.client_id = ingest::test::request_id<ingest::ClientId>(2U),
                            .client_batch_id = ingest::test::request_id<ingest::ClientBatchId>(34U),
                            .tablet_id = tablet_id()},
                           encoded_batch)
                           .value();
  return {encoded.bytes().begin(), encoded.bytes().end()};
}

[[nodiscard]] network::NetworkTask request(const std::uint64_t request_id = 1U) {
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
                               .request_id = request_id,
                               .payload_size = static_cast<std::uint32_t>(payload.size())},
                    .payload = std::move(payload)}};
}

[[nodiscard]] common::Result<ReplicatedIngestRuntimeConfig>
runtime_config(const std::filesystem::path& directory) {
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
  tablets.push_back({.group_id = group_id(0x61U),
                     .snapshot_storage = std::nullopt,
                     .retry_directory = std::move(*retries),
                     .tablet = std::move(*tablet),
                     .retained_schemas = {columnar::test::batch_schema()},
                     .decode_limits = {}});
  return ReplicatedIngestRuntimeConfig{.local_node_id = 1U,
                                       .log = {.directory_path = directory.string()},
                                       .groups = {{group_id(0x60U), {1U}}, {group_id(0x61U), {1U}}},
                                       .tablets = std::move(tablets),
                                       .metadata = {.group_id = group_id(0x60U)}};
}

void elect_and_publish_route(ReplicatedIngestRuntime& owner) {
  auto metadata_election =
      owner.runtime()->try_submit({{group_id(0x60U), raft::StartElectionOperation{}}});
  ASSERT_TRUE(metadata_election.has_value());
  ASSERT_TRUE(metadata_election->wait().has_value());
  const raft::ProposeOperation schema{
      raft::kRaftSchemaDefinitionEntryType,
      raft::encode_schema_definition_v1(
          {.name = "events", .quoted = false, .schema = columnar::test::batch_schema()})
          .value()};
  const raft::ProposeOperation placement{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TabletPlacementMetadata{
              columnar::test::batch_schema()->table_id(), tablet_id(), 1U, {1U}, 1U})
          .value()};
  const raft::ProposeOperation binding{
      raft::kRaftTabletGroupBindingEntryType,
      raft::encode_tablet_group_binding_v1({tablet_id(), group_id(0x61U)}).value()};
  auto metadata = owner.runtime()->try_submit(
      {{group_id(0x60U), schema}, {group_id(0x60U), placement}, {group_id(0x60U), binding}});
  ASSERT_TRUE(metadata.has_value());
  ASSERT_TRUE(metadata->wait().has_value());
  auto tablet_election =
      owner.runtime()->try_submit({{group_id(0x61U), raft::StartElectionOperation{}}});
  ASSERT_TRUE(tablet_election.has_value());
  ASSERT_TRUE(tablet_election->wait().has_value());
}

[[nodiscard]] ReplicatedIngestRuntime create_runtime(const std::filesystem::path& directory) {
  auto configured = runtime_config(directory);
  EXPECT_TRUE(configured.has_value());
  auto owner = ReplicatedIngestRuntime::create_new(std::move(*configured));
  EXPECT_TRUE(owner.has_value()) << owner.error().to_string();
  return std::move(*owner);
}

TEST(ReplicatedIngestServiceTest, RetainsOneAppliedResponseAcrossQueueBackpressure) {
  TemporaryDirectory directory;
  ReplicatedIngestRuntime owner = create_runtime(directory.path());
  elect_and_publish_route(owner);
  auto requests = network::SpscNetworkTaskQueue::create(4U).value();
  auto responses = network::SpscNetworkTaskQueue::create(1U).value();
  ASSERT_TRUE(responses.try_push(
      {.connection_id = 99U, .frame = {.header = {.message_type = network::MessageType::kPong}}}));
  auto service = ReplicatedIngestService::create(
      {.coordinator = owner.coordinator(), .requests = &requests, .responses = &responses});
  ASSERT_TRUE(service.has_value()) << service.error().to_string();
  ASSERT_TRUE(requests.try_push(request()));

  for (std::size_t attempt = 0U; attempt < 10'000U && !service->metrics().response_retained;
       ++attempt) {
    auto polled = service->poll_once();
    ASSERT_TRUE(polled.has_value()) << polled.error().to_string();
    std::this_thread::yield();
  }
  EXPECT_TRUE(service->metrics().response_retained);
  EXPECT_EQ(service->metrics().response_backpressure, 1U);
  EXPECT_EQ(service->metrics().admitted_requests, 1U);
  ASSERT_TRUE(responses.try_pop().has_value());
  auto released = service->poll_once();
  ASSERT_TRUE(released.has_value());
  EXPECT_TRUE(released->response_enqueued);
  auto response = responses.try_pop();
  if (!response.has_value()) {
    ADD_FAILURE() << "expected the retained ingest response";
    return;
  }
  ASSERT_EQ(response->frame.header.message_type,
            network::MessageType::kQuorumSyncIngestAcknowledgement);
  auto acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(response->frame.payload);
  ASSERT_TRUE(acknowledgement.has_value());
  EXPECT_EQ(acknowledgement->outcome, network::IngestOutcome::kApplied);
  EXPECT_EQ(acknowledgement->group_id, group_id(0x61U));
  service->begin_shutdown();
  EXPECT_FALSE(service->accepting());
  EXPECT_TRUE(service->drained());
  auto moved_service = std::move(*service);
  EXPECT_FALSE(service->accepting());
  EXPECT_TRUE(service->drained());
  EXPECT_EQ(service->poll_once().error().code(), common::StatusCode::kInvalidArgument);
  service->begin_shutdown();
  EXPECT_FALSE(moved_service.accepting());
  EXPECT_TRUE(moved_service.drained());
  EXPECT_TRUE(owner.shutdown().is_ok());
}

TEST(ReplicatedIngestServiceTest, CancelsExactlyAndRejectsNewWorkDuringDrain) {
  TemporaryDirectory directory;
  ReplicatedIngestRuntime owner = create_runtime(directory.path());
  elect_and_publish_route(owner);
  auto requests = network::SpscNetworkTaskQueue::create(4U).value();
  auto responses = network::SpscNetworkTaskQueue::create(4U).value();
  auto service = ReplicatedIngestService::create(
      {.coordinator = owner.coordinator(), .requests = &requests, .responses = &responses});
  ASSERT_TRUE(service.has_value());
  ASSERT_TRUE(owner.coordinator()->admit(request(7U)).is_ok());
  ASSERT_TRUE(requests.try_push(
      {.connection_id = 10U,
       .principal_id = 9U,
       .frame = {.header = {.message_type = network::MessageType::kCancel, .request_id = 7U}}}));
  auto cancelled = service->poll_once();
  ASSERT_TRUE(cancelled.has_value());
  EXPECT_EQ(service->metrics().cancelled_requests, 1U);
  EXPECT_EQ(owner.coordinator()->metrics().pending_requests, 0U);

  service->begin_shutdown();
  ASSERT_TRUE(requests.try_push(request(8U)));
  auto rejected = service->poll_once();
  ASSERT_TRUE(rejected.has_value());
  EXPECT_TRUE(rejected->response_enqueued);
  auto response = responses.try_pop();
  if (!response.has_value()) {
    ADD_FAILURE() << "expected the shutdown rejection response";
    return;
  }
  ASSERT_EQ(response->frame.header.message_type, network::MessageType::kError);
  auto error = network::decode_error_message(response->frame.payload);
  ASSERT_TRUE(error.has_value());
  EXPECT_EQ(error->code, network::ProtocolErrorCode::kExecutionFailure);
  EXPECT_EQ(service->metrics().shutdown_rejections, 1U);
  EXPECT_EQ(service->metrics().request_errors, 1U);
  EXPECT_TRUE(service->drained());
  EXPECT_TRUE(owner.shutdown().is_ok());
}

} // namespace
} // namespace chronos::service
