#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/ingest/async_raft_tablet_application.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/raft/async_durable_worker_extension_set.hpp"
#include "chronos/raft/async_metadata_application.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/schema_definition_codec.hpp"
#include "chronos/raft/tablet_group_binding_codec.hpp"
#include "chronos/service/replicated_ingest_coordinator.hpp"
#include "chronos/service/replicated_ingest_operation.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include <array>
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

[[nodiscard]] raft::GroupId metadata_group_id() {
  common::Uuid::Bytes bytes{};
  bytes.fill(std::byte{0x52U});
  return raft::GroupId{bytes};
}

[[nodiscard]] schema::TabletId tablet_id() {
  return columnar::test::id<schema::TabletId>(81U);
}

[[nodiscard]] std::vector<std::byte> command(const std::uint8_t seed = 1U) {
  auto batch = columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                                    columnar::test::batch_columns())
                   .value();
  const auto encoded_batch = columnar::encode_columnar_batch_v1(batch).value();
  const auto encoded =
      ingest::encode_columnar_append_v1(
          {.client_id = ingest::test::request_id<ingest::ClientId>(seed),
           .client_batch_id = ingest::test::request_id<ingest::ClientBatchId>(seed + 32U),
           .tablet_id = tablet_id()},
          encoded_batch)
          .value();
  return {encoded.bytes().begin(), encoded.bytes().end()};
}

struct QuorumRequestIdentity {
  std::uint64_t connection_id;
  std::uint64_t request_id;
  std::uint8_t seed;
};

class CoordinatorProgressRecorder final : public ReplicatedIngestCoordinatorProgressObserver {
public:
  void on_progress(const ReplicatedIngestCoordinatorProgress& progress) noexcept override {
    if (count_ < progress_.size())
      progress_[count_] = progress;
    else
      overflow_ = true;
    ++count_;
  }

  [[nodiscard]] std::size_t count() const noexcept {
    return count_;
  }

  [[nodiscard]] bool overflowed() const noexcept {
    return overflow_;
  }

  [[nodiscard]] const ReplicatedIngestCoordinatorProgress&
  operator[](const std::size_t index) const noexcept {
    return progress_[index];
  }

private:
  std::array<ReplicatedIngestCoordinatorProgress, 4U> progress_{};
  std::size_t count_{};
  bool overflow_{};
};

[[nodiscard]] network::NetworkTask quorum_request(const QuorumRequestIdentity identity) {
  const network::IngestProtocolContext context{.protocol_major = network::kProtocolV2Major,
                                               .protocol_minor = network::kProtocolV2LatestMinor,
                                               .feature_bits =
                                                   network::kProtocolV2QuorumSyncFeature};
  auto payload = network::encode_ingest_request(network::DurabilityMode::kQuorumSync,
                                                command(identity.seed), context)
                     .value();
  return {.connection_id = identity.connection_id,
          .principal_id = 9U,
          .protocol = {.protocol_major = context.protocol_major,
                       .protocol_minor = context.protocol_minor,
                       .feature_bits = context.feature_bits,
                       .maximum_payload_size = network::kDefaultMaximumPayloadSize},
          .frame = {.header = {.protocol_major = context.protocol_major,
                               .protocol_minor = context.protocol_minor,
                               .message_type = network::MessageType::kIngestRequest,
                               .request_id = identity.request_id,
                               .payload_size = static_cast<std::uint32_t>(payload.size())},
                    .payload = std::move(payload)}};
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

struct RoutedApplications {
  std::shared_ptr<ingest::AsyncRaftTabletApplication> tablets;
  std::shared_ptr<raft::AsyncRaftMetadataApplication> metadata;
  std::shared_ptr<raft::AsyncDurableRaftWorkerExtensionSet> extensions;
};

[[nodiscard]] common::Result<RoutedApplications> routed_applications() {
  auto tablets = application();
  if (!tablets.has_value())
    return common::make_unexpected(tablets.error());
  auto metadata = raft::AsyncRaftMetadataApplication::create({.group_id = metadata_group_id()});
  if (!metadata.has_value())
    return common::make_unexpected(metadata.error());
  auto extensions = raft::AsyncDurableRaftWorkerExtensionSet::create({*tablets, *metadata});
  if (!extensions.has_value())
    return common::make_unexpected(extensions.error());
  return RoutedApplications{std::move(*tablets), std::move(*metadata), std::move(*extensions)};
}

[[nodiscard]] raft::ProposeOperation placement_proposal(std::vector<raft::NodeId> replicas = {1U}) {
  return {raft::kRaftMetadataCommandEntryType,
          raft::encode_metadata_command_v1(
              raft::TabletPlacementMetadata{columnar::test::batch_schema()->table_id(), tablet_id(),
                                            1U, std::move(replicas), 1U})
              .value()};
}

[[nodiscard]] raft::ProposeOperation
schema_proposal(std::shared_ptr<const schema::TableSchema> schema) {
  return {raft::kRaftSchemaDefinitionEntryType,
          raft::encode_schema_definition_v1(
              {.name = "events", .quoted = false, .schema = std::move(schema)})
              .value()};
}

[[nodiscard]] raft::ProposeOperation binding_proposal() {
  return {raft::kRaftTabletGroupBindingEntryType,
          raft::encode_tablet_group_binding_v1({tablet_id(), group_id()}).value()};
}

void publish_route(raft::AsyncDurableMultiRaftRuntime& runtime,
                   std::vector<raft::NodeId> replicas = {1U}, const bool include_binding = true,
                   const bool publish_successor = false, const bool include_schema = true) {
  auto election = runtime.try_submit({{metadata_group_id(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_TRUE(election->wait().has_value());
  std::vector<raft::DurableRaftRequest> proposals;
  if (include_schema) {
    proposals.emplace_back(metadata_group_id(), schema_proposal(columnar::test::batch_schema()));
    if (publish_successor) {
      proposals.emplace_back(metadata_group_id(),
                             schema_proposal(columnar::test::successor_batch_schema()));
    }
  }
  proposals.emplace_back(metadata_group_id(), placement_proposal(std::move(replicas)));
  if (include_binding)
    proposals.emplace_back(metadata_group_id(), binding_proposal());
  auto published = runtime.try_submit(std::move(proposals));
  ASSERT_TRUE(published.has_value()) << published.error().to_string();
  ASSERT_TRUE(published->wait().has_value());
}

[[nodiscard]] common::Result<ReplicatedIngestResult> await(ReplicatedIngestOperation& operation) {
  for (std::size_t attempt = 0U; attempt < 10'000U; ++attempt) {
    auto polled = operation.poll();
    if (!polled.has_value())
      return common::make_unexpected(polled.error());
    auto& completed = *polled;
    if (completed.has_value())
      return *completed;
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

TEST(ReplicatedIngestCoordinatorTest, BoundsCancelsCompletesAndTimesOutCorrelatedRequests) {
  TemporaryDirectory directory;
  auto applications = routed_applications();
  ASSERT_TRUE(applications.has_value()) << applications.error().to_string();
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()},
      {{metadata_group_id(), {1U}}, {group_id(), {1U}}}, {}, applications->extensions);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  publish_route(*runtime);
  auto election = runtime->try_submit({{group_id(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
  auto coordinator = ReplicatedIngestCoordinator::create(
      *runtime, *applications->tablets, *applications->metadata,
      {.maximum_pending_requests = 2U, .request_timeout = std::chrono::milliseconds{100}});
  ASSERT_TRUE(coordinator.has_value()) << coordinator.error().to_string();
  const auto start = std::chrono::steady_clock::time_point{std::chrono::seconds{10}};
  EXPECT_TRUE(
      coordinator
          ->admit(quorum_request({.connection_id = 10U, .request_id = 1U, .seed = 1U}), start)
          .is_ok());
  EXPECT_TRUE(
      coordinator
          ->admit(quorum_request({.connection_id = 11U, .request_id = 1U, .seed = 2U}), start)
          .is_ok());
  EXPECT_EQ(coordinator
                ->admit(quorum_request({.connection_id = 12U, .request_id = 1U, .seed = 3U}), start)
                .code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(coordinator->cancel(11U, 1U));
  EXPECT_FALSE(coordinator->cancel(11U, 1U));

  CoordinatorProgressRecorder progress;
  std::optional<network::NetworkTask> response;
  for (std::size_t attempt = 0U; attempt < 10'000U && !response.has_value(); ++attempt) {
    auto polled = coordinator->poll(progress, start);
    ASSERT_TRUE(polled.has_value()) << polled.error().to_string();
    response = std::move(*polled);
    std::this_thread::yield();
  }
  if (!response.has_value()) {
    ADD_FAILURE() << "expected a completed replicated ingest response";
    return;
  }
  const network::NetworkTask& completed_response = *response;
  EXPECT_EQ(completed_response.connection_id, 10U);
  EXPECT_EQ(completed_response.frame.header.request_id, 1U);
  EXPECT_EQ(completed_response.frame.header.message_type,
            network::MessageType::kQuorumSyncIngestAcknowledgement);
  EXPECT_TRUE(network::decode_quorum_sync_ingest_acknowledgement(completed_response.frame.payload)
                  .has_value());
  ASSERT_EQ(progress.count(), 4U);
  EXPECT_FALSE(progress.overflowed());
  constexpr std::array expected_stages{ReplicatedIngestCoordinatorProgressStage::kRouteValidated,
                                       ReplicatedIngestCoordinatorProgressStage::kProposalAdmitted,
                                       ReplicatedIngestCoordinatorProgressStage::kApplicationProved,
                                       ReplicatedIngestCoordinatorProgressStage::kResponseReady};
  for (std::size_t index = 0U; index < expected_stages.size(); ++index) {
    EXPECT_EQ(progress[index].stage, expected_stages[index]);
    EXPECT_EQ(progress[index].connection_id, 10U);
    EXPECT_EQ(progress[index].request_id, 1U);
  }

  EXPECT_TRUE(
      coordinator
          ->admit(quorum_request({.connection_id = 13U, .request_id = 7U, .seed = 4U}), start)
          .is_ok());
  auto timed_out = coordinator->poll(progress, start + std::chrono::milliseconds{101});
  ASSERT_TRUE(timed_out.has_value()) << timed_out.error().to_string();
  auto& timed_out_response = *timed_out;
  if (!timed_out_response.has_value()) {
    ADD_FAILURE() << "expected a timed-out replicated ingest response";
    return;
  }
  const network::NetworkTask& timeout = *timed_out_response;
  EXPECT_EQ(timeout.connection_id, 13U);
  EXPECT_EQ(timeout.frame.header.message_type, network::MessageType::kError);
  const auto error = network::decode_error_message(timeout.frame.payload);
  ASSERT_TRUE(error.has_value());
  EXPECT_EQ(error->code, network::ProtocolErrorCode::kCancelled);
  auto empty = coordinator->poll(progress, start + std::chrono::milliseconds{101});
  ASSERT_TRUE(empty.has_value()) << empty.error().to_string();
  EXPECT_FALSE(empty->has_value());
  EXPECT_EQ(progress.count(), 4U);
  EXPECT_FALSE(progress.overflowed());
  const auto metrics = coordinator->metrics();
  EXPECT_EQ(metrics.pending_requests, 0U);
  EXPECT_EQ(metrics.admitted_requests, 3U);
  EXPECT_EQ(metrics.completed_requests, 2U);
  EXPECT_EQ(metrics.cancelled_requests, 1U);
  EXPECT_EQ(metrics.timed_out_requests, 1U);
  EXPECT_EQ(metrics.rejected_requests, 1U);
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

TEST(ReplicatedIngestCoordinatorTest, RejectsTaskWithoutNegotiatedQuorumFeature) {
  TemporaryDirectory directory;
  auto applications = routed_applications();
  ASSERT_TRUE(applications.has_value());
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()},
      {{metadata_group_id(), {1U}}, {group_id(), {1U}}}, {}, applications->extensions);
  ASSERT_TRUE(runtime.has_value());
  auto coordinator = ReplicatedIngestCoordinator::create(*runtime, *applications->tablets,
                                                         *applications->metadata);
  ASSERT_TRUE(coordinator.has_value());
  auto request = quorum_request({.connection_id = 10U, .request_id = 1U, .seed = 1U});
  request.protocol.feature_bits = 0U;
  EXPECT_EQ(coordinator->admit(std::move(request)).code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(coordinator->metrics().pending_requests, 0U);
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

TEST(ReplicatedIngestCoordinatorTest, RejectsAPlacedTabletWithoutAnAuthoritativeGroupBinding) {
  TemporaryDirectory directory;
  auto applications = routed_applications();
  ASSERT_TRUE(applications.has_value());
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()},
      {{metadata_group_id(), {1U}}, {group_id(), {1U}}}, {}, applications->extensions);
  ASSERT_TRUE(runtime.has_value());
  publish_route(*runtime, {1U}, false);
  auto coordinator = ReplicatedIngestCoordinator::create(*runtime, *applications->tablets,
                                                         *applications->metadata);
  ASSERT_TRUE(coordinator.has_value());
  EXPECT_EQ(coordinator->admit(quorum_request({.connection_id = 10U, .request_id = 1U, .seed = 1U}))
                .code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(coordinator->metrics().pending_requests, 0U);
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

TEST(ReplicatedIngestCoordinatorTest, RequiresTheCommittedActiveSchemaBeforeRouting) {
  TemporaryDirectory directory;
  auto applications = routed_applications();
  ASSERT_TRUE(applications.has_value());
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()},
      {{metadata_group_id(), {1U}}, {group_id(), {1U}}}, {}, applications->extensions);
  ASSERT_TRUE(runtime.has_value());
  publish_route(*runtime, {1U}, true, false, false);
  auto coordinator = ReplicatedIngestCoordinator::create(*runtime, *applications->tablets,
                                                         *applications->metadata);
  ASSERT_TRUE(coordinator.has_value());
  EXPECT_EQ(coordinator->admit(quorum_request({.connection_id = 10U, .request_id = 1U, .seed = 1U}))
                .code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(coordinator->metrics().pending_requests, 0U);
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

TEST(ReplicatedIngestCoordinatorTest, RejectsACommandForAnInactiveCommittedSchema) {
  TemporaryDirectory directory;
  auto applications = routed_applications();
  ASSERT_TRUE(applications.has_value());
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()},
      {{metadata_group_id(), {1U}}, {group_id(), {1U}}}, {}, applications->extensions);
  ASSERT_TRUE(runtime.has_value());
  publish_route(*runtime, {1U}, true, true);
  auto coordinator = ReplicatedIngestCoordinator::create(*runtime, *applications->tablets,
                                                         *applications->metadata);
  ASSERT_TRUE(coordinator.has_value());
  EXPECT_EQ(coordinator->admit(quorum_request({.connection_id = 10U, .request_id = 1U, .seed = 1U}))
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(coordinator->metrics().pending_requests, 0U);
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

TEST(ReplicatedIngestCoordinatorTest, RevalidatesActiveSchemaAfterTheOrderedObservation) {
  TemporaryDirectory directory;
  auto applications = routed_applications();
  ASSERT_TRUE(applications.has_value());
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()},
      {{metadata_group_id(), {1U}}, {group_id(), {1U}}}, {}, applications->extensions);
  ASSERT_TRUE(runtime.has_value());
  publish_route(*runtime);
  auto election = runtime->try_submit({{group_id(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
  auto coordinator = ReplicatedIngestCoordinator::create(*runtime, *applications->tablets,
                                                         *applications->metadata);
  ASSERT_TRUE(coordinator.has_value());
  EXPECT_TRUE(
      coordinator->admit(quorum_request({.connection_id = 10U, .request_id = 1U, .seed = 1U}))
          .is_ok());

  auto successor = runtime->try_submit(
      {{metadata_group_id(), schema_proposal(columnar::test::successor_batch_schema())}});
  ASSERT_TRUE(successor.has_value());
  ASSERT_TRUE(successor->wait().has_value());

  std::optional<network::NetworkTask> response;
  for (std::size_t attempt = 0U; attempt < 10'000U && !response.has_value(); ++attempt) {
    auto polled = coordinator->poll();
    ASSERT_TRUE(polled.has_value()) << polled.error().to_string();
    response = std::move(*polled);
    std::this_thread::yield();
  }
  if (!response.has_value()) {
    ADD_FAILURE() << "expected schema revalidation response";
    return;
  }
  const network::NetworkTask& revalidation_response = *response;
  EXPECT_EQ(revalidation_response.frame.header.message_type, network::MessageType::kError);
  const auto error = network::decode_error_message(revalidation_response.frame.payload);
  ASSERT_TRUE(error.has_value());
  EXPECT_EQ(error->code, network::ProtocolErrorCode::kInvalidRequest);
  auto observed = runtime->try_observe_group(group_id());
  ASSERT_TRUE(observed.has_value());
  auto result = observed->wait();
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->size(), 1U);
  const auto& observation = result->front().observation;
  if (!observation.has_value()) {
    ADD_FAILURE() << "expected an ordered group observation";
    return;
  }
  EXPECT_EQ(observation->last_log_index, 0U);
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

TEST(ReplicatedIngestCoordinatorTest, FailsClosedWithoutLocalStablePlacementLeadership) {
  TemporaryDirectory directory;
  auto applications = routed_applications();
  ASSERT_TRUE(applications.has_value());
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()},
      {{metadata_group_id(), {1U}}, {group_id(), {1U}}}, {}, applications->extensions);
  ASSERT_TRUE(runtime.has_value());
  publish_route(*runtime, {1U, 2U});
  auto election = runtime->try_submit({{group_id(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
  auto coordinator = ReplicatedIngestCoordinator::create(*runtime, *applications->tablets,
                                                         *applications->metadata);
  ASSERT_TRUE(coordinator.has_value());
  EXPECT_TRUE(
      coordinator->admit(quorum_request({.connection_id = 10U, .request_id = 1U, .seed = 1U}))
          .is_ok());
  std::optional<network::NetworkTask> response;
  for (std::size_t attempt = 0U; attempt < 10'000U && !response.has_value(); ++attempt) {
    auto polled = coordinator->poll();
    ASSERT_TRUE(polled.has_value()) << polled.error().to_string();
    response = std::move(*polled);
    std::this_thread::yield();
  }
  if (!response.has_value()) {
    ADD_FAILURE() << "expected unstable-leadership response";
    return;
  }
  const network::NetworkTask& leadership_response = *response;
  EXPECT_EQ(leadership_response.frame.header.message_type, network::MessageType::kError);
  const auto error = network::decode_error_message(leadership_response.frame.payload);
  ASSERT_TRUE(error.has_value());
  EXPECT_EQ(error->code, network::ProtocolErrorCode::kExecutionFailure);
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

TEST(ReplicatedIngestCoordinatorTest, RedirectsToOrderedStableRemoteLeaderWhenNegotiated) {
  TemporaryDirectory directory;
  auto applications = routed_applications();
  ASSERT_TRUE(applications.has_value());
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()},
      {{metadata_group_id(), {1U}}, {group_id(), {1U, 2U}}}, {}, applications->extensions);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  publish_route(*runtime, {1U, 2U});
  auto heartbeat = runtime->try_submit(
      {{group_id(), raft::ReceiveOperation{2U, raft::AppendEntriesRequest{.term = 1U,
                                                                          .leader_id = 2U,
                                                                          .previous_log_index = 0U,
                                                                          .previous_log_term = 0U,
                                                                          .entries = {},
                                                                          .leader_commit = 0U}}}});
  ASSERT_TRUE(heartbeat.has_value()) << heartbeat.error().to_string();
  ASSERT_TRUE(heartbeat->wait().has_value());
  auto coordinator = ReplicatedIngestCoordinator::create(*runtime, *applications->tablets,
                                                         *applications->metadata);
  ASSERT_TRUE(coordinator.has_value());
  auto redirected_request = quorum_request({.connection_id = 10U, .request_id = 1U, .seed = 1U});
  redirected_request.protocol.feature_bits |= network::kProtocolV2LeaderRedirectFeature;
  EXPECT_TRUE(coordinator->admit(std::move(redirected_request)).is_ok());

  std::optional<network::NetworkTask> response;
  for (std::size_t attempt = 0U; attempt < 10'000U && !response.has_value(); ++attempt) {
    auto polled = coordinator->poll();
    ASSERT_TRUE(polled.has_value()) << polled.error().to_string();
    response = std::move(*polled);
    std::this_thread::yield();
  }
  if (!response.has_value()) {
    ADD_FAILURE() << "expected leader redirect response";
    return;
  }
  const network::NetworkTask& redirect_response = *response;
  ASSERT_EQ(redirect_response.frame.header.message_type, network::MessageType::kLeaderRedirect);
  const auto redirect = network::decode_leader_redirect(redirect_response.frame.payload);
  ASSERT_TRUE(redirect.has_value()) << redirect.error().to_string();
  EXPECT_EQ(redirect->group_id, group_id());
  EXPECT_EQ(redirect->leader_node_id, 2U);
  EXPECT_EQ(redirect->leader_term, 1U);
  EXPECT_EQ(redirect->placement_epoch, 1U);
  EXPECT_EQ(coordinator->metrics().redirected_requests, 1U);

  EXPECT_TRUE(
      coordinator->admit(quorum_request({.connection_id = 11U, .request_id = 2U, .seed = 2U}))
          .is_ok());
  response.reset();
  for (std::size_t attempt = 0U; attempt < 10'000U && !response.has_value(); ++attempt) {
    auto polled = coordinator->poll();
    ASSERT_TRUE(polled.has_value()) << polled.error().to_string();
    response = std::move(*polled);
    std::this_thread::yield();
  }
  if (!response.has_value()) {
    ADD_FAILURE() << "expected non-negotiated redirect response";
    return;
  }
  EXPECT_EQ(response->frame.header.message_type, network::MessageType::kError);
  EXPECT_EQ(coordinator->metrics().redirected_requests, 1U);
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

} // namespace
} // namespace chronos::service
