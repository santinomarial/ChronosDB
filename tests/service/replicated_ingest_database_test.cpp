#include "chronos/cluster/distributed_mutable_query_control_tcp.hpp"
#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/common/byte_reader.hpp"
#include "chronos/ingest/raft_tablet_state_machine.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/metadata_runtime.hpp"
#include "chronos/raft/schema_definition_codec.hpp"
#include "chronos/raft/tablet_group_binding_codec.hpp"
#include "chronos/service/native_distributed_grouped_shuffle_job_provider.hpp"
#include "chronos/service/native_protocol_service.hpp"
#include "chronos/service/replicated_distributed_mutable_query_control_tcp_server.hpp"
#include "chronos/service/replicated_ingest_database.hpp"
#include "chronos/service/replicated_ingest_service.hpp"
#include "chronos/service/replicated_read_barrier.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

[[nodiscard]] std::string bytes_as_string(const std::span<const std::byte> bytes) {
  std::string result;
  result.reserve(bytes.size());
  for (const std::byte byte : bytes)
    result.push_back(static_cast<char>(byte));
  return result;
}

[[nodiscard]] std::filesystem::path network_fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] network::TlsServerConfig distributed_server_tls() {
  return {.certificate_chain_file = network_fixture("server.pem").string(),
          .private_key_file = network_fixture("server-key.pem").string(),
          .trust_store_file = network_fixture("ca.pem").string()};
}

[[nodiscard]] network::TlsClientConfig distributed_client_tls() {
  return {.certificate_chain_file = network_fixture("client.pem").string(),
          .private_key_file = network_fixture("client-key.pem").string(),
          .trust_store_file = network_fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

class DistributedTestAuthenticator final : public network::ConnectionAuthenticator {
public:
  explicit DistributedTestAuthenticator(const std::uint64_t principal) : principal_(principal) {}

  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = principal_};
  }

private:
  std::uint64_t principal_{};
};

class DistributedTestNodeAuthorizer final : public cluster::ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return (principal == 91U && node == 9U) || (principal == 92U && node == 1U) ||
           (principal == 93U && node == 2U);
  }
};

class LocalGroupedShuffleProvider final : public NativeDistributedGroupedShuffleProvider {
public:
  common::Result<NativeDistributedGroupedShufflePlan>
  prepare(const std::span<const query::DistributedMutableVectorFragment> fragments,
          std::span<const query::VectorGroupKeyDefinition>,
          std::span<const query::VectorAggregateDefinition>,
          std::span<const cluster::DistributedQueryNodeRoute>,
          const std::chrono::steady_clock::time_point execution_deadline) override {
    ++calls;
    if (fragments.empty() || execution_deadline <= std::chrono::steady_clock::now()) {
      return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                    "local grouped shuffle plan is invalid"});
    }
    const raft::NodeId node = fragments.front().serving_node;
    for (const auto& fragment : fragments) {
      if (fragment.serving_node != node) {
        return common::make_unexpected(common::Status{
            common::StatusCode::kNotSupported, "test shuffle provider requires one local node"});
      }
    }
    NativeDistributedGroupedShufflePlan plan;
    plan.execution.destinations.push_back({.local_node_id = node});
    return plan;
  }

  std::size_t calls{};
};

class EmptyRowsMutableWorker final : public cluster::DistributedMutableVectorQueryWorkerService {
public:
  common::Result<std::vector<cluster::DistributedVectorResultExchangeMessage>>
  execute(const query::DistributedMutableVectorFragment& fragment) override {
    ++calls;
    try {
      std::vector<network::QueryResultColumn> columns;
      columns.reserve(fragment.result_schema.columns.size());
      for (const query::DistributedVectorResultColumn& column : fragment.result_schema.columns)
        columns.push_back({column.name, column.type, column.nullable});
      auto encoded = network::encode_query_result_batch(0U, columns, {});
      if (!encoded.has_value())
        return common::make_unexpected(encoded.error());
      return std::vector<cluster::DistributedVectorResultExchangeMessage>{
          {.query_id = fragment.query_id,
           .tablet_id = fragment.tablet_id,
           .sequence = 1U,
           .terminal = true,
           .encoded_result_batch = std::move(*encoded)}};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                    "empty rows worker allocation failed"});
    }
  }

  std::size_t calls{};
};

class UnusedGroupedMutableWorker final
    : public cluster::DistributedMutableVectorGroupedAggregateQueryWorkerService {
public:
  common::Result<query::DistributedVectorGroupedAggregateAuthority>
  bind_authority(const query::DistributedMutableVectorFragment&) override {
    ++calls;
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "unexpected mutable grouped worker call"});
  }

  common::Result<query::DistributedVectorGroupedAggregateWorkerResultV2>
  execute(const query::DistributedMutableVectorFragment&) override {
    ++calls;
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable, "unexpected mutable grouped worker call"});
  }

  std::size_t calls{};
};

class ObservedLeaderAuthorityService final : public cluster::RaftReadAuthorityService {
public:
  explicit ObservedLeaderAuthorityService(std::vector<raft::RaftGroupObservation> observations)
      : observations_(std::move(observations)) {
    for (raft::RaftGroupObservation& observation : observations_) {
      observation.node_id = 2U;
      observation.role = raft::Role::kLeader;
      observation.leader_id = 2U;
    }
  }

  common::Result<cluster::RaftReadAuthority> acquire(const raft::GroupId& group_id) override {
    ++calls;
    last_group = group_id;
    const auto found =
        std::ranges::find(observations_, group_id, &raft::RaftGroupObservation::group_id);
    if (found == observations_.end()) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kNotFound, "authority group is absent"});
    }
    return cluster::RaftReadAuthority{.barrier = {.group_id = group_id,
                                                  .barrier = {.term = found->current_term,
                                                              .context = calls,
                                                              .read_index = found->applied_index}},
                                      .observation = *found};
  }

  std::uint64_t calls{};
  std::optional<raft::GroupId> last_group;

private:
  std::vector<raft::RaftGroupObservation> observations_;
};

class AdvancingFailOnceMutableWorker final
    : public cluster::DistributedMutableVectorQueryWorkerService {
public:
  AdvancingFailOnceMutableWorker(
      ReplicatedIngestDatabase& database,
      cluster::DistributedMutableVectorQueryWorkerService& delegate) noexcept
      : database_(&database), delegate_(&delegate) {}

  common::Result<std::vector<cluster::DistributedVectorResultExchangeMessage>>
  execute(const query::DistributedMutableVectorFragment& fragment) override {
    ++calls_;
    if (calls_ == 1U) {
      if (!fragment.linearizable_barrier.has_value()) {
        return common::make_unexpected(common::Status{common::StatusCode::kInternal,
                                                      "test fragment has no linearizable barrier"});
      }
      first_term_ = fragment.linearizable_barrier->term;
      auto advanced = database_->ingest_runtime()->runtime()->try_submit(
          {{fragment.raft_group_id, raft::StartElectionOperation{}},
           {fragment.raft_group_id, raft::CommitCurrentTermOperation{}}});
      if (!advanced.has_value())
        return common::make_unexpected(advanced.error());
      auto completed = advanced->wait();
      if (!completed.has_value())
        return common::make_unexpected(completed.error());
      for (const raft::DurableRaftResult& result : *completed) {
        if (!result.status.is_ok())
          return common::make_unexpected(result.status);
      }
      return common::make_unexpected(
          common::Status{common::StatusCode::kUnavailable, "injected stale local query authority"});
    }
    if (second_term_ == 0U && fragment.linearizable_barrier.has_value())
      second_term_ = fragment.linearizable_barrier->term;
    return delegate_->execute(fragment);
  }

  [[nodiscard]] std::size_t calls() const noexcept {
    return calls_;
  }
  [[nodiscard]] raft::Term first_term() const noexcept {
    return first_term_;
  }
  [[nodiscard]] raft::Term second_term() const noexcept {
    return second_term_;
  }

private:
  ReplicatedIngestDatabase* database_{};
  cluster::DistributedMutableVectorQueryWorkerService* delegate_{};
  std::size_t calls_{};
  raft::Term first_term_{};
  raft::Term second_term_{};
};

class RecordingStartupObserver final : public ReplicatedIngestDatabaseStartupObserver {
public:
  void on_startup_stage(const ReplicatedIngestDatabaseStartupStage stage) noexcept override {
    if (count < stages.size())
      stages[count] = stage;
    else
      overflow = true;
    ++count;
  }

  std::array<ReplicatedIngestDatabaseStartupStage, 4U> stages{};
  std::size_t count{};
  bool overflow{};
};

class RecordingShutdownObserver final : public ReplicatedIngestDatabaseShutdownObserver {
public:
  void on_shutdown_stage(const ReplicatedIngestDatabaseShutdownStage stage) noexcept override {
    if (count < stages.size())
      stages[count] = stage;
    else
      overflow = true;
    ++count;
  }

  std::array<ReplicatedIngestDatabaseShutdownStage, 6U> stages{};
  std::size_t count{};
  bool overflow{};
};

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-replicated-database-XXXXXX").string();
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

[[nodiscard]] common::Uuid id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(static_cast<std::byte>(seed));
  return common::Uuid{bytes};
}

[[nodiscard]] raft::GroupId metadata_group() {
  return id(0x70U);
}

[[nodiscard]] raft::GroupId tablet_group() {
  return id(0x71U);
}

[[nodiscard]] raft::GroupId remote_tablet_group() {
  return id(0x73U);
}

[[nodiscard]] raft::GroupId second_tablet_group() {
  return id(0x74U);
}

[[nodiscard]] schema::TabletId tablet_id() {
  return columnar::test::id<schema::TabletId>(83U);
}

[[nodiscard]] schema::TabletId second_tablet_id() {
  return columnar::test::id<schema::TabletId>(85U);
}

[[nodiscard]] runtime::DatabaseBootstrapDescriptor descriptor() {
  return {.database_id = id(0x72U),
          .metadata_group_id = metadata_group(),
          .local_node_id = 1U,
          .mutable_head_rows = 8U,
          .maximum_sealed_generations = 2U,
          .variable_column_bytes = 8U,
          .maximum_retry_entries = 8U,
          .wal_segment_target_bytes = std::uint64_t{64U} * 1024U,
          .raft_segment_target_bytes = std::uint64_t{64U} * 1024U};
}

[[nodiscard]] std::vector<raft::RaftGroupConfiguration> groups() {
  return {{metadata_group(), {1U}}, {tablet_group(), {1U}}};
}

[[nodiscard]] std::vector<raft::RaftGroupConfiguration> joint_groups() {
  return {{metadata_group(), {1U}}, {tablet_group(), {1U, 2U}}};
}

[[nodiscard]] std::vector<raft::RaftGroupConfiguration> split_leader_groups() {
  return {{metadata_group(), {1U}}, {tablet_group(), {1U, 2U}}};
}

[[nodiscard]] std::vector<raft::RaftGroupConfiguration> multi_tablet_groups() {
  return {{metadata_group(), {1U}}, {tablet_group(), {1U}}, {second_tablet_group(), {1U}}};
}

[[nodiscard]] std::vector<std::byte> command(std::shared_ptr<const schema::TableSchema> schema,
                                             std::vector<columnar::OwnedColumnVector> columns,
                                             const std::uint8_t request_seed,
                                             const schema::TabletId target_tablet = tablet_id()) {
  auto batch = columnar::OwnedColumnarBatch::create(std::move(schema), std::move(columns)).value();
  const auto batch_bytes = columnar::encode_columnar_batch_v1(batch).value();
  const auto append =
      ingest::encode_columnar_append_v1(
          {.client_id = ingest::test::request_id<ingest::ClientId>(request_seed),
           .client_batch_id = ingest::test::request_id<ingest::ClientBatchId>(request_seed + 32U),
           .tablet_id = target_tablet},
          batch_bytes)
          .value();
  return {append.bytes().begin(), append.bytes().end()};
}

[[nodiscard]] std::vector<std::byte> command() {
  return command(columnar::test::batch_schema(), columnar::test::batch_columns(), 3U);
}

[[nodiscard]] std::vector<std::byte> successor_command() {
  return command(columnar::test::successor_batch_schema(),
                 columnar::test::successor_batch_columns(), 4U);
}

[[nodiscard]] network::NetworkTask request(std::vector<std::byte> command_bytes = command(),
                                           const std::uint64_t request_id = 1U) {
  const network::IngestProtocolContext context{.protocol_major = network::kProtocolV2Major,
                                               .protocol_minor = network::kProtocolV2LatestMinor,
                                               .feature_bits =
                                                   network::kProtocolV2QuorumSyncFeature};
  auto payload =
      network::encode_ingest_request(network::DurabilityMode::kQuorumSync, command_bytes, context)
          .value();
  return {.connection_id = 20U,
          .principal_id = 19U,
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

[[nodiscard]] network::NetworkTask query_request(const std::string_view sql,
                                                 const bool redirects = false) {
  auto payload = network::encode_query_request(sql).value();
  const std::uint64_t features = network::kProtocolV2QuorumSyncFeature |
                                 (redirects ? network::kProtocolV2LeaderRedirectFeature : 0U);
  return {.connection_id = 21U,
          .principal_id = 19U,
          .protocol = {.protocol_major = network::kProtocolV2Major,
                       .protocol_minor = network::kProtocolV2LatestMinor,
                       .feature_bits = features,
                       .maximum_payload_size = network::kDefaultMaximumPayloadSize},
          .frame = {.header = {.protocol_major = network::kProtocolV2Major,
                               .protocol_minor = network::kProtocolV2LatestMinor,
                               .message_type = network::MessageType::kQueryRequest,
                               .request_id = 4U,
                               .payload_size = static_cast<std::uint32_t>(payload.size())},
                    .payload = std::move(payload)}};
}

[[nodiscard]] ReplicatedIngestRuntimeConfig
initial_runtime_config(const runtime::DatabaseBootstrap& bootstrap) {
  auto tablet = ingest::TabletState::create(
                    columnar::test::batch_schema(), tablet_id(),
                    {.head_capacity = {.row_capacity = 8U, .variable_value_bytes = {0U, 8U, 0U}},
                     .maximum_schema_versions = 1U,
                     .maximum_sealed_generations = 2U,
                     .maximum_retry_entries = 8U})
                    .value();
  auto retries = ingest::RetryDirectory::create({.maximum_entries = 8U}).value();
  std::vector<ingest::AsyncRaftTabletApplicationConfig> tablets;
  tablets.push_back({.group_id = tablet_group(),
                     .snapshot_storage = std::nullopt,
                     .retry_directory = std::move(retries),
                     .tablet = std::move(tablet),
                     .retained_schemas = {columnar::test::batch_schema()},
                     .decode_limits = {}});
  return {.local_node_id = 1U,
          .log = {.directory_path = bootstrap.raft_directory_path(),
                  .target_segment_size = descriptor().raft_segment_target_bytes},
          .groups = groups(),
          .tablets = std::move(tablets),
          .metadata = {.group_id = metadata_group()}};
}

[[nodiscard]] ReplicatedIngestRuntimeConfig
multi_tablet_runtime_config(const runtime::DatabaseBootstrap& bootstrap) {
  auto config = initial_runtime_config(bootstrap);
  config.groups = multi_tablet_groups();
  auto tablet = ingest::TabletState::create(
                    columnar::test::batch_schema(), second_tablet_id(),
                    {.head_capacity = {.row_capacity = 8U, .variable_value_bytes = {0U, 8U, 0U}},
                     .maximum_schema_versions = 1U,
                     .maximum_sealed_generations = 2U,
                     .maximum_retry_entries = 8U})
                    .value();
  auto retries = ingest::RetryDirectory::create({.maximum_entries = 8U}).value();
  config.tablets.push_back({.group_id = second_tablet_group(),
                            .snapshot_storage = std::nullopt,
                            .retry_directory = std::move(retries),
                            .tablet = std::move(tablet),
                            .retained_schemas = {columnar::test::batch_schema()},
                            .decode_limits = {}});
  return config;
}

void elect_and_provision(ReplicatedIngestRuntime& owner, const bool include_remote = true) {
  auto election = owner.runtime()->try_submit({{metadata_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
  const raft::ProposeOperation schema{
      raft::kRaftSchemaDefinitionEntryType,
      raft::encode_schema_definition_v1(
          {.name = "events", .quoted = false, .schema = columnar::test::batch_schema()})
          .value()};
  const raft::ProposeOperation policy{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TablePolicyMetadata{columnar::test::batch_schema()->table_id(), 1'000'000'000LL,
                                    86'400'000'000'000LL, 86'400'000'000'000LL, 0LL, 8U})
          .value()};
  const raft::ProposeOperation placement{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TabletPlacementMetadata{
              columnar::test::batch_schema()->table_id(), tablet_id(), 1U, {1U}, 1U})
          .value()};
  const raft::ProposeOperation binding{
      raft::kRaftTabletGroupBindingEntryType,
      raft::encode_tablet_group_binding_v1({tablet_id(), tablet_group()}).value()};
  const schema::TabletId remote_tablet = columnar::test::id<schema::TabletId>(84U);
  const raft::ProposeOperation remote_placement{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TabletPlacementMetadata{
              columnar::test::batch_schema()->table_id(), remote_tablet, 1U, {2U}, 2U})
          .value()};
  const raft::ProposeOperation remote_binding{
      raft::kRaftTabletGroupBindingEntryType,
      raft::encode_tablet_group_binding_v1({remote_tablet, remote_tablet_group()}).value()};
  auto metadata = include_remote
                      ? owner.runtime()->try_submit({{metadata_group(), schema},
                                                     {metadata_group(), policy},
                                                     {metadata_group(), placement},
                                                     {metadata_group(), binding},
                                                     {metadata_group(), remote_placement},
                                                     {metadata_group(), remote_binding}})
                      : owner.runtime()->try_submit({{metadata_group(), schema},
                                                     {metadata_group(), policy},
                                                     {metadata_group(), placement},
                                                     {metadata_group(), binding}});
  ASSERT_TRUE(metadata.has_value());
  ASSERT_TRUE(metadata->wait().has_value());
  election = owner.runtime()->try_submit({{tablet_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
}

[[nodiscard]] raft::RaftGroupObservation observe(ReplicatedIngestRuntime& owner,
                                                 const raft::GroupId& group_id) {
  auto completion = owner.runtime()->try_observe_group(group_id);
  EXPECT_TRUE(completion.has_value());
  if (!completion.has_value())
    return {};
  auto results = completion->wait();
  EXPECT_TRUE(results.has_value());
  if (!results.has_value() || results->size() != 1U)
    return {};
  EXPECT_TRUE(results->front().status.is_ok()) << results->front().status.to_string();
  EXPECT_FALSE(results->front().transition.has_value());
  auto observation = std::move(results->front().observation);
  if (!observation.has_value())
    return {};
  return std::move(*observation);
}

void replicate_current(ReplicatedIngestRuntime& owner, const raft::GroupId& group_id) {
  const raft::RaftGroupObservation observation = observe(owner, group_id);
  auto replicated = owner.runtime()->try_submit(
      {{group_id, raft::ReceiveOperation{2U, raft::AppendEntriesResponse{
                                                 .term = observation.current_term,
                                                 .success = true,
                                                 .match_index = observation.last_log_index}}}});
  ASSERT_TRUE(replicated.has_value()) << replicated.error().to_string();
  ASSERT_TRUE(replicated->wait().has_value());
}

void elect_two_node_group(ReplicatedIngestRuntime& owner, const raft::GroupId& group_id) {
  auto election = owner.runtime()->try_submit({{group_id, raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_TRUE(election->wait().has_value());
  const raft::RaftGroupObservation candidate = observe(owner, group_id);
  auto vote = owner.runtime()->try_submit(
      {{group_id,
        raft::ReceiveOperation{2U, raft::RequestVoteResponse{candidate.current_term, true}}}});
  ASSERT_TRUE(vote.has_value()) << vote.error().to_string();
  ASSERT_TRUE(vote->wait().has_value());
  replicate_current(owner, group_id);
}

void propose_and_replicate(ReplicatedIngestRuntime& owner, const raft::GroupId& group_id,
                           raft::ProposeOperation operation) {
  auto proposed = owner.runtime()->try_submit({{group_id, std::move(operation)}});
  ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();
  ASSERT_TRUE(proposed->wait().has_value());
  replicate_current(owner, group_id);
}

void provision_two_node_query(ReplicatedIngestRuntime& owner,
                              const network::Ipv4Endpoint remote_query_endpoint) {
  auto metadata_election =
      owner.runtime()->try_submit({{metadata_group(), raft::StartElectionOperation{}},
                                   {metadata_group(), raft::CommitCurrentTermOperation{}}});
  ASSERT_TRUE(metadata_election.has_value()) << metadata_election.error().to_string();
  ASSERT_TRUE(metadata_election->wait().has_value());
  propose_and_replicate(
      owner, metadata_group(),
      {raft::kRaftMetadataCommandEntryType,
       raft::encode_metadata_command_v1(raft::ClusterNodeMetadata{1U, "127.0.0.1:1"}).value()});
  propose_and_replicate(
      owner, metadata_group(),
      {raft::kRaftMetadataCommandEntryType,
       raft::encode_metadata_command_v1(
           raft::ClusterNodeMetadata{2U, "127.0.0.1:" + std::to_string(remote_query_endpoint.port)})
           .value()});
  propose_and_replicate(
      owner, metadata_group(),
      {raft::kRaftSchemaDefinitionEntryType,
       raft::encode_schema_definition_v1(
           {.name = "events", .quoted = false, .schema = columnar::test::batch_schema()})
           .value()});
  propose_and_replicate(
      owner, metadata_group(),
      {raft::kRaftMetadataCommandEntryType,
       raft::encode_metadata_command_v1(
           raft::TablePolicyMetadata{columnar::test::batch_schema()->table_id(), 1'000'000'000LL,
                                     86'400'000'000'000LL, 86'400'000'000'000LL, 0LL, 8U})
           .value()});
  propose_and_replicate(
      owner, metadata_group(),
      {raft::kRaftMetadataCommandEntryType,
       raft::encode_metadata_command_v1(
           raft::TabletPlacementMetadata{
               columnar::test::batch_schema()->table_id(), tablet_id(), 1U, {1U, 2U}, 1U})
           .value()});
  propose_and_replicate(
      owner, metadata_group(),
      {raft::kRaftTabletGroupBindingEntryType,
       raft::encode_tablet_group_binding_v1({tablet_id(), tablet_group()}).value()});
  elect_two_node_group(owner, tablet_group());
  propose_and_replicate(owner, tablet_group(), {ingest::kRaftColumnarAppendEntryType, command()});
}

void elect_and_provision_multiple_tablets(ReplicatedIngestRuntime& owner) {
  auto election = owner.runtime()->try_submit({{metadata_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
  const raft::ProposeOperation schema{
      raft::kRaftSchemaDefinitionEntryType,
      raft::encode_schema_definition_v1(
          {.name = "events", .quoted = false, .schema = columnar::test::batch_schema()})
          .value()};
  const raft::ProposeOperation policy{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TablePolicyMetadata{columnar::test::batch_schema()->table_id(), 1'000'000'000LL,
                                    86'400'000'000'000LL, 86'400'000'000'000LL, 0LL, 8U})
          .value()};
  const raft::ProposeOperation node{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(raft::ClusterNodeMetadata{1U, "127.0.0.1:7411"}).value()};
  const raft::ProposeOperation first_placement{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TabletPlacementMetadata{
              columnar::test::batch_schema()->table_id(), tablet_id(), 1U, {1U}, 1U})
          .value()};
  const raft::ProposeOperation first_binding{
      raft::kRaftTabletGroupBindingEntryType,
      raft::encode_tablet_group_binding_v1({tablet_id(), tablet_group()}).value()};
  const raft::ProposeOperation second_placement{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TabletPlacementMetadata{
              columnar::test::batch_schema()->table_id(), second_tablet_id(), 1U, {1U}, 1U})
          .value()};
  const raft::ProposeOperation second_binding{
      raft::kRaftTabletGroupBindingEntryType,
      raft::encode_tablet_group_binding_v1({second_tablet_id(), second_tablet_group()}).value()};
  auto metadata = owner.runtime()->try_submit({{metadata_group(), node},
                                               {metadata_group(), schema},
                                               {metadata_group(), policy},
                                               {metadata_group(), first_placement},
                                               {metadata_group(), first_binding},
                                               {metadata_group(), second_placement},
                                               {metadata_group(), second_binding}});
  ASSERT_TRUE(metadata.has_value());
  ASSERT_TRUE(metadata->wait().has_value());
  for (const raft::GroupId group_id : {tablet_group(), second_tablet_group()}) {
    election = owner.runtime()->try_submit({{group_id, raft::StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value());
    ASSERT_TRUE(election->wait().has_value());
  }
}

[[nodiscard]] network::NetworkTask await_response(ReplicatedIngestRuntime& owner) {
  for (std::size_t attempt = 0U; attempt < 10'000U; ++attempt) {
    auto response = owner.coordinator()->poll();
    if (!response.has_value()) {
      ADD_FAILURE() << response.error().to_string();
      return {};
    }
    auto& available_response = *response;
    if (available_response.has_value())
      return std::move(*available_response);
    std::this_thread::yield();
  }
  ADD_FAILURE() << "replicated database response timed out";
  return {};
}

TEST(ReplicatedIngestDatabaseTest, ReportsPackagedLifecycleStagesInOwnershipOrder) {
  TemporaryDirectory directory;
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  auto initial = ReplicatedIngestRuntime::create_new(initial_runtime_config(*bootstrap));
  ASSERT_TRUE(initial.has_value()) << initial.error().to_string();
  elect_and_provision(*initial);
  ASSERT_TRUE(initial->shutdown().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());

  RecordingStartupObserver observer;
  ReplicatedIngestDatabaseConfig config{.bootstrap = bootstrap_config, .groups = groups()};
  config.startup_observer = &observer;
  auto database = ReplicatedIngestDatabase::open_existing(std::move(config));
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  EXPECT_EQ(observer.count, observer.stages.size());
  EXPECT_FALSE(observer.overflow);
  EXPECT_EQ(observer.stages,
            (std::array{ReplicatedIngestDatabaseStartupStage::kRootOwnerReady,
                        ReplicatedIngestDatabaseStartupStage::kCatalogRecovered,
                        ReplicatedIngestDatabaseStartupStage::kTabletOwnersPrepared,
                        ReplicatedIngestDatabaseStartupStage::kRuntimeReady}));
  RecordingShutdownObserver shutdown_observer;
  ASSERT_TRUE(database->shutdown(shutdown_observer).is_ok());
  EXPECT_EQ(shutdown_observer.count, shutdown_observer.stages.size());
  EXPECT_FALSE(shutdown_observer.overflow);
  EXPECT_EQ(shutdown_observer.stages,
            (std::array{ReplicatedIngestDatabaseShutdownStage::kCoordinatorReleased,
                        ReplicatedIngestDatabaseShutdownStage::kAcceptedWorkDrained,
                        ReplicatedIngestDatabaseShutdownStage::kApplicationsStopped,
                        ReplicatedIngestDatabaseShutdownStage::kLogClosed,
                        ReplicatedIngestDatabaseShutdownStage::kRuntimeStopped,
                        ReplicatedIngestDatabaseShutdownStage::kRootReleased}));
  ASSERT_TRUE(database->shutdown(shutdown_observer).is_ok());
  EXPECT_EQ(shutdown_observer.count, shutdown_observer.stages.size());
}

TEST(ReplicatedIngestDatabaseTest, RebuildsTabletOwnersFromCommittedMetadataUnderRootLock) {
  TemporaryDirectory directory;
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  auto initial = ReplicatedIngestRuntime::create_new(initial_runtime_config(*bootstrap));
  ASSERT_TRUE(initial.has_value()) << initial.error().to_string();
  elect_and_provision(*initial);
  ASSERT_TRUE(initial->coordinator()->admit(request()).is_ok());
  auto applied = await_response(*initial);
  ASSERT_EQ(applied.frame.header.message_type,
            network::MessageType::kQuorumSyncIngestAcknowledgement);
  ASSERT_TRUE(initial->shutdown().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());

  auto database =
      ReplicatedIngestDatabase::open_existing({.bootstrap = bootstrap_config, .groups = groups()});
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  EXPECT_TRUE(database->is_running());
  ASSERT_NE(database->ingest_runtime(), nullptr);
  auto catalog = database->ingest_runtime()->metadata_application()->catalog_snapshot();
  ASSERT_TRUE(catalog.has_value());
  ASSERT_EQ((*catalog)->tablet_group_bindings.size(), 2U);
  auto snapshot = database->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(snapshot.has_value());
  EXPECT_EQ(snapshot->visible_row_count(), 2U);

  auto election = database->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
  ASSERT_TRUE(database->ingest_runtime()->coordinator()->admit(request()).is_ok());
  auto retry = await_response(*database->ingest_runtime());
  auto acknowledgement = network::decode_quorum_sync_ingest_acknowledgement(retry.frame.payload);
  ASSERT_TRUE(acknowledgement.has_value());
  EXPECT_EQ(acknowledgement->outcome, network::IngestOutcome::kMatchingRetry);
  EXPECT_TRUE(database->shutdown().is_ok());
  EXPECT_TRUE(database->shutdown().is_ok());
  EXPECT_FALSE(database->is_running());
  EXPECT_EQ(database->ingest_runtime(), nullptr);
}

TEST(ReplicatedIngestDatabaseTest, RebuildsCompactedApplicationPrefixesAndRetainedSuffixes) {
  TemporaryDirectory directory;
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  const raft::MetadataSnapshotStorageConfig metadata_snapshot_config{
      .directory_path = (directory.path() / "metadata-snapshots").string(),
      .group_id = metadata_group()};
  const ingest::RaftTabletSnapshotStorageConfig tablet_snapshot_config{
      .directory_path = (directory.path() / "tablet-snapshots").string(),
      .group_id = tablet_group()};
  ASSERT_TRUE(std::filesystem::create_directories(metadata_snapshot_config.directory_path));
  ASSERT_TRUE(std::filesystem::create_directories(tablet_snapshot_config.directory_path));

  auto configured = initial_runtime_config(*bootstrap);
  auto durable = raft::DurableMultiRaftRuntime::create_new(configured.local_node_id, configured.log,
                                                           configured.groups);
  ASSERT_TRUE(durable.has_value()) << durable.error().to_string();
  {
    auto metadata_storage = raft::MetadataSnapshotStorage::create(metadata_snapshot_config);
    ASSERT_TRUE(metadata_storage.has_value()) << metadata_storage.error().to_string();
    auto metadata = raft::DurableMetadataStateMachine::recover(metadata_group(), *durable,
                                                               std::move(*metadata_storage));
    ASSERT_TRUE(metadata.has_value()) << metadata.error().to_string();
    auto tablet_storage = ingest::RaftTabletSnapshotStorage::create(tablet_snapshot_config);
    ASSERT_TRUE(tablet_storage.has_value()) << tablet_storage.error().to_string();
    ASSERT_EQ(configured.tablets.size(), 1U);
    auto& tablet_config = configured.tablets.front();
    auto tablet = ingest::RaftTabletStateMachine::recover(
        tablet_group(), *durable, std::move(*tablet_storage),
        std::move(tablet_config.retry_directory), std::move(tablet_config.tablet),
        std::move(tablet_config.retained_schemas), tablet_config.decode_limits);
    ASSERT_TRUE(tablet.has_value()) << tablet.error().to_string();

    auto election = durable->execute_batch({{metadata_group(), raft::StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value()) << election.error().to_string();
    election = durable->execute_batch({{tablet_group(), raft::StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value()) << election.error().to_string();
    const raft::ProposeOperation schema{
        raft::kRaftSchemaDefinitionEntryType,
        raft::encode_schema_definition_v1(
            {.name = "events", .quoted = false, .schema = columnar::test::batch_schema()})
            .value()};
    const raft::ProposeOperation policy{
        raft::kRaftMetadataCommandEntryType,
        raft::encode_metadata_command_v1(
            raft::TablePolicyMetadata{columnar::test::batch_schema()->table_id(), 1'000'000'000LL,
                                      86'400'000'000'000LL, 86'400'000'000'000LL, 0LL, 8U})
            .value()};
    const raft::ProposeOperation placement{
        raft::kRaftMetadataCommandEntryType,
        raft::encode_metadata_command_v1(
            raft::TabletPlacementMetadata{
                columnar::test::batch_schema()->table_id(), tablet_id(), 1U, {1U}, 1U})
            .value()};
    const raft::ProposeOperation binding{
        raft::kRaftTabletGroupBindingEntryType,
        raft::encode_tablet_group_binding_v1({tablet_id(), tablet_group()}).value()};
    auto metadata_entries = durable->execute_batch({{metadata_group(), schema},
                                                    {metadata_group(), policy},
                                                    {metadata_group(), placement},
                                                    {metadata_group(), binding}});
    ASSERT_TRUE(metadata_entries.has_value()) << metadata_entries.error().to_string();
    auto tablet_entry = durable->execute_batch(
        {{tablet_group(),
          raft::ProposeOperation{ingest::kRaftColumnarAppendEntryType, command()}}});
    ASSERT_TRUE(tablet_entry.has_value()) << tablet_entry.error().to_string();
    auto metadata_applied = metadata->apply_committed();
    ASSERT_TRUE(metadata_applied.has_value()) << metadata_applied.error().to_string();
    ASSERT_EQ(metadata_applied->last_applied_index, 4U);
    auto tablet_applied = tablet->apply_committed();
    ASSERT_TRUE(tablet_applied.has_value()) << tablet_applied.error().to_string();
    ASSERT_EQ(tablet_applied->last_applied_index, 1U);
    auto metadata_compacted = metadata->compact_applied_prefix(4U);
    ASSERT_TRUE(metadata_compacted.has_value()) << metadata_compacted.error().to_string();
    auto tablet_compacted = tablet->compact_applied_prefix(1U, 1U, {});
    ASSERT_TRUE(tablet_compacted.has_value()) << tablet_compacted.error().to_string();

    const raft::ProposeOperation node{
        raft::kRaftMetadataCommandEntryType,
        raft::encode_metadata_command_v1(raft::ClusterNodeMetadata{1U, "node-1.example:7000"})
            .value()};
    metadata_entries = durable->execute_batch({{metadata_group(), node}});
    ASSERT_TRUE(metadata_entries.has_value()) << metadata_entries.error().to_string();
    auto suffix_command =
        command(columnar::test::batch_schema(), columnar::test::batch_columns(), 5U);
    tablet_entry = durable->execute_batch(
        {{tablet_group(),
          raft::ProposeOperation{ingest::kRaftColumnarAppendEntryType, suffix_command}}});
    ASSERT_TRUE(tablet_entry.has_value()) << tablet_entry.error().to_string();
    EXPECT_EQ(durable->find_group(metadata_group())->commit_index(), 5U);
    EXPECT_EQ(durable->find_group(metadata_group())->applied_index(), 4U);
    EXPECT_EQ(durable->find_group(tablet_group())->commit_index(), 2U);
    EXPECT_EQ(durable->find_group(tablet_group())->applied_index(), 1U);
  }
  ASSERT_TRUE(durable->close().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());

  auto missing_snapshots =
      ReplicatedIngestDatabase::open_existing({.bootstrap = bootstrap_config, .groups = groups()});
  ASSERT_FALSE(missing_snapshots.has_value());
  EXPECT_EQ(missing_snapshots.error().code(), common::StatusCode::kNotSupported);
  auto database =
      ReplicatedIngestDatabase::open_existing({.bootstrap = bootstrap_config,
                                               .groups = groups(),
                                               .metadata_snapshots = metadata_snapshot_config,
                                               .tablet_snapshots = {tablet_snapshot_config}});
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  auto catalog = database->ingest_runtime()->metadata_application()->catalog_snapshot();
  ASSERT_TRUE(catalog.has_value()) << catalog.error().to_string();
  EXPECT_EQ((*catalog)->applied_index, 5U);
  ASSERT_EQ((*catalog)->cluster_nodes.size(), 1U);
  EXPECT_EQ((*catalog)->cluster_nodes.front(),
            (raft::ClusterNodeMetadata{1U, "node-1.example:7000"}));
  auto recovered = database->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->visible_row_count(), 4U);
  EXPECT_EQ(recovered->retry_entry_count(), 2U);
  EXPECT_EQ(recovered->applied_position(), head::HeadCommitPosition::raft(tablet_group(), 2U));

  auto election = database->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_TRUE(election->wait().has_value());
  auto suffix_command =
      command(columnar::test::batch_schema(), columnar::test::batch_columns(), 5U);
  ASSERT_TRUE(
      database->ingest_runtime()->coordinator()->admit(request(suffix_command, 2U)).is_ok());
  auto retry_response = await_response(*database->ingest_runtime());
  auto retry_acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(retry_response.frame.payload);
  ASSERT_TRUE(retry_acknowledgement.has_value());
  EXPECT_EQ(retry_acknowledgement->outcome, network::IngestOutcome::kMatchingRetry);
  EXPECT_EQ(retry_acknowledgement->log_index, 3U);
  ASSERT_TRUE(database->shutdown().is_ok());

  auto repeated =
      ReplicatedIngestDatabase::open_existing({.bootstrap = bootstrap_config,
                                               .groups = groups(),
                                               .metadata_snapshots = metadata_snapshot_config,
                                               .tablet_snapshots = {tablet_snapshot_config}});
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  recovered = repeated->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->visible_row_count(), 4U);
  EXPECT_EQ(recovered->retry_entry_count(), 2U);
  EXPECT_EQ(recovered->applied_position(), head::HeadCommitPosition::raft(tablet_group(), 3U));
  ASSERT_TRUE(repeated->shutdown().is_ok());
}

TEST(ReplicatedIngestDatabaseTest, RebuildsMultipleTabletGroupsAndPinsTheirWholeTableView) {
  TemporaryDirectory directory;
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  auto initial = ReplicatedIngestRuntime::create_new(multi_tablet_runtime_config(*bootstrap));
  ASSERT_TRUE(initial.has_value()) << initial.error().to_string();
  elect_and_provision_multiple_tablets(*initial);

  ASSERT_TRUE(initial->coordinator()->admit(request(command(), 1U)).is_ok());
  auto first_response = await_response(*initial);
  auto first_acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(first_response.frame.payload);
  ASSERT_TRUE(first_acknowledgement.has_value());
  EXPECT_EQ(first_acknowledgement->outcome, network::IngestOutcome::kApplied);
  EXPECT_EQ(first_acknowledgement->log_index, 1U);
  auto second_command = command(columnar::test::batch_schema(), columnar::test::batch_columns(), 5U,
                                second_tablet_id());
  ASSERT_TRUE(initial->coordinator()->admit(request(second_command, 2U)).is_ok());
  auto second_response = await_response(*initial);
  auto second_acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(second_response.frame.payload);
  ASSERT_TRUE(second_acknowledgement.has_value());
  EXPECT_EQ(second_acknowledgement->outcome, network::IngestOutcome::kApplied);
  EXPECT_EQ(second_acknowledgement->log_index, 1U);
  ASSERT_TRUE(initial->shutdown().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());

  auto database = ReplicatedIngestDatabase::open_existing(
      {.bootstrap = bootstrap_config, .groups = multi_tablet_groups()});
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  EXPECT_EQ(database->query_barrier_groups().size(), 3U);
  for (const raft::GroupId group_id : {tablet_group(), second_tablet_group()}) {
    auto recovered = database->ingest_runtime()->tablet_application()->snapshot(group_id);
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    EXPECT_EQ(recovered->visible_row_count(), 2U);
    EXPECT_EQ(recovered->retry_entry_count(), 1U);
    auto election = database->ingest_runtime()->runtime()->try_submit(
        {{group_id, raft::StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value());
    ASSERT_TRUE(election->wait().has_value());
  }

  ASSERT_TRUE(database->ingest_runtime()->coordinator()->admit(request(command(), 3U)).is_ok());
  first_response = await_response(*database->ingest_runtime());
  first_acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(first_response.frame.payload);
  ASSERT_TRUE(first_acknowledgement.has_value());
  EXPECT_EQ(first_acknowledgement->outcome, network::IngestOutcome::kMatchingRetry);
  EXPECT_EQ(first_acknowledgement->log_index, 2U);
  ASSERT_TRUE(
      database->ingest_runtime()->coordinator()->admit(request(second_command, 4U)).is_ok());
  second_response = await_response(*database->ingest_runtime());
  second_acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(second_response.frame.payload);
  ASSERT_TRUE(second_acknowledgement.has_value());
  EXPECT_EQ(second_acknowledgement->outcome, network::IngestOutcome::kMatchingRetry);
  EXPECT_EQ(second_acknowledgement->log_index, 2U);

  auto metadata_election = database->ingest_runtime()->runtime()->try_submit(
      {{metadata_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(metadata_election.has_value()) << metadata_election.error().to_string();
  ASSERT_TRUE(metadata_election->wait().has_value());
  auto read_barrier = ReplicatedReadBarrier::create_local(
      database->ingest_runtime()->runtime(),
      {database->query_barrier_groups().begin(), database->query_barrier_groups().end()});
  ASSERT_TRUE(read_barrier.has_value()) << read_barrier.error().to_string();
  auto authorities = read_barrier->await_authority();
  ASSERT_TRUE(authorities.has_value()) << authorities.error().to_string();
  std::vector<raft::GroupReadBarrier> barriers;
  for (const ReplicatedReadAuthority& authority : *authorities)
    barriers.push_back(authority.barrier);

  auto mutable_snapshot = database->acquire_query_snapshot(barriers);
  ASSERT_TRUE(mutable_snapshot.has_value()) << mutable_snapshot.error().to_string();
  ASSERT_NE(mutable_snapshot->cluster_node(1U), nullptr);
  EXPECT_EQ(*mutable_snapshot->cluster_node(1U), (raft::ClusterNodeMetadata{1U, "127.0.0.1:7411"}));
  EXPECT_EQ(mutable_snapshot->cluster_node(2U), nullptr);
  const auto first_publication =
      database->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  const auto second_publication =
      database->ingest_runtime()->tablet_application()->snapshot(second_tablet_group());
  ASSERT_TRUE(first_publication.has_value());
  ASSERT_TRUE(second_publication.has_value());
  ASSERT_TRUE(first_publication->applied_position().has_value());
  ASSERT_TRUE(second_publication->applied_position().has_value());
  const auto first_authority_iterator =
      std::ranges::find(*authorities, tablet_group(), [](const ReplicatedReadAuthority& authority) {
        return authority.observation.group_id;
      });
  const auto second_authority_iterator = std::ranges::find(
      *authorities, second_tablet_group(),
      [](const ReplicatedReadAuthority& authority) { return authority.observation.group_id; });
  ASSERT_NE(first_authority_iterator, authorities->end());
  ASSERT_NE(second_authority_iterator, authorities->end());
  const ReplicatedReadAuthority& first_authority = *first_authority_iterator;
  const ReplicatedReadAuthority& second_authority = *second_authority_iterator;
  query::DistributedVectorQueryPlan mutable_plan{
      .query_id = id(0x79U),
      .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
      .fragments = {{.tablet_id = second_tablet_id(),
                     .minimum_event_time = 0,
                     .maximum_event_time = 0,
                     .leader_node = second_authority.observation.node_id,
                     .local_applied_position =
                         second_publication->applied_position()->record_sequence,
                     .known_leader_commit_position = second_authority.observation.commit_index},
                    {.tablet_id = tablet_id(),
                     .minimum_event_time = 0,
                     .maximum_event_time = 0,
                     .leader_node = first_authority.observation.node_id,
                     .local_applied_position =
                         first_publication->applied_position()->record_sequence,
                     .known_leader_commit_position = first_authority.observation.commit_index}},
      .intent = {.mode = query::DistributedVectorPlanMode::kRows, .row_output_indices = {0U, 1U}}};
  const std::array<std::uint32_t, 2U> projection{0U, 1U};
  const query::DistributedVectorResultSchema result_schema{
      .columns = {{.name = "ts",
                   .type = columnar::test::type(schema::LogicalTypeKind::kTimestampNs),
                   .nullable = false},
                  {.name = "tag",
                   .type = columnar::test::type(schema::LogicalTypeKind::kString),
                   .nullable = true}}};
  auto mutable_fragments = mutable_snapshot->bind_linearizable_mutable_vector_fragments(
      {.plan = std::cref(mutable_plan),
       .table_id = columnar::test::batch_schema()->table_id(),
       .group_authorities = *authorities,
       .destination_column_ordinals = projection,
       .result_schema = std::cref(result_schema)});
  ASSERT_TRUE(mutable_fragments.has_value()) << mutable_fragments.error().to_string();
  ASSERT_EQ(mutable_fragments->size(), 2U);
  EXPECT_EQ((*mutable_fragments)[0].tablet_id, second_tablet_id());
  EXPECT_EQ((*mutable_fragments)[0].raft_group_id, second_tablet_group());
  EXPECT_EQ((*mutable_fragments)[0].database_id.uuid(), descriptor().database_id);
  EXPECT_EQ((*mutable_fragments)[1].tablet_id, tablet_id());
  EXPECT_EQ((*mutable_fragments)[1].raft_group_id, tablet_group());
  EXPECT_EQ((*mutable_fragments)[1].linearizable_barrier, first_authority.barrier.barrier);
  network::TlsClientContext mutable_tls;
  const std::array mutable_tls_contexts{
      cluster::DistributedQueryNodeTlsContext{.node_id = 1U, .tls_context = &mutable_tls}};
  auto routed_mutable_query = mutable_snapshot->bind_and_resolve_linearizable_mutable_vector_query(
      {.plan = std::cref(mutable_plan),
       .table_id = columnar::test::batch_schema()->table_id(),
       .group_authorities = *authorities,
       .destination_column_ordinals = projection,
       .result_schema = std::cref(result_schema)},
      mutable_tls_contexts);
  ASSERT_TRUE(routed_mutable_query.has_value()) << routed_mutable_query.error().to_string();
  ASSERT_EQ(routed_mutable_query->fragments.size(), 2U);
  ASSERT_EQ(routed_mutable_query->routes.size(), 1U);
  EXPECT_EQ(routed_mutable_query->routes.front().node_id, 1U);
  EXPECT_EQ(routed_mutable_query->routes.front().endpoints,
            (std::vector<network::Ipv4Endpoint>{{{127U, 0U, 0U, 1U}, 7411U}}));
  EXPECT_EQ(routed_mutable_query->routes.front().tls_context, &mutable_tls);

  auto distributed_parsed =
      query::parse_sql_v1_select("SELECT tag AS label, ts, tag AS repeated FROM events "
                                 "WHERE ts >= TIMESTAMP '1970-01-01 00:00:00Z' "
                                 "ORDER BY label ASC, ts LIMIT 1");
  ASSERT_TRUE(distributed_parsed.has_value()) << distributed_parsed.error().status().to_string();
  auto distributed_bound =
      query::bind_sql_v1_select(std::move(*distributed_parsed), mutable_snapshot->catalog());
  ASSERT_TRUE(distributed_bound.has_value()) << distributed_bound.error().status().to_string();
  auto distributed_sql =
      query::lower_bound_sql_select_to_distributed_vector_rows(*distributed_bound);
  ASSERT_TRUE(distributed_sql.has_value()) << distributed_sql.error().status().to_string();
  auto prepared_sql_query = mutable_snapshot->prepare_linearizable_mutable_vector_rows_query(
      {.query_id = id(0x7aU),
       .sql_plan = std::cref(*distributed_sql),
       .group_authorities = *authorities},
      mutable_tls_contexts);
  ASSERT_TRUE(prepared_sql_query.has_value()) << prepared_sql_query.error().to_string();
  ASSERT_EQ(prepared_sql_query->fragments.size(), 2U);
  EXPECT_EQ(prepared_sql_query->fragments[0].tablet_id, tablet_id());
  EXPECT_EQ(prepared_sql_query->fragments[1].tablet_id, second_tablet_id());
  for (const query::DistributedMutableVectorFragment& fragment : prepared_sql_query->fragments) {
    EXPECT_EQ(fragment.query_id, id(0x7aU));
    EXPECT_EQ(fragment.destination_column_ordinals, (std::vector<std::uint32_t>{1U, 0U}));
    EXPECT_EQ(fragment.plan.row_output_indices, (std::vector<std::uint32_t>{0U, 1U, 0U}));
    ASSERT_TRUE(fragment.event_time_predicate.has_value());
    EXPECT_EQ(fragment.event_time_predicate->lower,
              (cseg::EventTimeBound{.value = 0, .inclusive = true}));
    EXPECT_EQ(fragment.result_schema, distributed_sql->result_schema);
  }
  ASSERT_EQ(prepared_sql_query->routes.size(), 1U);
  EXPECT_EQ(prepared_sql_query->routes.front().node_id, 1U);

  auto grouped_parsed = query::parse_sql_v1_select(
      "SELECT tag, count(*) AS n FROM events GROUP BY tag ORDER BY n DESC LIMIT 1");
  ASSERT_TRUE(grouped_parsed.has_value()) << grouped_parsed.error().status().to_string();
  auto grouped_bound =
      query::bind_sql_v1_select(std::move(*grouped_parsed), mutable_snapshot->catalog());
  ASSERT_TRUE(grouped_bound.has_value()) << grouped_bound.error().status().to_string();
  auto grouped_sql =
      query::lower_bound_sql_select_to_distributed_vector_grouped_aggregate(*grouped_bound);
  ASSERT_TRUE(grouped_sql.has_value()) << grouped_sql.error().status().to_string();
  auto prepared_grouped =
      mutable_snapshot->prepare_linearizable_mutable_vector_grouped_aggregate_query(
          {.query_id = id(0x7bU),
           .sql_plan = std::cref(*grouped_sql),
           .group_authorities = *authorities},
          mutable_tls_contexts);
  ASSERT_TRUE(prepared_grouped.has_value()) << prepared_grouped.error().to_string();
  ASSERT_EQ(prepared_grouped->fragments.size(), 2U);
  EXPECT_EQ(prepared_grouped->routes.size(), 1U);
  ASSERT_EQ(prepared_grouped->keys.size(), 1U);
  EXPECT_EQ(prepared_grouped->keys.front().column_ordinal, 0U);
  EXPECT_EQ(prepared_grouped->keys.front().type,
            columnar::test::type(schema::LogicalTypeKind::kString));
  EXPECT_TRUE(prepared_grouped->keys.front().nullable);
  ASSERT_EQ(prepared_grouped->aggregates.size(), 1U);
  EXPECT_EQ(prepared_grouped->aggregates.front().operation,
            query::VectorAggregateOperation::kCountStar);
  EXPECT_FALSE(prepared_grouped->aggregates.front().input.has_value());
  for (const query::DistributedMutableVectorFragment& fragment : prepared_grouped->fragments) {
    EXPECT_EQ(fragment.query_id, id(0x7bU));
    EXPECT_EQ(fragment.plan, grouped_sql->intent);
    EXPECT_EQ(fragment.result_schema, grouped_sql->result_schema);
    EXPECT_FALSE(fragment.pre_group_program.has_value());
  }

  auto computed_grouped_parsed = query::parse_sql_v1_select(
      "SELECT upper(tag) AS normalized, count(*) AS n FROM events GROUP BY upper(tag)");
  ASSERT_TRUE(computed_grouped_parsed.has_value())
      << computed_grouped_parsed.error().status().to_string();
  auto computed_grouped_bound =
      query::bind_sql_v1_select(std::move(*computed_grouped_parsed), mutable_snapshot->catalog());
  ASSERT_TRUE(computed_grouped_bound.has_value())
      << computed_grouped_bound.error().status().to_string();
  auto computed_grouped_sql = query::lower_bound_sql_select_to_distributed_vector_grouped_aggregate(
      *computed_grouped_bound);
  ASSERT_TRUE(computed_grouped_sql.has_value())
      << computed_grouped_sql.error().status().to_string();
  ASSERT_TRUE(computed_grouped_sql->pre_group_program.has_value());
  auto prepared_computed_grouped =
      mutable_snapshot->prepare_linearizable_mutable_vector_grouped_aggregate_query(
          {.query_id = id(0x7cU),
           .sql_plan = std::cref(*computed_grouped_sql),
           .group_authorities = *authorities},
          mutable_tls_contexts);
  ASSERT_TRUE(prepared_computed_grouped.has_value())
      << prepared_computed_grouped.error().to_string();
  ASSERT_EQ(prepared_computed_grouped->fragments.size(), 2U);
  ASSERT_EQ(prepared_computed_grouped->keys.size(), 1U);
  EXPECT_EQ(prepared_computed_grouped->keys.front().column_ordinal, 0U);
  EXPECT_EQ(prepared_computed_grouped->keys.front().type,
            columnar::test::type(schema::LogicalTypeKind::kString));
  EXPECT_TRUE(prepared_computed_grouped->keys.front().nullable);
  for (const query::DistributedMutableVectorFragment& fragment :
       prepared_computed_grouped->fragments) {
    EXPECT_EQ(fragment.query_id, id(0x7cU));
    EXPECT_EQ(fragment.destination_column_ordinals, (std::vector<std::uint32_t>{1U}));
    ASSERT_TRUE(fragment.pre_group_program.has_value());
    EXPECT_EQ(*fragment.pre_group_program, *computed_grouped_sql->pre_group_program);
    ASSERT_EQ(fragment.pre_group_program->outputs.size(), 1U);
    EXPECT_EQ(std::get<query::VectorUnaryExpression>(
                  fragment.pre_group_program->outputs.front().instructions()[1])
                  .operation,
              query::VectorUnaryOperation::kUpperAscii);
  }
  auto production_worker_context = database->acquire(prepared_sql_query->fragments.front());
  ASSERT_TRUE(production_worker_context.has_value())
      << production_worker_context.error().to_string();
  EXPECT_EQ(production_worker_context->snapshot.tablet_id(), tablet_id());
  EXPECT_EQ(production_worker_context->raft_group_id, tablet_group());
  EXPECT_EQ(production_worker_context->local_linearizable_barrier,
            prepared_sql_query->fragments.front().linearizable_barrier);
  auto wrong_worker_term = prepared_sql_query->fragments.front();
  ASSERT_TRUE(wrong_worker_term.linearizable_barrier.has_value());
  ++wrong_worker_term.linearizable_barrier->term;
  EXPECT_EQ(database->acquire(wrong_worker_term).error().code(), common::StatusCode::kUnavailable);
  auto wrong_worker_position = prepared_sql_query->fragments.front();
  ++wrong_worker_position.applied_position;
  EXPECT_EQ(database->acquire(wrong_worker_position).error().code(),
            common::StatusCode::kUnavailable);
  auto wrong_worker_database = prepared_sql_query->fragments.front();
  wrong_worker_database.database_id = manifest::DatabaseId::from_uuid(id(0x7bU)).value();
  EXPECT_EQ(database->acquire(wrong_worker_database).error().code(),
            common::StatusCode::kUnavailable);

  DistributedTestNodeAuthorizer node_authorizer;
  DistributedTestAuthenticator inbound_authenticator{91U};
  const cluster::DistributedMutableVectorQueryTlsLimits distributed_carrier{
      .handshake_timeout = std::chrono::milliseconds{1000},
      .exchange_timeout = std::chrono::milliseconds{1000},
      .maximum_response_frames = 4U,
      .maximum_response_bytes = std::size_t{1024U} * 1024U};
  auto client_context = network::TlsClientContext::create(distributed_client_tls());
  ASSERT_TRUE(client_context.has_value()) << client_context.error().to_string();
  const std::array distributed_tls_contexts{cluster::DistributedQueryNodeTlsContext{
      .node_id = 1U, .tls_context = std::addressof(*client_context)}};
  const std::array grouped_result_tls_contexts{cluster::DistributedQueryNodeTlsContext{
      .node_id = 9U, .tls_context = std::addressof(*client_context)}};
  DistributedTestAuthenticator outbound_authenticator{92U};
  auto distributed_server = ReplicatedDistributedMutableQueryControlTcpServer::start(
      {.worker = {.local_node_id = 1U, .context_provider = &*database},
       .read_barrier = &*read_barrier,
       .listener = {.bind_endpoint = {{127U, 0U, 0U, 1U}, 7411U}},
       .tls = distributed_server_tls(),
       .authenticator = &inbound_authenticator,
       .node_authorizer = &node_authorizer,
       .grouped_shuffle_jobs =
           cluster::DistributedVectorGroupedAggregateShuffleJobServiceConfig{
               .local_node_id = 1U,
               .shuffle_tls = distributed_server_tls(),
               .shuffle_authenticator = &inbound_authenticator,
               .result_authenticator = &inbound_authenticator,
               .node_authorizer = &node_authorizer,
               .result_tls_contexts = grouped_result_tls_contexts},
       .carrier_limits = {.handshake_timeout = distributed_carrier.handshake_timeout,
                          .exchange_timeout = distributed_carrier.exchange_timeout,
                          .maximum_mutable_response_frames = 4U,
                          .maximum_mutable_response_bytes = std::size_t{1024U} * 1024U,
                          .maximum_mutable_grouped_response_frames = 4U,
                          .maximum_mutable_grouped_response_bytes = std::size_t{1024U} * 1024U},
       .maximum_connections = 4U,
       .maximum_accepts_per_poll = 4U});
  ASSERT_TRUE(distributed_server.has_value()) << distributed_server.error().to_string();
  auto grouped_shuffle_provider = NativeDistributedGroupedShuffleJobProvider::create(
      {.coordinator_node_id = 9U,
       .result_tls = distributed_server_tls(),
       .authenticator = &outbound_authenticator,
       .node_authorizer = &node_authorizer,
       .connect_timeout = std::chrono::milliseconds{1000},
       .reducer_execution_timeout = std::chrono::milliseconds{5000}});
  ASSERT_TRUE(grouped_shuffle_provider.has_value()) << grouped_shuffle_provider.error().to_string();
  const NativeDistributedMutableVectorRowsQueryConfig distributed_config{
      .source_node_id = 9U,
      .authenticator = &outbound_authenticator,
      .node_authorizer = &node_authorizer,
      .grouped_shuffle_provider = std::addressof(*grouped_shuffle_provider),
      .tls_contexts = distributed_tls_contexts,
      .execution = {.sender = {.retry = {.maximum_attempts = 1U},
                               .maximum_response_frames = 4U,
                               .maximum_response_bytes = std::size_t{1024U} * 1024U}},
      .grouped_aggregate_execution = {.sender = {.retry = {.maximum_attempts = 1U},
                                                 .maximum_response_frames = 4U,
                                                 .maximum_response_bytes =
                                                     std::size_t{1024U} * 1024U}},
      .carrier = distributed_carrier,
      .grouped_aggregate_carrier = {.handshake_timeout = std::chrono::milliseconds{1000},
                                    .exchange_timeout = std::chrono::milliseconds{1000},
                                    .maximum_response_frames = 4U,
                                    .maximum_response_bytes = std::size_t{1024U} * 1024U},
      .finalization = {.output_batch = {.maximum_rows = 2U}},
      .grouped_aggregate_finalization = {.output_batch = {.maximum_rows = 2U}},
      .connect_timeout = std::chrono::milliseconds{1000},
      .execution_timeout = std::chrono::milliseconds{5000},
      .maximum_poll_wait = std::chrono::milliseconds{1}};
  NativeProtocolService distributed_native{*database, *read_barrier, distributed_config};
  std::atomic<bool> stop_server{};
  std::atomic<bool> server_failed{};
  std::thread server_thread{[&] {
    while (!stop_server.load(std::memory_order_acquire)) {
      if (!distributed_server->poll_once(std::chrono::milliseconds{1}).is_ok()) {
        server_failed.store(true, std::memory_order_release);
        return;
      }
    }
  }};
  auto native_distributed = distributed_native.execute_query(
      query_request("SELECT tag AS label, ts, tag AS repeated FROM events "
                    "WHERE ts >= TIMESTAMP '1970-01-01 00:00:00Z' "
                    "ORDER BY label ASC, ts LIMIT 1"));
  auto native_distributed_hidden = distributed_native.execute_query(
      query_request("SELECT tag AS label FROM events ORDER BY ts, label LIMIT 1"));
  auto native_distributed_aggregate = distributed_native.execute_query(
      query_request("SELECT count(*) + 1 AS rows_plus, count(tag) * 2 AS tags_twice, "
                    "upper(coalesce(min(tag), 'none')) AS first_tag, "
                    "max(enabled) AS any_enabled FROM events "
                    "WHERE enabled AND lower(tag) = 'x' ORDER BY rows_plus DESC LIMIT 1"));
  auto native_distributed_grouped = distributed_native.execute_query(query_request(
      "SELECT coalesce(lower(tag), 'missing') AS bucket, enabled, count(*) AS n, "
      "count(coalesce(lower(tag), 'missing')) AS labeled FROM events "
      "GROUP BY coalesce(lower(tag), 'missing'), enabled ORDER BY n DESC, bucket ASC LIMIT 2"));
  auto native_distributed_grouped_sufficient = distributed_native.execute_query(query_request(
      "SELECT tag, count(*) AS n FROM events GROUP BY tag ORDER BY n DESC, tag ASC LIMIT 2"));
  auto native_distributed_constants = distributed_native.execute_query(
      query_request("SELECT 7 AS marker, upper('ok') AS word FROM events LIMIT 1"));
  auto native_distributed_expressions = distributed_native.execute_query(
      query_request("SELECT lower(tag) AS folded, enabled, "
                    "time_bucket(INTERVAL '1 nanosecond', ts) AS shifted FROM events "
                    "WHERE enabled AND lower(tag) = 'x' "
                    "ORDER BY lower(tag), shifted DESC LIMIT 1"));
  stop_server.store(true, std::memory_order_release);
  server_thread.join();
  ASSERT_FALSE(server_failed.load(std::memory_order_acquire));
  ASSERT_TRUE(native_distributed.has_value()) << native_distributed.error().to_string();
  if (native_distributed->responses.size() == 1U &&
      native_distributed->responses.front().frame.header.message_type ==
          network::MessageType::kError) {
    auto error = network::decode_error_message(native_distributed->responses.front().frame.payload);
    ASSERT_TRUE(error.has_value());
    std::string message;
    message.reserve(error->message.size());
    for (const std::byte byte : error->message)
      message.push_back(static_cast<char>(byte));
    ADD_FAILURE() << "distributed Native query returned error: " << message;
  }
  ASSERT_EQ(native_distributed->responses.size(), 2U);
  EXPECT_EQ(native_distributed->result_rows, 1U);
  for (const network::NetworkTask& response : native_distributed->responses) {
    EXPECT_EQ(response.connection_id, 21U);
    EXPECT_EQ(response.principal_id, 19U);
    EXPECT_EQ(response.frame.header.request_id, 4U);
  }
  EXPECT_EQ(native_distributed->responses[0].frame.header.message_type,
            network::MessageType::kQueryResult);
  EXPECT_NE(native_distributed->responses[0].frame.header.flags & network::kFrameFlagEndStream, 0U);
  auto native_batch =
      network::decode_query_result_batch(native_distributed->responses[0].frame.payload);
  ASSERT_TRUE(native_batch.has_value()) << native_batch.error().to_string();
  EXPECT_EQ(native_batch->row_count(), 1U);
  ASSERT_EQ(native_batch->columns().size(), 3U);
  EXPECT_EQ(native_batch->columns()[0].name, "label");
  EXPECT_EQ(native_batch->columns()[1].name, "ts");
  EXPECT_EQ(native_batch->columns()[2].name, "repeated");
  const network::QueryResultCell* label = native_batch->cell(0U, 0U);
  const network::QueryResultCell* timestamp = native_batch->cell(0U, 1U);
  const network::QueryResultCell* repeated = native_batch->cell(0U, 2U);
  ASSERT_NE(label, nullptr);
  ASSERT_NE(timestamp, nullptr);
  ASSERT_NE(repeated, nullptr);
  EXPECT_FALSE(label->is_null);
  EXPECT_FALSE(timestamp->is_null);
  EXPECT_FALSE(repeated->is_null);
  ASSERT_EQ(label->value.size(), 1U);
  EXPECT_EQ(label->value.front(), std::byte{'x'});
  EXPECT_TRUE(std::ranges::equal(repeated->value, label->value));
  common::ByteReader timestamp_reader{timestamp->value};
  const auto timestamp_value = timestamp_reader.read_i64_le();
  ASSERT_TRUE(timestamp_value.has_value());
  EXPECT_EQ(*timestamp_value, 0);
  EXPECT_EQ(native_distributed->responses[1].frame.header.message_type,
            network::MessageType::kQueryEnd);
  ASSERT_TRUE(native_distributed_hidden.has_value())
      << native_distributed_hidden.error().to_string();
  ASSERT_EQ(native_distributed_hidden->responses.size(), 2U);
  const auto remote_hidden_batch =
      network::decode_query_result_batch(native_distributed_hidden->responses[0].frame.payload);
  ASSERT_TRUE(remote_hidden_batch.has_value()) << remote_hidden_batch.error().to_string();
  ASSERT_EQ(remote_hidden_batch->columns().size(), 1U);
  EXPECT_EQ(remote_hidden_batch->columns().front().name, "label");
  ASSERT_EQ(remote_hidden_batch->row_count(), 1U);
  ASSERT_TRUE(native_distributed_aggregate.has_value())
      << native_distributed_aggregate.error().to_string();
  ASSERT_EQ(native_distributed_aggregate->responses.size(), 2U);
  EXPECT_EQ(native_distributed_aggregate->result_rows, 1U);
  ASSERT_EQ(native_distributed_aggregate->responses[0].frame.header.message_type,
            network::MessageType::kQueryResult);
  EXPECT_NE(native_distributed_aggregate->responses[0].frame.header.flags &
                network::kFrameFlagEndStream,
            0U);
  const auto remote_aggregate_batch =
      network::decode_query_result_batch(native_distributed_aggregate->responses[0].frame.payload);
  ASSERT_TRUE(remote_aggregate_batch.has_value()) << remote_aggregate_batch.error().to_string();
  ASSERT_EQ(remote_aggregate_batch->row_count(), 1U);
  ASSERT_EQ(remote_aggregate_batch->columns().size(), 4U);
  EXPECT_EQ(remote_aggregate_batch->columns()[0].name, "rows_plus");
  EXPECT_EQ(remote_aggregate_batch->columns()[1].name, "tags_twice");
  EXPECT_EQ(remote_aggregate_batch->columns()[2].name, "first_tag");
  EXPECT_EQ(remote_aggregate_batch->columns()[3].name, "any_enabled");
  const network::QueryResultCell* aggregate_rows = remote_aggregate_batch->cell(0U, 0U);
  const network::QueryResultCell* aggregate_tags = remote_aggregate_batch->cell(0U, 1U);
  const network::QueryResultCell* aggregate_first_tag = remote_aggregate_batch->cell(0U, 2U);
  const network::QueryResultCell* aggregate_any_enabled = remote_aggregate_batch->cell(0U, 3U);
  ASSERT_NE(aggregate_rows, nullptr);
  ASSERT_NE(aggregate_tags, nullptr);
  ASSERT_NE(aggregate_first_tag, nullptr);
  ASSERT_NE(aggregate_any_enabled, nullptr);
  common::ByteReader aggregate_rows_reader{aggregate_rows->value};
  common::ByteReader aggregate_tags_reader{aggregate_tags->value};
  EXPECT_EQ(aggregate_rows_reader.read_i64_le().value(), 3);
  EXPECT_EQ(aggregate_tags_reader.read_i64_le().value(), 4);
  ASSERT_EQ(aggregate_first_tag->value.size(), 1U);
  EXPECT_EQ(aggregate_first_tag->value.front(), std::byte{'X'});
  ASSERT_EQ(aggregate_any_enabled->value.size(), 1U);
  EXPECT_EQ(aggregate_any_enabled->value.front(), std::byte{1U});
  EXPECT_EQ(native_distributed_aggregate->responses[1].frame.header.message_type,
            network::MessageType::kQueryEnd);
  ASSERT_TRUE(native_distributed_grouped.has_value())
      << native_distributed_grouped.error().to_string();
  if (native_distributed_grouped->responses.size() == 1U &&
      native_distributed_grouped->responses.front().frame.header.message_type ==
          network::MessageType::kError) {
    auto error =
        network::decode_error_message(native_distributed_grouped->responses.front().frame.payload);
    ASSERT_TRUE(error.has_value());
    std::string message;
    message.reserve(error->message.size());
    for (const std::byte byte : error->message)
      message.push_back(static_cast<char>(byte));
    ADD_FAILURE() << "distributed computed sufficient-state GROUP BY returned error: " << message;
  }
  ASSERT_EQ(native_distributed_grouped->responses.size(), 2U);
  EXPECT_EQ(native_distributed_grouped->result_rows, 2U);
  EXPECT_NE(native_distributed_grouped->responses[0].frame.header.flags &
                network::kFrameFlagEndStream,
            0U);
  const auto remote_grouped_first =
      network::decode_query_result_batch(native_distributed_grouped->responses[0].frame.payload);
  ASSERT_TRUE(remote_grouped_first.has_value()) << remote_grouped_first.error().to_string();
  ASSERT_EQ(remote_grouped_first->row_count(), 2U);
  EXPECT_EQ(bytes_as_string(remote_grouped_first->cell(0U, 0U)->value), "missing");
  EXPECT_EQ(bytes_as_string(remote_grouped_first->cell(1U, 0U)->value), "x");
  common::ByteReader grouped_first_count{remote_grouped_first->cell(0U, 2U)->value};
  common::ByteReader grouped_second_count{remote_grouped_first->cell(1U, 2U)->value};
  EXPECT_EQ(grouped_first_count.read_i64_le().value(), 2);
  EXPECT_EQ(grouped_second_count.read_i64_le().value(), 2);
  common::ByteReader grouped_first_labeled{remote_grouped_first->cell(0U, 3U)->value};
  common::ByteReader grouped_second_labeled{remote_grouped_first->cell(1U, 3U)->value};
  EXPECT_EQ(grouped_first_labeled.read_i64_le().value(), 2);
  EXPECT_EQ(grouped_second_labeled.read_i64_le().value(), 2);
  EXPECT_EQ(native_distributed_grouped->responses[1].frame.header.message_type,
            network::MessageType::kQueryEnd);
  ASSERT_TRUE(native_distributed_grouped_sufficient.has_value())
      << native_distributed_grouped_sufficient.error().to_string();
  if (native_distributed_grouped_sufficient->responses.size() == 1U &&
      native_distributed_grouped_sufficient->responses.front().frame.header.message_type ==
          network::MessageType::kError) {
    auto error = network::decode_error_message(
        native_distributed_grouped_sufficient->responses.front().frame.payload);
    ASSERT_TRUE(error.has_value());
    std::string message;
    message.reserve(error->message.size());
    for (const std::byte byte : error->message)
      message.push_back(static_cast<char>(byte));
    ADD_FAILURE() << "distributed sufficient-state GROUP BY returned error: " << message;
  }
  ASSERT_EQ(native_distributed_grouped_sufficient->responses.size(), 2U);
  EXPECT_EQ(native_distributed_grouped_sufficient->result_rows, 2U);
  const auto remote_grouped_sufficient_batch = network::decode_query_result_batch(
      native_distributed_grouped_sufficient->responses[0].frame.payload);
  ASSERT_TRUE(remote_grouped_sufficient_batch.has_value())
      << remote_grouped_sufficient_batch.error().to_string();
  ASSERT_EQ(remote_grouped_sufficient_batch->row_count(), 2U);
  ASSERT_EQ(remote_grouped_sufficient_batch->columns().size(), 2U);
  EXPECT_EQ(remote_grouped_sufficient_batch->columns()[0].name, "tag");
  EXPECT_EQ(remote_grouped_sufficient_batch->columns()[1].name, "n");
  ASSERT_FALSE(remote_grouped_sufficient_batch->cell(0U, 0U)->is_null);
  EXPECT_EQ(bytes_as_string(remote_grouped_sufficient_batch->cell(0U, 0U)->value), "x");
  EXPECT_TRUE(remote_grouped_sufficient_batch->cell(1U, 0U)->is_null);
  common::ByteReader grouped_sufficient_first_count{
      remote_grouped_sufficient_batch->cell(0U, 1U)->value};
  common::ByteReader grouped_sufficient_second_count{
      remote_grouped_sufficient_batch->cell(1U, 1U)->value};
  EXPECT_EQ(grouped_sufficient_first_count.read_i64_le().value(), 2);
  EXPECT_EQ(grouped_sufficient_second_count.read_i64_le().value(), 2);
  EXPECT_EQ(native_distributed_grouped_sufficient->responses[1].frame.header.message_type,
            network::MessageType::kQueryEnd);
  ASSERT_TRUE(native_distributed_constants.has_value())
      << native_distributed_constants.error().to_string();
  ASSERT_EQ(native_distributed_constants->responses.size(), 2U);
  EXPECT_EQ(native_distributed_constants->result_rows, 1U);
  const auto remote_constant_batch =
      network::decode_query_result_batch(native_distributed_constants->responses[0].frame.payload);
  ASSERT_TRUE(remote_constant_batch.has_value()) << remote_constant_batch.error().to_string();
  ASSERT_EQ(remote_constant_batch->row_count(), 1U);
  ASSERT_EQ(remote_constant_batch->columns().size(), 2U);
  EXPECT_EQ(remote_constant_batch->columns()[0].name, "marker");
  EXPECT_EQ(remote_constant_batch->columns()[1].name, "word");
  const network::QueryResultCell* marker = remote_constant_batch->cell(0U, 0U);
  const network::QueryResultCell* word = remote_constant_batch->cell(0U, 1U);
  ASSERT_NE(marker, nullptr);
  ASSERT_NE(word, nullptr);
  common::ByteReader marker_reader{marker->value};
  EXPECT_EQ(marker_reader.read_i64_le().value(), 7);
  const std::string_view expected_word{"OK"};
  EXPECT_TRUE(std::ranges::equal(
      word->value, std::as_bytes(std::span{expected_word.data(), expected_word.size()})));
  ASSERT_TRUE(native_distributed_expressions.has_value())
      << native_distributed_expressions.error().to_string();
  ASSERT_EQ(native_distributed_expressions->responses.size(), 2U);
  EXPECT_EQ(native_distributed_expressions->result_rows, 1U);
  const auto remote_expression_batch = network::decode_query_result_batch(
      native_distributed_expressions->responses[0].frame.payload);
  ASSERT_TRUE(remote_expression_batch.has_value()) << remote_expression_batch.error().to_string();
  ASSERT_EQ(remote_expression_batch->row_count(), 1U);
  ASSERT_EQ(remote_expression_batch->columns().size(), 3U);
  EXPECT_EQ(remote_expression_batch->columns()[0].name, "folded");
  EXPECT_EQ(remote_expression_batch->columns()[1].name, "enabled");
  EXPECT_EQ(remote_expression_batch->columns()[2].name, "shifted");
  const auto distributed_server_metrics = distributed_server->metrics();
  // Each of the five row-backed queries and both direct and computed sufficient-state queries
  // open one request for each of the two tablets hosted by this serving node.
  EXPECT_EQ(distributed_server_metrics.completed_mutable_queries, 10U);
  EXPECT_EQ(distributed_server_metrics.completed_mutable_grouped_queries, 4U);
  EXPECT_EQ(distributed_server_metrics.completed_read_authorities, 0U);
  ASSERT_TRUE(distributed_server->shutdown().is_ok());

  auto local_worker = ReplicatedDistributedMutableVectorQueryWorker::create(
      {.local_node_id = 1U, .context_provider = &*database});
  ASSERT_TRUE(local_worker.has_value()) << local_worker.error().to_string();
  auto local_grouped_worker = ReplicatedDistributedMutableVectorGroupedAggregateQueryWorker::create(
      {.local_node_id = 1U, .context_provider = &*database});
  ASSERT_TRUE(local_grouped_worker.has_value()) << local_grouped_worker.error().to_string();
  auto local_distributed_config = distributed_config;
  local_distributed_config.source_node_id = 1U;
  local_distributed_config.local_worker = &*local_worker;
  local_distributed_config.local_grouped_worker = &*local_grouped_worker;
  LocalGroupedShuffleProvider local_shuffle_provider;
  local_distributed_config.grouped_shuffle_provider = &local_shuffle_provider;
  NativeProtocolService local_distributed_native{*database, *read_barrier,
                                                 local_distributed_config};
  auto native_local = local_distributed_native.execute_query(
      query_request("SELECT tag AS label, ts, tag AS repeated FROM events "
                    "WHERE ts >= TIMESTAMP '1970-01-01 00:00:00Z' "
                    "ORDER BY label ASC, ts LIMIT 1"));
  ASSERT_TRUE(native_local.has_value()) << native_local.error().to_string();
  ASSERT_EQ(native_local->responses.size(), 2U);
  EXPECT_EQ(native_local->result_rows, native_distributed->result_rows);
  EXPECT_EQ(native_local->payload_bytes, native_distributed->payload_bytes);
  ASSERT_EQ(native_local->responses[0].frame.header.message_type,
            network::MessageType::kQueryResult);
  EXPECT_NE(native_local->responses[0].frame.header.flags & network::kFrameFlagEndStream, 0U);
  EXPECT_EQ(native_local->responses[0].frame.payload,
            native_distributed->responses[0].frame.payload);
  EXPECT_EQ(native_local->responses[1].frame.header.message_type, network::MessageType::kQueryEnd);

  auto native_local_grouped_sufficient = local_distributed_native.execute_query(query_request(
      "SELECT tag, count(*) AS n FROM events GROUP BY tag ORDER BY n DESC, tag ASC LIMIT 2"));
  ASSERT_TRUE(native_local_grouped_sufficient.has_value())
      << native_local_grouped_sufficient.error().to_string();
  ASSERT_EQ(native_local_grouped_sufficient->responses.size(), 2U);
  EXPECT_EQ(native_local_grouped_sufficient->result_rows,
            native_distributed_grouped_sufficient->result_rows);
  EXPECT_EQ(native_local_grouped_sufficient->payload_bytes,
            native_distributed_grouped_sufficient->payload_bytes);
  EXPECT_EQ(native_local_grouped_sufficient->responses[0].frame.payload,
            native_distributed_grouped_sufficient->responses[0].frame.payload);
  EXPECT_EQ(native_local_grouped_sufficient->responses[1].frame.header.message_type,
            network::MessageType::kQueryEnd);
  EXPECT_EQ(local_shuffle_provider.calls, 1U);

  auto native_local_aggregate = local_distributed_native.execute_query(
      query_request("SELECT count(*) + 1 AS rows_plus, count(tag) * 2 AS tags_twice, "
                    "upper(coalesce(min(tag), 'none')) AS first_tag, "
                    "max(enabled) AS any_enabled FROM events "
                    "WHERE enabled AND lower(tag) = 'x' ORDER BY rows_plus DESC LIMIT 1"));
  ASSERT_TRUE(native_local_aggregate.has_value()) << native_local_aggregate.error().to_string();
  ASSERT_EQ(native_local_aggregate->responses.size(), 2U);
  EXPECT_EQ(native_local_aggregate->result_rows, native_distributed_aggregate->result_rows);
  EXPECT_EQ(native_local_aggregate->responses[0].frame.payload,
            native_distributed_aggregate->responses[0].frame.payload);

  auto native_local_grouped = local_distributed_native.execute_query(query_request(
      "SELECT coalesce(lower(tag), 'missing') AS bucket, enabled, count(*) AS n, "
      "count(coalesce(lower(tag), 'missing')) AS labeled FROM events "
      "GROUP BY coalesce(lower(tag), 'missing'), enabled ORDER BY n DESC, bucket ASC LIMIT 2"));
  ASSERT_TRUE(native_local_grouped.has_value()) << native_local_grouped.error().to_string();
  ASSERT_EQ(native_local_grouped->responses.size(), native_distributed_grouped->responses.size());
  EXPECT_EQ(native_local_grouped->result_rows, native_distributed_grouped->result_rows);
  EXPECT_EQ(native_local_grouped->responses[0].frame.payload,
            native_distributed_grouped->responses[0].frame.payload);
  EXPECT_EQ(local_shuffle_provider.calls, 2U);

  auto native_local_constants = local_distributed_native.execute_query(
      query_request("SELECT 7 AS marker, upper('ok') AS word FROM events LIMIT 1"));
  ASSERT_TRUE(native_local_constants.has_value()) << native_local_constants.error().to_string();
  ASSERT_EQ(native_local_constants->responses.size(), 2U);
  EXPECT_EQ(native_local_constants->responses[0].frame.payload,
            native_distributed_constants->responses[0].frame.payload);

  auto native_local_expressions = local_distributed_native.execute_query(
      query_request("SELECT lower(tag) AS folded, enabled, "
                    "time_bucket(INTERVAL '1 nanosecond', ts) AS shifted FROM events "
                    "WHERE enabled AND lower(tag) = 'x' "
                    "ORDER BY lower(tag), shifted DESC LIMIT 1"));
  ASSERT_TRUE(native_local_expressions.has_value()) << native_local_expressions.error().to_string();
  ASSERT_EQ(native_local_expressions->responses.size(), 2U);
  EXPECT_EQ(native_local_expressions->responses[0].frame.payload,
            native_distributed_expressions->responses[0].frame.payload);

  auto native_zero_aggregate = local_distributed_native.execute_query(
      query_request("SELECT count(*) AS rows FROM events LIMIT 0"));
  ASSERT_TRUE(native_zero_aggregate.has_value()) << native_zero_aggregate.error().to_string();
  ASSERT_EQ(native_zero_aggregate->responses.size(), 2U);
  EXPECT_EQ(native_zero_aggregate->result_rows, 0U);
  const auto zero_aggregate_batch =
      network::decode_query_result_batch(native_zero_aggregate->responses[0].frame.payload);
  ASSERT_TRUE(zero_aggregate_batch.has_value()) << zero_aggregate_batch.error().to_string();
  EXPECT_EQ(zero_aggregate_batch->row_count(), 0U);
  ASSERT_EQ(zero_aggregate_batch->columns().size(), 1U);
  EXPECT_EQ(zero_aggregate_batch->columns().front().name, "rows");

  auto bounded_aggregate_config = local_distributed_config;
  bounded_aggregate_config.aggregate_finalization.maximum_input_rows = 1U;
  NativeProtocolService bounded_aggregate_native{*database, *read_barrier,
                                                 bounded_aggregate_config};
  auto bounded_aggregate =
      bounded_aggregate_native.execute_query(query_request("SELECT count(*) AS rows FROM events"));
  ASSERT_TRUE(bounded_aggregate.has_value()) << bounded_aggregate.error().to_string();
  ASSERT_EQ(bounded_aggregate->responses.size(), 1U);
  ASSERT_EQ(bounded_aggregate->responses.front().frame.header.message_type,
            network::MessageType::kError);
  const auto bounded_aggregate_error =
      network::decode_error_message(bounded_aggregate->responses.front().frame.payload);
  ASSERT_TRUE(bounded_aggregate_error.has_value()) << bounded_aggregate_error.error().to_string();
  EXPECT_EQ(bounded_aggregate_error->code, network::ProtocolErrorCode::kOverloaded);

  auto native_between = local_distributed_native.execute_query(
      query_request("SELECT tag AS label, ts, tag AS repeated FROM events "
                    "WHERE ts BETWEEN TIMESTAMP '1970-01-01 00:00:00Z' "
                    "AND TIMESTAMP '1970-01-01 00:00:00Z' "
                    "ORDER BY label ASC, ts LIMIT 1"));
  ASSERT_TRUE(native_between.has_value()) << native_between.error().to_string();
  ASSERT_EQ(native_between->responses.size(), 2U);
  EXPECT_EQ(native_between->responses[0].frame.payload,
            native_distributed->responses[0].frame.payload);
  EXPECT_EQ(native_between->responses[1].frame.header.message_type,
            network::MessageType::kQueryEnd);

  auto native_hidden_order = local_distributed_native.execute_query(
      query_request("SELECT tag AS label FROM events ORDER BY ts, label LIMIT 1"));
  ASSERT_TRUE(native_hidden_order.has_value()) << native_hidden_order.error().to_string();
  ASSERT_EQ(native_hidden_order->responses.size(), 2U);
  const auto hidden_order_batch =
      network::decode_query_result_batch(native_hidden_order->responses[0].frame.payload);
  ASSERT_TRUE(hidden_order_batch.has_value()) << hidden_order_batch.error().to_string();
  ASSERT_EQ(hidden_order_batch->columns().size(), 1U);
  EXPECT_EQ(hidden_order_batch->columns().front().name, "label");
  ASSERT_EQ(hidden_order_batch->row_count(), 1U);
  const network::QueryResultCell* hidden_order_label = hidden_order_batch->cell(0U, 0U);
  ASSERT_NE(hidden_order_label, nullptr);
  ASSERT_EQ(hidden_order_label->value.size(), 1U);
  EXPECT_EQ(hidden_order_label->value.front(), std::byte{'x'});

  AdvancingFailOnceMutableWorker rebinding_worker{*database, *local_worker};
  auto rebinding_config = local_distributed_config;
  rebinding_config.local_worker = &rebinding_worker;
  rebinding_config.maximum_authority_rebindings = 1U;
  NativeProtocolService rebinding_native{*database, *read_barrier, rebinding_config};
  auto rebound = rebinding_native.execute_query(
      query_request("SELECT tag AS label, ts, tag AS repeated FROM events "
                    "WHERE ts >= TIMESTAMP '1970-01-01 00:00:00Z' "
                    "ORDER BY label ASC, ts LIMIT 1"));
  ASSERT_TRUE(rebound.has_value()) << rebound.error().to_string();
  ASSERT_EQ(rebound->responses.size(), 2U);
  EXPECT_EQ(rebound->responses[0].frame.payload, native_local->responses[0].frame.payload);
  EXPECT_EQ(rebinding_worker.calls(), 3U);
  EXPECT_GT(rebinding_worker.second_term(), rebinding_worker.first_term());

  auto missing_local_worker_config = local_distributed_config;
  missing_local_worker_config.local_worker = nullptr;
  NativeProtocolService missing_local_worker_native{*database, *read_barrier,
                                                    missing_local_worker_config};
  auto missing_local_worker = missing_local_worker_native.execute_query(
      query_request("SELECT tag, ts FROM events ORDER BY tag ASC, ts LIMIT 1"));
  ASSERT_TRUE(missing_local_worker.has_value()) << missing_local_worker.error().to_string();
  ASSERT_EQ(missing_local_worker->responses.size(), 1U);
  ASSERT_EQ(missing_local_worker->responses.front().frame.header.message_type,
            network::MessageType::kError);
  auto missing_local_worker_error =
      network::decode_error_message(missing_local_worker->responses.front().frame.payload);
  ASSERT_TRUE(missing_local_worker_error.has_value())
      << missing_local_worker_error.error().to_string();
  std::string missing_local_worker_message;
  missing_local_worker_message.reserve(missing_local_worker_error->message.size());
  for (const std::byte byte : missing_local_worker_error->message)
    missing_local_worker_message.push_back(static_cast<char>(byte));
  EXPECT_EQ(missing_local_worker_message, "distributed Native local fragment worker is absent");

  auto excessive_rebinding_config = local_distributed_config;
  excessive_rebinding_config.maximum_authority_rebindings = 1025U;
  NativeProtocolService excessive_rebinding_native{*database, *read_barrier,
                                                   excessive_rebinding_config};
  auto excessive_rebinding = excessive_rebinding_native.execute_query(
      query_request("SELECT tag, ts FROM events ORDER BY tag ASC, ts LIMIT 1"));
  ASSERT_TRUE(excessive_rebinding.has_value()) << excessive_rebinding.error().to_string();
  ASSERT_EQ(excessive_rebinding->responses.size(), 1U);
  ASSERT_EQ(excessive_rebinding->responses.front().frame.header.message_type,
            network::MessageType::kError);
  auto excessive_rebinding_error =
      network::decode_error_message(excessive_rebinding->responses.front().frame.payload);
  ASSERT_TRUE(excessive_rebinding_error.has_value())
      << excessive_rebinding_error.error().to_string();
  EXPECT_EQ(excessive_rebinding_error->code, network::ProtocolErrorCode::kInvalidRequest);

  auto nil_sql_query = mutable_snapshot->prepare_linearizable_mutable_vector_rows_query(
      {.query_id = {}, .sql_plan = std::cref(*distributed_sql), .group_authorities = *authorities},
      mutable_tls_contexts);
  ASSERT_FALSE(nil_sql_query.has_value());
  EXPECT_EQ(nil_sql_query.error().code(), common::StatusCode::kInvalidArgument);

  ++mutable_plan.fragments.front().local_applied_position;
  auto mixed_publication = mutable_snapshot->bind_linearizable_mutable_vector_fragments(
      {.plan = std::cref(mutable_plan),
       .table_id = columnar::test::batch_schema()->table_id(),
       .group_authorities = *authorities,
       .destination_column_ordinals = projection,
       .result_schema = std::cref(result_schema)});
  ASSERT_FALSE(mixed_publication.has_value());
  EXPECT_EQ(mixed_publication.error().code(), common::StatusCode::kUnavailable);
  --mutable_plan.fragments.front().local_applied_position;
  std::vector<ReplicatedReadAuthority> incomplete_authorities = *authorities;
  incomplete_authorities.pop_back();
  auto incomplete_fragments = mutable_snapshot->bind_linearizable_mutable_vector_fragments(
      {.plan = std::cref(mutable_plan),
       .table_id = columnar::test::batch_schema()->table_id(),
       .group_authorities = incomplete_authorities,
       .destination_column_ordinals = projection,
       .result_schema = std::cref(result_schema)});
  ASSERT_FALSE(incomplete_fragments.has_value());
  EXPECT_EQ(incomplete_fragments.error().code(), common::StatusCode::kUnavailable);
  auto incomplete_sql_query = mutable_snapshot->prepare_linearizable_mutable_vector_rows_query(
      {.query_id = id(0x7bU),
       .sql_plan = std::cref(*distributed_sql),
       .group_authorities = incomplete_authorities},
      mutable_tls_contexts);
  ASSERT_FALSE(incomplete_sql_query.has_value());
  EXPECT_EQ(incomplete_sql_query.error().code(), common::StatusCode::kUnavailable);
  query::DistributedVectorRowsSqlPlan stale_sql = *distributed_sql;
  stale_sql.destination_schema_id = columnar::test::id<schema::SchemaId>(99U);
  auto stale_sql_query = mutable_snapshot->prepare_linearizable_mutable_vector_rows_query(
      {.query_id = id(0x7cU), .sql_plan = std::cref(stale_sql), .group_authorities = *authorities},
      mutable_tls_contexts);
  ASSERT_FALSE(stale_sql_query.has_value());
  EXPECT_EQ(stale_sql_query.error().code(), common::StatusCode::kUnavailable);
  ASSERT_TRUE(read_barrier->shutdown().is_ok());

  auto snapshot = database->acquire_query_snapshot();
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  auto parsed = query::parse_sql_v1_select("SELECT count(*) AS rows FROM events");
  ASSERT_TRUE(parsed.has_value()) << parsed.error().status().to_string();
  auto bound = query::bind_sql_v1_select(std::move(*parsed), snapshot->catalog());
  ASSERT_TRUE(bound.has_value()) << bound.error().status().to_string();
  auto lowered = query::lower_bound_sql_select(*bound);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  ASSERT_TRUE(database->shutdown().is_ok());

  query::QueryResourceContext resources =
      query::QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
  const auto schema = columnar::test::batch_schema();
  auto pipeline = snapshot->instantiate_table_pipeline(resources, schema->table_id(),
                                                       schema->schema_id(), *lowered);
  ASSERT_TRUE(pipeline.has_value()) << pipeline.error().to_string();
  auto step = (*pipeline)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  ASSERT_EQ(step->kind(), query::PhysicalOperatorStepKind::kChunk);
  const auto cell = step->chunk()->chunk().cell({.column_ordinal = 0U, .selected_row = 0U});
  ASSERT_TRUE(cell.has_value());
  common::ByteReader count{cell->bytes().value()};
  EXPECT_EQ(count.read_i64_le().value(), 4);
  step = (*pipeline)->next(resources);
  ASSERT_TRUE(step.has_value());
  EXPECT_EQ(step->kind(), query::PhysicalOperatorStepKind::kEnd);
  pipeline->reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(ReplicatedIngestDatabaseTest, RebuildsRetainedSchemaLineageAfterCommittedCatalogEvolution) {
  TemporaryDirectory directory;
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  auto initial = ReplicatedIngestRuntime::create_new(initial_runtime_config(*bootstrap));
  ASSERT_TRUE(initial.has_value()) << initial.error().to_string();
  elect_and_provision(*initial, false);
  ASSERT_TRUE(initial->coordinator()->admit(request()).is_ok());
  auto base_response = await_response(*initial);
  auto base_acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(base_response.frame.payload);
  ASSERT_TRUE(base_acknowledgement.has_value());
  EXPECT_EQ(base_acknowledgement->outcome, network::IngestOutcome::kApplied);
  EXPECT_EQ(base_acknowledgement->log_index, 1U);

  const raft::ProposeOperation successor{
      raft::kRaftSchemaDefinitionEntryType,
      raft::encode_schema_definition_v1(
          {.name = "events", .quoted = false, .schema = columnar::test::successor_batch_schema()})
          .value()};
  auto evolved = initial->runtime()->try_submit({{metadata_group(), successor}});
  ASSERT_TRUE(evolved.has_value()) << evolved.error().to_string();
  ASSERT_TRUE(evolved->wait().has_value());
  auto evolved_catalog = initial->metadata_application()->catalog_snapshot();
  ASSERT_TRUE(evolved_catalog.has_value());
  ASSERT_EQ((*evolved_catalog)->schema_definitions.size(), 2U);
  ASSERT_EQ((*evolved_catalog)->active_schemas.size(), 1U);
  EXPECT_EQ((*evolved_catalog)->active_schemas.front().schema_id,
            columnar::test::successor_batch_schema()->schema_id());
  ASSERT_TRUE(initial->shutdown().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());

  auto database =
      ReplicatedIngestDatabase::open_existing({.bootstrap = bootstrap_config, .groups = groups()});
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  auto recovered_catalog = database->ingest_runtime()->metadata_application()->catalog_snapshot();
  ASSERT_TRUE(recovered_catalog.has_value());
  ASSERT_EQ((*recovered_catalog)->schema_definitions.size(), 2U);
  auto recovered = database->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->visible_row_count(), 2U);
  EXPECT_EQ(recovered->retry_entry_count(), 1U);
  EXPECT_EQ(recovered->schema_ptr()->schema_id(), columnar::test::batch_schema()->schema_id());

  auto election = database->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_TRUE(election->wait().has_value());
  ASSERT_TRUE(
      database->ingest_runtime()->coordinator()->admit(request(successor_command(), 2U)).is_ok());
  auto successor_response = await_response(*database->ingest_runtime());
  auto successor_acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(successor_response.frame.payload);
  ASSERT_TRUE(successor_acknowledgement.has_value());
  EXPECT_EQ(successor_acknowledgement->outcome, network::IngestOutcome::kApplied);
  EXPECT_EQ(successor_acknowledgement->log_index, 2U);
  recovered = database->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->visible_row_count(), 4U);
  EXPECT_EQ(recovered->retry_entry_count(), 2U);
  EXPECT_EQ(recovered->schema_ptr()->schema_id(),
            columnar::test::successor_batch_schema()->schema_id());
  ASSERT_EQ(recovered->sealed_generations().size(), 1U);
  EXPECT_EQ(recovered->sealed_generations().front().schema_ptr()->schema_id(),
            columnar::test::batch_schema()->schema_id());
  ASSERT_TRUE(database->shutdown().is_ok());

  auto repeated =
      ReplicatedIngestDatabase::open_existing({.bootstrap = bootstrap_config, .groups = groups()});
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  auto repeated_snapshot =
      repeated->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(repeated_snapshot.has_value()) << repeated_snapshot.error().to_string();
  EXPECT_EQ(repeated_snapshot->visible_row_count(), 4U);
  EXPECT_EQ(repeated_snapshot->retry_entry_count(), 2U);
  EXPECT_EQ(repeated_snapshot->schema_ptr()->schema_id(),
            columnar::test::successor_batch_schema()->schema_id());
  election = repeated->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_TRUE(election->wait().has_value());
  ASSERT_TRUE(
      repeated->ingest_runtime()->coordinator()->admit(request(successor_command(), 3U)).is_ok());
  auto retry_response = await_response(*repeated->ingest_runtime());
  auto retry_acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(retry_response.frame.payload);
  ASSERT_TRUE(retry_acknowledgement.has_value());
  EXPECT_EQ(retry_acknowledgement->outcome, network::IngestOutcome::kMatchingRetry);
  EXPECT_EQ(retry_acknowledgement->log_index, 3U);
  ASSERT_TRUE(repeated->shutdown().is_ok());
}

TEST(ReplicatedIngestDatabaseTest, ReopensAndCompletesACommittedJointReconfiguration) {
  TemporaryDirectory directory;
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  auto configured = initial_runtime_config(*bootstrap);
  configured.groups = joint_groups();
  auto initial = ReplicatedIngestRuntime::create_new(std::move(configured));
  ASSERT_TRUE(initial.has_value()) << initial.error().to_string();

  auto metadata_election =
      initial->runtime()->try_submit({{metadata_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(metadata_election.has_value()) << metadata_election.error().to_string();
  ASSERT_TRUE(metadata_election->wait().has_value());
  const raft::ProposeOperation schema{
      raft::kRaftSchemaDefinitionEntryType,
      raft::encode_schema_definition_v1(
          {.name = "events", .quoted = false, .schema = columnar::test::batch_schema()})
          .value()};
  const raft::ProposeOperation policy{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TablePolicyMetadata{columnar::test::batch_schema()->table_id(), 1'000'000'000LL,
                                    86'400'000'000'000LL, 86'400'000'000'000LL, 0LL, 8U})
          .value()};
  const raft::ProposeOperation placement{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TabletPlacementMetadata{
              columnar::test::batch_schema()->table_id(), tablet_id(), 1U, {1U, 2U}, 1U})
          .value()};
  const raft::ProposeOperation binding{
      raft::kRaftTabletGroupBindingEntryType,
      raft::encode_tablet_group_binding_v1({tablet_id(), tablet_group()}).value()};
  auto metadata = initial->runtime()->try_submit({{metadata_group(), schema},
                                                  {metadata_group(), policy},
                                                  {metadata_group(), placement},
                                                  {metadata_group(), binding}});
  ASSERT_TRUE(metadata.has_value()) << metadata.error().to_string();
  ASSERT_TRUE(metadata->wait().has_value());

  auto tablet_election =
      initial->runtime()->try_submit({{tablet_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(tablet_election.has_value()) << tablet_election.error().to_string();
  ASSERT_TRUE(tablet_election->wait().has_value());
  auto vote = initial->runtime()->try_submit(
      {{tablet_group(), raft::ReceiveOperation{2U, raft::RequestVoteResponse{1U, true}}}});
  ASSERT_TRUE(vote.has_value()) << vote.error().to_string();
  ASSERT_TRUE(vote->wait().has_value());
  auto proposed = initial->runtime()->try_submit(
      {{tablet_group(), raft::ProposeOperation{ingest::kRaftColumnarAppendEntryType, command()}}});
  ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();
  ASSERT_TRUE(proposed->wait().has_value());
  auto replicated = initial->runtime()->try_submit(
      {{tablet_group(),
        raft::ReceiveOperation{
            2U, raft::AppendEntriesResponse{.term = 1U, .success = true, .match_index = 1U}}}});
  ASSERT_TRUE(replicated.has_value()) << replicated.error().to_string();
  ASSERT_TRUE(replicated->wait().has_value());
  auto before_joint = initial->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(before_joint.has_value()) << before_joint.error().to_string();
  EXPECT_EQ(before_joint->visible_row_count(), 2U);

  auto joint = initial->runtime()->try_submit(
      {{tablet_group(), raft::BeginMembershipChangeOperation{{1U}}}});
  ASSERT_TRUE(joint.has_value()) << joint.error().to_string();
  ASSERT_TRUE(joint->wait().has_value());
  replicated = initial->runtime()->try_submit(
      {{tablet_group(),
        raft::ReceiveOperation{
            2U, raft::AppendEntriesResponse{.term = 1U, .success = true, .match_index = 2U}}}});
  ASSERT_TRUE(replicated.has_value()) << replicated.error().to_string();
  ASSERT_TRUE(replicated->wait().has_value());
  ASSERT_TRUE(initial->shutdown().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());

  auto database = ReplicatedIngestDatabase::open_existing(
      {.bootstrap = bootstrap_config, .groups = joint_groups()});
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  auto recovered = database->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->visible_row_count(), 2U);
  EXPECT_EQ(recovered->retry_entry_count(), 1U);
  auto observed = database->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(), raft::ObserveGroupOperation{}}});
  ASSERT_TRUE(observed.has_value()) << observed.error().to_string();
  auto observation = observed->wait();
  ASSERT_TRUE(observation.has_value()) << observation.error().to_string();
  ASSERT_EQ(observation->size(), 1U);
  const auto& joint_observation = observation->front().observation;
  if (!joint_observation.has_value()) {
    ADD_FAILURE() << "expected recovered joint-group observation";
    return;
  }
  EXPECT_TRUE(joint_observation->joint_membership_active);
  EXPECT_TRUE(joint_observation->joint_membership_can_finalize);
  EXPECT_EQ(joint_observation->joint_old_voters, (std::vector<raft::NodeId>{1U, 2U}));
  EXPECT_EQ(joint_observation->joint_new_voters, (std::vector<raft::NodeId>{1U}));

  tablet_election = database->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(tablet_election.has_value()) << tablet_election.error().to_string();
  ASSERT_TRUE(tablet_election->wait().has_value());
  vote = database->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(), raft::ReceiveOperation{2U, raft::RequestVoteResponse{2U, true}}}});
  ASSERT_TRUE(vote.has_value()) << vote.error().to_string();
  ASSERT_TRUE(vote->wait().has_value());
  auto finalized = database->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(), raft::FinalizeMembershipChangeOperation{}}});
  ASSERT_TRUE(finalized.has_value()) << finalized.error().to_string();
  ASSERT_TRUE(finalized->wait().has_value());
  replicated = database->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(),
        raft::ReceiveOperation{
            2U, raft::AppendEntriesResponse{.term = 2U, .success = true, .match_index = 3U}}}});
  ASSERT_TRUE(replicated.has_value()) << replicated.error().to_string();
  ASSERT_TRUE(replicated->wait().has_value());

  metadata_election = database->ingest_runtime()->runtime()->try_submit(
      {{metadata_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(metadata_election.has_value()) << metadata_election.error().to_string();
  ASSERT_TRUE(metadata_election->wait().has_value());
  const raft::ProposeOperation final_placement{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TabletPlacementMetadata{
              columnar::test::batch_schema()->table_id(), tablet_id(), 2U, {1U}, 1U})
          .value()};
  metadata =
      database->ingest_runtime()->runtime()->try_submit({{metadata_group(), final_placement}});
  ASSERT_TRUE(metadata.has_value()) << metadata.error().to_string();
  ASSERT_TRUE(metadata->wait().has_value());
  ASSERT_TRUE(database->ingest_runtime()->coordinator()->admit(request()).is_ok());
  auto retry_response = await_response(*database->ingest_runtime());
  auto acknowledgement =
      network::decode_quorum_sync_ingest_acknowledgement(retry_response.frame.payload);
  ASSERT_TRUE(acknowledgement.has_value());
  EXPECT_EQ(acknowledgement->outcome, network::IngestOutcome::kMatchingRetry);
  EXPECT_EQ(acknowledgement->log_index, 4U);
  ASSERT_TRUE(database->shutdown().is_ok());

  auto repeated = ReplicatedIngestDatabase::open_existing(
      {.bootstrap = bootstrap_config, .groups = joint_groups()});
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  observed = repeated->ingest_runtime()->runtime()->try_submit(
      {{tablet_group(), raft::ObserveGroupOperation{}}});
  ASSERT_TRUE(observed.has_value()) << observed.error().to_string();
  observation = observed->wait();
  ASSERT_TRUE(observation.has_value()) << observation.error().to_string();
  ASSERT_EQ(observation->size(), 1U);
  const auto& stable_observation = observation->front().observation;
  if (!stable_observation.has_value()) {
    ADD_FAILURE() << "expected recovered stable-group observation";
    return;
  }
  EXPECT_FALSE(stable_observation->joint_membership_active);
  EXPECT_EQ(stable_observation->committed_voters, (std::vector<raft::NodeId>{1U}));
  recovered = repeated->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->visible_row_count(), 2U);
  EXPECT_EQ(recovered->retry_entry_count(), 1U);
  ASSERT_TRUE(repeated->shutdown().is_ok());
}

TEST(ReplicatedIngestDatabaseTest, PinsCommittedWholeTableQueryStateBeyondOwnerShutdown) {
  TemporaryDirectory directory;
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  auto initial = ReplicatedIngestRuntime::create_new(initial_runtime_config(*bootstrap));
  ASSERT_TRUE(initial.has_value()) << initial.error().to_string();
  elect_and_provision(*initial, false);
  ASSERT_TRUE(initial->coordinator()->admit(request()).is_ok());
  ASSERT_EQ(await_response(*initial).frame.header.message_type,
            network::MessageType::kQuorumSyncIngestAcknowledgement);
  ASSERT_TRUE(initial->shutdown().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());

  auto database =
      ReplicatedIngestDatabase::open_existing({.bootstrap = bootstrap_config, .groups = groups()});
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  for (const raft::GroupId group_id : {metadata_group(), tablet_group()}) {
    auto election = database->ingest_runtime()->runtime()->try_submit(
        {{group_id, raft::StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value());
    ASSERT_TRUE(election->wait().has_value());
  }
  auto read_barrier = ReplicatedReadBarrier::create_local(
      database->ingest_runtime()->runtime(),
      {database->query_barrier_groups().begin(), database->query_barrier_groups().end()});
  ASSERT_TRUE(read_barrier.has_value()) << read_barrier.error().to_string();
  NativeProtocolService native_queries{*database, *read_barrier};
  auto requests = network::SpscNetworkTaskQueue::create(4U).value();
  auto responses = network::SpscNetworkTaskQueue::create(1U).value();
  ASSERT_TRUE(responses.try_push(
      {.connection_id = 99U, .frame = {.header = {.message_type = network::MessageType::kPong}}}));
  auto native_service =
      ReplicatedIngestService::create({.coordinator = database->ingest_runtime()->coordinator(),
                                       .queries = &native_queries,
                                       .requests = &requests,
                                       .responses = &responses});
  ASSERT_TRUE(native_service.has_value()) << native_service.error().to_string();
  ASSERT_TRUE(requests.try_push(query_request("SELECT count(*) AS rows FROM events", true)));
  common::Result<ReplicatedIngestServicePoll> polled = ReplicatedIngestServicePoll{};
  for (std::size_t attempt = 0U; attempt < 10'000U && !native_service->metrics().response_retained;
       ++attempt) {
    polled = native_service->poll_once();
    ASSERT_TRUE(polled.has_value()) << polled.error().to_string();
    std::this_thread::yield();
  }
  ASSERT_TRUE(native_service->metrics().response_retained);
  ASSERT_TRUE(responses.try_pop().has_value());
  polled = native_service->poll_once();
  ASSERT_TRUE(polled.has_value());
  ASSERT_TRUE(polled->response_enqueued);
  auto native_result = responses.try_pop();
  if (!native_result.has_value()) {
    ADD_FAILURE() << "expected native query result batch";
    return;
  }
  const network::NetworkTask& native_batch = *native_result;
  ASSERT_EQ(native_batch.frame.header.message_type, network::MessageType::kQueryResult);
  auto batch = network::decode_query_result_batch(native_batch.frame.payload);
  ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
  common::ByteReader native_count{batch->cell(0U, 0U)->value};
  EXPECT_EQ(native_count.read_i64_le().value(), 2);
  polled = native_service->poll_once();
  ASSERT_TRUE(polled.has_value());
  ASSERT_TRUE(polled->response_enqueued);
  native_result = responses.try_pop();
  if (!native_result.has_value()) {
    ADD_FAILURE() << "expected native query end";
    return;
  }
  EXPECT_EQ(native_result->frame.header.message_type, network::MessageType::kQueryEnd);
  EXPECT_EQ(native_service->metrics().query_requests, 1U);
  EXPECT_EQ(native_service->metrics().response_backpressure, 1U);
  native_service->begin_shutdown();
  EXPECT_TRUE(native_service->drained());
  auto unsupported_ddl = native_queries.execute_query(query_request("CREATE TABLE denied"));
  ASSERT_TRUE(unsupported_ddl.has_value()) << unsupported_ddl.error().to_string();
  ASSERT_EQ(unsupported_ddl->responses.size(), 1U);
  ASSERT_EQ(unsupported_ddl->responses.front().frame.header.message_type,
            network::MessageType::kError);
  auto ddl_error = network::decode_error_message(unsupported_ddl->responses.front().frame.payload);
  ASSERT_TRUE(ddl_error.has_value());
  EXPECT_EQ(ddl_error->code, network::ProtocolErrorCode::kExecutionFailure);

  auto snapshot = database->acquire_query_snapshot();
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  ASSERT_EQ(snapshot->catalog()->tables().size(), 1U);
  const ReplicatedSingleGroupQueryRoute* const query_route =
      snapshot->single_group_route(columnar::test::batch_schema()->table_id());
  ASSERT_NE(query_route, nullptr);
  EXPECT_EQ(query_route->group_id, tablet_group());
  EXPECT_EQ(query_route->placement_epoch, 1U);
  EXPECT_EQ(query_route->replicas, std::vector<raft::NodeId>{1U});
  EXPECT_EQ(snapshot->single_group_route(schema::TableId::from_uuid(id(0xfeU)).value()), nullptr);
  auto local_leader = database->resolve_query_leader(*query_route);
  ASSERT_TRUE(local_leader.has_value()) << local_leader.error().to_string();
  EXPECT_FALSE(local_leader->has_value());
  const auto catalog_publication =
      database->ingest_runtime()->metadata_application()->catalog_snapshot();
  const auto tablet_publication =
      database->ingest_runtime()->tablet_application()->snapshot(tablet_group());
  ASSERT_TRUE(catalog_publication.has_value());
  ASSERT_TRUE(tablet_publication.has_value());
  const auto applied_position = tablet_publication->applied_position();
  if (!applied_position.has_value()) {
    ADD_FAILURE() << "expected a published tablet applied position";
    return;
  }
  std::vector<raft::GroupReadBarrier> barriers{
      {metadata_group(),
       {.term = 1U, .context = 1U, .read_index = (*catalog_publication)->applied_index}},
      {tablet_group(),
       {.term = 1U, .context = 2U, .read_index = applied_position->record_sequence}}};
  auto confirmed = database->acquire_query_snapshot(barriers);
  ASSERT_TRUE(confirmed.has_value()) << confirmed.error().to_string();
  ++barriers.back().barrier.read_index;
  auto trailing = database->acquire_query_snapshot(barriers);
  ASSERT_FALSE(trailing.has_value());
  EXPECT_EQ(trailing.error().code(), common::StatusCode::kUnavailable);
  barriers.pop_back();
  auto incomplete = database->acquire_query_snapshot(barriers);
  ASSERT_FALSE(incomplete.has_value());
  EXPECT_EQ(incomplete.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(database->query_barrier_groups().size(), 2U);
  auto parsed = query::parse_sql_v1_select("SELECT count(*) AS rows FROM events");
  ASSERT_TRUE(parsed.has_value()) << parsed.error().status().to_string();
  auto bound = query::bind_sql_v1_select(std::move(*parsed), snapshot->catalog());
  ASSERT_TRUE(bound.has_value()) << bound.error().status().to_string();
  auto lowered = query::lower_bound_sql_select(*bound);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  ASSERT_TRUE(database->shutdown().is_ok());

  query::QueryResourceContext resources =
      query::QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
  const auto schema = columnar::test::batch_schema();
  auto pipeline = snapshot->instantiate_table_pipeline(resources, schema->table_id(),
                                                       schema->schema_id(), *lowered);
  ASSERT_TRUE(pipeline.has_value()) << pipeline.error().to_string();
  auto step = (*pipeline)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  ASSERT_EQ(step->kind(), query::PhysicalOperatorStepKind::kChunk);
  const auto cell = step->chunk()->chunk().cell({.column_ordinal = 0U, .selected_row = 0U});
  ASSERT_TRUE(cell.has_value());
  common::ByteReader count{cell->bytes().value()};
  EXPECT_EQ(count.read_i64_le().value(), 2);
  step = (*pipeline)->next(resources);
  ASSERT_TRUE(step.has_value());
  EXPECT_EQ(step->kind(), query::PhysicalOperatorStepKind::kEnd);
  pipeline->reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(ReplicatedIngestDatabaseTest, CoordinatesNativeQueryAcrossSplitLocalAndRemoteLeaders) {
  TemporaryDirectory directory;
  auto reservation = network::TcpListener::bind({});
  if (!reservation.has_value())
    GTEST_SKIP() << "workspace does not permit loopback listener creation";
  const network::Ipv4Endpoint remote_query_endpoint = reservation->bound_endpoint();
  ASSERT_TRUE(reservation->close().is_ok());
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  auto config = initial_runtime_config(*bootstrap);
  config.groups = split_leader_groups();
  auto initial = ReplicatedIngestRuntime::create_new(std::move(config));
  ASSERT_TRUE(initial.has_value()) << initial.error().to_string();
  provision_two_node_query(*initial, remote_query_endpoint);
  ASSERT_TRUE(initial->shutdown().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());

  auto database = ReplicatedIngestDatabase::open_existing(
      {.bootstrap = bootstrap_config, .groups = split_leader_groups()});
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  ReplicatedIngestRuntime* const runtime = database->ingest_runtime();
  ASSERT_NE(runtime, nullptr);
  auto metadata_election =
      runtime->runtime()->try_submit({{metadata_group(), raft::StartElectionOperation{}},
                                      {metadata_group(), raft::CommitCurrentTermOperation{}}});
  ASSERT_TRUE(metadata_election.has_value()) << metadata_election.error().to_string();
  ASSERT_TRUE(metadata_election->wait().has_value());
  const raft::RaftGroupObservation before = observe(*runtime, tablet_group());
  ASSERT_NE(before.current_term, 0U);
  auto followed = runtime->runtime()->try_submit(
      {{tablet_group(), raft::ReceiveOperation{2U, raft::AppendEntriesRequest{
                                                       .term = before.current_term + 1U,
                                                       .leader_id = 2U,
                                                       .previous_log_index = before.last_log_index,
                                                       .previous_log_term = before.current_term,
                                                       .entries = {},
                                                       .leader_commit = before.commit_index}}}});
  ASSERT_TRUE(followed.has_value()) << followed.error().to_string();
  auto followed_result = followed->wait();
  ASSERT_TRUE(followed_result.has_value()) << followed_result.error().to_string();
  ASSERT_EQ(followed_result->size(), 1U);
  ASSERT_TRUE(followed_result->front().status.is_ok())
      << followed_result->front().status.to_string();

  auto snapshot = database->acquire_query_snapshot();
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  const ReplicatedSingleGroupQueryRoute* const query_route =
      snapshot->single_group_route(columnar::test::batch_schema()->table_id());
  ASSERT_NE(query_route, nullptr);
  auto resolved = database->resolve_query_leader(*query_route);
  ASSERT_FALSE(resolved.has_value());
  EXPECT_EQ(resolved.error().code(), common::StatusCode::kUnavailable);
  auto follower_observations = database->observe_query_groups();
  ASSERT_TRUE(follower_observations.has_value()) << follower_observations.error().to_string();
  ASSERT_EQ(follower_observations->size(), 2U);
  EXPECT_EQ((*follower_observations)[0].group_id, metadata_group());
  EXPECT_EQ((*follower_observations)[0].role, raft::Role::kLeader);
  EXPECT_EQ((*follower_observations)[0].leader_id, 1U);
  EXPECT_EQ((*follower_observations)[1].group_id, tablet_group());
  EXPECT_EQ((*follower_observations)[1].role, raft::Role::kFollower);
  EXPECT_EQ((*follower_observations)[1].leader_id, 2U);

  auto read_barrier = ReplicatedReadBarrier::create_local(
      runtime->runtime(),
      {database->query_barrier_groups().begin(), database->query_barrier_groups().end()});
  ASSERT_TRUE(read_barrier.has_value()) << read_barrier.error().to_string();
  NativeProtocolService service{*database, *read_barrier};
  auto response = service.execute_query(query_request("SELECT count(*) AS rows FROM events", true));
  ASSERT_TRUE(response.has_value()) << response.error().to_string();
  ASSERT_EQ(response->responses.size(), 1U);
  EXPECT_EQ(response->responses.front().frame.header.message_type, network::MessageType::kError);

  EmptyRowsMutableWorker remote_worker;
  UnusedGroupedMutableWorker remote_grouped_worker;
  ObservedLeaderAuthorityService remote_authority{*follower_observations};
  DistributedTestNodeAuthorizer distributed_authorizer;
  DistributedTestAuthenticator inbound_authenticator{92U};
  auto mutable_receiver = cluster::DistributedMutableVectorQueryReceiver::create(
      {.local_node_id = 2U,
       .authorizer = &distributed_authorizer,
       .worker = &remote_worker,
       .maximum_response_frames = 4U,
       .maximum_response_bytes = std::size_t{1024U} * 1024U});
  auto mutable_grouped_receiver =
      cluster::DistributedMutableVectorGroupedAggregateQueryReceiver::create(
          {.local_node_id = 2U,
           .authorizer = &distributed_authorizer,
           .worker = &remote_grouped_worker,
           .maximum_response_frames = 4U,
           .maximum_response_bytes = std::size_t{1024U} * 1024U});
  auto authority_receiver = cluster::RaftReadAuthorityReceiver::create(
      {.local_node_id = 2U, .authorizer = &distributed_authorizer, .service = &remote_authority});
  ASSERT_TRUE(mutable_receiver.has_value()) << mutable_receiver.error().to_string();
  ASSERT_TRUE(mutable_grouped_receiver.has_value()) << mutable_grouped_receiver.error().to_string();
  ASSERT_TRUE(authority_receiver.has_value()) << authority_receiver.error().to_string();
  auto remote_server = cluster::DistributedMutableQueryControlTcpServer::start(
      {.listener = {.bind_endpoint = remote_query_endpoint},
       .tls = distributed_server_tls(),
       .authenticator = &inbound_authenticator,
       .mutable_receiver = &*mutable_receiver,
       .mutable_grouped_receiver = &*mutable_grouped_receiver,
       .read_authority_receiver = &*authority_receiver,
       .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                          .exchange_timeout = std::chrono::milliseconds{1000},
                          .maximum_mutable_response_frames = 4U,
                          .maximum_mutable_response_bytes = std::size_t{1024U} * 1024U},
       .maximum_connections = 8U,
       .maximum_accepts_per_poll = 8U});
  ASSERT_TRUE(remote_server.has_value()) << remote_server.error().to_string();
  auto remote_tls = network::TlsClientContext::create(distributed_client_tls());
  ASSERT_TRUE(remote_tls.has_value()) << remote_tls.error().to_string();
  const std::array remote_tls_contexts{cluster::DistributedQueryNodeTlsContext{
      .node_id = 2U, .tls_context = std::addressof(*remote_tls)}};
  DistributedTestAuthenticator distributed_authenticator{93U};
  const NativeDistributedMutableVectorRowsQueryConfig distributed_config{
      .source_node_id = 1U,
      .authenticator = &distributed_authenticator,
      .node_authorizer = &distributed_authorizer,
      .tls_contexts = remote_tls_contexts,
      .execution = {.sender = {.retry = {.maximum_attempts = 1U},
                               .maximum_response_frames = 4U,
                               .maximum_response_bytes = std::size_t{1024U} * 1024U}},
      .carrier = {.handshake_timeout = std::chrono::milliseconds{1000},
                  .exchange_timeout = std::chrono::milliseconds{1000},
                  .maximum_response_frames = 4U,
                  .maximum_response_bytes = std::size_t{1024U} * 1024U},
      .authority_carrier = {.handshake_timeout = std::chrono::milliseconds{1000},
                            .exchange_timeout = std::chrono::milliseconds{1000}},
      .authority_retry = {.maximum_attempts = 1U},
      .connect_timeout = std::chrono::milliseconds{1000},
      .authority_connect_timeout = std::chrono::milliseconds{1000},
      .execution_timeout = std::chrono::milliseconds{5000},
      .maximum_poll_wait = std::chrono::milliseconds{1}};
  NativeProtocolService distributed_service{*database, *read_barrier, distributed_config};
  std::atomic<bool> stop_remote{};
  std::thread remote_thread{[&] {
    while (!stop_remote.load(std::memory_order_acquire))
      EXPECT_TRUE(remote_server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }};
  auto distributed_response =
      distributed_service.execute_query(query_request("SELECT count(*) AS rows FROM events", true));
  ASSERT_TRUE(distributed_response.has_value()) << distributed_response.error().to_string();
  if (distributed_response->responses.size() == 1U &&
      distributed_response->responses.front().frame.header.message_type ==
          network::MessageType::kError) {
    auto error =
        network::decode_error_message(distributed_response->responses.front().frame.payload);
    ASSERT_TRUE(error.has_value());
    std::string message;
    for (const std::byte byte : error->message)
      message.push_back(static_cast<char>(byte));
    ADD_FAILURE() << "distributed authority query returned error: " << message;
  }
  ASSERT_EQ(distributed_response->responses.size(), 2U);
  ASSERT_EQ(distributed_response->responses.front().frame.header.message_type,
            network::MessageType::kQueryResult);
  EXPECT_EQ(distributed_response->responses.back().frame.header.message_type,
            network::MessageType::kQueryEnd);
  auto distributed_batch =
      network::decode_query_result_batch(distributed_response->responses.front().frame.payload);
  ASSERT_TRUE(distributed_batch.has_value()) << distributed_batch.error().to_string();
  ASSERT_EQ(distributed_batch->row_count(), 1U);
  const network::QueryResultCell* const count = distributed_batch->cell(0U, 0U);
  ASSERT_NE(count, nullptr);
  common::ByteReader count_reader{count->value};
  EXPECT_EQ(count_reader.read_i64_le().value(), 0);
  EXPECT_EQ(remote_authority.calls, 1U);
  EXPECT_EQ(remote_authority.last_group, tablet_group());
  EXPECT_EQ(remote_worker.calls, 1U);
  EXPECT_EQ(remote_grouped_worker.calls, 0U);
  const auto remote_metrics = remote_server->metrics();
  EXPECT_EQ(remote_metrics.completed_read_authorities, 1U);
  EXPECT_EQ(remote_metrics.completed_mutable_queries, 1U);
  // Release/acquire publishes the stop request to the sole polling thread.
  stop_remote.store(true, std::memory_order_release);
  remote_thread.join();
  EXPECT_TRUE(remote_server->shutdown().is_ok());
  ASSERT_TRUE(read_barrier->shutdown().is_ok());
  ASSERT_TRUE(database->shutdown().is_ok());
}

TEST(ReplicatedIngestDatabaseTest, RejectsAQueryOverAPartiallyResidentTable) {
  TemporaryDirectory directory;
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value());
  auto initial = ReplicatedIngestRuntime::create_new(initial_runtime_config(*bootstrap));
  ASSERT_TRUE(initial.has_value());
  elect_and_provision(*initial);
  ASSERT_TRUE(initial->shutdown().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());

  auto database =
      ReplicatedIngestDatabase::open_existing({.bootstrap = bootstrap_config, .groups = groups()});
  ASSERT_TRUE(database.has_value()) << database.error().to_string();
  auto snapshot = database->acquire_query_snapshot();
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  EXPECT_EQ(snapshot->single_group_route(columnar::test::batch_schema()->table_id()), nullptr);
  auto parsed = query::parse_sql_v1_select("SELECT count(*) FROM events");
  ASSERT_TRUE(parsed.has_value());
  auto bound = query::bind_sql_v1_select(std::move(*parsed), snapshot->catalog());
  ASSERT_TRUE(bound.has_value());
  auto lowered = query::lower_bound_sql_select(*bound);
  ASSERT_TRUE(lowered.has_value());
  query::QueryResourceContext resources =
      query::QueryResourceContext::create(std::size_t{1024U} * 1024U).value();
  const auto schema = columnar::test::batch_schema();
  auto pipeline = snapshot->instantiate_table_pipeline(resources, schema->table_id(),
                                                       schema->schema_id(), *lowered);
  ASSERT_FALSE(pipeline.has_value());
  EXPECT_EQ(pipeline.error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  EXPECT_TRUE(database->shutdown().is_ok());
}

TEST(ReplicatedIngestDatabaseTest, RejectsAnOmittedLocallyPlacedTabletGroup) {
  TemporaryDirectory directory;
  runtime::DatabaseBootstrapConfig bootstrap_config{.database_root = directory.path().string(),
                                                    .new_database = descriptor()};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(bootstrap_config);
  ASSERT_TRUE(bootstrap.has_value());
  auto initial = ReplicatedIngestRuntime::create_new(initial_runtime_config(*bootstrap));
  ASSERT_TRUE(initial.has_value());
  elect_and_provision(*initial);
  ASSERT_TRUE(initial->shutdown().is_ok());
  ASSERT_TRUE(bootstrap->close().is_ok());
  auto database = ReplicatedIngestDatabase::open_existing(
      {.bootstrap = bootstrap_config, .groups = {{metadata_group(), {1U}}}});
  ASSERT_FALSE(database.has_value());
  EXPECT_EQ(database.error().code(), common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::service
