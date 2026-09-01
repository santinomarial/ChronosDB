#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tcp_client.hpp"
#include "chronos/service/replicated_distributed_mutable_vector_grouped_aggregate_query_tcp_server.hpp"
#include "chronos/service/replicated_distributed_mutable_vector_query_tcp_server.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <poll.h>
#include <string>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] network::TlsServerConfig server_tls() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
}

[[nodiscard]] network::TlsClientConfig client_tls() {
  return {.certificate_chain_file = fixture("client.pem").string(),
          .private_key_file = fixture("client-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

[[nodiscard]] common::Uuid uuid(const std::uint16_t value) {
  common::Uuid::Bytes bytes{};
  bytes[14] = static_cast<std::byte>((value >> 8U) & 0xffU);
  bytes[15] = static_cast<std::byte>(value & 0xffU);
  return common::Uuid{bytes};
}

[[nodiscard]] ingest::Sha256Digest digest(const std::uint8_t seed) {
  ingest::Sha256Digest::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return ingest::Sha256Digest{bytes};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  explicit Authenticator(const std::uint64_t principal) : principal_(principal) {}

  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest& request) override {
    saw_fingerprint = request.peer_certificate_sha256.has_value();
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = principal_};
  }

  bool saw_fingerprint{};

private:
  std::uint64_t principal_{};
};

class NodeAuthorizer final : public cluster::ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return (principal == 91U && node == 1U) || (principal == 92U && node == 11U);
  }
};

class ContextProvider final : public ReplicatedDistributedMutableVectorQueryWorkerContextProvider {
public:
  ContextProvider(ingest::TabletSnapshot snapshot,
                  std::shared_ptr<const schema::SchemaLineage> lineage,
                  raft::TabletPlacementMetadata placement, const raft::GroupId group_id,
                  const raft::ReadBarrier barrier)
      : snapshot_(std::move(snapshot)), lineage_(std::move(lineage)),
        placement_(std::move(placement)), group_id_(group_id), barrier_(barrier) {}

  common::Result<ReplicatedDistributedMutableVectorQueryWorkerContext>
  acquire(const query::DistributedMutableVectorFragment&) override {
    ++calls;
    return ReplicatedDistributedMutableVectorQueryWorkerContext{.snapshot = snapshot_,
                                                                .lineage = lineage_,
                                                                .placement = placement_,
                                                                .raft_group_id = group_id_,
                                                                .local_linearizable_barrier =
                                                                    barrier_};
  }

  std::size_t calls{};

private:
  ingest::TabletSnapshot snapshot_;
  std::shared_ptr<const schema::SchemaLineage> lineage_;
  raft::TabletPlacementMetadata placement_;
  raft::GroupId group_id_;
  raft::ReadBarrier barrier_;
};

struct MutableFixture {
  std::shared_ptr<const schema::TableSchema> schema_value{columnar::test::batch_schema()};
  schema::TabletId tablet_id{columnar::test::id<schema::TabletId>(52U)};
  raft::GroupId group_id{uuid(80U)};
  manifest::DatabaseId database_id{manifest::DatabaseId::from_uuid(uuid(81U)).value()};
  raft::ReadBarrier barrier{3U, 4U, 5U};
  raft::TabletPlacementMetadata placement{.table_id = schema_value->table_id(),
                                          .tablet_id = tablet_id,
                                          .placement_epoch = 7U,
                                          .replicas = {11U, 12U},
                                          .leader_hint = 11U};
  ingest::TabletState tablet{
      ingest::TabletState::create(
          schema_value, tablet_id,
          {.head_capacity = {.row_capacity = 4U, .variable_value_bytes = {0U, 2U, 0U}},
           .maximum_schema_versions = 1U,
           .maximum_sealed_generations = 1U,
           .maximum_retry_entries = 2U,
           .flush_queue = nullptr})
          .value()};
  std::shared_ptr<const schema::SchemaLineage> lineage{
      std::make_shared<const schema::SchemaLineage>(
          schema::SchemaLineage::create(*schema_value).value())};

  [[nodiscard]] ingest::TabletSnapshot append() {
    auto batch = std::make_shared<const columnar::OwnedColumnarBatch>(
        columnar::OwnedColumnarBatch::create(schema_value, columnar::test::batch_columns())
            .value());
    const ingest::RetryIdentity retry{.client_id = ingest::test::request_id<ingest::ClientId>(1U),
                                      .client_batch_id =
                                          ingest::test::request_id<ingest::ClientBatchId>(33U)};
    const ingest::ColumnarAppendMutationIdentity mutation{
        .table_id = schema_value->table_id(), .tablet_id = tablet_id, .request_digest = digest(1U)};
    auto prepared = tablet.prepare_append(retry, mutation, std::move(batch));
    EXPECT_TRUE(prepared.has_value()) << prepared.error().to_string();
    EXPECT_TRUE(prepared->mark_wal_started().is_ok());
    auto published = prepared->publish(head::HeadCommitPosition::raft(group_id, 5U));
    EXPECT_TRUE(published.has_value()) << published.error().to_string();
    return std::move(published->snapshot);
  }

  [[nodiscard]] query::DistributedMutableVectorFragment
  fragment(const ingest::TabletSnapshot& snapshot) const {
    const query::DistributedVectorQueryPlan plan{
        .query_id = uuid(82U),
        .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
        .fragments = {{.tablet_id = tablet_id,
                       .leader_node = 11U,
                       .local_applied_position = 5U,
                       .known_leader_commit_position = 5U}},
        .intent = {.mode = query::DistributedVectorPlanMode::kRows, .row_output_indices = {1U}}};
    const query::DistributedReadAdmission admission{.tablet_id = tablet_id,
                                                    .serving_node = 11U,
                                                    .applied_position = 5U,
                                                    .observed_leader_commit_position = 5U,
                                                    .linearizable_barrier = barrier};
    const std::array<std::uint32_t, 3U> projection{0U, 1U, 2U};
    const query::DistributedVectorResultSchema result_schema{
        .columns = {{"tag", schema_value->columns()[1].type(), true}}};
    auto bound = query::bind_distributed_mutable_vector_fragment(
        {.plan = std::cref(plan),
         .admission = std::cref(admission),
         .database_id = database_id,
         .snapshot = std::cref(snapshot),
         .lineage = std::cref(*lineage),
         .raft_group_id = group_id,
         .placement = std::cref(placement),
         .destination_column_ordinals = projection,
         .event_time_predicate = std::nullopt,
         .result_schema = std::cref(result_schema)});
    EXPECT_TRUE(bound.has_value()) << bound.error().to_string();
    return std::move(*bound);
  }

  [[nodiscard]] query::DistributedMutableVectorFragment
  grouped_fragment(const ingest::TabletSnapshot& snapshot) const {
    const query::DistributedVectorQueryPlan plan{
        .query_id = uuid(83U),
        .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
        .fragments = {{.tablet_id = tablet_id,
                       .leader_node = 11U,
                       .local_applied_position = 5U,
                       .known_leader_commit_position = 5U}},
        .intent = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
                   .group_key_input_indices = {0U},
                   .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}}}};
    const query::DistributedReadAdmission admission{.tablet_id = tablet_id,
                                                    .serving_node = 11U,
                                                    .applied_position = 5U,
                                                    .observed_leader_commit_position = 5U,
                                                    .linearizable_barrier = barrier};
    const std::array<std::uint32_t, 1U> projection{1U};
    const auto int64 = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
    const query::DistributedVectorResultSchema result_schema{
        .columns = {{"tag", schema_value->columns()[1].type(), true}, {"rows", int64, false}}};
    auto bound = query::bind_distributed_mutable_vector_fragment(
        {.plan = std::cref(plan),
         .admission = std::cref(admission),
         .database_id = database_id,
         .snapshot = std::cref(snapshot),
         .lineage = std::cref(*lineage),
         .raft_group_id = group_id,
         .placement = std::cref(placement),
         .destination_column_ordinals = projection,
         .event_time_predicate = std::nullopt,
         .result_schema = std::cref(result_schema)});
    EXPECT_TRUE(bound.has_value()) << bound.error().to_string();
    return std::move(*bound);
  }
};

[[nodiscard]] cluster::DistributedMutableVectorQueryTlsLimits carrier_limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .maximum_response_frames = 4U,
          .maximum_response_bytes = std::size_t{1024U} * 1024U};
}

[[nodiscard]] cluster::DistributedMutableVectorGroupedAggregateQueryTlsLimits
grouped_carrier_limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .maximum_response_frames = 4U,
          .maximum_response_bytes = std::size_t{1024U} * 1024U};
}

TEST(ReplicatedDistributedMutableVectorGroupedAggregateQueryWorkerTest,
     ReacquiresAndExecutesExactCurrentTabletPublication) {
  EXPECT_EQ(
      ReplicatedDistributedMutableVectorGroupedAggregateQueryWorker::create({}).error().code(),
      common::StatusCode::kInvalidArgument);
  MutableFixture data;
  const ingest::TabletSnapshot snapshot = data.append();
  ContextProvider provider{snapshot, data.lineage, data.placement, data.group_id, data.barrier};
  const auto fragment = data.grouped_fragment(snapshot);
  auto worker = ReplicatedDistributedMutableVectorGroupedAggregateQueryWorker::create(
      {.local_node_id = 11U, .context_provider = &provider});
  ASSERT_TRUE(worker.has_value()) << worker.error().to_string();

  auto authority = worker->bind_authority(fragment);
  ASSERT_TRUE(authority.has_value()) << authority.error().to_string();
  ASSERT_EQ(authority->keys.size(), 1U);
  ASSERT_EQ(authority->aggregates.size(), 1U);
  EXPECT_TRUE(authority->keys[0].nullable);
  EXPECT_EQ(authority->aggregates[0].operation, query::VectorAggregateOperation::kCountStar);

  auto result = worker->execute(fragment);
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->input_rows, 2U);
  EXPECT_EQ(result->group_count, 2U);
  ASSERT_EQ(result->messages.size(), 2U);
  auto resources = query::QueryResourceContext::create(1U << 20U);
  ASSERT_TRUE(resources.has_value()) << resources.error().to_string();
  for (std::size_t ordinal = 0U; ordinal < result->messages.size(); ++ordinal) {
    auto decoded = query::decode_distributed_vector_grouped_aggregate_exchange_message_exact(
        result->messages[ordinal].bytes(), result->authority.keys, result->authority.aggregates,
        *resources);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
    EXPECT_EQ(decoded->position().group_ordinal, ordinal);
    EXPECT_EQ(decoded->position().group_count, 2U);
    EXPECT_EQ(decoded->position().terminal, ordinal == 1U);
    ASSERT_EQ(decoded->keys().size(), 1U);
    if (ordinal == 0U) {
      EXPECT_FALSE(decoded->keys()[0].is_null());
      EXPECT_EQ(std::get<std::string>(decoded->keys()[0].storage()), "x");
    } else {
      EXPECT_TRUE(decoded->keys()[0].is_null());
    }
    auto states = std::move(*decoded).take_states();
    ASSERT_EQ(states.size(), 1U);
    auto count = std::move(states[0]).take_result();
    ASSERT_TRUE(count.has_value()) << count.error().to_string();
    EXPECT_EQ(std::get<std::int64_t>(count->storage()), 1);
  }
  EXPECT_EQ(provider.calls, 2U);
}

TEST(ReplicatedDistributedMutableVectorQueryTcpServerTest,
     ReacquiresAndServesExactCurrentTabletPublication) {
  EXPECT_EQ(ReplicatedDistributedMutableVectorQueryWorker::create({}).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(ReplicatedDistributedMutableVectorQueryTcpServer::start({}).error().code(),
            common::StatusCode::kInvalidArgument);

  MutableFixture data;
  const ingest::TabletSnapshot snapshot = data.append();
  ContextProvider provider{snapshot, data.lineage, data.placement, data.group_id, data.barrier};
  NodeAuthorizer authorizer;
  Authenticator client_authenticator{91U};
  auto server = ReplicatedDistributedMutableVectorQueryTcpServer::start(
      {.worker = {.local_node_id = 11U, .context_provider = &provider},
       .listener = {},
       .tls = server_tls(),
       .authenticator = &client_authenticator,
       .node_authorizer = &authorizer,
       .carrier_limits = carrier_limits(),
       .maximum_connections = 4U,
       .maximum_accepts_per_poll = 4U});
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  auto moved_server = std::move(*server);

  auto context = network::TlsClientContext::create(client_tls());
  auto sender = cluster::DistributedMutableVectorQuerySender::create(1U, data.fragment(snapshot));
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  ASSERT_TRUE(sender.has_value()) << sender.error().to_string();
  auto attempt = sender->begin_attempt({});
  ASSERT_TRUE(attempt.has_value()) << attempt.error().to_string();
  Authenticator server_authenticator{92U};
  auto client = cluster::DistributedMutableVectorQueryTcpClient::begin(
      std::move(*attempt),
      {.remote_endpoint = moved_server.bound_endpoint(),
       .tls_context = std::addressof(*context),
       .carrier = {.authenticator = &server_authenticator,
                   .node_authorizer = &authorizer,
                   .peer_ipv4_address = {127U, 0U, 0U, 1U},
                   .limits = carrier_limits()},
       .connect_timeout = std::chrono::milliseconds{1000}},
      cluster::DistributedMutableVectorQueryTcpClient::TimePoint::clock::now());
  ASSERT_TRUE(client.has_value()) << client.error().to_string();

  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       client->state() != cluster::DistributedMutableVectorQueryTcpClientState::kComplete;
       ++iteration) {
    const auto interest = client->interest();
    pollfd descriptor{.fd = client->descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    ASSERT_GE(::poll(&descriptor, 1U, 1), 0);
    ASSERT_TRUE(
        client
            ->on_ready((descriptor.revents & POLLIN) != 0, (descriptor.revents & POLLOUT) != 0,
                       cluster::DistributedMutableVectorQueryTcpClient::TimePoint::clock::now())
            .is_ok())
        << client->failure().to_string();
    ASSERT_TRUE(moved_server.poll_once(std::chrono::milliseconds{1}).is_ok());
  }

  ASSERT_EQ(client->state(), cluster::DistributedMutableVectorQueryTcpClientState::kComplete)
      << client->failure().to_string();
  const auto responses = client->responses();
  ASSERT_TRUE(responses.has_value()) << responses.error().to_string();
  ASSERT_EQ(responses->size(), 1U);
  const auto& payload = responses->front().payload;
  if (!payload.has_value()) {
    ADD_FAILURE() << "completed mutable vector response has no payload";
    return;
  }
  EXPECT_TRUE(payload->terminal);
  const auto batch = network::decode_query_result_batch(payload->encoded_result_batch);
  ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
  EXPECT_EQ(batch->row_count(), 2U);
  EXPECT_EQ(batch->columns().size(), 1U);
  EXPECT_EQ(provider.calls, 1U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  EXPECT_EQ(moved_server.metrics().completed_connections, 1U);
  EXPECT_TRUE(moved_server.shutdown().is_ok());
  EXPECT_TRUE(moved_server.shutdown().is_ok());
}

TEST(ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServerTest,
     OwnsWorkerReceiverAndServesExactCurrentTabletPublication) {
  EXPECT_EQ(
      ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer::start({}).error().code(),
      common::StatusCode::kInvalidArgument);

  MutableFixture data;
  const ingest::TabletSnapshot snapshot = data.append();
  ContextProvider provider{snapshot, data.lineage, data.placement, data.group_id, data.barrier};
  NodeAuthorizer authorizer;
  Authenticator client_authenticator{91U};
  auto server = ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer::start(
      {.worker = {.local_node_id = 11U, .context_provider = &provider},
       .listener = {},
       .tls = server_tls(),
       .authenticator = &client_authenticator,
       .node_authorizer = &authorizer,
       .carrier_limits = grouped_carrier_limits(),
       .maximum_connections = 4U,
       .maximum_accepts_per_poll = 4U});
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  auto moved_server = std::move(*server);
  EXPECT_FALSE(server->is_running());
  EXPECT_EQ(server->poll_once(std::chrono::milliseconds{0}).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(server->shutdown().code(), common::StatusCode::kInvalidArgument);

  auto context = network::TlsClientContext::create(client_tls());
  auto resources = query::QueryResourceContext::create(1U << 20U);
  const auto fragment = data.grouped_fragment(snapshot);
  std::vector<query::VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = data.schema_value->columns()[1].type(), .nullable = true}};
  std::vector<query::VectorAggregateDefinition> aggregates{
      {.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
  auto sender_keys = keys;
  auto sender_aggregates = aggregates;
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  ASSERT_TRUE(resources.has_value()) << resources.error().to_string();
  auto sender = cluster::DistributedMutableVectorGroupedAggregateQuerySender::create(
      1U, fragment, std::move(sender_keys), std::move(sender_aggregates), *resources);
  ASSERT_TRUE(sender.has_value()) << sender.error().to_string();
  auto attempt = sender->begin_attempt({});
  ASSERT_TRUE(attempt.has_value()) << attempt.error().to_string();
  Authenticator server_authenticator{92U};
  auto client = cluster::DistributedMutableVectorGroupedAggregateQueryTcpClient::begin(
      std::move(*attempt), std::move(keys), std::move(aggregates), std::move(*resources),
      {.remote_endpoint = moved_server.bound_endpoint(),
       .tls_context = std::addressof(*context),
       .carrier = {.authenticator = &server_authenticator,
                   .node_authorizer = &authorizer,
                   .peer_ipv4_address = {127U, 0U, 0U, 1U},
                   .limits = grouped_carrier_limits()},
       .connect_timeout = std::chrono::milliseconds{1000}},
      cluster::DistributedMutableVectorGroupedAggregateQueryTcpClient::TimePoint::clock::now());
  ASSERT_TRUE(client.has_value()) << client.error().to_string();

  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       client->state() !=
           cluster::DistributedMutableVectorGroupedAggregateQueryTcpClientState::kComplete;
       ++iteration) {
    const auto interest = client->interest();
    pollfd descriptor{.fd = client->descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    ASSERT_GE(::poll(&descriptor, 1U, 1), 0);
    ASSERT_TRUE(client
                    ->on_ready((descriptor.revents & POLLIN) != 0,
                               (descriptor.revents & POLLOUT) != 0,
                               cluster::DistributedMutableVectorGroupedAggregateQueryTcpClient::
                                   TimePoint::clock::now())
                    .is_ok())
        << client->failure().to_string();
    ASSERT_TRUE(moved_server.poll_once(std::chrono::milliseconds{1}).is_ok());
  }

  ASSERT_EQ(client->state(),
            cluster::DistributedMutableVectorGroupedAggregateQueryTcpClientState::kComplete)
      << client->failure().to_string();
  const auto responses = client->responses();
  ASSERT_TRUE(responses.has_value()) << responses.error().to_string();
  ASSERT_EQ(responses->size(), 2U);
  for (std::size_t ordinal = 0U; ordinal < responses->size(); ++ordinal) {
    const auto& response_payload = (*responses)[ordinal].payload;
    if (!response_payload.has_value()) {
      ADD_FAILURE() << "completed mutable grouped response has no payload";
      return;
    }
    const auto& payload = *response_payload;
    EXPECT_EQ(payload.position().group_ordinal, ordinal);
    EXPECT_EQ(payload.position().group_count, 2U);
    EXPECT_EQ(payload.position().terminal, ordinal == 1U);
    ASSERT_EQ(payload.keys().size(), 1U);
    ASSERT_EQ(payload.states().size(), 1U);
    EXPECT_EQ(payload.states().front().definition().operation,
              query::VectorAggregateOperation::kCountStar);
  }
  EXPECT_EQ(provider.calls, 2U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  EXPECT_EQ(moved_server.metrics().completed_connections, 1U);
  EXPECT_TRUE(moved_server.shutdown().is_ok());
  EXPECT_TRUE(moved_server.shutdown().is_ok());
}

} // namespace
} // namespace chronos::service
