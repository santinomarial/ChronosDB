#include "chronos/cluster/distributed_mutable_query_control_tcp.hpp"
#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tcp_client.hpp"
#include "chronos/cluster/distributed_mutable_vector_query_tcp.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_tcp_client.hpp"
#include "chronos/cluster/raft_read_authority_tcp_client.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <poll.h>
#include <utility>
#include <vector>

namespace chronos::cluster {
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

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {
      .columns = {
          {"value", schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(), false}}};
}

[[nodiscard]] query::DistributedMutableVectorFragment fragment() {
  return {.query_id = uuid(1U),
          .database_id = id<manifest::DatabaseId>(2U),
          .table_id = id<schema::TableId>(3U),
          .tablet_id = id<schema::TabletId>(4U),
          .destination_schema_id = id<schema::SchemaId>(5U),
          .raft_group_id = uuid(6U),
          .serving_node = 2U,
          .applied_position = 10U,
          .observed_leader_commit_position = 10U,
          .placement_epoch = 7U,
          .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
          .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
          .destination_column_ordinals = {0U},
          .plan = {.mode = query::DistributedVectorPlanMode::kRows, .row_output_indices = {0U}},
          .result_schema = result_schema()};
}

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] std::vector<query::VectorGroupKeyDefinition> grouped_keys() {
  return {{.column_ordinal = 0U, .type = string_type(), .nullable = false}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> grouped_aggregates() {
  return {{.operation = query::VectorAggregateOperation::kCountStar}};
}

[[nodiscard]] query::DistributedMutableVectorFragment grouped_fragment() {
  auto result = fragment();
  result.destination_column_ordinals = {1U};
  result.plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
                 .group_key_input_indices = {0U},
                 .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}}};
  result.result_schema = {
      .columns = {
          {"region", string_type(), false},
          {"count", schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(), false}}};
  return result;
}

[[nodiscard]] RaftReadAuthority authority() {
  return {
      .barrier = {.group_id = uuid(6U), .barrier = {.term = 2U, .context = 3U, .read_index = 10U}},
      .observation = {.group_id = uuid(6U),
                      .node_id = 2U,
                      .role = raft::Role::kLeader,
                      .current_term = 2U,
                      .leader_id = 2U,
                      .last_log_index = 11U,
                      .commit_index = 10U,
                      .applied_index = 10U,
                      .voters = {1U, 2U, 3U},
                      .committed_voters = {1U, 2U, 3U}}};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleJobPrepare job_prepare() {
  return {.coordinator_node_id = 1U,
          .target_node_id = 2U,
          .coordinator_result_endpoint = {{127U, 0U, 0U, 1U}, 8137U},
          .execution_timeout = std::chrono::milliseconds{30'000},
          .authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                           uuid(31U), {{id<schema::TabletId>(32U), 2U}}, {{0U, 2U}},
                           {{0U, string_type(), false}},
                           {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
                           .value(),
          .result_schema = {
              .columns = {{"region", string_type(), false},
                          {"count",
                           schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
                           false}}}};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  Authenticator(const std::uint64_t principal, const bool authorized = true)
      : principal_(principal), authorized_(authorized) {}

  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest& request) override {
    ++calls;
    saw_fingerprint = request.peer_certificate_sha256.has_value();
    return network::PeerAuthenticationResult{.authorized = authorized_,
                                             .principal_id = authorized_ ? principal_ : 0U};
  }

  std::size_t calls{};
  bool saw_fingerprint{};

private:
  std::uint64_t principal_{};
  bool authorized_{};
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return (principal == 91U && node == 1U) || (principal == 92U && node == 2U);
  }
};

class Worker final : public DistributedMutableVectorQueryWorkerService {
public:
  common::Result<std::vector<DistributedVectorResultExchangeMessage>>
  execute(const query::DistributedMutableVectorFragment& received) override {
    ++calls;
    return std::vector<DistributedVectorResultExchangeMessage>{{.query_id = received.query_id,
                                                                .tablet_id = received.tablet_id,
                                                                .sequence = 1U,
                                                                .terminal = true}};
  }
  std::size_t calls{};
};

class GroupedWorker final : public DistributedMutableVectorGroupedAggregateQueryWorkerService {
public:
  common::Result<query::DistributedVectorGroupedAggregateAuthority>
  bind_authority(const query::DistributedMutableVectorFragment&) override {
    ++bind_calls;
    return query::DistributedVectorGroupedAggregateAuthority{grouped_keys(), grouped_aggregates()};
  }

  common::Result<query::DistributedVectorGroupedAggregateWorkerResultV2>
  execute(const query::DistributedMutableVectorFragment& received) override {
    ++execute_calls;
    query::DistributedVectorGroupedAggregateWorkerResultV2 result{
        .authority = {.keys = grouped_keys(), .aggregates = grouped_aggregates()},
        .input_rows = 1U,
        .group_count = 1U};
    auto state =
        query::MergeableVectorAggregateState::create(result.authority.aggregates.front()).value();
    EXPECT_TRUE(state.accumulate_count_star().has_value());
    std::vector<query::ScalarValue> key_values;
    key_values.push_back(query::ScalarValue::text(string_type(), "east").value());
    std::vector<query::MergeableVectorAggregateState> states;
    states.push_back(std::move(state));
    query::DistributedVectorGroupedAggregateExchangeMessage message{
        {.query_id = received.query_id,
         .tablet_id = received.tablet_id,
         .sequence = 1U,
         .group_ordinal = 0U,
         .group_count = 1U,
         .terminal = true,
         .empty = false},
        std::move(key_values),
        std::move(states)};
    auto encoded = query::encode_distributed_vector_grouped_aggregate_exchange_message(
        message, result.authority.keys, result.authority.aggregates);
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    result.encoded_bytes = encoded->bytes().size();
    result.messages.push_back(std::move(*encoded));
    return result;
  }

  std::size_t bind_calls{};
  std::size_t execute_calls{};
};

class AuthorityService final : public RaftReadAuthorityService {
public:
  common::Result<RaftReadAuthority> acquire(const raft::GroupId& group_id) override {
    ++calls;
    EXPECT_EQ(group_id, uuid(6U));
    return authority();
  }
  std::size_t calls{};
};

[[nodiscard]] DistributedMutableQueryControlTlsServerLimits limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .maximum_mutable_response_frames = 2U,
          .maximum_mutable_response_bytes = 1024U,
          .maximum_mutable_grouped_response_frames = 2U,
          .maximum_mutable_grouped_response_bytes = 4096U,
          .maximum_mutable_grouped_decode_memory_bytes = 4096U};
}

// Receivers retain pointers to their worker/service, so the final owner constructs them only after
// its dependencies have stable addresses.
struct ReceiverOwner {
  explicit ReceiverOwner(Authorizer& authorizer)
      : mutable_receiver(DistributedMutableVectorQueryReceiver::create(
                             {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker})
                             .value()),
        mutable_grouped_receiver(DistributedMutableVectorGroupedAggregateQueryReceiver::create(
                                     {.local_node_id = 2U,
                                      .authorizer = &authorizer,
                                      .worker = &grouped_worker,
                                      .maximum_response_frames = 2U,
                                      .maximum_response_bytes = 4096U,
                                      .maximum_decode_memory_bytes = 4096U})
                                     .value()),
        authority_receiver(
            RaftReadAuthorityReceiver::create(
                {.local_node_id = 2U, .authorizer = &authorizer, .service = &authority_service})
                .value()) {}
  Worker worker;
  GroupedWorker grouped_worker;
  AuthorityService authority_service;
  DistributedMutableVectorQueryReceiver mutable_receiver;
  DistributedMutableVectorGroupedAggregateQueryReceiver mutable_grouped_receiver;
  RaftReadAuthorityReceiver authority_receiver;
};

[[nodiscard]] DistributedMutableVectorQueryTcpClient
mutable_client(const network::Ipv4Endpoint endpoint,
               const network::TlsClientContext& client_context, Authenticator& authenticator,
               Authorizer& authorizer) {
  auto sender = DistributedMutableVectorQuerySender::create(1U, fragment()).value();
  auto attempt = sender.begin_attempt({}).value();
  return DistributedMutableVectorQueryTcpClient::begin(
             std::move(attempt),
             {.remote_endpoint = endpoint,
              .tls_context = &client_context,
              .carrier = {.authenticator = &authenticator,
                          .node_authorizer = &authorizer,
                          .peer_ipv4_address = endpoint.address,
                          .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                                     .exchange_timeout = std::chrono::milliseconds{1000},
                                     .maximum_response_frames = 2U,
                                     .maximum_response_bytes = 1024U}},
              .connect_timeout = std::chrono::milliseconds{1000}},
             DistributedMutableVectorQueryTcpClient::TimePoint::clock::now())
      .value();
}

[[nodiscard]] DistributedMutableVectorGroupedAggregateQueryTcpClient
mutable_grouped_client(const network::Ipv4Endpoint endpoint,
                       const network::TlsClientContext& client_context,
                       Authenticator& authenticator, Authorizer& authorizer) {
  auto request = encode_distributed_mutable_vector_query_request(
                     {.source_node_id = 1U, .target_node_id = 2U, .fragment = grouped_fragment()})
                     .value();
  auto resources = query::QueryResourceContext::create(4096U).value();
  return DistributedMutableVectorGroupedAggregateQueryTcpClient::begin(
             {1U, 2U, std::move(request)}, grouped_keys(), grouped_aggregates(),
             std::move(resources),
             {.remote_endpoint = endpoint,
              .tls_context = &client_context,
              .carrier = {.authenticator = &authenticator,
                          .node_authorizer = &authorizer,
                          .peer_ipv4_address = endpoint.address,
                          .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                                     .exchange_timeout = std::chrono::milliseconds{1000},
                                     .maximum_response_frames = 2U,
                                     .maximum_response_bytes = 4096U}},
              .connect_timeout = std::chrono::milliseconds{1000}},
             std::chrono::steady_clock::now())
      .value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleJobControlTcpClient
job_control_client(const network::Ipv4Endpoint endpoint,
                   const network::TlsClientContext& client_context, Authenticator& authenticator,
                   Authorizer& authorizer) {
  return DistributedVectorGroupedAggregateShuffleJobControlTcpClient::begin(
             {.remote_endpoint = endpoint,
              .tls_context = &client_context,
              .carrier = {.authenticator = &authenticator,
                          .node_authorizer = &authorizer,
                          .peer_ipv4_address = endpoint.address,
                          .request =
                              DistributedVectorGroupedAggregateShuffleJobControlRequest{
                                  job_prepare()},
                          .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                                     .exchange_timeout = std::chrono::milliseconds{1000}}},
              .connect_timeout = std::chrono::milliseconds{1000}},
             std::chrono::steady_clock::now())
      .value();
}

TEST(DistributedMutableQueryControlTcpTest, RoutesAllProtocolsAfterOneAuthenticatedTlsBoundary) {
  Authorizer authorizer;
  ReceiverOwner receiver_owner{authorizer};
  Authenticator client_authenticator{91U};
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(client_context.has_value());
  const std::array result_contexts{DistributedQueryNodeTlsContext{1U, &*client_context}};
  Authenticator server_authenticator{92U};
  auto job_service = DistributedVectorGroupedAggregateShuffleJobService::create(
                         {.local_node_id = 2U,
                          .shuffle_tls = server_tls(),
                          .shuffle_authenticator = &client_authenticator,
                          .result_authenticator = &server_authenticator,
                          .node_authorizer = &authorizer,
                          .result_tls_contexts = result_contexts})
                         .value();
  auto server = DistributedMutableQueryControlTcpServer::start(
      {.listener = {},
       .tls = server_tls(),
       .authenticator = &client_authenticator,
       .mutable_receiver = &receiver_owner.mutable_receiver,
       .mutable_grouped_receiver = &receiver_owner.mutable_grouped_receiver,
       .read_authority_receiver = &receiver_owner.authority_receiver,
       .grouped_shuffle_job_service = &job_service,
       .carrier_limits = limits(),
       .maximum_connections = 8U,
       .maximum_accepts_per_poll = 8U});
  ASSERT_TRUE(server.has_value()) << server.error().to_string();

  auto mutable_query =
      mutable_client(server->bound_endpoint(), *client_context, server_authenticator, authorizer);
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       mutable_query.state() != DistributedMutableVectorQueryTcpClientState::kComplete;
       ++iteration) {
    const auto interest = mutable_query.interest();
    pollfd descriptor{.fd = mutable_query.descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    ASSERT_GE(::poll(&descriptor, 1U, 1), 0);
    const auto progress = mutable_query.on_ready((descriptor.revents & POLLIN) != 0,
                                                 (descriptor.revents & POLLOUT) != 0,
                                                 std::chrono::steady_clock::now());
    ASSERT_TRUE(progress.is_ok()) << progress.to_string();
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(mutable_query.state(), DistributedMutableVectorQueryTcpClientState::kComplete);
  ASSERT_TRUE(mutable_query.responses().has_value());
  EXPECT_EQ(receiver_owner.worker.calls, 1U);

  auto mutable_grouped_query = mutable_grouped_client(server->bound_endpoint(), *client_context,
                                                      server_authenticator, authorizer);
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       mutable_grouped_query.state() !=
           DistributedMutableVectorGroupedAggregateQueryTcpClientState::kComplete;
       ++iteration) {
    const auto interest = mutable_grouped_query.interest();
    pollfd descriptor{.fd = mutable_grouped_query.descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    ASSERT_GE(::poll(&descriptor, 1U, 1), 0);
    const auto progress = mutable_grouped_query.on_ready((descriptor.revents & POLLIN) != 0,
                                                         (descriptor.revents & POLLOUT) != 0,
                                                         std::chrono::steady_clock::now());
    ASSERT_TRUE(progress.is_ok()) << progress.to_string();
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(mutable_grouped_query.state(),
            DistributedMutableVectorGroupedAggregateQueryTcpClientState::kComplete)
      << mutable_grouped_query.failure().to_string();
  ASSERT_TRUE(mutable_grouped_query.responses().has_value());
  EXPECT_EQ(receiver_owner.grouped_worker.bind_calls, 1U);
  EXPECT_EQ(receiver_owner.grouped_worker.execute_calls, 1U);

  auto read_authority = RaftReadAuthorityTcpClient::begin(
      {.remote_endpoint = server->bound_endpoint(),
       .tls_context = &*client_context,
       .carrier = {.authenticator = &server_authenticator,
                   .node_authorizer = &authorizer,
                   .peer_ipv4_address = server->bound_endpoint().address,
                   .request = {1U, 2U, uuid(6U), 20U},
                   .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                              .exchange_timeout = std::chrono::milliseconds{1000}}},
       .connect_timeout = std::chrono::milliseconds{1000}},
      std::chrono::steady_clock::now());
  ASSERT_TRUE(read_authority.has_value()) << read_authority.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 4096U && read_authority->state() != RaftReadAuthorityTcpClientState::kComplete;
       ++iteration) {
    const auto interest = read_authority->interest();
    pollfd descriptor{.fd = read_authority->descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    ASSERT_GE(::poll(&descriptor, 1U, 1), 0);
    const auto progress = read_authority->on_ready((descriptor.revents & POLLIN) != 0,
                                                   (descriptor.revents & POLLOUT) != 0,
                                                   std::chrono::steady_clock::now());
    ASSERT_TRUE(progress.is_ok()) << progress.to_string();
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(read_authority->state(), RaftReadAuthorityTcpClientState::kComplete)
      << read_authority->failure().to_string();
  ASSERT_TRUE(read_authority->result().has_value());
  EXPECT_EQ(*read_authority->result(), authority());
  EXPECT_EQ(receiver_owner.authority_service.calls, 1U);

  auto job_control = job_control_client(server->bound_endpoint(), *client_context,
                                        server_authenticator, authorizer);
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       job_control.state() !=
           DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kComplete;
       ++iteration) {
    const auto interest = job_control.interest();
    pollfd descriptor{.fd = job_control.descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    ASSERT_GE(::poll(&descriptor, 1U, 1), 0);
    const auto progress =
        job_control.on_ready((descriptor.revents & POLLIN) != 0,
                             (descriptor.revents & POLLOUT) != 0, std::chrono::steady_clock::now());
    ASSERT_TRUE(progress.is_ok()) << progress.to_string();
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(job_control.state(),
            DistributedVectorGroupedAggregateShuffleJobControlTcpClientState::kComplete)
      << job_control.failure().to_string();
  auto prepared = job_control.result();
  ASSERT_TRUE(prepared.has_value()) << prepared.error().to_string();
  EXPECT_EQ(prepared->status_code, common::StatusCode::kOk);
  EXPECT_EQ(prepared->query_id, uuid(31U));
  EXPECT_EQ(job_service.metrics().active_jobs, 1U);

  EXPECT_EQ(client_authenticator.calls, 4U);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  const auto metrics = server->metrics();
  EXPECT_EQ(metrics.accepted_connections, 4U);
  EXPECT_EQ(metrics.completed_mutable_queries, 1U);
  EXPECT_EQ(metrics.completed_mutable_grouped_queries, 1U);
  EXPECT_EQ(metrics.completed_read_authorities, 1U);
  EXPECT_EQ(metrics.completed_grouped_shuffle_job_controls, 1U);
  EXPECT_EQ(metrics.failed_connections, 0U);
  EXPECT_EQ(metrics.active_connections, 0U);
}

TEST(DistributedMutableQueryControlTcpTest, RejectsPrincipalBeforeProtocolOrReceiverDispatch) {
  Authorizer authorizer;
  ReceiverOwner receiver_owner{authorizer};
  Authenticator denied_client{91U, false};
  auto server = DistributedMutableQueryControlTcpServer::start(
      {.listener = {},
       .tls = server_tls(),
       .authenticator = &denied_client,
       .mutable_receiver = &receiver_owner.mutable_receiver,
       .mutable_grouped_receiver = &receiver_owner.mutable_grouped_receiver,
       .read_authority_receiver = &receiver_owner.authority_receiver,
       .carrier_limits = limits(),
       .maximum_connections = 2U,
       .maximum_accepts_per_poll = 2U});
  ASSERT_TRUE(server.has_value());
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(client_context.has_value());
  Authenticator server_authenticator{92U};
  auto client =
      mutable_client(server->bound_endpoint(), *client_context, server_authenticator, authorizer);
  for (std::size_t iteration = 0U;
       iteration < 4096U && client.state() != DistributedMutableVectorQueryTcpClientState::kFailed;
       ++iteration) {
    const auto interest = client.interest();
    pollfd descriptor{.fd = client.descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    static_cast<void>(::poll(&descriptor, 1U, 1));
    static_cast<void>(client.on_ready((descriptor.revents & POLLIN) != 0,
                                      (descriptor.revents & POLLOUT) != 0,
                                      std::chrono::steady_clock::now()));
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  EXPECT_EQ(client.state(), DistributedMutableVectorQueryTcpClientState::kFailed);
  EXPECT_EQ(denied_client.calls, 1U);
  EXPECT_TRUE(denied_client.saw_fingerprint);
  EXPECT_EQ(receiver_owner.worker.calls, 0U);
  EXPECT_EQ(receiver_owner.authority_service.calls, 0U);
  EXPECT_EQ(server->metrics().failed_connections, 1U);
}

} // namespace
} // namespace chronos::cluster
