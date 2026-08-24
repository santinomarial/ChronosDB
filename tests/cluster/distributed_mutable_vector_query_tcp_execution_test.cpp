#include "chronos/cluster/distributed_mutable_vector_query_tcp_execution.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
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
  bytes.front() = std::byte{seed};
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

[[nodiscard]] query::DistributedMutableVectorFragment fragment(const std::uint8_t tablet_seed,
                                                               const raft::NodeId serving_node) {
  return {.query_id = uuid(1U),
          .database_id = id<manifest::DatabaseId>(2U),
          .table_id = id<schema::TableId>(3U),
          .tablet_id = id<schema::TabletId>(tablet_seed),
          .destination_schema_id = id<schema::SchemaId>(5U),
          .raft_group_id = uuid(static_cast<std::uint8_t>(tablet_seed + 20U)),
          .serving_node = serving_node,
          .applied_position = 8U,
          .observed_leader_commit_position = 8U,
          .placement_epoch = 9U,
          .read_policy = {.consistency = query::DistributedReadConsistency::kLocalEventual},
          .destination_column_ordinals = {0U},
          .plan = {.mode = query::DistributedVectorPlanMode::kRows, .row_output_indices = {0U}},
          .result_schema = result_schema()};
}

[[nodiscard]] std::vector<std::byte> zero_row_batch() {
  const auto schema_value = result_schema();
  const std::array columns{network::QueryResultColumn{schema_value.columns[0].name,
                                                      schema_value.columns[0].type,
                                                      schema_value.columns[0].nullable}};
  return network::encode_query_result_batch(0U, columns, {}).value();
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  explicit Authenticator(const std::uint64_t principal) : principal_(principal) {}

  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = principal_};
  }

private:
  std::uint64_t principal_{};
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return (principal == 91U && node == 1U) || (principal == 92U && (node == 7U || node == 8U));
  }
};

class Worker final : public DistributedMutableVectorQueryWorkerService {
public:
  common::Result<std::vector<DistributedVectorResultExchangeMessage>>
  execute(const query::DistributedMutableVectorFragment& value) override {
    ++calls;
    return std::vector<DistributedVectorResultExchangeMessage>{
        {.query_id = value.query_id,
         .tablet_id = value.tablet_id,
         .sequence = 1U,
         .terminal = true,
         .encoded_result_batch = zero_row_batch()}};
  }

  std::size_t calls{};
};

[[nodiscard]] DistributedMutableVectorQueryTlsLimits carrier_limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .maximum_response_frames = 4U,
          .maximum_response_bytes = std::size_t{1024U} * 1024U};
}

[[nodiscard]] common::Result<DistributedMutableVectorQueryTcpServer>
start_server(Authenticator& authenticator, DistributedMutableVectorQueryReceiver& receiver) {
  return DistributedMutableVectorQueryTcpServer::start({.listener = {},
                                                        .tls = server_tls(),
                                                        .authenticator = &authenticator,
                                                        .receiver = &receiver,
                                                        .carrier_limits = carrier_limits(),
                                                        .maximum_connections = 4U,
                                                        .maximum_accepts_per_poll = 4U});
}

[[nodiscard]] common::Result<DistributedMutableVectorQueryExecution> execution() {
  std::vector fragments{fragment(4U, 7U), fragment(6U, 8U)};
  return DistributedMutableVectorQueryExecution::create(
      1U, std::move(fragments),
      {.sender = {.retry = {.maximum_attempts = 2U,
                            .initial_backoff = std::chrono::milliseconds{1},
                            .maximum_backoff = std::chrono::milliseconds{1}},
                  .maximum_response_frames = 4U,
                  .maximum_response_bytes = std::size_t{1024U} * 1024U},
       .coordinator = {
           .messages = {.maximum_messages_per_fragment = 4U, .maximum_total_messages = 8U},
           .maximum_total_encoded_bytes = std::size_t{2U} * 1024U * 1024U}});
}

[[nodiscard]] common::Result<DistributedMutableVectorQueryExecution>
single_execution(query::DistributedMutableVectorFragment value) {
  return DistributedMutableVectorQueryExecution::create(
      1U, {std::move(value)},
      {.sender = {.retry = {.maximum_attempts = 1U,
                            .initial_backoff = std::chrono::milliseconds{1},
                            .maximum_backoff = std::chrono::milliseconds{1}},
                  .maximum_response_frames = 4U,
                  .maximum_response_bytes = std::size_t{1024U} * 1024U}});
}

TEST(DistributedMutableVectorQueryTcpExecutionTest,
     SchedulesSplitLeadersAndRotatesOnlyPrevalidatedAddresses) {
  auto refused_listener = network::TcpListener::bind();
  ASSERT_TRUE(refused_listener.has_value()) << refused_listener.error().to_string();
  const network::Ipv4Endpoint refused_endpoint = refused_listener->bound_endpoint();
  ASSERT_TRUE(refused_listener->close().is_ok());

  Authorizer authorizer;
  Worker first_worker;
  Worker second_worker;
  auto first_receiver = DistributedMutableVectorQueryReceiver::create(
      {.local_node_id = 7U, .authorizer = &authorizer, .worker = &first_worker});
  auto second_receiver = DistributedMutableVectorQueryReceiver::create(
      {.local_node_id = 8U, .authorizer = &authorizer, .worker = &second_worker});
  ASSERT_TRUE(first_receiver.has_value()) << first_receiver.error().to_string();
  ASSERT_TRUE(second_receiver.has_value()) << second_receiver.error().to_string();
  Authenticator client_authenticator{91U};
  auto first_server = start_server(client_authenticator, *first_receiver);
  auto second_server = start_server(client_authenticator, *second_receiver);
  ASSERT_TRUE(first_server.has_value()) << first_server.error().to_string();
  ASSERT_TRUE(second_server.has_value()) << second_server.error().to_string();
  auto context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  Authenticator server_authenticator{92U};
  auto portable = execution();
  ASSERT_TRUE(portable.has_value()) << portable.error().to_string();
  auto scheduled = DistributedMutableVectorQueryTcpExecution::create(
      std::move(*portable),
      {.authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .routes = {{.node_id = 7U,
                   .endpoints = {refused_endpoint, first_server->bound_endpoint()},
                   .tls_context = std::addressof(*context)},
                  {.node_id = 8U,
                   .endpoints = {second_server->bound_endpoint()},
                   .tls_context = std::addressof(*context)}},
       .carrier_limits = carrier_limits(),
       .connect_timeout = std::chrono::milliseconds{1000},
       .execution_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5}});
  ASSERT_TRUE(scheduled.has_value()) << scheduled.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 4096U &&
       scheduled->state() == DistributedMutableVectorQueryTcpExecutionState::kRunning;
       ++iteration) {
    ASSERT_TRUE(scheduled->poll_once(std::chrono::milliseconds{1}).is_ok())
        << scheduled->failure().to_string();
    ASSERT_TRUE(first_server->poll_once(std::chrono::milliseconds{0}).is_ok());
    ASSERT_TRUE(second_server->poll_once(std::chrono::milliseconds{0}).is_ok());
  }
  ASSERT_EQ(scheduled->state(), DistributedMutableVectorQueryTcpExecutionState::kComplete)
      << scheduled->failure().to_string();
  ASSERT_TRUE(scheduled->result().has_value());
  // Guarded by the completion state and result assertion above.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_EQ(scheduled->result()->result.messages.size(), 2U);
  EXPECT_EQ(first_worker.calls, 1U);
  EXPECT_EQ(second_worker.calls, 1U);
  const auto metrics = scheduled->metrics();
  EXPECT_EQ(metrics.attempts_started, 3U);
  EXPECT_EQ(metrics.retries_started, 1U);
  EXPECT_EQ(metrics.transport_completed_attempts, 2U);
  EXPECT_EQ(metrics.transport_failed_attempts, 1U);
  EXPECT_EQ(metrics.active_attempts, 0U);
  EXPECT_TRUE(first_server->shutdown().is_ok());
  EXPECT_TRUE(second_server->shutdown().is_ok());
}

TEST(DistributedMutableVectorQueryTcpExecutionTest,
     RejectsIncompleteRoutesAndOwnsDeadlineAndExplicitCancellation) {
  Authorizer authorizer;
  Authenticator authenticator{92U};
  auto context = network::TlsClientContext::create(client_tls());
  auto listener = network::TcpListener::bind();
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  const DistributedQueryNodeRoute first_route{.node_id = 7U,
                                              .endpoints = {listener->bound_endpoint()},
                                              .tls_context = std::addressof(*context)};
  auto missing = execution();
  ASSERT_TRUE(missing.has_value());
  EXPECT_EQ(DistributedMutableVectorQueryTcpExecution::create(std::move(*missing),
                                                              {.authenticator = &authenticator,
                                                               .node_authorizer = &authorizer,
                                                               .routes = {first_route}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto expired_execution = execution();
  ASSERT_TRUE(expired_execution.has_value());
  auto expired = DistributedMutableVectorQueryTcpExecution::create(
      std::move(*expired_execution),
      {.authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .routes = {first_route,
                  {.node_id = 8U,
                   .endpoints = {listener->bound_endpoint()},
                   .tls_context = std::addressof(*context)}},
       .execution_deadline = std::chrono::steady_clock::now() - std::chrono::milliseconds{1}});
  ASSERT_TRUE(expired.has_value()) << expired.error().to_string();
  EXPECT_EQ(expired->poll_once(std::chrono::milliseconds{0}).code(),
            common::StatusCode::kCancelled);
  EXPECT_EQ(expired->state(), DistributedMutableVectorQueryTcpExecutionState::kCancelled);
  EXPECT_EQ(expired->metrics().attempts_started, 0U);

  auto cancelled_execution = execution();
  ASSERT_TRUE(cancelled_execution.has_value());
  auto cancelled = DistributedMutableVectorQueryTcpExecution::create(
      std::move(*cancelled_execution), {.authenticator = &authenticator,
                                        .node_authorizer = &authorizer,
                                        .routes = {first_route,
                                                   {.node_id = 8U,
                                                    .endpoints = {listener->bound_endpoint()},
                                                    .tls_context = std::addressof(*context)}}});
  ASSERT_TRUE(cancelled.has_value()) << cancelled.error().to_string();
  ASSERT_TRUE(cancelled->poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_GT(cancelled->metrics().active_attempts, 0U);
  EXPECT_EQ(cancelled->cancel().code(), common::StatusCode::kCancelled);
  EXPECT_EQ(cancelled->state(), DistributedMutableVectorQueryTcpExecutionState::kCancelled);
  EXPECT_EQ(cancelled->metrics().active_attempts, 0U);
  EXPECT_TRUE(listener->close().is_ok());
}

TEST(DistributedMutableVectorQueryTcpExecutionTest,
     RebindsOnlyFreshCompatibleAuthorityAfterTerminalFailure) {
  auto refused_listener = network::TcpListener::bind();
  ASSERT_TRUE(refused_listener.has_value()) << refused_listener.error().to_string();
  const network::Ipv4Endpoint refused_endpoint = refused_listener->bound_endpoint();
  ASSERT_TRUE(refused_listener->close().is_ok());

  Authorizer authorizer;
  Authenticator server_authenticator{92U};
  auto context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(context.has_value()) << context.error().to_string();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
  auto stale_execution = single_execution(fragment(4U, 7U));
  ASSERT_TRUE(stale_execution.has_value()) << stale_execution.error().to_string();
  auto scheduled = DistributedMutableVectorQueryTcpExecution::create(
      std::move(*stale_execution), {.authenticator = &server_authenticator,
                                    .node_authorizer = &authorizer,
                                    .routes = {{.node_id = 7U,
                                                .endpoints = {refused_endpoint},
                                                .tls_context = std::addressof(*context)}},
                                    .carrier_limits = carrier_limits(),
                                    .connect_timeout = std::chrono::milliseconds{1000},
                                    .execution_deadline = deadline,
                                    .maximum_rebindings = 1U});
  ASSERT_TRUE(scheduled.has_value()) << scheduled.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 1024U &&
       scheduled->state() == DistributedMutableVectorQueryTcpExecutionState::kRunning;
       ++iteration) {
    static_cast<void>(scheduled->poll_once(std::chrono::milliseconds{1}));
  }
  ASSERT_EQ(scheduled->state(), DistributedMutableVectorQueryTcpExecutionState::kFailed);
  EXPECT_EQ(scheduled->failure().code(), common::StatusCode::kIoError);

  Worker worker;
  auto receiver = DistributedMutableVectorQueryReceiver::create(
      {.local_node_id = 8U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value()) << receiver.error().to_string();
  Authenticator client_authenticator{91U};
  auto server = start_server(client_authenticator, *receiver);
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  auto fresh_fragment = fragment(4U, 8U);
  fresh_fragment.applied_position = 9U;
  fresh_fragment.observed_leader_commit_position = 9U;
  fresh_fragment.placement_epoch = 10U;
  auto fresh_execution = single_execution(fresh_fragment);
  ASSERT_TRUE(fresh_execution.has_value()) << fresh_execution.error().to_string();
  auto incompatible_fragment = fresh_fragment;
  incompatible_fragment.query_id = uuid(99U);
  auto incompatible = single_execution(std::move(incompatible_fragment));
  ASSERT_TRUE(incompatible.has_value()) << incompatible.error().to_string();
  const DistributedMutableVectorQueryTcpExecutionConfig replacement_config{
      .authenticator = &server_authenticator,
      .node_authorizer = &authorizer,
      .routes = {{.node_id = 8U,
                  .endpoints = {server->bound_endpoint()},
                  .tls_context = std::addressof(*context)}},
      .carrier_limits = carrier_limits(),
      .connect_timeout = std::chrono::milliseconds{1000},
      .execution_deadline = deadline,
      .maximum_rebindings = 1U};
  EXPECT_EQ(scheduled->rebind(std::move(*incompatible), replacement_config).code(),
            common::StatusCode::kInvalidArgument);
  ASSERT_TRUE(scheduled->rebind(std::move(*fresh_execution), replacement_config).is_ok());
  EXPECT_EQ(scheduled->state(), DistributedMutableVectorQueryTcpExecutionState::kRunning);
  for (std::size_t iteration = 0U;
       iteration < 2048U &&
       scheduled->state() == DistributedMutableVectorQueryTcpExecutionState::kRunning;
       ++iteration) {
    ASSERT_TRUE(scheduled->poll_once(std::chrono::milliseconds{1}).is_ok())
        << scheduled->failure().to_string();
    ASSERT_TRUE(server->poll_once(std::chrono::milliseconds{0}).is_ok());
  }
  ASSERT_EQ(scheduled->state(), DistributedMutableVectorQueryTcpExecutionState::kComplete)
      << scheduled->failure().to_string();
  EXPECT_EQ(worker.calls, 1U);
  const auto metrics = scheduled->metrics();
  EXPECT_EQ(metrics.attempts_started, 2U);
  EXPECT_EQ(metrics.transport_failed_attempts, 1U);
  EXPECT_EQ(metrics.transport_completed_attempts, 1U);
  EXPECT_EQ(metrics.rebindings_started, 1U);
  EXPECT_EQ(metrics.active_attempts, 0U);
  auto late = single_execution(fresh_fragment);
  ASSERT_TRUE(late.has_value());
  EXPECT_EQ(scheduled->rebind(std::move(*late), replacement_config).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(server->shutdown().is_ok());
}

} // namespace
} // namespace chronos::cluster
