#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_service.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tcp_server.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_source_plan.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tcp_execution.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <thread>
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
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet(const std::uint8_t seed) {
  return schema::TabletId::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] schema::LogicalType int64_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
}

[[nodiscard]] std::vector<query::VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U, .type = string_type(), .nullable = false}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> aggregates() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority authority() {
  return DistributedVectorGroupedAggregateShuffleAuthority::create(
             uuid(1U),
             {{.tablet_id = tablet(2U), .node_id = 2U}, {.tablet_id = tablet(3U), .node_id = 3U}},
             {{.partition_id = 0U, .node_id = 3U}}, keys(), aggregates())
      .value();
}

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {.columns = {{"region", string_type(), false}, {"count", int64_type(), false}}};
}

[[nodiscard]] std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage>
input(const schema::TabletId& tablet_id) {
  auto state = query::MergeableVectorAggregateState::create(aggregates().front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(string_type(), "shared-key").value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> encoded;
  encoded.push_back(query::encode_distributed_vector_grouped_aggregate_exchange_message(
                        {.query_id = uuid(1U),
                         .tablet_id = tablet_id,
                         .sequence = 1U,
                         .group_ordinal = 0U,
                         .group_count = 1U,
                         .terminal = true,
                         .empty = false},
                        values, states, keys(), aggregates())
                        .value());
  return encoded;
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleTlsLimits shuffle_limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .stream = {.maximum_frames = 1U, .maximum_encoded_bytes = 1U << 20U}};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleResultTlsLimits result_limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .stream = {.maximum_frames = 1U, .maximum_encoded_bytes = 1U << 20U}};
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
    return common::Result<bool>{
        (principal == 91U && node == 2U) || (principal == 92U && node == 9U) ||
        (principal == 93U && node == 9U) || (principal == 94U && node == 3U) ||
        (principal == 95U && node == 3U)};
  }
};

[[nodiscard]] DistributedVectorGroupedAggregateShuffleJobPrepare
prepare(const network::Ipv4Endpoint result_endpoint,
        const std::chrono::milliseconds timeout = std::chrono::seconds{5}) {
  return {.coordinator_node_id = 9U,
          .target_node_id = 3U,
          .coordinator_result_endpoint = result_endpoint,
          .execution_timeout = timeout,
          .authority = authority(),
          .result_schema = result_schema()};
}

TEST(DistributedVectorGroupedAggregateShuffleJobServiceTest,
     IdempotentlyPreparesReceivesSealsAndReturnsOneReceiptProvenPartition) {
  auto expected = authority();
  auto schema = result_schema();
  Authenticator shuffle_server_authenticator{91U};
  Authenticator shuffle_client_authenticator{95U};
  Authenticator result_client_authenticator{92U};
  Authenticator result_server_authenticator{94U};
  Authorizer authorizer;
  auto shuffle_client_context = network::TlsClientContext::create(client_tls()).value();
  auto result_client_context = network::TlsClientContext::create(client_tls()).value();
  const std::array result_contexts{DistributedQueryNodeTlsContext{3U, &shuffle_client_context},
                                   DistributedQueryNodeTlsContext{9U, &result_client_context}};
  auto result_server = DistributedVectorGroupedAggregateShuffleResultTcpServer::start(
                           {.listener = {},
                            .tls = server_tls(),
                            .authenticator = &result_server_authenticator,
                            .node_authorizer = &authorizer,
                            .authority = &expected,
                            .result_schema = &schema,
                            .coordinator_node_id = 9U,
                            .carrier_limits = result_limits(),
                            .maximum_retained_streams = 1U,
                            .maximum_accepts_per_poll = 1U})
                           .value();
  auto service =
      DistributedVectorGroupedAggregateShuffleJobService::create(
          {.local_node_id = 3U,
           .shuffle_listener = {},
           .shuffle_tls = server_tls(),
           .shuffle_authenticator = &shuffle_server_authenticator,
           .result_authenticator = &result_client_authenticator,
           .node_authorizer = &authorizer,
           .result_tls_contexts = result_contexts,
           .shuffle_carrier_limits = shuffle_limits(),
           .result_retry_limits = {.retry = {.maximum_attempts = 4U,
                                             .initial_backoff = std::chrono::milliseconds{1},
                                             .maximum_backoff = std::chrono::milliseconds{4}},
                                   .stream = result_limits().stream},
           .result_carrier_limits = result_limits(),
           .maximum_jobs = 2U,
           .maximum_job_query_memory_bytes = 16U << 20U,
           .maximum_retained_streams_per_job = 2U,
           .maximum_accepts_per_job_poll = 2U,
           .maximum_reducer_admissions_per_job_poll = 2U})
          .value();
  const network::PeerAuthenticationResult control_peer{.authorized = true, .principal_id = 93U};
  auto admitted = service.receive(DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare(
                                      result_server.bound_endpoint())},
                                  control_peer, std::chrono::steady_clock::now());
  ASSERT_TRUE(admitted.has_value()) << admitted.error().to_string();
  ASSERT_EQ(admitted->status_code, common::StatusCode::kOk);
  ASSERT_TRUE(admitted->reducer_shuffle_endpoint.has_value());

  const DistributedVectorGroupedAggregateShuffleJobInstallRoutes routes{
      .query_id = uuid(1U),
      .coordinator_node_id = 9U,
      .target_node_id = 3U,
      .routes = {{.node_id = 3U, .endpoint = *admitted->reducer_shuffle_endpoint}}};
  auto installed =
      service.receive(DistributedVectorGroupedAggregateShuffleJobControlRequest{routes},
                      control_peer, std::chrono::steady_clock::now());
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  EXPECT_EQ(installed->status_code, common::StatusCode::kOk);
  auto duplicate_routes =
      service.receive(DistributedVectorGroupedAggregateShuffleJobControlRequest{routes},
                      control_peer, std::chrono::steady_clock::now());
  ASSERT_TRUE(duplicate_routes.has_value());
  EXPECT_EQ(duplicate_routes->status_code, common::StatusCode::kOk);

  auto duplicate =
      service.receive(DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare(
                          result_server.bound_endpoint())},
                      control_peer, std::chrono::steady_clock::now());
  ASSERT_TRUE(duplicate.has_value());
  EXPECT_EQ(duplicate->status_code, common::StatusCode::kOk);
  EXPECT_EQ(duplicate->reducer_shuffle_endpoint, admitted->reducer_shuffle_endpoint);

  auto conflicting =
      service.receive(DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare(
                          result_server.bound_endpoint(), std::chrono::seconds{4})},
                      control_peer, std::chrono::steady_clock::now());
  ASSERT_TRUE(conflicting.has_value());
  EXPECT_EQ(conflicting->status_code, common::StatusCode::kAlreadyExists);

  auto local_input = input(tablet(3U));
  auto published = service.publish_local_source(uuid(1U), tablet(3U), local_input);
  ASSERT_TRUE(published.has_value()) << published.error().to_string();
  EXPECT_TRUE(*published);
  auto duplicate_source = service.publish_local_source(uuid(1U), tablet(3U), local_input);
  ASSERT_TRUE(duplicate_source.has_value()) << duplicate_source.error().to_string();
  EXPECT_TRUE(*duplicate_source);

  auto resources = query::QueryResourceContext::create(16U << 20U).value();
  auto remote_plan = DistributedVectorGroupedAggregateShuffleSourcePlan::create(
                         expected, tablet(2U), input(tablet(2U)), resources)
                         .value();
  auto remote =
      DistributedVectorGroupedAggregateShuffleTcpExecution::create(
          expected, remote_plan.take_remote_retries(),
          {.authenticator = &shuffle_client_authenticator,
           .node_authorizer = &authorizer,
           .routes = {{.node_id = 3U,
                       .endpoints = {*admitted->reducer_shuffle_endpoint},
                       .tls_context = &shuffle_client_context}},
           .carrier_limits = shuffle_limits(),
           .connect_timeout = std::chrono::milliseconds{1000},
           .execution_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5}})
          .value();
  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    ASSERT_TRUE(remote.poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_TRUE(
        service.poll_once(std::chrono::milliseconds{1}, std::chrono::steady_clock::now()).is_ok());
    if (remote.state() == DistributedVectorGroupedAggregateShuffleTcpExecutionState::kComplete)
      break;
  }
  ASSERT_EQ(remote.state(), DistributedVectorGroupedAggregateShuffleTcpExecutionState::kComplete);

  auto sealed = service.receive(
      DistributedVectorGroupedAggregateShuffleJobControlRequest{
          DistributedVectorGroupedAggregateShuffleJobSeal{uuid(1U), 9U, 3U}},
      control_peer, std::chrono::steady_clock::now());
  ASSERT_TRUE(sealed.has_value()) << sealed.error().to_string();
  ASSERT_EQ(sealed->status_code, common::StatusCode::kOk);
  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    ASSERT_TRUE(
        service.poll_once(std::chrono::milliseconds{1}, std::chrono::steady_clock::now()).is_ok());
    ASSERT_TRUE(result_server.poll_once(std::chrono::milliseconds{1}).is_ok());
    if (result_server.metrics().retained_streams == 1U)
      break;
  }
  ASSERT_EQ(result_server.metrics().retained_streams, 1U);
  auto result = result_server.take_next_complete_stream();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->encoded_result_batches.size(), 1U);
  auto decoded = network::decode_query_result_batch(result->encoded_result_batches.front());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_EQ(decoded->row_count(), 1U);
  const auto* count = decoded->cell(0U, 1U);
  ASSERT_NE(count, nullptr);
  ASSERT_EQ(count->value.size(), sizeof(std::int64_t));
  EXPECT_EQ(count->value.front(), std::byte{2U});
  for (std::size_t index = 1U; index < count->value.size(); ++index)
    EXPECT_EQ(count->value[index], std::byte{});
  for (std::size_t iteration = 0U; iteration < 4096U && service.metrics().completed_jobs == 0U;
       ++iteration) {
    ASSERT_TRUE(
        service.poll_once(std::chrono::milliseconds{1}, std::chrono::steady_clock::now()).is_ok());
    ASSERT_TRUE(result_server.poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  EXPECT_EQ(service.metrics().completed_jobs, 1U);
  auto duplicate_seal = service.receive(
      DistributedVectorGroupedAggregateShuffleJobControlRequest{
          DistributedVectorGroupedAggregateShuffleJobSeal{uuid(1U), 9U, 3U}},
      control_peer, std::chrono::steady_clock::now());
  ASSERT_TRUE(duplicate_seal.has_value());
  EXPECT_EQ(duplicate_seal->status_code, common::StatusCode::kOk);
  EXPECT_EQ(service.metrics().duplicate_prepares, 1U);
  EXPECT_EQ(service.metrics().conflicting_prepares, 1U);
  EXPECT_EQ(service.metrics().route_install_requests, 2U);
  EXPECT_EQ(service.metrics().duplicate_route_installs, 1U);
  EXPECT_EQ(service.metrics().submitted_source_tablets, 1U);
  EXPECT_EQ(service.metrics().duplicate_source_submissions, 1U);
  EXPECT_EQ(service.metrics().completed_source_transports, 1U);
}

TEST(DistributedVectorGroupedAggregateShuffleJobServiceTest,
     RejectsPrepareWhenCoordinatorHasNoResultTlsRoute) {
  Authenticator authenticator{93U};
  Authorizer authorizer;
  auto result_context = network::TlsClientContext::create(client_tls()).value();
  const std::array result_contexts{DistributedQueryNodeTlsContext{8U, &result_context}};
  auto service = DistributedVectorGroupedAggregateShuffleJobService::create(
                     {.local_node_id = 3U,
                      .shuffle_authenticator = &authenticator,
                      .result_authenticator = &authenticator,
                      .node_authorizer = &authorizer,
                      .result_tls_contexts = result_contexts})
                     .value();
  const network::PeerAuthenticationResult peer{.authorized = true, .principal_id = 93U};
  auto response =
      service.receive(DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare({})}, peer,
                      std::chrono::steady_clock::now());
  ASSERT_TRUE(response.has_value()) << response.error().to_string();
  EXPECT_EQ(response->status_code, common::StatusCode::kNotFound);
  EXPECT_EQ(service.metrics().active_jobs, 0U);
}

TEST(DistributedVectorGroupedAggregateShuffleJobServiceTest,
     RetainsIdempotentCancellationTombstoneAndCancelsAnInstalledJob) {
  Authenticator authenticator{93U};
  Authorizer authorizer;
  auto result_context = network::TlsClientContext::create(client_tls()).value();
  const std::array result_contexts{DistributedQueryNodeTlsContext{9U, &result_context}};
  auto service = DistributedVectorGroupedAggregateShuffleJobService::create(
                     {.local_node_id = 3U,
                      .shuffle_tls = server_tls(),
                      .shuffle_authenticator = &authenticator,
                      .result_authenticator = &authenticator,
                      .node_authorizer = &authorizer,
                      .result_tls_contexts = result_contexts,
                      .maximum_jobs = 1U,
                      .maximum_job_query_memory_bytes = 16U << 20U,
                      .maximum_cancel_tombstones = 1U,
                      .cancel_tombstone_retention = std::chrono::milliseconds{5}})
                     .value();
  const network::PeerAuthenticationResult peer{.authorized = true, .principal_id = 93U};
  const DistributedVectorGroupedAggregateShuffleJobCancel cancel{
      .query_id = uuid(1U), .coordinator_node_id = 9U, .target_node_id = 3U};
  const auto now = std::chrono::steady_clock::now();
  auto cancelled =
      service.receive(DistributedVectorGroupedAggregateShuffleJobControlRequest{cancel}, peer, now);
  ASSERT_TRUE(cancelled.has_value()) << cancelled.error().to_string();
  EXPECT_EQ(cancelled->status_code, common::StatusCode::kOk);
  auto duplicate =
      service.receive(DistributedVectorGroupedAggregateShuffleJobControlRequest{cancel}, peer, now);
  ASSERT_TRUE(duplicate.has_value());
  EXPECT_EQ(duplicate->status_code, common::StatusCode::kOk);
  auto other_cancel = cancel;
  other_cancel.query_id = uuid(4U);
  auto exhausted = service.receive(
      DistributedVectorGroupedAggregateShuffleJobControlRequest{other_cancel}, peer, now);
  ASSERT_TRUE(exhausted.has_value());
  EXPECT_EQ(exhausted->status_code, common::StatusCode::kResourceExhausted);
  auto blocked = service.receive(DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare(
                                     {{127U, 0U, 0U, 1U}, 9123U})},
                                 peer, now);
  ASSERT_TRUE(blocked.has_value());
  EXPECT_EQ(blocked->status_code, common::StatusCode::kCancelled);
  EXPECT_EQ(service.metrics().cancel_tombstones, 1U);
  EXPECT_EQ(service.metrics().active_jobs, 0U);

  const auto after_retention = now + std::chrono::milliseconds{6};
  ASSERT_TRUE(service.poll_once(std::chrono::milliseconds{0}, after_retention).is_ok());
  EXPECT_EQ(service.metrics().cancel_tombstones, 0U);
  auto prepared = service.receive(DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare(
                                      {{127U, 0U, 0U, 1U}, 9123U})},
                                  peer, after_retention);
  ASSERT_TRUE(prepared.has_value()) << prepared.error().to_string();
  ASSERT_EQ(prepared->status_code, common::StatusCode::kOk);
  auto installed_cancel = service.receive(
      DistributedVectorGroupedAggregateShuffleJobControlRequest{cancel}, peer, after_retention);
  ASSERT_TRUE(installed_cancel.has_value()) << installed_cancel.error().to_string();
  EXPECT_EQ(installed_cancel->status_code, common::StatusCode::kOk);
  auto installed_duplicate = service.receive(
      DistributedVectorGroupedAggregateShuffleJobControlRequest{cancel}, peer, after_retention);
  ASSERT_TRUE(installed_duplicate.has_value());
  EXPECT_EQ(installed_duplicate->status_code, common::StatusCode::kOk);
  EXPECT_EQ(service.metrics().cancel_requests, 5U);
  EXPECT_EQ(service.metrics().duplicate_cancels, 2U);
  EXPECT_EQ(service.metrics().cancelled_jobs, 1U);
}

TEST(DistributedVectorGroupedAggregateShuffleJobServiceTest,
     ActivatesRenewsAndExpiresAnAuthenticatedCoordinatorLease) {
  Authenticator authenticator{93U};
  Authorizer authorizer;
  auto result_context = network::TlsClientContext::create(client_tls()).value();
  const std::array result_contexts{DistributedQueryNodeTlsContext{3U, &result_context},
                                   DistributedQueryNodeTlsContext{9U, &result_context}};
  auto service = DistributedVectorGroupedAggregateShuffleJobService::create(
                     {.local_node_id = 3U,
                      .shuffle_tls = server_tls(),
                      .shuffle_authenticator = &authenticator,
                      .result_authenticator = &authenticator,
                      .node_authorizer = &authorizer,
                      .result_tls_contexts = result_contexts,
                      .maximum_jobs = 1U,
                      .maximum_job_query_memory_bytes = 16U << 20U})
                     .value();
  const network::PeerAuthenticationResult peer{.authorized = true, .principal_id = 93U};
  const auto now = std::chrono::steady_clock::now();
  const network::Ipv4Endpoint result_endpoint{{127U, 0U, 0U, 1U}, 9123U};
  auto prepared = service.receive(
      DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare(result_endpoint)}, peer,
      now);
  ASSERT_TRUE(prepared.has_value()) << prepared.error().to_string();
  ASSERT_EQ(prepared->status_code, common::StatusCode::kOk);
  ASSERT_TRUE(prepared->reducer_shuffle_endpoint.has_value());

  const DistributedVectorGroupedAggregateShuffleJobRenewLease renewal{
      .query_id = uuid(1U),
      .coordinator_node_id = 9U,
      .target_node_id = 3U,
      .lease_duration = std::chrono::milliseconds{10}};
  auto too_early = service.receive(
      DistributedVectorGroupedAggregateShuffleJobControlRequest{renewal}, peer, now);
  ASSERT_TRUE(too_early.has_value());
  EXPECT_EQ(too_early->status_code, common::StatusCode::kUnavailable);

  const DistributedVectorGroupedAggregateShuffleJobInstallRoutes routes{
      .query_id = uuid(1U),
      .coordinator_node_id = 9U,
      .target_node_id = 3U,
      .routes = {{.node_id = 3U, .endpoint = *prepared->reducer_shuffle_endpoint}}};
  auto installed =
      service.receive(DistributedVectorGroupedAggregateShuffleJobControlRequest{routes}, peer, now);
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  ASSERT_EQ(installed->status_code, common::StatusCode::kOk);

  auto activated = service.receive(
      DistributedVectorGroupedAggregateShuffleJobControlRequest{renewal}, peer, now);
  ASSERT_TRUE(activated.has_value()) << activated.error().to_string();
  EXPECT_EQ(activated->status_code, common::StatusCode::kOk);
  auto conflict = renewal;
  conflict.lease_duration = std::chrono::milliseconds{20};
  auto conflicted =
      service.receive(DistributedVectorGroupedAggregateShuffleJobControlRequest{conflict}, peer,
                      now + std::chrono::milliseconds{1});
  ASSERT_TRUE(conflicted.has_value());
  EXPECT_EQ(conflicted->status_code, common::StatusCode::kAlreadyExists);
  auto renewed = service.receive(DistributedVectorGroupedAggregateShuffleJobControlRequest{renewal},
                                 peer, now + std::chrono::milliseconds{4});
  ASSERT_TRUE(renewed.has_value());
  EXPECT_EQ(renewed->status_code, common::StatusCode::kOk);

  ASSERT_TRUE(
      service.poll_once(std::chrono::milliseconds{0}, now + std::chrono::milliseconds{11}).is_ok());
  EXPECT_EQ(service.metrics().lease_expirations, 0U);
  ASSERT_TRUE(
      service.poll_once(std::chrono::milliseconds{0}, now + std::chrono::milliseconds{15}).is_ok());
  EXPECT_EQ(service.metrics().lease_renew_requests, 4U);
  EXPECT_EQ(service.metrics().lease_activations, 1U);
  EXPECT_EQ(service.metrics().lease_renewals, 1U);
  EXPECT_EQ(service.metrics().lease_expirations, 1U);
  EXPECT_EQ(service.metrics().cancelled_jobs, 1U);

  auto late_renewal =
      service.receive(DistributedVectorGroupedAggregateShuffleJobControlRequest{renewal}, peer,
                      now + std::chrono::milliseconds{15});
  ASSERT_TRUE(late_renewal.has_value());
  EXPECT_EQ(late_renewal->status_code, common::StatusCode::kCancelled);
  EXPECT_EQ(service.metrics().lease_renew_requests, 5U);
  EXPECT_EQ(service.metrics().lease_renewals, 1U);

  auto duplicate_prepare = service.receive(
      DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare(result_endpoint)}, peer,
      now + std::chrono::milliseconds{15});
  ASSERT_TRUE(duplicate_prepare.has_value());
  EXPECT_EQ(duplicate_prepare->status_code, common::StatusCode::kCancelled);
}

TEST(DistributedVectorGroupedAggregateShuffleJobServiceTest,
     LocalControlRejectsForeignIdentityAndRetainsCancellationTombstone) {
  Authenticator authenticator{93U};
  Authorizer authorizer;
  auto result_context = network::TlsClientContext::create(client_tls()).value();
  const std::array result_contexts{DistributedQueryNodeTlsContext{3U, &result_context}};
  auto service = DistributedVectorGroupedAggregateShuffleJobService::create(
                     {.local_node_id = 3U,
                      .shuffle_authenticator = &authenticator,
                      .result_authenticator = &authenticator,
                      .node_authorizer = &authorizer,
                      .result_tls_contexts = result_contexts,
                      .maximum_jobs = 1U,
                      .maximum_job_query_memory_bytes = 16U << 20U})
                     .value();
  const auto now = std::chrono::steady_clock::now();
  const DistributedVectorGroupedAggregateShuffleJobCancel local_cancel{
      .query_id = uuid(1U), .coordinator_node_id = 3U, .target_node_id = 3U};
  auto cancelled = service.receive_local(
      DistributedVectorGroupedAggregateShuffleJobControlRequest{local_cancel}, now);
  ASSERT_TRUE(cancelled.has_value()) << cancelled.error().to_string();
  EXPECT_EQ(cancelled->status_code, common::StatusCode::kOk);
  auto duplicate = service.receive_local(
      DistributedVectorGroupedAggregateShuffleJobControlRequest{local_cancel}, now);
  ASSERT_TRUE(duplicate.has_value());
  EXPECT_EQ(duplicate->status_code, common::StatusCode::kOk);

  auto foreign = local_cancel;
  foreign.coordinator_node_id = 9U;
  EXPECT_EQ(
      service.receive_local(DistributedVectorGroupedAggregateShuffleJobControlRequest{foreign}, now)
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);

  auto local_prepare = prepare({});
  local_prepare.coordinator_node_id = 3U;
  local_prepare.target_node_id = 3U;
  auto blocked = service.receive_local(
      DistributedVectorGroupedAggregateShuffleJobControlRequest{std::move(local_prepare)}, now);
  ASSERT_TRUE(blocked.has_value()) << blocked.error().to_string();
  EXPECT_EQ(blocked->status_code, common::StatusCode::kCancelled);
  EXPECT_EQ(service.metrics().cancel_tombstones, 1U);
  EXPECT_EQ(service.metrics().duplicate_cancels, 1U);
}

TEST(DistributedVectorGroupedAggregateShuffleJobServiceTest,
     SerializesLocalControlMetricsAndPollAcrossThreads) {
  Authenticator authenticator{93U};
  Authorizer authorizer;
  auto result_context = network::TlsClientContext::create(client_tls()).value();
  const std::array result_contexts{DistributedQueryNodeTlsContext{3U, &result_context}};
  auto service = DistributedVectorGroupedAggregateShuffleJobService::create(
                     {.local_node_id = 3U,
                      .shuffle_authenticator = &authenticator,
                      .result_authenticator = &authenticator,
                      .node_authorizer = &authorizer,
                      .result_tls_contexts = result_contexts,
                      .maximum_jobs = 1U,
                      .maximum_job_query_memory_bytes = 16U << 20U})
                     .value();
  std::atomic<bool> poll_succeeded{true};
  std::thread poller{[&] {
    for (std::size_t iteration = 0U; iteration < 1024U; ++iteration) {
      if (!service.poll_once(std::chrono::milliseconds{0}, std::chrono::steady_clock::now())
               .is_ok())
        poll_succeeded.store(false, std::memory_order_relaxed);
    }
  }};
  bool local_succeeded{true};
  for (std::uint8_t seed = 1U; seed < 32U; ++seed) {
    const DistributedVectorGroupedAggregateShuffleJobCancel cancel{
        .query_id = uuid(seed), .coordinator_node_id = 3U, .target_node_id = 3U};
    auto response =
        service.receive_local(DistributedVectorGroupedAggregateShuffleJobControlRequest{cancel},
                              std::chrono::steady_clock::now());
    if (!response.has_value() || response->status_code != common::StatusCode::kOk) {
      local_succeeded = false;
      break;
    }
    static_cast<void>(service.metrics());
  }
  poller.join();
  EXPECT_TRUE(local_succeeded);
  EXPECT_TRUE(poll_succeeded.load(std::memory_order_relaxed));
  EXPECT_EQ(service.metrics().cancel_tombstones, 31U);
}

} // namespace
} // namespace chronos::cluster
