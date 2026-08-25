#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_execution.hpp"
#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tcp_client.hpp"
#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tcp_server.hpp"
#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tls.hpp"
#include "support/failing_allocator.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] network::TlsClientConfig client_tls() {
  return {.certificate_chain_file = fixture("client.pem").string(),
          .private_key_file = fixture("client-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

[[nodiscard]] network::TlsServerConfig server_tls() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
}

template <typename Operation>
[[nodiscard]] auto run_failure(const std::size_t fail_after, Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    test::ScopedAllocationFailure failure{fail_after};
    try {
      result.emplace(operation());
    } catch (...) {
      failure.disable();
      throw;
    }
    failure.disable();
  }
  return std::move(*result);
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] std::vector<query::VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U, .type = string_type(), .nullable = false}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> aggregates() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] query::DistributedMutableVectorFragment fragment() {
  const auto int64 = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  return {
      .query_id = uuid(1U),
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
      .linearizable_barrier = raft::ReadBarrier{.term = 2U, .context = 3U, .read_index = 10U},
      .destination_column_ordinals = {0U},
      .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
               .group_key_input_indices = {0U},
               .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}}},
      .result_schema = {.columns = {{.name = "region", .type = string_type(), .nullable = false},
                                    {.name = "count", .type = int64, .nullable = false}}}};
}

[[nodiscard]] DistributedVectorGroupedAggregateQueryResponseV2 response() {
  const auto expected = aggregates();
  auto state = query::MergeableVectorAggregateState::create(expected.front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(
                       string_type(), "a mutable grouped response key larger than inline storage")
                       .value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  return {.source_node_id = 2U,
          .target_node_id = 1U,
          .query_id = uuid(1U),
          .tablet_id = id<schema::TabletId>(4U),
          .status_code = common::StatusCode::kOk,
          .payload = query::DistributedVectorGroupedAggregateExchangeMessage{
              {.query_id = uuid(1U),
               .tablet_id = id<schema::TabletId>(4U),
               .sequence = 1U,
               .group_ordinal = 0U,
               .group_count = 1U,
               .terminal = true,
               .empty = false},
              std::move(values),
              std::move(states)}};
}

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId claimed_node_id) const override {
    return principal_id == 91U && claimed_node_id == 1U;
  }
};

class Authenticator final : public network::ConnectionAuthenticator {
public:
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = 91U};
  }
};

class Worker final : public DistributedMutableVectorGroupedAggregateQueryWorkerService {
public:
  common::Result<query::DistributedVectorGroupedAggregateAuthority>
  bind_authority(const query::DistributedMutableVectorFragment&) override {
    return query::DistributedVectorGroupedAggregateAuthority{keys(), aggregates()};
  }

  common::Result<query::DistributedVectorGroupedAggregateWorkerResultV2>
  execute(const query::DistributedMutableVectorFragment&) override {
    auto value = response();
    auto authority = query::DistributedVectorGroupedAggregateAuthority{keys(), aggregates()};
    auto encoded = query::encode_distributed_vector_grouped_aggregate_exchange_message(
        *value.payload, authority.keys, authority.aggregates);
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    query::DistributedVectorGroupedAggregateWorkerResultV2 result{
        .authority = std::move(authority), .input_rows = 1U, .group_count = 1U};
    result.encoded_bytes = encoded->bytes().size();
    result.messages.push_back(std::move(*encoded));
    return result;
  }
};

TEST(DistributedMutableVectorGroupedAggregateQueryReceiverAllocationFailureTest,
     ClassifiesAuthorityExecutionAndAtomicPublicationAllocations) {
  Authorizer authorizer;
  const auto request =
      encode_distributed_mutable_vector_query_request({1U, 2U, fragment()}).value();
  bool success{};
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    Worker worker;
    auto receiver = DistributedMutableVectorGroupedAggregateQueryReceiver::create(
        {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
    ASSERT_TRUE(receiver.has_value());
    auto result = run_failure(fail_after, [&] {
      return receiver->receive(request, {.authorized = true, .principal_id = 91U});
    });
    if (result.has_value()) {
      ASSERT_EQ(result->size(), 1U);
      success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(success);
}

TEST(DistributedMutableVectorGroupedAggregateQueryExecutionAllocationFailureTest,
     ClassifiesSenderAuthorityAndCoordinatorConstructionAllocations) {
  bool saw_failure{};
  bool saw_success{};
  for (std::size_t fail_after = 0U; fail_after < 512U; ++fail_after) {
    SCOPED_TRACE(testing::Message{} << "fail_after=" << fail_after);
    auto first = fragment();
    auto second = fragment();
    second.tablet_id = id<schema::TabletId>(8U);
    second.raft_group_id = uuid(9U);
    std::vector fragments{std::move(first), std::move(second)};
    auto owned_keys = keys();
    auto owned_aggregates = aggregates();
    auto result = run_failure(fail_after, [&] {
      return DistributedMutableVectorGroupedAggregateQueryExecution::create(
          1U, std::move(fragments), std::move(owned_keys), std::move(owned_aggregates),
          {.coordinator = {.messages = {.maximum_messages_per_fragment = 2U,
                                        .maximum_total_messages = 4U},
                           .maximum_total_encoded_bytes = 1U << 20U},
           .maximum_decode_memory_bytes = 1U << 20U});
    });
    if (!result.has_value()) {
      saw_failure = true;
      EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted)
          << result.error().to_string();
      continue;
    }
    saw_success = true;
    break;
  }
  EXPECT_TRUE(saw_failure);
  EXPECT_TRUE(saw_success);
}

TEST(DistributedMutableVectorGroupedAggregateQuerySenderAllocationFailureTest,
     ClassifiesCanonicalReconstructionAndReleasesDecodedKeyCredit) {
  const auto response_value = response();
  bool success{};
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    auto resources = query::QueryResourceContext::create(4U << 20U).value();
    {
      auto sender = DistributedMutableVectorGroupedAggregateQuerySender::create(
          1U, fragment(), keys(), aggregates(), resources);
      ASSERT_TRUE(sender.has_value()) << sender.error().to_string();
      ASSERT_TRUE(sender->begin_attempt({}).has_value());
      const common::Status status = run_failure(
          fail_after, [&] { return sender->accept_responses(std::span{&response_value, 1U}, {}); });
      if (status.is_ok()) {
        ASSERT_TRUE(sender->result().has_value());
        EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
        success = true;
      } else {
        EXPECT_EQ(status.code(), common::StatusCode::kResourceExhausted);
        EXPECT_EQ(sender->state(), DistributedQuerySenderState::kWaitingForResponse);
        EXPECT_FALSE(sender->result().has_value());
        EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      }
    }
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
    if (success)
      break;
  }
  EXPECT_TRUE(success);
}

TEST(DistributedMutableVectorGroupedAggregateQueryTlsAllocationFailureTest,
     ClassifiesClientAndServerOwnerConstructionAllocations) {
  Authorizer authorizer;
  Authenticator authenticator;
  Worker worker;
  auto receiver = DistributedMutableVectorGroupedAggregateQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  const DistributedMutableVectorGroupedAggregateQueryTlsLimits limits{
      .handshake_timeout = std::chrono::milliseconds{100},
      .exchange_timeout = std::chrono::milliseconds{100},
      .maximum_response_frames = 2U,
      .maximum_response_bytes = 1U << 20U};

  bool client_success{};
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    auto resources = query::QueryResourceContext::create(4U << 20U).value();
    auto sender = DistributedMutableVectorGroupedAggregateQuerySender::create(
        1U, fragment(), keys(), aggregates(), resources);
    ASSERT_TRUE(sender.has_value());
    auto attempt = sender->begin_attempt({});
    ASSERT_TRUE(attempt.has_value());
    auto owned_keys = keys();
    auto owned_aggregates = aggregates();
    auto result = run_failure(fail_after, [&] {
      return DistributedMutableVectorGroupedAggregateQueryTlsClient::create(
          network::TlsSocket{}, std::move(*attempt), std::move(owned_keys),
          std::move(owned_aggregates), resources,
          {.authenticator = &authenticator, .node_authorizer = &authorizer, .limits = limits}, {});
    });
    if (result.has_value()) {
      client_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(client_success);

  bool server_success{};
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return DistributedMutableVectorGroupedAggregateQueryTlsServer::create(
          network::TlsSocket{},
          {.authenticator = &authenticator, .receiver = &*receiver, .limits = limits}, {});
    });
    if (result.has_value()) {
      server_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(server_success);
}

TEST(DistributedMutableVectorGroupedAggregateQueryTcpClientAllocationFailureTest,
     ClassifiesValidationAndOwnerAllocations) {
  auto listener = network::TcpListener::bind();
  auto tls_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  Authorizer authorizer;
  Authenticator authenticator;
  bool saw_failure{};
  bool saw_success{};
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    SCOPED_TRACE(testing::Message{} << "fail_after=" << fail_after);
    auto resources = query::QueryResourceContext::create(4U << 20U).value();
    auto sender = DistributedMutableVectorGroupedAggregateQuerySender::create(
        1U, fragment(), keys(), aggregates(), resources);
    ASSERT_TRUE(sender.has_value());
    auto attempt = sender->begin_attempt({});
    ASSERT_TRUE(attempt.has_value());
    auto owned_keys = keys();
    auto owned_aggregates = aggregates();
    auto result = run_failure(fail_after, [&] {
      return DistributedMutableVectorGroupedAggregateQueryTcpClient::begin(
          std::move(*attempt), std::move(owned_keys), std::move(owned_aggregates), resources,
          {.remote_endpoint = listener->bound_endpoint(),
           .tls_context = &*tls_context,
           .carrier = {.authenticator = &authenticator,
                       .node_authorizer = &authorizer,
                       .peer_ipv4_address = {127U, 0U, 0U, 1U},
                       .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                                  .exchange_timeout = std::chrono::milliseconds{1000},
                                  .maximum_response_frames = 2U,
                                  .maximum_response_bytes = 1U << 20U}},
           .connect_timeout = std::chrono::milliseconds{1000}},
          DistributedMutableVectorGroupedAggregateQueryTcpClient::TimePoint::clock::now());
    });
    if (!result.has_value()) {
      saw_failure = true;
      EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted)
          << result.error().to_string();
      continue;
    }
    saw_success = true;
    break;
  }
  EXPECT_TRUE(saw_failure);
  EXPECT_TRUE(saw_success);
}

TEST(DistributedMutableVectorGroupedAggregateQueryTcpExecutionAllocationFailureTest,
     ClassifiesRouteAndSchedulerOwnerAllocations) {
  auto listener = network::TcpListener::bind();
  auto tls_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  Authorizer authorizer;
  Authenticator authenticator;
  bool saw_failure{};
  bool saw_success{};
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    SCOPED_TRACE(testing::Message{} << "fail_after=" << fail_after);
    auto portable = DistributedMutableVectorGroupedAggregateQueryExecution::create(
        1U, {fragment()}, keys(), aggregates());
    ASSERT_TRUE(portable.has_value()) << portable.error().to_string();
    DistributedMutableVectorGroupedAggregateQueryTcpExecutionConfig config{
        .authenticator = &authenticator,
        .node_authorizer = &authorizer,
        .routes = {{.node_id = 2U,
                    .endpoints = {listener->bound_endpoint()},
                    .tls_context = &*tls_context}}};
    auto result = run_failure(fail_after, [&] {
      return DistributedMutableVectorGroupedAggregateQueryTcpExecution::create(std::move(*portable),
                                                                               std::move(config));
    });
    if (!result.has_value()) {
      saw_failure = true;
      EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted)
          << result.error().to_string();
      continue;
    }
    saw_success = true;
    break;
  }
  EXPECT_TRUE(saw_failure);
  EXPECT_TRUE(saw_success);
}

TEST(DistributedMutableVectorGroupedAggregateQueryTcpServerAllocationFailureTest,
     ClassifiesTlsListenerAndBoundedOwnerAllocations) {
  Authorizer authorizer;
  Authenticator authenticator;
  Worker worker;
  auto receiver = DistributedMutableVectorGroupedAggregateQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  bool saw_failure{};
  bool saw_success{};
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    SCOPED_TRACE(testing::Message{} << "fail_after=" << fail_after);
    DistributedMutableVectorGroupedAggregateQueryTcpServerConfig config{
        .listener = {},
        .tls = server_tls(),
        .authenticator = &authenticator,
        .receiver = &*receiver,
        .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{100},
                           .exchange_timeout = std::chrono::milliseconds{100},
                           .maximum_response_frames = 2U,
                           .maximum_response_bytes = 1U << 20U},
        .maximum_connections = 8U,
        .maximum_accepts_per_poll = 2U};
    auto result = run_failure(fail_after, [&] {
      return DistributedMutableVectorGroupedAggregateQueryTcpServer::start(std::move(config));
    });
    if (!result.has_value()) {
      saw_failure = true;
      EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted)
          << result.error().to_string();
      continue;
    }
    saw_success = true;
    EXPECT_TRUE(result->shutdown().is_ok());
    break;
  }
  EXPECT_TRUE(saw_failure);
  EXPECT_TRUE(saw_success);
}

} // namespace
} // namespace chronos::cluster
