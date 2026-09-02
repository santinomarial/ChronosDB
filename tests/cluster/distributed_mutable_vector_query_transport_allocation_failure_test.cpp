#include "chronos/cluster/distributed_mutable_vector_query_execution.hpp"
#include "chronos/cluster/distributed_mutable_vector_query_tcp.hpp"
#include "chronos/cluster/distributed_mutable_vector_query_tcp_execution.hpp"
#include "chronos/cluster/distributed_mutable_vector_query_tls.hpp"
#include "chronos/cluster/distributed_mutable_vector_query_transport.hpp"
#include "chronos/cluster/distributed_mutable_vector_rows_query_tcp_execution.hpp"
#include "support/failing_allocator.hpp"

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <utility>

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

template <typename Operation>
[[nodiscard]] auto run_failure(const std::size_t fail_after, Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
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
          .serving_node = 7U,
          .applied_position = 8U,
          .observed_leader_commit_position = 8U,
          .placement_epoch = 9U,
          .read_policy = {.consistency = query::DistributedReadConsistency::kLocalEventual},
          .destination_column_ordinals = {0U},
          .plan = {.mode = query::DistributedVectorPlanMode::kRows, .row_output_indices = {0U}},
          .result_schema = result_schema()};
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

class Worker final : public DistributedMutableVectorQueryWorkerService {
public:
  Worker()
      : messages_{{.query_id = uuid(1U),
                   .tablet_id = id<schema::TabletId>(4U),
                   .sequence = 1U,
                   .terminal = true}} {}

  common::Result<std::vector<DistributedVectorResultExchangeMessage>>
  execute(const query::DistributedMutableVectorFragment&) override {
    return std::move(messages_);
  }

private:
  std::vector<DistributedVectorResultExchangeMessage> messages_;
};

template <typename Operation>
void expect_eventual_success(const char* label, Operation&& operation) {
  SCOPED_TRACE(label);
  bool success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    auto result = run_failure(fail_after, operation);
    if (result.has_value()) {
      success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(success);
}

TEST(DistributedMutableVectorQueryTransportAllocationFailureTest,
     ClassifiesOwnedRequestAllocations) {
  const DistributedMutableVectorQueryRequest request{1U, 7U, fragment()};
  expect_eventual_success("request encode",
                          [&] { return encode_distributed_mutable_vector_query_request(request); });
  const auto encoded = encode_distributed_mutable_vector_query_request(request);
  ASSERT_TRUE(encoded.has_value());
  expect_eventual_success("request decode", [&] {
    return decode_distributed_mutable_vector_query_request_exact(*encoded);
  });
  expect_eventual_success("request reader", [&] {
    DistributedMutableVectorQueryRequestReader reader;
    return reader.consume(*encoded);
  });
}

TEST(DistributedMutableVectorQueryReceiverAllocationFailureTest,
     ClassifiesOwnedResponsePublicationAllocations) {
  Authorizer authorizer;
  const auto encoded = encode_distributed_mutable_vector_query_request({1U, 7U, fragment()});
  ASSERT_TRUE(encoded.has_value());
  bool success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    Worker worker;
    auto receiver = DistributedMutableVectorQueryReceiver::create(
        {.local_node_id = 7U, .authorizer = &authorizer, .worker = &worker});
    ASSERT_TRUE(receiver.has_value());
    auto result = run_failure(fail_after, [&] {
      return receiver->receive(*encoded, {.authorized = true, .principal_id = 91U});
    });
    if (result.has_value()) {
      success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(success);
}

TEST(DistributedMutableVectorQuerySenderAllocationFailureTest,
     ClassifiesSchemaValidationAndResultRetentionAllocations) {
  const DistributedVectorQueryResponseV2 response{
      .source_node_id = 7U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = id<schema::TabletId>(4U),
      .status_code = common::StatusCode::kOk,
      .payload = DistributedVectorResultExchangeMessage{.query_id = uuid(1U),
                                                        .tablet_id = id<schema::TabletId>(4U),
                                                        .sequence = 1U,
                                                        .terminal = true}};
  bool success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    auto sender = DistributedMutableVectorQuerySender::create(1U, fragment());
    ASSERT_TRUE(sender.has_value());
    ASSERT_TRUE(sender->begin_attempt({}).has_value());
    const common::Status status = run_failure(
        fail_after, [&] { return sender->accept_responses(std::span{&response, 1U}, {}); });
    if (status.is_ok()) {
      ASSERT_TRUE(sender->result().has_value());
      success = true;
      break;
    }
    EXPECT_EQ(status.code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(sender->state(), DistributedQuerySenderState::kWaitingForResponse);
    EXPECT_FALSE(sender->result().has_value());
  }
  EXPECT_TRUE(success);
}

TEST(DistributedMutableVectorQueryExecutionAllocationFailureTest,
     ClassifiesMultiTabletOwnerConstructionAllocations) {
  bool saw_failure = false;
  bool saw_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    SCOPED_TRACE(testing::Message{} << "fail_after=" << fail_after);
    auto second = fragment();
    second.tablet_id = id<schema::TabletId>(8U);
    second.raft_group_id = uuid(9U);
    second.serving_node = 10U;
    std::vector fragments{fragment(), std::move(second)};
    auto result = run_failure(fail_after, [&] {
      return DistributedMutableVectorQueryExecution::create(1U, std::move(fragments));
    });
    if (!result.has_value()) {
      saw_failure = true;
      EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
      continue;
    }
    saw_success = true;
    break;
  }
  EXPECT_TRUE(saw_failure);
  EXPECT_TRUE(saw_success);
}

TEST(DistributedMutableVectorQueryTlsAllocationFailureTest,
     ClassifiesClientOwnerConstructionAllocations) {
  Authorizer authorizer;
  Authenticator authenticator;
  const DistributedMutableVectorQueryTlsClientConfig client_config{
      .authenticator = &authenticator,
      .node_authorizer = &authorizer,
      .limits = {.maximum_response_frames = 2U, .maximum_response_bytes = 1024U}};
  bool client_failure = false;
  bool client_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    auto encoded = encode_distributed_mutable_vector_query_request({1U, 7U, fragment()});
    ASSERT_TRUE(encoded.has_value());
    DistributedMutableVectorQueryAttempt attempt{1U, 7U, std::move(*encoded)};
    auto result = run_failure(fail_after, [&] {
      return DistributedMutableVectorQueryTlsClient::create(network::TlsSocket{},
                                                            std::move(attempt), client_config, {});
    });
    if (result.has_value()) {
      client_success = true;
      break;
    }
    client_failure = true;
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(client_failure);
  EXPECT_TRUE(client_success);
}

TEST(DistributedMutableVectorQueryTlsAllocationFailureTest,
     ClassifiesServerOwnerConstructionAllocations) {
  Authorizer authorizer;
  Authenticator authenticator;
  Worker worker;
  auto receiver = DistributedMutableVectorQueryReceiver::create(
      {.local_node_id = 7U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());
  expect_eventual_success("TLS server", [&] {
    return DistributedMutableVectorQueryTlsServer::create(
        network::TlsSocket{},
        {.authenticator = &authenticator,
         .receiver = &*receiver,
         .limits = {.maximum_response_frames = 2U, .maximum_response_bytes = 1024U}},
        {});
  });
}

TEST(DistributedMutableVectorQueryTcpAllocationFailureTest,
     ClassifiesClientValidationAndOwnerAllocations) {
  auto listener = network::TcpListener::bind();
  auto tls_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  Authorizer authorizer;
  Authenticator authenticator;
  bool saw_failure = false;
  bool saw_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    SCOPED_TRACE(testing::Message{} << "fail_after=" << fail_after);
    auto sender = DistributedMutableVectorQuerySender::create(1U, fragment());
    ASSERT_TRUE(sender.has_value());
    auto attempt = sender->begin_attempt({});
    ASSERT_TRUE(attempt.has_value());
    auto result = run_failure(fail_after, [&] {
      return DistributedMutableVectorQueryTcpClient::begin(
          std::move(*attempt),
          {.remote_endpoint = listener->bound_endpoint(),
           .tls_context = &*tls_context,
           .carrier = {.authenticator = &authenticator,
                       .node_authorizer = &authorizer,
                       .peer_ipv4_address = {127U, 0U, 0U, 1U},
                       .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                                  .exchange_timeout = std::chrono::milliseconds{1000},
                                  .maximum_response_frames = 2U,
                                  .maximum_response_bytes = 1024U}},
           .connect_timeout = std::chrono::milliseconds{1000}},
          DistributedMutableVectorQueryTcpClient::TimePoint::clock::now());
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

TEST(DistributedMutableVectorQueryTcpExecutionAllocationFailureTest,
     ClassifiesSchedulerConstructionAllocations) {
  auto listener = network::TcpListener::bind();
  auto tls_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  Authorizer authorizer;
  Authenticator authenticator;
  bool saw_failure = false;
  bool saw_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    SCOPED_TRACE(testing::Message{} << "fail_after=" << fail_after);
    auto portable = DistributedMutableVectorQueryExecution::create(1U, {fragment()});
    ASSERT_TRUE(portable.has_value()) << portable.error().to_string();
    DistributedMutableVectorQueryTcpExecutionConfig config{
        .authenticator = &authenticator,
        .node_authorizer = &authorizer,
        .routes = {{.node_id = 7U,
                    .endpoints = {listener->bound_endpoint()},
                    .tls_context = std::addressof(*tls_context)}}};
    auto result = run_failure(fail_after, [&] {
      return DistributedMutableVectorQueryTcpExecution::create(std::move(*portable),
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

  auto portable = DistributedMutableVectorQueryExecution::create(1U, {fragment()});
  ASSERT_TRUE(portable.has_value()) << portable.error().to_string();
  auto scheduler = DistributedMutableVectorQueryTcpExecution::create(
      std::move(*portable), {.authenticator = &authenticator,
                             .node_authorizer = &authorizer,
                             .routes = {{.node_id = 7U,
                                         .endpoints = {listener->bound_endpoint()},
                                         .tls_context = std::addressof(*tls_context)}}});
  ASSERT_TRUE(scheduler.has_value()) << scheduler.error().to_string();
  auto transfer_failure = run_failure(0U, [&] { return scheduler->take_result(); });
  ASSERT_FALSE(transfer_failure.has_value());
  EXPECT_EQ(transfer_failure.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(scheduler->take_result().error().code(), common::StatusCode::kUnavailable);
}

TEST(DistributedMutableVectorRowsQueryTcpExecutionAllocationFailureTest,
     ClassifiesNativeRowOwnerConstructionAllocations) {
  auto listener = network::TcpListener::bind();
  auto tls_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  Authorizer authorizer;
  Authenticator authenticator;
  bool saw_failure = false;
  bool saw_success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    SCOPED_TRACE(testing::Message{} << "fail_after=" << fail_after);
    std::vector fragments{fragment()};
    DistributedMutableVectorRowsQueryTcpExecutionConfig config{
        .source_node_id = 1U,
        .execution = {},
        .tcp = {.authenticator = &authenticator,
                .node_authorizer = &authorizer,
                .routes = {{.node_id = 7U,
                            .endpoints = {listener->bound_endpoint()},
                            .tls_context = std::addressof(*tls_context)}}},
        .finalization = {}};
    auto result = run_failure(fail_after, [&] {
      return DistributedMutableVectorRowsQueryTcpExecution::create(std::move(fragments),
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

  DistributedMutableVectorRowsQueryTcpExecutionConfig transfer_config{
      .source_node_id = 1U,
      .execution = {},
      .tcp = {.authenticator = &authenticator,
              .node_authorizer = &authorizer,
              .routes = {{.node_id = 7U,
                          .endpoints = {listener->bound_endpoint()},
                          .tls_context = std::addressof(*tls_context)}}},
      .finalization = {}};
  auto owner = DistributedMutableVectorRowsQueryTcpExecution::create({fragment()},
                                                                     std::move(transfer_config));
  ASSERT_TRUE(owner.has_value()) << owner.error().to_string();
  auto unavailable_failure = run_failure(0U, [&] { return owner->take_result(); });
  ASSERT_FALSE(unavailable_failure.has_value());
  EXPECT_EQ(unavailable_failure.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(owner->take_result().error().code(), common::StatusCode::kUnavailable);
}

} // namespace
} // namespace chronos::cluster
