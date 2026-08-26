#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tcp_client.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <poll.h>
#include <span>
#include <string>
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

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority authority() {
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  return DistributedVectorGroupedAggregateShuffleAuthority::create(
             uuid(1U), {{tablet, 2U}}, {{0U, 3U}}, {{0U, string_type(), false}},
             {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
      .value();
}

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {.columns = {{"region", string_type(), false}}};
}

[[nodiscard]] std::vector<std::byte> batch(const std::string& value) {
  const std::array columns{network::QueryResultColumn{"region", string_type(), false}};
  const std::array cells{network::QueryResultCell{.value = std::as_bytes(std::span{value})}};
  return network::encode_query_result_batch(1U, columns, cells).value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleResultAttempt
attempt(const DistributedVectorGroupedAggregateShuffleAuthority& expected,
        const query::DistributedVectorResultSchema& schema) {
  const std::vector<std::vector<std::byte>> batches{batch("east"), batch("west")};
  auto stream = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
                    expected, schema, 0U, 3U, 9U, batches,
                    {.maximum_frames = 2U, .maximum_encoded_bytes = 1U << 20U})
                    .value();
  return {.attempt_number = 1U, .target_node_id = 9U, .stream = std::move(stream)};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleResultTlsLimits limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .stream = {.maximum_frames = 2U, .maximum_encoded_bytes = 1U << 20U}};
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

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return common::Result<bool>{(principal == 91U && node == 3U) ||
                                (principal == 92U && node == 9U)};
  }
};

TEST(DistributedVectorGroupedAggregateShuffleResultTcpClientTest,
     OwnsConnectAndCompletesOnlyAfterExactAuthenticatedReceipt) {
  auto expected = authority();
  auto schema = result_schema();
  auto listener = network::TcpListener::bind();
  auto server_context = network::TlsServerContext::create(server_tls());
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(listener.has_value());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());
  Authenticator reducer_authenticator{91U};
  Authenticator coordinator_authenticator{92U};
  Authorizer authorizer;
  const auto start =
      DistributedVectorGroupedAggregateShuffleResultTcpClient::TimePoint::clock::now();
  auto client = DistributedVectorGroupedAggregateShuffleResultTcpClient::begin(
      attempt(expected, schema), expected, schema,
      {.remote_endpoint = listener->bound_endpoint(),
       .tls_context = &*client_context,
       .carrier = {.authenticator = &coordinator_authenticator,
                   .node_authorizer = &authorizer,
                   .peer_ipv4_address = {127U, 0U, 0U, 1U},
                   .limits = limits()},
       .connect_timeout = std::chrono::milliseconds{1000}},
      start);
  ASSERT_TRUE(client.has_value()) << client.error().to_string();
  EXPECT_EQ(client->attempt_number(), 1U);
  EXPECT_EQ(client->target_node_id(), 9U);

  std::optional<network::TcpSocket> accepted_socket;
  std::optional<DistributedVectorGroupedAggregateShuffleResultTlsServer> server;
  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    if (!accepted_socket.has_value()) {
      auto accepted = listener->accept_one();
      ASSERT_TRUE(accepted.has_value());
      if (accepted->has_value()) {
        accepted_socket.emplace(std::move(accepted->value()));
        auto tls = network::TlsSocket::accept(*server_context, accepted_socket->descriptor());
        ASSERT_TRUE(tls.has_value());
        auto carrier = DistributedVectorGroupedAggregateShuffleResultTlsServer::create(
            std::move(*tls),
            {.authenticator = &reducer_authenticator,
             .node_authorizer = &authorizer,
             .authority = &expected,
             .result_schema = &schema,
             .coordinator_node_id = 9U,
             .peer_ipv4_address = accepted_socket->peer_endpoint().value().address,
             .limits = limits()},
            DistributedVectorGroupedAggregateShuffleResultTlsServer::TimePoint::clock::now());
        ASSERT_TRUE(carrier.has_value()) << carrier.error().to_string();
        server.emplace(std::move(*carrier));
      }
    }

    std::array<pollfd, 2U> descriptors{};
    std::size_t count{};
    const auto client_interest = client->interest();
    descriptors[count++] = {.fd = client->descriptor(),
                            .events =
                                static_cast<short>((client_interest.want_read ? POLLIN : 0) |
                                                   (client_interest.want_write ? POLLOUT : 0))};
    if (server.has_value()) {
      const auto server_interest = server->interest();
      descriptors[count++] = {.fd = accepted_socket->descriptor(),
                              .events =
                                  static_cast<short>((server_interest.want_read ? POLLIN : 0) |
                                                     (server_interest.want_write ? POLLOUT : 0))};
    }
    ASSERT_GE(::poll(descriptors.data(), static_cast<nfds_t>(count), 1), 0);
    const auto now =
        DistributedVectorGroupedAggregateShuffleResultTcpClient::TimePoint::clock::now();
    ASSERT_TRUE(client
                    ->on_ready((descriptors[0].revents & POLLIN) != 0,
                               (descriptors[0].revents & POLLOUT) != 0, now)
                    .is_ok())
        << client->failure().to_string();
    if (server.has_value()) {
      ASSERT_TRUE(server
                      ->on_ready((descriptors[1].revents & POLLIN) != 0,
                                 (descriptors[1].revents & POLLOUT) != 0, now)
                      .is_ok())
          << server->failure().to_string();
    }
    if (client->state() ==
            DistributedVectorGroupedAggregateShuffleResultTcpClientState::kComplete &&
        server.has_value() &&
        server->state() == DistributedVectorGroupedAggregateShuffleResultTlsState::kComplete) {
      break;
    }
  }

  ASSERT_EQ(client->state(),
            DistributedVectorGroupedAggregateShuffleResultTcpClientState::kComplete);
  ASSERT_TRUE(server.has_value());
  ASSERT_EQ(server->state(), DistributedVectorGroupedAggregateShuffleResultTlsState::kComplete);
  EXPECT_TRUE(reducer_authenticator.saw_fingerprint);
  EXPECT_TRUE(coordinator_authenticator.saw_fingerprint);
  auto complete = server->take_complete_stream();
  ASSERT_TRUE(complete.has_value()) << complete.error().to_string();
  EXPECT_EQ(complete->partition_id, 0U);
  EXPECT_EQ(complete->source_node_id, 3U);
  EXPECT_EQ(complete->target_node_id, 9U);
  EXPECT_EQ(complete->encoded_result_batches.size(), 2U);
}

TEST(DistributedVectorGroupedAggregateShuffleResultTcpClientTest,
     ValidatesBeforeConnectAndExpiresAtExactDeadline) {
  auto expected = authority();
  auto schema = result_schema();
  auto listener = network::TcpListener::bind();
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(listener.has_value());
  ASSERT_TRUE(client_context.has_value());
  Authenticator authenticator{92U};
  Authorizer authorizer;
  const auto start = DistributedVectorGroupedAggregateShuffleResultTcpClient::TimePoint{};
  auto config = DistributedVectorGroupedAggregateShuffleResultTcpClientConfig{
      .remote_endpoint = listener->bound_endpoint(),
      .tls_context = &*client_context,
      .carrier = {.authenticator = &authenticator,
                  .node_authorizer = &authorizer,
                  .peer_ipv4_address = {127U, 0U, 0U, 1U},
                  .limits = limits()},
      .connect_timeout = std::chrono::milliseconds{5}};
  auto client = DistributedVectorGroupedAggregateShuffleResultTcpClient::begin(
      attempt(expected, schema), expected, schema, config, start);
  ASSERT_TRUE(client.has_value()) << client.error().to_string();
  EXPECT_EQ(client->deadline(), start + std::chrono::milliseconds{5});
  EXPECT_TRUE(client->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const common::Status expired =
      client->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(expired.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->state(), DistributedVectorGroupedAggregateShuffleResultTcpClientState::kFailed);
  EXPECT_EQ(client->descriptor(), -1);
  EXPECT_EQ(client->on_ready(true, true, start + std::chrono::milliseconds{6}), expired);

  config.carrier.peer_ipv4_address = {127U, 0U, 0U, 2U};
  auto invalid = DistributedVectorGroupedAggregateShuffleResultTcpClient::begin(
      attempt(expected, schema), expected, schema, config, start);
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().code(), common::StatusCode::kInvalidArgument);

  config.carrier.peer_ipv4_address = config.remote_endpoint.address;
  auto wrong_target = attempt(expected, schema);
  wrong_target.target_node_id = 3U;
  auto mismatched = DistributedVectorGroupedAggregateShuffleResultTcpClient::begin(
      std::move(wrong_target), expected, schema, config, start);
  ASSERT_FALSE(mismatched.has_value());
  EXPECT_EQ(mismatched.error().code(), common::StatusCode::kInvalidArgument);

  const std::vector<std::vector<std::byte>> batches{batch("wrong-coordinator")};
  auto wrong_stream = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
                          expected, schema, 0U, 3U, 10U, batches)
                          .value();
  auto encoded_mismatch = DistributedVectorGroupedAggregateShuffleResultTcpClient::begin(
      {.attempt_number = 1U, .target_node_id = 9U, .stream = std::move(wrong_stream)}, expected,
      schema, config, start);
  ASSERT_FALSE(encoded_mismatch.has_value());
  EXPECT_EQ(encoded_mismatch.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cluster
