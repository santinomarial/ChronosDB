#include "chronos/cluster/raft_observation_tcp_client.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <poll.h>
#include <utility>

namespace chronos::cluster {
namespace {

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] network::TlsServerConfig server_tls_config() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
}

[[nodiscard]] network::TlsClientConfig client_tls_config() {
  return {.certificate_chain_file = fixture("client.pem").string(),
          .private_key_file = fixture("client-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

[[nodiscard]] raft::GroupId group() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{7U};
  return raft::GroupId{bytes};
}

[[nodiscard]] raft::RaftGroupObservation observation() {
  return {.group_id = group(),
          .node_id = 2U,
          .role = raft::Role::kFollower,
          .current_term = 5U,
          .leader_id = 1U,
          .last_log_index = 12U,
          .commit_index = 11U,
          .applied_index = 10U,
          .voters = {1U, 2U, 3U},
          .committed_voters = {1U, 2U, 3U}};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest& request) override {
    saw_fingerprint = request.peer_certificate_sha256.has_value();
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = 91U};
  }
  bool saw_fingerprint{};
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return principal == 91U && node == 2U;
  }
};

[[nodiscard]] RaftObservationTcpClientConfig config(const network::Ipv4Endpoint endpoint,
                                                    const network::TlsClientContext& tls,
                                                    Authenticator& authenticator,
                                                    Authorizer& authorizer) {
  return {.remote_endpoint = endpoint,
          .tls_context = &tls,
          .carrier = {.authenticator = &authenticator,
                      .node_authorizer = &authorizer,
                      .peer_ipv4_address = endpoint.address,
                      .request = {1U, 2U, group(), 19U},
                      .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                                 .exchange_timeout = std::chrono::milliseconds{1000}}},
          .connect_timeout = std::chrono::milliseconds{1000}};
}

TEST(RaftObservationTcpClientTest, OwnsRealConnectTlsAndCorrelatedExchange) {
  auto listener = network::TcpListener::bind();
  auto server_context = network::TlsServerContext::create(server_tls_config());
  auto client_context = network::TlsClientContext::create(client_tls_config());
  ASSERT_TRUE(listener.has_value());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());
  Authenticator authenticator;
  Authorizer authorizer;
  const auto start = RaftObservationTcpClient::TimePoint::clock::now();
  auto client = RaftObservationTcpClient::begin(
      config(listener->bound_endpoint(), *client_context, authenticator, authorizer), start);
  ASSERT_TRUE(client.has_value()) << client.error().to_string();
  EXPECT_TRUE(client->interest().want_write);
  EXPECT_FALSE(client->interest().want_read);
  EXPECT_EQ(client->result().error().code(), common::StatusCode::kInvalidArgument);

  std::optional<network::TcpSocket> accepted;
  std::optional<network::TlsSocket> server;
  RaftObservationRequestReader request_reader;
  std::array<std::byte, 256U> scratch{};
  std::optional<RaftObservationFrameWriteCursor> response;
  for (std::size_t iteration = 0U; iteration < 2048U; ++iteration) {
    const auto interest = client->interest();
    pollfd descriptor{.fd = client->descriptor(),
                      .events = static_cast<short>((interest.want_read ? POLLIN : 0) |
                                                   (interest.want_write ? POLLOUT : 0))};
    ASSERT_GE(::poll(&descriptor, 1U, 1), 0);
    ASSERT_TRUE(client
                    ->on_ready((descriptor.revents & POLLIN) != 0,
                               (descriptor.revents & POLLOUT) != 0,
                               RaftObservationTcpClient::TimePoint::clock::now())
                    .is_ok())
        << client->failure().to_string();

    if (!accepted.has_value()) {
      auto next = listener->accept_one();
      ASSERT_TRUE(next.has_value()) << next.error().to_string();
      auto* next_socket = next->transform([](auto& value) { return &value; }).value_or(nullptr);
      if (next_socket != nullptr) {
        accepted.emplace(std::move(*next_socket));
        auto tls = network::TlsSocket::accept(*server_context, accepted->descriptor());
        ASSERT_TRUE(tls.has_value()) << tls.error().to_string();
        server.emplace(std::move(*tls));
      }
    } else {
      auto* server_socket = server.transform([](auto& value) { return &value; }).value_or(nullptr);
      ASSERT_NE(server_socket, nullptr);
      if (!server_socket->handshake_complete()) {
        auto progress = server_socket->handshake();
        ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
      } else if (!response.has_value()) {
        auto progress = server_socket->read(scratch);
        ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
        if (progress->state == network::TlsIoState::kComplete) {
          auto step =
              request_reader.consume(common::ByteView{scratch}.first(progress->bytes_transferred));
          ASSERT_TRUE(step.has_value()) << step.error().to_string();
          const auto* decoded_request =
              step->request.transform([](const auto& value) { return &value; }).value_or(nullptr);
          if (decoded_request != nullptr) {
            EXPECT_EQ(decoded_request->target_node_id, 2U);
            auto encoded =
                encode_raft_observation_response_v1({.source_node_id = 2U,
                                                     .target_node_id = 1U,
                                                     .group_id = group(),
                                                     .correlation_id = 19U,
                                                     .status_code = common::StatusCode::kOk,
                                                     .observation = observation()});
            ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
            response.emplace(RaftObservationFrameWriteCursor::create(std::move(*encoded)).value());
          }
        }
      } else {
        auto* writer = response.transform([](auto& value) { return &value; }).value_or(nullptr);
        ASSERT_NE(writer, nullptr);
        if (!writer->complete()) {
          auto progress = server_socket->write(writer->pending_write());
          ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
          if (progress->state == network::TlsIoState::kComplete)
            ASSERT_TRUE(writer->consume_written(progress->bytes_transferred).is_ok());
        }
      }
    }
    if (client->state() == RaftObservationTcpClientState::kComplete)
      break;
  }

  ASSERT_EQ(client->state(), RaftObservationTcpClientState::kComplete);
  EXPECT_TRUE(authenticator.saw_fingerprint);
  EXPECT_FALSE(client->interest().want_read);
  EXPECT_FALSE(client->interest().want_write);
  EXPECT_FALSE(client->deadline().has_value());
  auto acquired = client->result();
  ASSERT_TRUE(acquired.has_value()) << acquired.error().to_string();
  EXPECT_EQ(*acquired, observation());
}

TEST(RaftObservationTcpClientTest, RejectsRouteMismatchAndExpiresConnectExactly) {
  auto listener = network::TcpListener::bind();
  auto tls_context = network::TlsClientContext::create(client_tls_config());
  ASSERT_TRUE(listener.has_value());
  ASSERT_TRUE(tls_context.has_value());
  Authenticator authenticator;
  Authorizer authorizer;
  auto invalid = config(listener->bound_endpoint(), *tls_context, authenticator, authorizer);
  invalid.carrier.peer_ipv4_address = {127U, 0U, 0U, 2U};
  EXPECT_EQ(RaftObservationTcpClient::begin(invalid, {}).error().code(),
            common::StatusCode::kInvalidArgument);

  auto value = config(listener->bound_endpoint(), *tls_context, authenticator, authorizer);
  value.connect_timeout = std::chrono::milliseconds{5};
  const auto start = RaftObservationTcpClient::TimePoint{};
  auto client = RaftObservationTcpClient::begin(value, start);
  ASSERT_TRUE(client.has_value()) << client.error().to_string();
  const auto connect_deadline = client->deadline();
  ASSERT_TRUE(connect_deadline.has_value());
  EXPECT_EQ(connect_deadline.value_or(RaftObservationTcpClient::TimePoint::min()),
            start + std::chrono::milliseconds{5});
  EXPECT_TRUE(client->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const auto failure = client->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(failure.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->state(), RaftObservationTcpClientState::kFailed);
  EXPECT_EQ(client->descriptor(), -1);
  EXPECT_FALSE(client->interest().want_read);
  EXPECT_FALSE(client->interest().want_write);
  EXPECT_FALSE(client->deadline().has_value());
  EXPECT_EQ(client->result().error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(client->on_ready(true, true, start + std::chrono::milliseconds{6}), failure);
}

} // namespace
} // namespace chronos::cluster
