#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_tls.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

namespace chronos::cluster {
namespace {

struct SocketPair {
  std::array<int, 2U> sockets{-1, -1};
  ~SocketPair() {
    for (const int socket : sockets)
      if (socket >= 0)
        ::close(socket);
  }
};

[[nodiscard]] SocketPair sockets() {
  SocketPair pair;
  EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, pair.sockets.data()), 0);
  for (const int descriptor : pair.sockets) {
    const int flags = ::fcntl(descriptor, F_GETFL, 0);
    EXPECT_GE(flags, 0);
    EXPECT_EQ(::fcntl(descriptor, F_SETFL, flags | O_NONBLOCK), 0);
  }
  return pair;
}

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

[[nodiscard]] DistributedVectorGroupedAggregateShuffleJobPrepare prepare() {
  const auto string = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const auto count = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  return {.coordinator_node_id = 9U,
          .target_node_id = 3U,
          .coordinator_result_endpoint = {{127U, 0U, 0U, 1U}, 8137U},
          .execution_timeout = std::chrono::milliseconds{30'000},
          .authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                           uuid(1U), {{schema::TabletId::from_uuid(uuid(2U)).value(), 3U}},
                           {{0U, 3U}}, {{0U, string, false}},
                           {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
                           .value(),
          .result_schema = {.columns = {{"region", string, false}, {"count", count, false}}}};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  explicit Authenticator(const std::uint64_t principal) : principal_(principal) {}

  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest& request) override {
    saw_fingerprint = request.peer_certificate_sha256.has_value();
    return network::PeerAuthenticationResult{.authorized = allow,
                                             .principal_id = allow ? principal_ : 0U};
  }

  bool allow{true};
  bool saw_fingerprint{};

private:
  std::uint64_t principal_{};
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return (principal == 93U && node == 9U) || (principal == 94U && node == 3U);
  }
};

[[nodiscard]] DistributedVectorGroupedAggregateShuffleJobControlTlsLimits limits() {
  return {.handshake_timeout = std::chrono::milliseconds{100},
          .exchange_timeout = std::chrono::milliseconds{100}};
}

struct ExchangeAuthentication {
  Authenticator* client{};
  Authenticator* server{};
};

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponse>
exchange(DistributedVectorGroupedAggregateShuffleJobControlRequest request,
         DistributedVectorGroupedAggregateShuffleJobService& service,
         const ExchangeAuthentication authentication, Authorizer& authorizer,
         network::TlsServerContext& server_context, network::TlsClientContext& client_context) {
  SocketPair pair = sockets();
  auto server_socket = network::TlsSocket::accept(server_context, pair.sockets[0]).value();
  auto client_socket = network::TlsSocket::connect(client_context, pair.sockets[1]).value();
  const auto start = DistributedVectorGroupedAggregateShuffleJobControlTlsClient::TimePoint{};
  auto server = DistributedVectorGroupedAggregateShuffleJobControlTlsServer::create(
                    std::move(server_socket),
                    {.authenticator = authentication.client,
                     .service = &service,
                     .peer_ipv4_address = {127U, 0U, 0U, 1U},
                     .limits = limits()},
                    start)
                    .value();
  auto client = DistributedVectorGroupedAggregateShuffleJobControlTlsClient::create(
                    std::move(client_socket),
                    {.authenticator = authentication.server,
                     .node_authorizer = &authorizer,
                     .peer_ipv4_address = {127U, 0U, 0U, 1U},
                     .request = std::move(request),
                     .limits = limits()},
                    start)
                    .value();
  for (std::size_t iteration = 0U; iteration < 1024U; ++iteration) {
    common::Status client_progress =
        client.on_ready(true, true, start + std::chrono::milliseconds{1});
    if (!client_progress.is_ok())
      return common::make_unexpected(client_progress);
    common::Status server_progress =
        server.on_ready(true, true, start + std::chrono::milliseconds{1});
    if (!server_progress.is_ok())
      return common::make_unexpected(server_progress);
    if (client.state() ==
            DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kComplete &&
        server.state() ==
            DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kComplete) {
      return client.result();
    }
  }
  return common::make_unexpected(
      common::Status{common::StatusCode::kUnavailable,
                     "grouped shuffle reducer-job TLS test exchange did not complete"});
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTlsTest,
     AuthenticatesAndCorrelatesPrepareAndSealAcrossDistinctConnections) {
  Authenticator client_authenticator{93U};
  Authenticator server_authenticator{94U};
  Authorizer authorizer;
  auto server_context = network::TlsServerContext::create(server_tls()).value();
  auto client_context = network::TlsClientContext::create(client_tls()).value();
  auto service = DistributedVectorGroupedAggregateShuffleJobService::create(
                     {.local_node_id = 3U,
                      .shuffle_tls = server_tls(),
                      .shuffle_authenticator = &client_authenticator,
                      .result_authenticator = &server_authenticator,
                      .node_authorizer = &authorizer,
                      .result_tls_context = &client_context})
                     .value();

  auto prepared =
      exchange(DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare()}, service,
               {.client = &client_authenticator, .server = &server_authenticator}, authorizer,
               server_context, client_context);
  ASSERT_TRUE(prepared.has_value()) << prepared.error().to_string();
  EXPECT_EQ(prepared->action, DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare);
  EXPECT_EQ(prepared->status_code, common::StatusCode::kOk);
  EXPECT_EQ(prepared->query_id, uuid(1U));
  EXPECT_FALSE(prepared->reducer_shuffle_endpoint.has_value());
  EXPECT_EQ(service.metrics().active_jobs, 1U);

  auto sealed = exchange(
      DistributedVectorGroupedAggregateShuffleJobControlRequest{
          DistributedVectorGroupedAggregateShuffleJobSeal{uuid(1U), 9U, 3U}},
      service, {.client = &client_authenticator, .server = &server_authenticator}, authorizer,
      server_context, client_context);
  ASSERT_TRUE(sealed.has_value()) << sealed.error().to_string();
  EXPECT_EQ(sealed->action, DistributedVectorGroupedAggregateShuffleJobControlAction::kSeal);
  EXPECT_EQ(sealed->status_code, common::StatusCode::kUnavailable);
  EXPECT_EQ(sealed->query_id, uuid(1U));
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTlsTest,
     RejectsClientPrincipalBeforeJobAdmissionAndFailsClosedAtDeadline) {
  Authenticator client_authenticator{93U};
  client_authenticator.allow = false;
  Authenticator server_authenticator{94U};
  Authorizer authorizer;
  auto server_context = network::TlsServerContext::create(server_tls()).value();
  auto client_context = network::TlsClientContext::create(client_tls()).value();
  auto service = DistributedVectorGroupedAggregateShuffleJobService::create(
                     {.local_node_id = 3U,
                      .shuffle_tls = server_tls(),
                      .shuffle_authenticator = &client_authenticator,
                      .result_authenticator = &server_authenticator,
                      .node_authorizer = &authorizer,
                      .result_tls_context = &client_context})
                     .value();
  auto denied =
      exchange(DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare()}, service,
               {.client = &client_authenticator, .server = &server_authenticator}, authorizer,
               server_context, client_context);
  ASSERT_FALSE(denied.has_value());
  EXPECT_EQ(denied.error().code(), common::StatusCode::kUnauthenticated);
  EXPECT_EQ(service.metrics().prepare_requests, 0U);

  auto config = DistributedVectorGroupedAggregateShuffleJobControlTlsClientConfig{
      .authenticator = &server_authenticator,
      .node_authorizer = &authorizer,
      .peer_ipv4_address = {127U, 0U, 0U, 1U},
      .request = DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare()},
      .limits = {.handshake_timeout = std::chrono::milliseconds{5},
                 .exchange_timeout = std::chrono::milliseconds{100}}};
  const auto start = DistributedVectorGroupedAggregateShuffleJobControlTlsClient::TimePoint{};
  auto client = DistributedVectorGroupedAggregateShuffleJobControlTlsClient::create(
                    network::TlsSocket{}, std::move(config), start)
                    .value();
  EXPECT_TRUE(client.on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const common::Status timeout =
      client.on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(timeout.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client.state(),
            DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kFailed);
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTlsTest,
     RejectsAValidButDifferentlyCorrelatedResponse) {
  Authenticator server_authenticator{94U};
  Authorizer authorizer;
  auto server_context = network::TlsServerContext::create(server_tls()).value();
  auto client_context = network::TlsClientContext::create(client_tls()).value();
  SocketPair pair = sockets();
  auto server_socket = network::TlsSocket::accept(server_context, pair.sockets[0]).value();
  auto client_socket = network::TlsSocket::connect(client_context, pair.sockets[1]).value();
  const auto start = DistributedVectorGroupedAggregateShuffleJobControlTlsClient::TimePoint{};
  auto client =
      DistributedVectorGroupedAggregateShuffleJobControlTlsClient::create(
          std::move(client_socket),
          {.authenticator = &server_authenticator,
           .node_authorizer = &authorizer,
           .peer_ipv4_address = {127U, 0U, 0U, 1U},
           .request = DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare()},
           .limits = limits()},
          start)
          .value();
  auto wrong = encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1(
                   {.action = DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
                    .status_code = common::StatusCode::kOk,
                    .query_id = uuid(2U),
                    .coordinator_node_id = 9U,
                    .target_node_id = 3U})
                   .value();
  bool server_handshake_complete{};
  std::size_t response_bytes{};
  common::Status client_progress = common::Status::ok();
  for (std::size_t iteration = 0U; iteration < 1024U && client_progress.is_ok(); ++iteration) {
    client_progress = client.on_ready(true, true, start + std::chrono::milliseconds{1});
    if (!server_handshake_complete) {
      auto progress = server_socket.handshake();
      ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
      server_handshake_complete = progress->state == network::TlsIoState::kComplete;
    } else if (response_bytes != wrong.bytes().size()) {
      auto progress = server_socket.write(wrong.bytes().subspan(response_bytes));
      ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
      if (progress->state == network::TlsIoState::kComplete)
        response_bytes += progress->bytes_transferred;
    }
  }
  EXPECT_EQ(client_progress.code(), common::StatusCode::kCorruption);
  EXPECT_EQ(client.state(),
            DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kFailed);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
}

} // namespace
} // namespace chronos::cluster
