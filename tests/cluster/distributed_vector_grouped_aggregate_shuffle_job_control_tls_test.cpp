#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_tls.hpp"
#include "chronos/network/tcp_socket.hpp"

#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <poll.h>
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

struct TcpPair {
  network::TcpSocket server;
  network::TcpSocket client;
};

[[nodiscard]] common::Result<TcpPair> connect_tcp_pair(network::TcpListener& listener) {
  auto client = network::TcpSocket::begin_connect(listener.bound_endpoint());
  if (!client.has_value())
    return common::make_unexpected(client.error());
  std::optional<network::TcpSocket> server;
  for (std::size_t attempt = 0U; attempt < 64U; ++attempt) {
    std::array<pollfd, 2U> descriptors{{
        {.fd = listener.descriptor(), .events = POLLIN, .revents = 0},
        {.fd = client->descriptor(), .events = POLLOUT, .revents = 0},
    }};
    const int ready = ::poll(descriptors.data(), descriptors.size(), 100);
    if (ready < 0 && errno != EINTR) {
      return common::make_unexpected(common::Status{common::StatusCode::kUnavailable,
                                                    "loopback Job Control connection poll failed"});
    }
    if (client->connect_state() == network::TcpConnectState::kInProgress &&
        (descriptors[1].revents & (POLLOUT | POLLERR | POLLHUP)) != 0) {
      auto connected = client->finish_connect();
      if (!connected.has_value())
        return common::make_unexpected(connected.error());
    }
    if (!server.has_value() && (descriptors[0].revents & POLLIN) != 0) {
      auto accepted = listener.accept_one();
      if (!accepted.has_value())
        return common::make_unexpected(accepted.error());
      if (accepted->has_value())
        server.emplace(std::move(**accepted));
    }
    if (server.has_value() && client->connect_state() == network::TcpConnectState::kConnected)
      return TcpPair{.server = std::move(*server), .client = std::move(*client)};
  }
  return common::make_unexpected(common::Status{
      common::StatusCode::kUnavailable, "loopback Job Control connection did not complete"});
}

[[nodiscard]] common::Status reset_tcp_connection(network::TlsSocket& tls,
                                                  network::TcpSocket& tcp) {
  const linger reset{.l_onoff = 1, .l_linger = 0};
  if (::setsockopt(tcp.descriptor(), SOL_SOCKET, SO_LINGER, &reset, sizeof(reset)) != 0) {
    return {common::StatusCode::kIoError, "configuring test TCP reset failed"};
  }
  tls = network::TlsSocket{};
  return tcp.close();
}

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

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponse>
exchange_tcp(network::TcpListener& listener,
             DistributedVectorGroupedAggregateShuffleJobControlRequest request,
             DistributedVectorGroupedAggregateShuffleJobService& service,
             const ExchangeAuthentication authentication, Authorizer& authorizer,
             network::TlsServerContext& server_context, network::TlsClientContext& client_context) {
  auto connected = connect_tcp_pair(listener);
  if (!connected.has_value())
    return common::make_unexpected(connected.error());
  TcpPair pair = std::move(*connected);
  auto server_socket = network::TlsSocket::accept(server_context, pair.server.descriptor());
  if (!server_socket.has_value())
    return common::make_unexpected(server_socket.error());
  auto client_socket = network::TlsSocket::connect(client_context, pair.client.descriptor());
  if (!client_socket.has_value())
    return common::make_unexpected(client_socket.error());
  const auto start = DistributedVectorGroupedAggregateShuffleJobControlTlsClient::TimePoint{};
  auto server = DistributedVectorGroupedAggregateShuffleJobControlTlsServer::create(
      std::move(*server_socket),
      {.authenticator = authentication.client,
       .service = &service,
       .peer_ipv4_address = {127U, 0U, 0U, 1U},
       .limits = limits()},
      start);
  if (!server.has_value())
    return common::make_unexpected(server.error());
  auto client = DistributedVectorGroupedAggregateShuffleJobControlTlsClient::create(
      std::move(*client_socket),
      {.authenticator = authentication.server,
       .node_authorizer = &authorizer,
       .peer_ipv4_address = {127U, 0U, 0U, 1U},
       .request = std::move(request),
       .limits = limits()},
      start);
  if (!client.has_value())
    return common::make_unexpected(client.error());
  for (std::size_t iteration = 0U; iteration < 1024U; ++iteration) {
    const common::Status client_progress =
        client->on_ready(true, true, start + std::chrono::milliseconds{1});
    if (!client_progress.is_ok())
      return common::make_unexpected(client_progress);
    const common::Status server_progress =
        server->on_ready(true, true, start + std::chrono::milliseconds{1});
    if (!server_progress.is_ok())
      return common::make_unexpected(server_progress);
    if (client->state() ==
            DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kComplete &&
        server->state() ==
            DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kComplete) {
      return client->result();
    }
  }
  return common::make_unexpected(
      common::Status{common::StatusCode::kUnavailable,
                     "grouped shuffle reducer-job real-TCP test exchange did not complete"});
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTlsTest,
     AuthenticatesAndCorrelatesPrepareAndSealAcrossDistinctConnections) {
  Authenticator client_authenticator{93U};
  Authenticator server_authenticator{94U};
  Authorizer authorizer;
  auto server_context = network::TlsServerContext::create(server_tls()).value();
  auto client_context = network::TlsClientContext::create(client_tls()).value();
  const std::array contexts{DistributedQueryNodeTlsContext{9U, &client_context}};
  auto service = DistributedVectorGroupedAggregateShuffleJobService::create(
                     {.local_node_id = 3U,
                      .shuffle_tls = server_tls(),
                      .shuffle_authenticator = &client_authenticator,
                      .result_authenticator = &server_authenticator,
                      .node_authorizer = &authorizer,
                      .result_tls_contexts = contexts})
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
  const std::array contexts{DistributedQueryNodeTlsContext{9U, &client_context}};
  auto service = DistributedVectorGroupedAggregateShuffleJobService::create(
                     {.local_node_id = 3U,
                      .shuffle_tls = server_tls(),
                      .shuffle_authenticator = &client_authenticator,
                      .result_authenticator = &server_authenticator,
                      .node_authorizer = &authorizer,
                      .result_tls_contexts = contexts})
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

TEST(DistributedVectorGroupedAggregateShuffleJobControlTlsTest,
     TimesOutAfterAnAuthenticatedCorrelatedPartialResponse) {
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
           .limits = {.handshake_timeout = std::chrono::milliseconds{100},
                      .exchange_timeout = std::chrono::milliseconds{20}}},
          start)
          .value();

  for (std::size_t iteration = 0U;
       iteration < 1024U &&
       client.state() !=
           DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kReadingResponse;
       ++iteration) {
    const common::Status progress =
        client.on_ready(true, true, start + std::chrono::milliseconds{1});
    ASSERT_TRUE(progress.is_ok()) << progress.to_string();
    if (!server_socket.handshake_complete()) {
      auto handshake = server_socket.handshake();
      ASSERT_TRUE(handshake.has_value()) << handshake.error().to_string();
      ASSERT_NE(handshake->state, network::TlsIoState::kClosed);
    }
  }
  ASSERT_EQ(client.state(),
            DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kReadingResponse);
  ASSERT_TRUE(server_socket.handshake_complete());
  EXPECT_TRUE(server_authenticator.saw_fingerprint);

  auto encoded = encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1(
      {.action = DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
       .status_code = common::StatusCode::kOk,
       .query_id = uuid(1U),
       .coordinator_node_id = 9U,
       .target_node_id = 3U});
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  constexpr std::size_t kPartialResponseBytes = 37U;
  static_assert(
      kPartialResponseBytes <
      distributed_vector_grouped_aggregate_shuffle_job_control_format::kResponseFrameLength);
  std::size_t written{};
  for (std::size_t iteration = 0U; iteration < 1024U && written != kPartialResponseBytes;
       ++iteration) {
    auto progress =
        server_socket.write(encoded->bytes().subspan(written, kPartialResponseBytes - written));
    ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
    ASSERT_NE(progress->state, network::TlsIoState::kClosed);
    if (progress->state == network::TlsIoState::kComplete) {
      ASSERT_GT(progress->bytes_transferred, 0U);
      written += progress->bytes_transferred;
    }
  }
  ASSERT_EQ(written, kPartialResponseBytes);

  ASSERT_TRUE(client.on_ready(true, false, start + std::chrono::milliseconds{2}).is_ok());
  EXPECT_EQ(client.state(),
            DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kReadingResponse);
  EXPECT_FALSE(client.result().has_value());
  const common::Status timeout = client.on_ready(false, false, client.deadline());
  EXPECT_EQ(timeout.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client.state(),
            DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kFailed);
  EXPECT_FALSE(client.result().has_value());
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlTlsTest,
     FailsClosedOnAuthenticatedPartialFrameTcpResetsAndReusesTheListener) {
  Authenticator client_authenticator{93U};
  Authenticator server_authenticator{94U};
  Authorizer authorizer;
  auto server_context = network::TlsServerContext::create(server_tls());
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(server_context.has_value()) << server_context.error().to_string();
  ASSERT_TRUE(client_context.has_value()) << client_context.error().to_string();
  const std::array result_contexts{DistributedQueryNodeTlsContext{9U, &*client_context}};
  auto service = DistributedVectorGroupedAggregateShuffleJobService::create(
      {.local_node_id = 3U,
       .shuffle_tls = server_tls(),
       .shuffle_authenticator = &client_authenticator,
       .result_authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .result_tls_contexts = result_contexts});
  ASSERT_TRUE(service.has_value()) << service.error().to_string();
  auto listener = network::TcpListener::bind();
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  const auto start = DistributedVectorGroupedAggregateShuffleJobControlTlsClient::TimePoint{};

  auto encoded_request =
      encode_distributed_vector_grouped_aggregate_shuffle_job_prepare_v1(prepare());
  ASSERT_TRUE(encoded_request.has_value()) << encoded_request.error().to_string();
  constexpr std::array<std::size_t, 3U> kRequestPrefixes{
      3U, 25U, distributed_vector_grouped_aggregate_shuffle_job_control_format::kHeaderLength + 1U};
  static_assert(
      kRequestPrefixes.back() <
      distributed_vector_grouped_aggregate_shuffle_job_control_format::kMaximumFrameLength);
  for (const std::size_t prefix_bytes : kRequestPrefixes) {
    SCOPED_TRACE(prefix_bytes);
    ASSERT_LT(prefix_bytes, encoded_request->bytes().size());
    auto connected = connect_tcp_pair(*listener);
    ASSERT_TRUE(connected.has_value()) << connected.error().to_string();
    TcpPair pair = std::move(*connected);
    auto accepted_tls = network::TlsSocket::accept(*server_context, pair.server.descriptor());
    auto connected_tls = network::TlsSocket::connect(*client_context, pair.client.descriptor());
    ASSERT_TRUE(accepted_tls.has_value()) << accepted_tls.error().to_string();
    ASSERT_TRUE(connected_tls.has_value()) << connected_tls.error().to_string();
    network::TlsSocket scripted_client = std::move(*connected_tls);
    auto server = DistributedVectorGroupedAggregateShuffleJobControlTlsServer::create(
        std::move(*accepted_tls),
        {.authenticator = &client_authenticator,
         .service = &*service,
         .peer_ipv4_address = {127U, 0U, 0U, 1U},
         .limits = limits()},
        start);
    ASSERT_TRUE(server.has_value()) << server.error().to_string();

    for (std::size_t iteration = 0U;
         iteration < 1024U &&
         (!scripted_client.handshake_complete() ||
          server->state() ==
              DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kHandshaking);
         ++iteration) {
      if (!scripted_client.handshake_complete()) {
        auto progress = scripted_client.handshake();
        ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
        ASSERT_NE(progress->state, network::TlsIoState::kClosed);
      }
      if (server->state() ==
          DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kHandshaking) {
        const common::Status progress =
            server->on_ready(true, true, start + std::chrono::milliseconds{1});
        ASSERT_TRUE(progress.is_ok()) << progress.to_string();
      }
    }
    ASSERT_TRUE(scripted_client.handshake_complete());
    ASSERT_EQ(server->state(),
              DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kReadingRequest);

    std::size_t written{};
    for (std::size_t iteration = 0U; iteration < 1024U && written != prefix_bytes; ++iteration) {
      auto progress =
          scripted_client.write(encoded_request->bytes().subspan(written, prefix_bytes - written));
      ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
      ASSERT_NE(progress->state, network::TlsIoState::kClosed);
      if (progress->state == network::TlsIoState::kComplete) {
        ASSERT_GT(progress->bytes_transferred, 0U);
        written += progress->bytes_transferred;
      }
      const common::Status received =
          server->on_ready(true, true, start + std::chrono::milliseconds{2});
      ASSERT_TRUE(received.is_ok()) << received.to_string();
    }
    ASSERT_EQ(written, prefix_bytes);
    for (std::size_t drain = 0U; drain < 16U; ++drain) {
      const common::Status received =
          server->on_ready(true, true, start + std::chrono::milliseconds{2});
      ASSERT_TRUE(received.is_ok()) << received.to_string();
    }
    ASSERT_EQ(server->state(),
              DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kReadingRequest);
    ASSERT_EQ(service->metrics().prepare_requests, 0U);
    ASSERT_TRUE(reset_tcp_connection(scripted_client, pair.client).is_ok());

    common::Status reset_failure = common::Status::ok();
    for (std::size_t iteration = 0U; iteration < 1024U && reset_failure.is_ok(); ++iteration) {
      reset_failure = server->on_ready(true, true, start + std::chrono::milliseconds{3});
    }
    EXPECT_FALSE(reset_failure.is_ok());
    EXPECT_TRUE(reset_failure.code() == common::StatusCode::kIoError ||
                reset_failure.code() == common::StatusCode::kUnavailable)
        << reset_failure.to_string();
    EXPECT_EQ(server->state(),
              DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kFailed);
    EXPECT_EQ(service->metrics().prepare_requests, 0U);
    EXPECT_EQ(service->metrics().active_jobs, 0U);
  }

  auto connected = connect_tcp_pair(*listener);
  ASSERT_TRUE(connected.has_value()) << connected.error().to_string();
  TcpPair pair = std::move(*connected);
  auto scripted_server_socket =
      network::TlsSocket::accept(*server_context, pair.server.descriptor());
  auto client_socket = network::TlsSocket::connect(*client_context, pair.client.descriptor());
  ASSERT_TRUE(scripted_server_socket.has_value()) << scripted_server_socket.error().to_string();
  ASSERT_TRUE(client_socket.has_value()) << client_socket.error().to_string();
  network::TlsSocket scripted_server = std::move(*scripted_server_socket);
  auto client = DistributedVectorGroupedAggregateShuffleJobControlTlsClient::create(
      std::move(*client_socket),
      {.authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .peer_ipv4_address = {127U, 0U, 0U, 1U},
       .request = DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare()},
       .limits = limits()},
      start);
  ASSERT_TRUE(client.has_value()) << client.error().to_string();
  for (std::size_t iteration = 0U;
       iteration < 1024U &&
       (!scripted_server.handshake_complete() ||
        client->state() ==
            DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kHandshaking);
       ++iteration) {
    if (client->state() ==
        DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kHandshaking) {
      const common::Status progress =
          client->on_ready(true, true, start + std::chrono::milliseconds{1});
      ASSERT_TRUE(progress.is_ok()) << progress.to_string();
    }
    if (!scripted_server.handshake_complete()) {
      auto progress = scripted_server.handshake();
      ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
      ASSERT_NE(progress->state, network::TlsIoState::kClosed);
    }
  }
  ASSERT_TRUE(scripted_server.handshake_complete());
  ASSERT_EQ(client->state(),
            DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kWritingRequest);
  for (std::size_t iteration = 0U;
       iteration < 1024U &&
       client->state() !=
           DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kReadingResponse;
       ++iteration) {
    const common::Status progress =
        client->on_ready(true, true, start + std::chrono::milliseconds{2});
    ASSERT_TRUE(progress.is_ok()) << progress.to_string();
  }
  ASSERT_EQ(client->state(),
            DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kReadingResponse);

  auto encoded_response =
      encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1(
          {.action = DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
           .status_code = common::StatusCode::kOk,
           .query_id = uuid(1U),
           .coordinator_node_id = 9U,
           .target_node_id = 3U});
  ASSERT_TRUE(encoded_response.has_value()) << encoded_response.error().to_string();
  constexpr std::size_t kResponsePrefixBytes = 37U;
  static_assert(
      kResponsePrefixBytes <
      distributed_vector_grouped_aggregate_shuffle_job_control_format::kResponseFrameLength);
  std::size_t response_written{};
  for (std::size_t iteration = 0U; iteration < 1024U && response_written != kResponsePrefixBytes;
       ++iteration) {
    auto progress = scripted_server.write(encoded_response->bytes().subspan(
        response_written, kResponsePrefixBytes - response_written));
    ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
    ASSERT_NE(progress->state, network::TlsIoState::kClosed);
    if (progress->state == network::TlsIoState::kComplete) {
      ASSERT_GT(progress->bytes_transferred, 0U);
      response_written += progress->bytes_transferred;
    }
    const common::Status received =
        client->on_ready(true, true, start + std::chrono::milliseconds{3});
    ASSERT_TRUE(received.is_ok()) << received.to_string();
  }
  ASSERT_EQ(response_written, kResponsePrefixBytes);
  ASSERT_EQ(client->state(),
            DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kReadingResponse);
  ASSERT_FALSE(client->result().has_value());
  ASSERT_TRUE(reset_tcp_connection(scripted_server, pair.server).is_ok());

  common::Status reset_failure = common::Status::ok();
  for (std::size_t iteration = 0U; iteration < 1024U && reset_failure.is_ok(); ++iteration) {
    reset_failure = client->on_ready(true, true, start + std::chrono::milliseconds{4});
  }
  EXPECT_FALSE(reset_failure.is_ok());
  EXPECT_TRUE(reset_failure.code() == common::StatusCode::kIoError ||
              reset_failure.code() == common::StatusCode::kUnavailable)
      << reset_failure.to_string();
  EXPECT_EQ(client->state(),
            DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kFailed);
  EXPECT_FALSE(client->result().has_value());
  EXPECT_EQ(service->metrics().prepare_requests, 0U);

  auto prepared =
      exchange_tcp(*listener, DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare()},
                   *service, {.client = &client_authenticator, .server = &server_authenticator},
                   authorizer, *server_context, *client_context);
  ASSERT_TRUE(prepared.has_value()) << prepared.error().to_string();
  EXPECT_EQ(prepared->status_code, common::StatusCode::kOk);
  EXPECT_EQ(service->metrics().prepare_requests, 1U);
  EXPECT_EQ(service->metrics().active_jobs, 1U);

  auto cancelled =
      exchange_tcp(*listener,
                   DistributedVectorGroupedAggregateShuffleJobControlRequest{
                       DistributedVectorGroupedAggregateShuffleJobCancel{uuid(1U), 9U, 3U}},
                   *service, {.client = &client_authenticator, .server = &server_authenticator},
                   authorizer, *server_context, *client_context);
  ASSERT_TRUE(cancelled.has_value()) << cancelled.error().to_string();
  EXPECT_EQ(cancelled->status_code, common::StatusCode::kOk);
  EXPECT_EQ(service->metrics().cancel_requests, 1U);
  EXPECT_EQ(service->metrics().cancelled_jobs, 1U);
  EXPECT_EQ(service->metrics().active_jobs, 1U);
  const common::Status reaped =
      service->poll_once(std::chrono::milliseconds{0}, start + std::chrono::milliseconds{30'002});
  ASSERT_TRUE(reaped.is_ok()) << reaped.to_string();
  EXPECT_EQ(service->metrics().active_jobs, 0U);
}

} // namespace
} // namespace chronos::cluster
