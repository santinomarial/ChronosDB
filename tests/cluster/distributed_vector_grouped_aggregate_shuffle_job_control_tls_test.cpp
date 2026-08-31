#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_tls.hpp"
#include "chronos/network/tcp_socket.hpp"

#include <algorithm>
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
#include <span>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

struct SocketPair {
  std::array<int, 2U> sockets{-1, -1};
  // The raw AF_UNIX descriptors are intentionally owned and closed by this fixture.
  // NOLINTNEXTLINE(modernize-use-equals-default)
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

[[nodiscard]] common::Status reset_tcp_owner(network::TcpSocket& tcp) {
  const linger reset{.l_onoff = 1, .l_linger = 0};
  if (::setsockopt(tcp.descriptor(), SOL_SOCKET, SO_LINGER, &reset, sizeof(reset)) != 0) {
    return {common::StatusCode::kIoError, "configuring test TCP reset failed"};
  }
  return tcp.close();
}

[[nodiscard]] common::Status reset_tcp_connection(network::TlsSocket& tls,
                                                  network::TcpSocket& tcp) {
  tls = network::TlsSocket{};
  return reset_tcp_owner(tcp);
}

[[nodiscard]] bool wait_for_tcp_failure(const int descriptor) {
  for (std::size_t attempt = 0U; attempt < 64U; ++attempt) {
    pollfd socket{.fd = descriptor, .events = POLLIN, .revents = 0};
    const int ready = ::poll(&socket, 1U, 100);
    if (ready > 0 && (socket.revents & (POLLIN | POLLERR | POLLHUP)) != 0)
      return true;
    if (ready < 0 && errno != EINTR)
      return false;
  }
  return false;
}

[[nodiscard]] common::Status wait_for_raw_tcp(const int descriptor, const short events) {
  for (std::size_t attempt = 0U; attempt < 64U; ++attempt) {
    pollfd socket{.fd = descriptor, .events = events, .revents = 0};
    const int ready = ::poll(&socket, 1U, 100);
    if (ready > 0 && (socket.revents & (events | POLLERR | POLLHUP)) != 0)
      return common::Status::ok();
    if (ready < 0 && errno != EINTR) {
      return {common::StatusCode::kIoError, "polling the test TLS-record proxy failed"};
    }
  }
  return {common::StatusCode::kUnavailable, "the test TLS-record proxy timed out"};
}

[[nodiscard]] bool raw_tcp_would_block(const int error) noexcept {
  if (error == EAGAIN)
    return true;
#if EWOULDBLOCK != EAGAIN
  return error == EWOULDBLOCK;
#else
  return false;
#endif
}

[[nodiscard]] common::Status send_raw_tcp(network::TcpSocket& destination,
                                          const std::span<const std::byte> bytes) {
  std::size_t offset{};
  while (offset != bytes.size()) {
    const ssize_t sent = ::send(destination.descriptor(), bytes.data() + offset,
                                bytes.size() - offset, MSG_NOSIGNAL);
    if (sent > 0) {
      offset += static_cast<std::size_t>(sent);
      continue;
    }
    if (sent < 0 && errno == EINTR)
      continue;
    if (sent < 0 && raw_tcp_would_block(errno)) {
      const common::Status ready = wait_for_raw_tcp(destination.descriptor(), POLLOUT);
      if (!ready.is_ok())
        return ready;
      continue;
    }
    return {common::StatusCode::kIoError, "sending through the test TLS-record proxy failed"};
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status receive_raw_tcp(network::TcpSocket& source,
                                             const std::span<std::byte> bytes) {
  std::size_t offset{};
  while (offset != bytes.size()) {
    const ssize_t received =
        ::recv(source.descriptor(), bytes.data() + offset, bytes.size() - offset, 0);
    if (received > 0) {
      offset += static_cast<std::size_t>(received);
      continue;
    }
    if (received == 0) {
      return {common::StatusCode::kUnavailable, "the test TLS-record proxy source closed early"};
    }
    if (errno == EINTR)
      continue;
    if (raw_tcp_would_block(errno)) {
      const common::Status ready = wait_for_raw_tcp(source.descriptor(), POLLIN);
      if (!ready.is_ok())
        return ready;
      continue;
    }
    return {common::StatusCode::kIoError, "receiving through the test TLS-record proxy failed"};
  }
  return common::Status::ok();
}

struct RawTcpDirection {
  network::TcpSocket& source;
  network::TcpSocket& destination;
};

[[nodiscard]] common::Status forward_raw_tcp(const RawTcpDirection direction) {
  std::array<std::byte, std::size_t{16U} * 1024U> scratch{};
  while (true) {
    const ssize_t received =
        ::recv(direction.source.descriptor(), scratch.data(), scratch.size(), 0);
    if (received > 0) {
      const auto size = static_cast<std::size_t>(received);
      const common::Status sent =
          send_raw_tcp(direction.destination, std::span{scratch}.first(size));
      if (!sent.is_ok())
        return sent;
      continue;
    }
    if (received == 0) {
      return {common::StatusCode::kUnavailable, "the test TLS-record proxy source closed"};
    }
    if (errno == EINTR)
      continue;
    if (raw_tcp_would_block(errno))
      return common::Status::ok();
    return {common::StatusCode::kIoError, "forwarding through the test TLS-record proxy failed"};
  }
}

[[nodiscard]] common::Result<std::vector<std::byte>>
receive_tls_record(network::TcpSocket& source) {
  constexpr std::size_t kTlsRecordHeaderBytes = 5U;
  std::array<std::byte, kTlsRecordHeaderBytes> header{};
  const common::Status received_header = receive_raw_tcp(source, header);
  if (!received_header.is_ok())
    return common::make_unexpected(received_header);
  const std::size_t payload_bytes =
      (static_cast<std::size_t>(std::to_integer<std::uint8_t>(header[3])) << 8U) |
      static_cast<std::size_t>(std::to_integer<std::uint8_t>(header[4]));
  constexpr std::size_t kMaximumTestTlsRecordPayloadBytes = std::size_t{18U} * 1024U;
  if (payload_bytes == 0U || payload_bytes > kMaximumTestTlsRecordPayloadBytes) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kCorruption, "the test TLS-record proxy received an invalid record"});
  }
  std::vector<std::byte> record(kTlsRecordHeaderBytes + payload_bytes);
  std::ranges::copy(header, record.begin());
  const common::Status received_payload =
      receive_raw_tcp(source, std::span{record}.subspan(kTlsRecordHeaderBytes));
  if (!received_payload.is_ok())
    return common::make_unexpected(received_payload);
  return record;
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
  // This virtual authorization boundary cannot be static even though this fixture has no fields.
  // NOLINTNEXTLINE(readability-convert-member-functions-to-static,bugprone-easily-swappable-parameters)
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return common::Result<bool>{(principal == 93U && node == 9U) ||
                                (principal == 94U && node == 3U)};
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

TEST(DistributedVectorGroupedAggregateShuffleJobControlTlsTest,
     RetainsOneIdempotentPrepareAfterResetBeforeFirstResponseWrite) {
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

  auto connected = connect_tcp_pair(*listener);
  ASSERT_TRUE(connected.has_value()) << connected.error().to_string();
  TcpPair pair = std::move(*connected);
  auto server_socket = network::TlsSocket::accept(*server_context, pair.server.descriptor());
  auto client_socket = network::TlsSocket::connect(*client_context, pair.client.descriptor());
  ASSERT_TRUE(server_socket.has_value()) << server_socket.error().to_string();
  ASSERT_TRUE(client_socket.has_value()) << client_socket.error().to_string();
  auto server = DistributedVectorGroupedAggregateShuffleJobControlTlsServer::create(
      std::move(*server_socket),
      {.authenticator = &client_authenticator,
       .service = &*service,
       .peer_ipv4_address = {127U, 0U, 0U, 1U},
       .limits = limits()},
      start);
  ASSERT_TRUE(server.has_value()) << server.error().to_string();

  {
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
         (client->state() !=
              DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kReadingResponse ||
          server->state() !=
              DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kWritingResponse);
         ++iteration) {
      const common::Status client_progress =
          client->on_ready(true, true, start + std::chrono::milliseconds{1});
      ASSERT_TRUE(client_progress.is_ok()) << client_progress.to_string();
      if (server->state() !=
          DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kWritingResponse) {
        const common::Status server_progress =
            server->on_ready(true, true, start + std::chrono::milliseconds{1});
        ASSERT_TRUE(server_progress.is_ok()) << server_progress.to_string();
      }
    }
    ASSERT_EQ(client->state(),
              DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kReadingResponse);
    ASSERT_EQ(server->state(),
              DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kWritingResponse);
    EXPECT_FALSE(client->result().has_value());
    EXPECT_EQ(service->metrics().prepare_requests, 1U);
    EXPECT_EQ(service->metrics().duplicate_prepares, 0U);
    EXPECT_EQ(service->metrics().active_jobs, 1U);
  }

  ASSERT_TRUE(reset_tcp_owner(pair.client).is_ok());
  ASSERT_TRUE(wait_for_tcp_failure(pair.server.descriptor()));
  common::Status reset_failure = common::Status::ok();
  for (std::size_t iteration = 0U; iteration < 1024U && reset_failure.is_ok(); ++iteration) {
    reset_failure = server->on_ready(true, true, start + std::chrono::milliseconds{2});
  }
  EXPECT_FALSE(reset_failure.is_ok());
  EXPECT_TRUE(reset_failure.code() == common::StatusCode::kIoError ||
              reset_failure.code() == common::StatusCode::kUnavailable)
      << reset_failure.to_string();
  EXPECT_EQ(server->state(),
            DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kFailed);
  EXPECT_EQ(service->metrics().prepare_requests, 1U);
  EXPECT_EQ(service->metrics().duplicate_prepares, 0U);
  EXPECT_EQ(service->metrics().active_jobs, 1U);

  auto prepared =
      exchange_tcp(*listener, DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare()},
                   *service, {.client = &client_authenticator, .server = &server_authenticator},
                   authorizer, *server_context, *client_context);
  ASSERT_TRUE(prepared.has_value()) << prepared.error().to_string();
  EXPECT_EQ(prepared->status_code, common::StatusCode::kOk);
  EXPECT_EQ(service->metrics().prepare_requests, 2U);
  EXPECT_EQ(service->metrics().duplicate_prepares, 1U);
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

TEST(DistributedVectorGroupedAggregateShuffleJobControlTlsTest,
     FailsClosedOnControlledPartialEncryptedRequestAndResponseRecords) {
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
  auto server_listener = network::TcpListener::bind();
  ASSERT_TRUE(server_listener.has_value()) << server_listener.error().to_string();
  const auto start = DistributedVectorGroupedAggregateShuffleJobControlTlsClient::TimePoint{};

  constexpr std::size_t kTlsRecordHeaderBytes = 5U;
  constexpr std::array<std::size_t, 3U> kCutKinds{0U, 1U, 2U};
  for (const std::size_t cut_kind : kCutKinds) {
    SCOPED_TRACE(cut_kind);
    auto proxy_listener = network::TcpListener::bind();
    ASSERT_TRUE(proxy_listener.has_value()) << proxy_listener.error().to_string();
    auto client_leg_result = connect_tcp_pair(*proxy_listener);
    auto server_leg_result = connect_tcp_pair(*server_listener);
    ASSERT_TRUE(client_leg_result.has_value()) << client_leg_result.error().to_string();
    ASSERT_TRUE(server_leg_result.has_value()) << server_leg_result.error().to_string();
    TcpPair client_leg = std::move(*client_leg_result);
    TcpPair server_leg = std::move(*server_leg_result);

    auto accepted_tls = network::TlsSocket::accept(*server_context, server_leg.server.descriptor());
    auto connected_tls =
        network::TlsSocket::connect(*client_context, client_leg.client.descriptor());
    ASSERT_TRUE(accepted_tls.has_value()) << accepted_tls.error().to_string();
    ASSERT_TRUE(connected_tls.has_value()) << connected_tls.error().to_string();
    auto server = DistributedVectorGroupedAggregateShuffleJobControlTlsServer::create(
        std::move(*accepted_tls),
        {.authenticator = &client_authenticator,
         .service = &*service,
         .peer_ipv4_address = {127U, 0U, 0U, 1U},
         .limits = limits()},
        start);
    auto client = DistributedVectorGroupedAggregateShuffleJobControlTlsClient::create(
        std::move(*connected_tls),
        {.authenticator = &server_authenticator,
         .node_authorizer = &authorizer,
         .peer_ipv4_address = {127U, 0U, 0U, 1U},
         .request = DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare()},
         .limits = limits()},
        start);
    ASSERT_TRUE(server.has_value()) << server.error().to_string();
    ASSERT_TRUE(client.has_value()) << client.error().to_string();

    for (std::size_t iteration = 0U;
         iteration < 1024U &&
         (client->state() !=
              DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kWritingRequest ||
          server->state() !=
              DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kReadingRequest);
         ++iteration) {
      if (client->state() ==
          DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kHandshaking) {
        const common::Status progress =
            client->on_ready(true, true, start + std::chrono::milliseconds{1});
        ASSERT_TRUE(progress.is_ok()) << progress.to_string();
      }
      if (server->state() ==
          DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kHandshaking) {
        const common::Status progress =
            server->on_ready(true, true, start + std::chrono::milliseconds{1});
        ASSERT_TRUE(progress.is_ok()) << progress.to_string();
      }
      const common::Status client_to_server =
          forward_raw_tcp({.source = client_leg.server, .destination = server_leg.client});
      const common::Status server_to_client =
          forward_raw_tcp({.source = server_leg.client, .destination = client_leg.server});
      ASSERT_TRUE(client_to_server.is_ok()) << client_to_server.to_string();
      ASSERT_TRUE(server_to_client.is_ok()) << server_to_client.to_string();
    }
    ASSERT_EQ(client->state(),
              DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kWritingRequest);
    ASSERT_EQ(server->state(),
              DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kReadingRequest);
    EXPECT_TRUE(client_authenticator.saw_fingerprint);
    EXPECT_TRUE(server_authenticator.saw_fingerprint);

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
    ASSERT_FALSE(client->result().has_value());

    auto encrypted_record = receive_tls_record(client_leg.server);
    ASSERT_TRUE(encrypted_record.has_value()) << encrypted_record.error().to_string();
    ASSERT_GT(encrypted_record->size(), kTlsRecordHeaderBytes + 1U);
    EXPECT_EQ(std::to_integer<std::uint8_t>((*encrypted_record)[0]), 23U);
    EXPECT_EQ(std::to_integer<std::uint8_t>((*encrypted_record)[1]), 3U);
    EXPECT_EQ(std::to_integer<std::uint8_t>((*encrypted_record)[2]), 3U);
    const std::size_t prefix_bytes = cut_kind == 0U   ? 2U
                                     : cut_kind == 1U ? kTlsRecordHeaderBytes + 1U
                                                      : encrypted_record->size() - 1U;
    SCOPED_TRACE(prefix_bytes);
    ASSERT_LT(prefix_bytes, encrypted_record->size());
    const common::Status forwarded =
        send_raw_tcp(server_leg.client, std::span{*encrypted_record}.first(prefix_bytes));
    ASSERT_TRUE(forwarded.is_ok()) << forwarded.to_string();
    ASSERT_TRUE(reset_tcp_owner(server_leg.client).is_ok());
    ASSERT_TRUE(wait_for_tcp_failure(server_leg.server.descriptor()));

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

  for (const std::size_t cut_kind : kCutKinds) {
    SCOPED_TRACE(cut_kind);
    auto proxy_listener = network::TcpListener::bind();
    ASSERT_TRUE(proxy_listener.has_value()) << proxy_listener.error().to_string();
    auto client_leg_result = connect_tcp_pair(*proxy_listener);
    auto server_leg_result = connect_tcp_pair(*server_listener);
    ASSERT_TRUE(client_leg_result.has_value()) << client_leg_result.error().to_string();
    ASSERT_TRUE(server_leg_result.has_value()) << server_leg_result.error().to_string();
    TcpPair client_leg = std::move(*client_leg_result);
    TcpPair server_leg = std::move(*server_leg_result);

    auto accepted_tls = network::TlsSocket::accept(*server_context, server_leg.server.descriptor());
    auto connected_tls =
        network::TlsSocket::connect(*client_context, client_leg.client.descriptor());
    ASSERT_TRUE(accepted_tls.has_value()) << accepted_tls.error().to_string();
    ASSERT_TRUE(connected_tls.has_value()) << connected_tls.error().to_string();
    auto server = DistributedVectorGroupedAggregateShuffleJobControlTlsServer::create(
        std::move(*accepted_tls),
        {.authenticator = &client_authenticator,
         .service = &*service,
         .peer_ipv4_address = {127U, 0U, 0U, 1U},
         .limits = limits()},
        start);
    auto client = DistributedVectorGroupedAggregateShuffleJobControlTlsClient::create(
        std::move(*connected_tls),
        {.authenticator = &server_authenticator,
         .node_authorizer = &authorizer,
         .peer_ipv4_address = {127U, 0U, 0U, 1U},
         .request = DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare()},
         .limits = limits()},
        start);
    ASSERT_TRUE(server.has_value()) << server.error().to_string();
    ASSERT_TRUE(client.has_value()) << client.error().to_string();

    for (std::size_t iteration = 0U;
         iteration < 1024U &&
         (client->state() !=
              DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kWritingRequest ||
          server->state() !=
              DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kReadingRequest);
         ++iteration) {
      if (client->state() ==
          DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kHandshaking) {
        const common::Status progress =
            client->on_ready(true, true, start + std::chrono::milliseconds{1});
        ASSERT_TRUE(progress.is_ok()) << progress.to_string();
      }
      if (server->state() ==
          DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kHandshaking) {
        const common::Status progress =
            server->on_ready(true, true, start + std::chrono::milliseconds{1});
        ASSERT_TRUE(progress.is_ok()) << progress.to_string();
      }
      const common::Status client_to_server =
          forward_raw_tcp({.source = client_leg.server, .destination = server_leg.client});
      const common::Status server_to_client =
          forward_raw_tcp({.source = server_leg.client, .destination = client_leg.server});
      ASSERT_TRUE(client_to_server.is_ok()) << client_to_server.to_string();
      ASSERT_TRUE(server_to_client.is_ok()) << server_to_client.to_string();
    }
    ASSERT_EQ(client->state(),
              DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kWritingRequest);
    ASSERT_EQ(server->state(),
              DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kReadingRequest);

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
    ASSERT_FALSE(client->result().has_value());
    auto encrypted_request = receive_tls_record(client_leg.server);
    ASSERT_TRUE(encrypted_request.has_value()) << encrypted_request.error().to_string();
    ASSERT_GT(encrypted_request->size(), kTlsRecordHeaderBytes + 1U);
    ASSERT_EQ(std::to_integer<std::uint8_t>((*encrypted_request)[0]), 23U);
    const common::Status request_forwarded = send_raw_tcp(server_leg.client, *encrypted_request);
    ASSERT_TRUE(request_forwarded.is_ok()) << request_forwarded.to_string();
    for (std::size_t iteration = 0U;
         iteration < 1024U &&
         server->state() !=
             DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kComplete;
         ++iteration) {
      const common::Status progress =
          server->on_ready(true, true, start + std::chrono::milliseconds{2});
      ASSERT_TRUE(progress.is_ok()) << progress.to_string();
    }
    ASSERT_EQ(server->state(),
              DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kComplete);
    EXPECT_EQ(service->metrics().prepare_requests, cut_kind + 1U);
    EXPECT_EQ(service->metrics().duplicate_prepares, cut_kind);
    EXPECT_EQ(service->metrics().active_jobs, 1U);

    auto encrypted_record = receive_tls_record(server_leg.client);
    ASSERT_TRUE(encrypted_record.has_value()) << encrypted_record.error().to_string();
    ASSERT_GT(encrypted_record->size(), kTlsRecordHeaderBytes + 1U);
    EXPECT_EQ(std::to_integer<std::uint8_t>((*encrypted_record)[0]), 23U);
    EXPECT_EQ(std::to_integer<std::uint8_t>((*encrypted_record)[1]), 3U);
    EXPECT_EQ(std::to_integer<std::uint8_t>((*encrypted_record)[2]), 3U);
    const std::size_t prefix_bytes = cut_kind == 0U   ? 2U
                                     : cut_kind == 1U ? kTlsRecordHeaderBytes + 1U
                                                      : encrypted_record->size() - 1U;
    SCOPED_TRACE(prefix_bytes);
    ASSERT_LT(prefix_bytes, encrypted_record->size());
    const common::Status forwarded =
        send_raw_tcp(client_leg.server, std::span{*encrypted_record}.first(prefix_bytes));
    ASSERT_TRUE(forwarded.is_ok()) << forwarded.to_string();
    ASSERT_TRUE(reset_tcp_owner(client_leg.server).is_ok());
    ASSERT_TRUE(wait_for_tcp_failure(client_leg.client.descriptor()));

    common::Status reset_failure = common::Status::ok();
    for (std::size_t iteration = 0U; iteration < 1024U && reset_failure.is_ok(); ++iteration) {
      reset_failure = client->on_ready(true, true, start + std::chrono::milliseconds{3});
    }
    EXPECT_FALSE(reset_failure.is_ok());
    EXPECT_TRUE(reset_failure.code() == common::StatusCode::kIoError ||
                reset_failure.code() == common::StatusCode::kUnavailable)
        << reset_failure.to_string();
    EXPECT_EQ(client->state(),
              DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kFailed);
    EXPECT_FALSE(client->result().has_value());
    EXPECT_EQ(service->metrics().active_jobs, 1U);
  }

  auto prepared = exchange_tcp(
      *server_listener, DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare()},
      *service, {.client = &client_authenticator, .server = &server_authenticator}, authorizer,
      *server_context, *client_context);
  ASSERT_TRUE(prepared.has_value()) << prepared.error().to_string();
  EXPECT_EQ(prepared->status_code, common::StatusCode::kOk);
  EXPECT_EQ(service->metrics().prepare_requests, 4U);
  EXPECT_EQ(service->metrics().duplicate_prepares, 3U);
  EXPECT_EQ(service->metrics().active_jobs, 1U);

  auto cancelled =
      exchange_tcp(*server_listener,
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

TEST(DistributedVectorGroupedAggregateShuffleJobControlTlsTest,
     FailsClosedOnControlledPartialClientAndServerHandshakeRecords) {
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
  auto server_listener = network::TcpListener::bind();
  ASSERT_TRUE(server_listener.has_value()) << server_listener.error().to_string();
  const auto start = DistributedVectorGroupedAggregateShuffleJobControlTlsClient::TimePoint{};

  constexpr std::size_t kTlsRecordHeaderBytes = 5U;
  constexpr std::array<std::size_t, 3U> kCutKinds{0U, 1U, 2U};
  for (const std::size_t cut_kind : kCutKinds) {
    SCOPED_TRACE(cut_kind);
    auto proxy_listener = network::TcpListener::bind();
    ASSERT_TRUE(proxy_listener.has_value()) << proxy_listener.error().to_string();
    auto client_leg_result = connect_tcp_pair(*proxy_listener);
    auto server_leg_result = connect_tcp_pair(*server_listener);
    ASSERT_TRUE(client_leg_result.has_value()) << client_leg_result.error().to_string();
    ASSERT_TRUE(server_leg_result.has_value()) << server_leg_result.error().to_string();
    TcpPair client_leg = std::move(*client_leg_result);
    TcpPair server_leg = std::move(*server_leg_result);
    auto accepted_tls = network::TlsSocket::accept(*server_context, server_leg.server.descriptor());
    auto connected_tls =
        network::TlsSocket::connect(*client_context, client_leg.client.descriptor());
    ASSERT_TRUE(accepted_tls.has_value()) << accepted_tls.error().to_string();
    ASSERT_TRUE(connected_tls.has_value()) << connected_tls.error().to_string();
    auto server = DistributedVectorGroupedAggregateShuffleJobControlTlsServer::create(
        std::move(*accepted_tls),
        {.authenticator = &client_authenticator,
         .service = &*service,
         .peer_ipv4_address = {127U, 0U, 0U, 1U},
         .limits = limits()},
        start);
    auto client = DistributedVectorGroupedAggregateShuffleJobControlTlsClient::create(
        std::move(*connected_tls),
        {.authenticator = &server_authenticator,
         .node_authorizer = &authorizer,
         .peer_ipv4_address = {127U, 0U, 0U, 1U},
         .request = DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare()},
         .limits = limits()},
        start);
    ASSERT_TRUE(server.has_value()) << server.error().to_string();
    ASSERT_TRUE(client.has_value()) << client.error().to_string();

    const common::Status client_progress =
        client->on_ready(true, true, start + std::chrono::milliseconds{1});
    ASSERT_TRUE(client_progress.is_ok()) << client_progress.to_string();
    ASSERT_EQ(client->state(),
              DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kHandshaking);
    auto client_hello = receive_tls_record(client_leg.server);
    ASSERT_TRUE(client_hello.has_value()) << client_hello.error().to_string();
    ASSERT_GT(client_hello->size(), kTlsRecordHeaderBytes + 1U);
    EXPECT_EQ(std::to_integer<std::uint8_t>((*client_hello)[0]), 22U);
    EXPECT_EQ(std::to_integer<std::uint8_t>((*client_hello)[1]), 3U);
    EXPECT_LE(std::to_integer<std::uint8_t>((*client_hello)[2]), 3U);
    const std::size_t prefix_bytes = cut_kind == 0U   ? 2U
                                     : cut_kind == 1U ? kTlsRecordHeaderBytes + 1U
                                                      : client_hello->size() - 1U;
    SCOPED_TRACE(prefix_bytes);
    const common::Status forwarded =
        send_raw_tcp(server_leg.client, std::span{*client_hello}.first(prefix_bytes));
    ASSERT_TRUE(forwarded.is_ok()) << forwarded.to_string();
    ASSERT_TRUE(reset_tcp_owner(server_leg.client).is_ok());
    ASSERT_TRUE(reset_tcp_owner(client_leg.server).is_ok());
    ASSERT_TRUE(wait_for_tcp_failure(server_leg.server.descriptor()));
    ASSERT_TRUE(wait_for_tcp_failure(client_leg.client.descriptor()));

    common::Status server_failure = common::Status::ok();
    common::Status client_failure = common::Status::ok();
    for (std::size_t iteration = 0U;
         iteration < 1024U && (server_failure.is_ok() || client_failure.is_ok()); ++iteration) {
      if (server_failure.is_ok()) {
        server_failure = server->on_ready(true, true, start + std::chrono::milliseconds{2});
      }
      if (client_failure.is_ok()) {
        client_failure = client->on_ready(true, true, start + std::chrono::milliseconds{2});
      }
    }
    EXPECT_FALSE(server_failure.is_ok());
    EXPECT_FALSE(client_failure.is_ok());
    EXPECT_EQ(server->state(),
              DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kFailed);
    EXPECT_EQ(client->state(),
              DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kFailed);
    EXPECT_FALSE(client_authenticator.saw_fingerprint);
    EXPECT_FALSE(server_authenticator.saw_fingerprint);
    EXPECT_EQ(service->metrics().prepare_requests, 0U);
    EXPECT_EQ(service->metrics().active_jobs, 0U);
  }

  for (const std::size_t cut_kind : kCutKinds) {
    SCOPED_TRACE(cut_kind);
    auto proxy_listener = network::TcpListener::bind();
    ASSERT_TRUE(proxy_listener.has_value()) << proxy_listener.error().to_string();
    auto client_leg_result = connect_tcp_pair(*proxy_listener);
    auto server_leg_result = connect_tcp_pair(*server_listener);
    ASSERT_TRUE(client_leg_result.has_value()) << client_leg_result.error().to_string();
    ASSERT_TRUE(server_leg_result.has_value()) << server_leg_result.error().to_string();
    TcpPair client_leg = std::move(*client_leg_result);
    TcpPair server_leg = std::move(*server_leg_result);
    auto accepted_tls = network::TlsSocket::accept(*server_context, server_leg.server.descriptor());
    auto connected_tls =
        network::TlsSocket::connect(*client_context, client_leg.client.descriptor());
    ASSERT_TRUE(accepted_tls.has_value()) << accepted_tls.error().to_string();
    ASSERT_TRUE(connected_tls.has_value()) << connected_tls.error().to_string();
    auto server = DistributedVectorGroupedAggregateShuffleJobControlTlsServer::create(
        std::move(*accepted_tls),
        {.authenticator = &client_authenticator,
         .service = &*service,
         .peer_ipv4_address = {127U, 0U, 0U, 1U},
         .limits = limits()},
        start);
    auto client = DistributedVectorGroupedAggregateShuffleJobControlTlsClient::create(
        std::move(*connected_tls),
        {.authenticator = &server_authenticator,
         .node_authorizer = &authorizer,
         .peer_ipv4_address = {127U, 0U, 0U, 1U},
         .request = DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare()},
         .limits = limits()},
        start);
    ASSERT_TRUE(server.has_value()) << server.error().to_string();
    ASSERT_TRUE(client.has_value()) << client.error().to_string();

    const common::Status client_progress =
        client->on_ready(true, true, start + std::chrono::milliseconds{1});
    ASSERT_TRUE(client_progress.is_ok()) << client_progress.to_string();
    auto client_hello = receive_tls_record(client_leg.server);
    ASSERT_TRUE(client_hello.has_value()) << client_hello.error().to_string();
    ASSERT_EQ(std::to_integer<std::uint8_t>((*client_hello)[0]), 22U);
    const common::Status hello_forwarded = send_raw_tcp(server_leg.client, *client_hello);
    ASSERT_TRUE(hello_forwarded.is_ok()) << hello_forwarded.to_string();
    const common::Status hello_readable = wait_for_raw_tcp(server_leg.server.descriptor(), POLLIN);
    ASSERT_TRUE(hello_readable.is_ok()) << hello_readable.to_string();
    for (std::size_t iteration = 0U; iteration < 64U; ++iteration) {
      const common::Status server_progress =
          server->on_ready(true, true, start + std::chrono::milliseconds{1});
      ASSERT_TRUE(server_progress.is_ok()) << server_progress.to_string();
      const auto interest = server->interest();
      if (interest.want_read && !interest.want_write)
        break;
    }
    ASSERT_EQ(server->state(),
              DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kHandshaking);

    std::optional<std::vector<std::byte>> encrypted_server_handshake;
    for (std::size_t record_index = 0U;
         record_index < 8U && !encrypted_server_handshake.has_value(); ++record_index) {
      auto record = receive_tls_record(server_leg.client);
      const auto server_interest = server->interest();
      const auto client_interest = client->interest();
      ASSERT_TRUE(record.has_value())
          << record.error().to_string() << ", record index " << record_index
          << ", server read/write " << server_interest.want_read << '/'
          << server_interest.want_write << ", client read/write " << client_interest.want_read
          << '/' << client_interest.want_write;
      const auto content_type = std::to_integer<std::uint8_t>((*record)[0]);
      if (content_type == 23U) {
        ASSERT_GT(record->size(), kTlsRecordHeaderBytes + 1U);
        encrypted_server_handshake.emplace(std::move(*record));
      } else {
        EXPECT_TRUE(content_type == 20U || content_type == 22U);
        const common::Status complete_record = send_raw_tcp(client_leg.server, *record);
        ASSERT_TRUE(complete_record.is_ok()) << complete_record.to_string();
        const common::Status record_readable =
            wait_for_raw_tcp(client_leg.client.descriptor(), POLLIN);
        ASSERT_TRUE(record_readable.is_ok()) << record_readable.to_string();
        const common::Status client_consumed =
            client->on_ready(true, true, start + std::chrono::milliseconds{1});
        ASSERT_TRUE(client_consumed.is_ok()) << client_consumed.to_string();
        const common::Status server_continued =
            server->on_ready(true, true, start + std::chrono::milliseconds{1});
        ASSERT_TRUE(server_continued.is_ok()) << server_continued.to_string();
      }
    }
    ASSERT_TRUE(encrypted_server_handshake.has_value());
    EXPECT_EQ(std::to_integer<std::uint8_t>((*encrypted_server_handshake)[0]), 23U);
    EXPECT_EQ(std::to_integer<std::uint8_t>((*encrypted_server_handshake)[1]), 3U);
    EXPECT_EQ(std::to_integer<std::uint8_t>((*encrypted_server_handshake)[2]), 3U);
    const std::size_t prefix_bytes = cut_kind == 0U   ? 2U
                                     : cut_kind == 1U ? kTlsRecordHeaderBytes + 1U
                                                      : encrypted_server_handshake->size() - 1U;
    SCOPED_TRACE(prefix_bytes);
    const common::Status forwarded =
        send_raw_tcp(client_leg.server, std::span{*encrypted_server_handshake}.first(prefix_bytes));
    ASSERT_TRUE(forwarded.is_ok()) << forwarded.to_string();
    ASSERT_TRUE(reset_tcp_owner(client_leg.server).is_ok());
    ASSERT_TRUE(reset_tcp_owner(server_leg.client).is_ok());
    ASSERT_TRUE(wait_for_tcp_failure(client_leg.client.descriptor()));
    ASSERT_TRUE(wait_for_tcp_failure(server_leg.server.descriptor()));

    common::Status client_failure = common::Status::ok();
    common::Status server_failure = common::Status::ok();
    for (std::size_t iteration = 0U;
         iteration < 1024U && (client_failure.is_ok() || server_failure.is_ok()); ++iteration) {
      if (client_failure.is_ok()) {
        client_failure = client->on_ready(true, true, start + std::chrono::milliseconds{2});
      }
      if (server_failure.is_ok()) {
        server_failure = server->on_ready(true, true, start + std::chrono::milliseconds{2});
      }
    }
    EXPECT_FALSE(client_failure.is_ok());
    EXPECT_FALSE(server_failure.is_ok());
    EXPECT_EQ(client->state(),
              DistributedVectorGroupedAggregateShuffleJobControlTlsClientState::kFailed);
    EXPECT_EQ(server->state(),
              DistributedVectorGroupedAggregateShuffleJobControlTlsServerState::kFailed);
    EXPECT_FALSE(client_authenticator.saw_fingerprint);
    EXPECT_FALSE(server_authenticator.saw_fingerprint);
    EXPECT_EQ(service->metrics().prepare_requests, 0U);
    EXPECT_EQ(service->metrics().active_jobs, 0U);
  }

  auto prepared = exchange_tcp(
      *server_listener, DistributedVectorGroupedAggregateShuffleJobControlRequest{prepare()},
      *service, {.client = &client_authenticator, .server = &server_authenticator}, authorizer,
      *server_context, *client_context);
  ASSERT_TRUE(prepared.has_value()) << prepared.error().to_string();
  EXPECT_EQ(prepared->status_code, common::StatusCode::kOk);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  EXPECT_EQ(service->metrics().prepare_requests, 1U);
  EXPECT_EQ(service->metrics().active_jobs, 1U);

  auto cancelled =
      exchange_tcp(*server_listener,
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
