#include "chronos/network/client_session.hpp"
#include "chronos/network/epoll_reactor.hpp"
#include "chronos/network/messages.hpp"

#include <gtest/gtest.h>

#if defined(__linux__)
#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <filesystem>
#include <memory>
#include <openssl/ssl.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>
#endif

namespace chronos::network {
namespace {

class TestAuthenticator final : public ConnectionAuthenticator {
public:
  explicit TestAuthenticator(const std::uint64_t principal_id = 77U) noexcept
      : principal_id_(principal_id) {}

  common::Result<PeerAuthenticationResult>
  authenticate(const PeerAuthenticationRequest& request) override {
    last_request = request;
    ++calls;
    return PeerAuthenticationResult{.authorized = true, .principal_id = principal_id_};
  }

  PeerAuthenticationRequest last_request;
  std::size_t calls{};

private:
  std::uint64_t principal_id_;
};

TEST(EpollReactorTest, PlatformBoundaryIsExplicit) {
  SpscNetworkTaskQueue requests = SpscNetworkTaskQueue::create(4U).value();
  SpscNetworkTaskQueue responses = SpscNetworkTaskQueue::create(4U).value();
  auto reactor = EpollReactor::start({}, {.requests = &requests, .responses = &responses});
#if defined(__linux__)
  ASSERT_TRUE(reactor.has_value()) << reactor.error().to_string();
  EXPECT_NE(reactor->bound_port(), 0U);
  EXPECT_TRUE(reactor->is_running());
  EXPECT_TRUE(reactor->shutdown().is_ok());
#else
  ASSERT_FALSE(reactor.has_value());
  EXPECT_EQ(reactor.error().code(), common::StatusCode::kNotSupported);
  EpollReactor empty;
  EXPECT_EQ(empty.notify_response_ready().code(), common::StatusCode::kNotSupported);
  EXPECT_EQ(empty.reload_tls_security({}).code(), common::StatusCode::kNotSupported);
#endif
}

#if defined(__linux__)
[[nodiscard]] int connect_client(const std::uint16_t port) {
  const int client = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (client < 0)
    return -1;
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  if (::connect(client, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
    ::close(client);
    return -1;
  }
  return client;
}

void send_all(const int socket, const common::ByteView bytes) {
  std::size_t offset = 0U;
  while (offset < bytes.size()) {
    const ssize_t count =
        ::send(socket, bytes.data() + offset, bytes.size() - offset, MSG_NOSIGNAL);
    ASSERT_GT(count, 0);
    offset += static_cast<std::size_t>(count);
  }
}

void send_client_pending(const int socket, NativeClientSession& client) {
  const common::ByteView pending = client.pending_write();
  send_all(socket, pending);
  ASSERT_TRUE(client.consume_written(pending.size()).is_ok());
}

[[nodiscard]] std::vector<std::byte> receive_available(const int socket) {
  std::vector<std::byte> bytes;
  std::array<std::byte, 4096> buffer{};
  for (;;) {
    const ssize_t count = ::recv(socket, buffer.data(), buffer.size(), MSG_DONTWAIT);
    if (count < 0 && errno == EAGAIN)
      break;
    if (count <= 0)
      break;
    bytes.insert(bytes.end(), buffer.begin(), buffer.begin() + static_cast<std::ptrdiff_t>(count));
  }
  return bytes;
}

void drive_handshake(EpollReactor& reactor, const int socket, NativeClientSession& client) {
  ASSERT_TRUE(client.queue_handshake().is_ok());
  send_client_pending(socket, client);
  for (std::size_t attempt = 0U; attempt < 32U && client.phase() != ClientSessionPhase::kActive;
       ++attempt) {
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
    const std::vector<std::byte> available = receive_available(socket);
    if (!available.empty()) {
      ASSERT_TRUE(client.receive(available).has_value());
    }
  }
  ASSERT_EQ(client.phase(), ClientSessionPhase::kActive);
}

struct SslContextDeleter {
  void operator()(SSL_CTX* context) const noexcept {
    SSL_CTX_free(context);
  }
};

struct SslDeleter {
  void operator()(SSL* session) const noexcept {
    SSL_free(session);
  }
};

using ClientTlsContext = std::unique_ptr<SSL_CTX, SslContextDeleter>;
using ClientTlsSession = std::unique_ptr<SSL, SslDeleter>;

[[nodiscard]] std::filesystem::path tls_fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] TlsServerConfig epoll_tls_config() {
  return {.certificate_chain_file = tls_fixture("server.pem").string(),
          .private_key_file = tls_fixture("server-key.pem").string(),
          .trust_store_file = tls_fixture("ca.pem").string()};
}

[[nodiscard]] ClientTlsContext create_client_tls_context() {
  ClientTlsContext context{SSL_CTX_new(TLS_client_method())};
  EXPECT_NE(context, nullptr);
  if (!context)
    return context;
  EXPECT_EQ(SSL_CTX_set_min_proto_version(context.get(), TLS1_2_VERSION), 1);
  SSL_CTX_set_verify(context.get(), SSL_VERIFY_PEER, nullptr);
  EXPECT_EQ(SSL_CTX_load_verify_locations(context.get(), tls_fixture("ca.pem").c_str(), nullptr),
            1);
  EXPECT_EQ(SSL_CTX_use_certificate_chain_file(context.get(), tls_fixture("client.pem").c_str()),
            1);
  EXPECT_EQ(SSL_CTX_use_PrivateKey_file(context.get(), tls_fixture("client-key.pem").c_str(),
                                        SSL_FILETYPE_PEM),
            1);
  EXPECT_EQ(SSL_CTX_check_private_key(context.get()), 1);
  return context;
}

[[nodiscard]] ClientTlsSession create_client_tls_session(SSL_CTX* context, const int socket) {
  ClientTlsSession session{SSL_new(context)};
  EXPECT_NE(session, nullptr);
  if (!session)
    return session;
  EXPECT_EQ(SSL_set_fd(session.get(), socket), 1);
  SSL_set_connect_state(session.get());
  return session;
}

void drive_tls_handshake(EpollReactor& reactor, SSL* client,
                         const std::uint64_t expected_accepted_connections = 1U) {
  bool client_complete = false;
  for (std::size_t attempt = 0U; attempt < 1024U; ++attempt) {
    if (!client_complete) {
      const int result = SSL_do_handshake(client);
      if (result == 1) {
        client_complete = true;
      } else {
        const int error = SSL_get_error(client, result);
        ASSERT_TRUE(error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE);
      }
    }
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
    if (client_complete && reactor.metrics().accepted_connections == expected_accepted_connections)
      return;
  }
  FAIL() << "epoll mutual TLS handshake did not converge";
}

void tls_write_all(EpollReactor& reactor, SSL* client, const common::ByteView bytes) {
  std::size_t offset{};
  for (std::size_t attempt = 0U; attempt < 1024U && offset < bytes.size(); ++attempt) {
    std::size_t written{};
    const int result = SSL_write_ex(client, bytes.data() + offset, bytes.size() - offset, &written);
    if (result == 1) {
      offset += written;
    } else {
      const int error = SSL_get_error(client, result);
      ASSERT_TRUE(error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE);
    }
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  }
  ASSERT_EQ(offset, bytes.size());
}

[[nodiscard]] std::vector<std::byte> tls_receive_available(EpollReactor& reactor, SSL* client) {
  std::vector<std::byte> bytes;
  std::array<std::byte, 4096U> buffer{};
  for (std::size_t attempt = 0U; attempt < 1024U && bytes.empty(); ++attempt) {
    const common::Status poll = reactor.poll_once(std::chrono::milliseconds{1});
    if (!poll.is_ok()) {
      ADD_FAILURE() << poll.to_string();
      return bytes;
    }
    for (;;) {
      std::size_t received{};
      const int result = SSL_read_ex(client, buffer.data(), buffer.size(), &received);
      if (result == 1) {
        bytes.insert(bytes.end(), buffer.begin(),
                     buffer.begin() + static_cast<std::ptrdiff_t>(received));
        continue;
      }
      const int error = SSL_get_error(client, result);
      EXPECT_TRUE(error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE);
      break;
    }
  }
  return bytes;
}

TEST(EpollReactorTest, MutualTlsAuthenticatesBeforeProtocolDispatch) {
  SpscNetworkTaskQueue requests = SpscNetworkTaskQueue::create(4U).value();
  SpscNetworkTaskQueue responses = SpscNetworkTaskQueue::create(4U).value();
  TestAuthenticator authenticator;
  EpollServerConfig config;
  config.maximum_io_operations_per_event = 1U;
  config.read_chunk_bytes = 17U;
  config.security = {.mode = TransportSecurityMode::kTlsRequired,
                     .authenticator = &authenticator,
                     .tls = epoll_tls_config()};
  EpollReactor reactor =
      EpollReactor::start(config, {.requests = &requests, .responses = &responses}).value();
  const int socket = connect_client(reactor.bound_port());
  ASSERT_GE(socket, 0);
  const int flags = ::fcntl(socket, F_GETFL, 0);
  ASSERT_GE(flags, 0);
  ASSERT_EQ(::fcntl(socket, F_SETFL, flags | O_NONBLOCK), 0);
  ClientTlsContext client_context = create_client_tls_context();
  ASSERT_NE(client_context, nullptr);
  ClientTlsSession client = create_client_tls_session(client_context.get(), socket);
  ASSERT_NE(client, nullptr);

  drive_tls_handshake(reactor, client.get());
  ASSERT_EQ(authenticator.calls, 1U);
  EXPECT_TRUE(authenticator.last_request.transport_authenticated);
  EXPECT_TRUE(authenticator.last_request.peer_certificate_sha256.has_value());
  EXPECT_TRUE(requests.empty());

  const auto hello_payload = encode_client_hello({}).value();
  const auto hello =
      encode_frame({.message_type = MessageType::kClientHello}, hello_payload).value();
  tls_write_all(reactor, client.get(), hello);
  const auto hello_response = decode_frame(tls_receive_available(reactor, client.get()));
  ASSERT_TRUE(hello_response.has_value()) << hello_response.error().to_string();
  EXPECT_EQ(hello_response->header.message_type, MessageType::kServerHello);

  const auto query_payload = encode_query_request("SELECT 1").value();
  const auto query =
      encode_frame({.message_type = MessageType::kQueryRequest, .request_id = 1U}, query_payload)
          .value();
  tls_write_all(reactor, client.get(), query);
  for (std::size_t attempt = 0U; attempt < 128U && requests.empty(); ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  const auto dispatched = requests.try_pop();
  ASSERT_TRUE(dispatched.has_value());
  EXPECT_EQ(dispatched->principal_id, 77U);
  EXPECT_EQ(dispatched->frame.header.message_type, MessageType::kQueryRequest);
  EXPECT_GT(reactor.metrics().bytes_read, 0U);
  EXPECT_GT(reactor.metrics().bytes_written, 0U);

  client.reset();
  ::close(socket);
  EXPECT_TRUE(reactor.shutdown().is_ok());
}

TEST(EpollReactorTest, TlsSecurityReloadIsTransactionalAndPreservesEstablishedSessions) {
  SpscNetworkTaskQueue requests = SpscNetworkTaskQueue::create(4U).value();
  SpscNetworkTaskQueue responses = SpscNetworkTaskQueue::create(4U).value();
  TestAuthenticator initial_authenticator{77U};
  TestAuthenticator replacement_authenticator{88U};
  EpollServerConfig config;
  config.security = {.mode = TransportSecurityMode::kTlsRequired,
                     .authenticator = &initial_authenticator,
                     .tls = epoll_tls_config()};
  EpollReactor reactor =
      EpollReactor::start(config, {.requests = &requests, .responses = &responses}).value();

  const int established_socket = connect_client(reactor.bound_port());
  ASSERT_GE(established_socket, 0);
  const int established_flags = ::fcntl(established_socket, F_GETFL, 0);
  ASSERT_GE(established_flags, 0);
  ASSERT_EQ(::fcntl(established_socket, F_SETFL, established_flags | O_NONBLOCK), 0);
  ClientTlsContext initial_client_context = create_client_tls_context();
  ASSERT_NE(initial_client_context, nullptr);
  ClientTlsSession established_client =
      create_client_tls_session(initial_client_context.get(), established_socket);
  ASSERT_NE(established_client, nullptr);
  drive_tls_handshake(reactor, established_client.get());

  const int incomplete_socket = connect_client(reactor.bound_port());
  ASSERT_GE(incomplete_socket, 0);
  ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  ASSERT_EQ(reactor.metrics().active_connections, 2U);

  const auto malformed_credentials = std::make_shared<const TlsPemCredentials>(
      TlsPemCredentials{.certificate_chain = "not a certificate",
                        .private_key = "not a key",
                        .trust_store = "not a trust store"});
  const common::Status rejected = reactor.reload_tls_security(
      {.mode = TransportSecurityMode::kTlsRequired,
       .authenticator = &replacement_authenticator,
       .tls = TlsServerConfig{.pem_credentials = malformed_credentials}});
  EXPECT_FALSE(rejected.is_ok());
  EXPECT_EQ(reactor.metrics().tls_security_reload_failures, 1U);
  EXPECT_EQ(reactor.metrics().active_connections, 2U);
  EXPECT_EQ(reactor.metrics().closed_connections, 0U);

  ASSERT_TRUE(reactor
                  .reload_tls_security({.mode = TransportSecurityMode::kTlsRequired,
                                        .authenticator = &replacement_authenticator,
                                        .tls = epoll_tls_config()})
                  .is_ok());
  const EpollServerMetrics reloaded = reactor.metrics();
  EXPECT_EQ(reloaded.tls_security_reloads, 1U);
  EXPECT_EQ(reloaded.tls_security_reload_closed_handshakes, 1U);
  EXPECT_EQ(reloaded.active_connections, 1U);
  EXPECT_EQ(reloaded.closed_connections, 1U);

  const auto hello_payload = encode_client_hello({}).value();
  const auto hello =
      encode_frame({.message_type = MessageType::kClientHello}, hello_payload).value();
  tls_write_all(reactor, established_client.get(), hello);
  ASSERT_TRUE(decode_frame(tls_receive_available(reactor, established_client.get())).has_value());
  const auto query_payload = encode_query_request("SELECT 1").value();
  const auto query =
      encode_frame({.message_type = MessageType::kQueryRequest, .request_id = 1U}, query_payload)
          .value();
  tls_write_all(reactor, established_client.get(), query);
  for (std::size_t attempt = 0U; attempt < 128U && requests.empty(); ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  auto dispatched = requests.try_pop();
  ASSERT_TRUE(dispatched.has_value());
  EXPECT_EQ(dispatched->principal_id, 77U);

  const int replacement_socket = connect_client(reactor.bound_port());
  ASSERT_GE(replacement_socket, 0);
  const int replacement_flags = ::fcntl(replacement_socket, F_GETFL, 0);
  ASSERT_GE(replacement_flags, 0);
  ASSERT_EQ(::fcntl(replacement_socket, F_SETFL, replacement_flags | O_NONBLOCK), 0);
  ClientTlsSession replacement_client =
      create_client_tls_session(initial_client_context.get(), replacement_socket);
  ASSERT_NE(replacement_client, nullptr);
  drive_tls_handshake(reactor, replacement_client.get(), 2U);
  tls_write_all(reactor, replacement_client.get(), hello);
  ASSERT_TRUE(decode_frame(tls_receive_available(reactor, replacement_client.get())).has_value());
  tls_write_all(reactor, replacement_client.get(), query);
  for (std::size_t attempt = 0U; attempt < 128U && requests.empty(); ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  dispatched = requests.try_pop();
  ASSERT_TRUE(dispatched.has_value());
  EXPECT_EQ(dispatched->principal_id, 88U);
  EXPECT_EQ(initial_authenticator.calls, 1U);
  EXPECT_EQ(replacement_authenticator.calls, 1U);

  replacement_client.reset();
  established_client.reset();
  ::close(replacement_socket);
  ::close(established_socket);
  ::close(incomplete_socket);
  EXPECT_TRUE(reactor.shutdown().is_ok());
}

TEST(EpollReactorTest, RealSocketsHandshakeDispatchRespondAndExposeQueueOverload) {
  SpscNetworkTaskQueue requests = SpscNetworkTaskQueue::create(1U).value();
  SpscNetworkTaskQueue responses = SpscNetworkTaskQueue::create(4U).value();
  EpollServerConfig config;
  config.read_chunk_bytes = 3U;
  config.maximum_connections = 4U;
  config.maximum_events_per_poll = 16U;
  TestAuthenticator authenticator;
  config.security.authenticator = &authenticator;
  EpollReactor reactor =
      EpollReactor::start(config, {.requests = &requests, .responses = &responses}).value();
  const int client = connect_client(reactor.bound_port());
  ASSERT_GE(client, 0);
  ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{10}).is_ok());

  const auto hello_payload = encode_client_hello({}).value();
  const auto hello =
      encode_frame({.message_type = MessageType::kClientHello}, hello_payload).value();
  send_all(client, hello);
  for (std::size_t attempt = 0U; attempt < 64U; ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  const std::vector<std::byte> hello_response = receive_available(client);
  const auto decoded_hello = decode_frame(hello_response);
  ASSERT_TRUE(decoded_hello.has_value()) << decoded_hello.error().to_string();
  EXPECT_EQ(decoded_hello->header.message_type, MessageType::kServerHello);

  const auto query_payload = encode_query_request("SELECT 1").value();
  const auto query =
      encode_frame({.message_type = MessageType::kQueryRequest, .request_id = 1U}, query_payload)
          .value();
  send_all(client, query);
  for (std::size_t attempt = 0U; attempt < 64U && requests.empty(); ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  auto dispatched = requests.try_pop();
  ASSERT_TRUE(dispatched.has_value());
  EXPECT_EQ(dispatched->frame.header.request_id, 1U); // NOLINT(bugprone-unchecked-optional-access)
  EXPECT_EQ(dispatched->principal_id, 77U);           // NOLINT(bugprone-unchecked-optional-access)

  const schema::LogicalType result_type =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  const std::array<QueryResultColumn, 1> result_columns{
      QueryResultColumn{.name = "value", .type = result_type, .nullable = false}};
  ASSERT_TRUE(responses.try_push(
      {.connection_id = dispatched->connection_id,
       .frame = {.header = {.message_type = MessageType::kQueryResult,
                            .flags = kFrameFlagEndStream,
                            .request_id = 1U},
                 .payload = *encode_query_result_batch(0U, result_columns, {})}}));
  for (std::size_t attempt = 0U; attempt < 4U; ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  const auto result = decode_frame(receive_available(client));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->header.message_type, MessageType::kQueryResult);

  ASSERT_TRUE(responses.try_push(
      {.connection_id = dispatched->connection_id,
       .frame = {.header = {.message_type = MessageType::kQueryEnd, .request_id = 1U},
                 .payload = {}}}));
  for (std::size_t attempt = 0U; attempt < 4U; ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  const auto end = decode_frame(receive_available(client));
  ASSERT_TRUE(end.has_value());
  EXPECT_EQ(end->header.message_type, MessageType::kQueryEnd);

  ASSERT_TRUE(requests.try_push({.connection_id = 999U, .frame = {}}));
  const auto second_query =
      encode_frame({.message_type = MessageType::kQueryRequest, .request_id = 2U}, query_payload)
          .value();
  send_all(client, second_query);
  for (std::size_t attempt = 0U; attempt < 64U; ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  const auto overloaded = decode_frame(receive_available(client));
  ASSERT_TRUE(overloaded.has_value());
  EXPECT_EQ(overloaded->header.message_type, MessageType::kError);
  EXPECT_EQ(reactor.metrics().queue_overloads, 1U);
  ::close(client);
  EXPECT_TRUE(reactor.shutdown().is_ok());
}

TEST(EpollReactorTest, SlowHandshakeTimesOutAndConnectionAdmissionIsBounded) {
  SpscNetworkTaskQueue requests = SpscNetworkTaskQueue::create(4U).value();
  SpscNetworkTaskQueue responses = SpscNetworkTaskQueue::create(4U).value();
  EpollServerConfig config;
  config.maximum_connections = 1U;
  config.handshake_timeout = std::chrono::milliseconds{2};
  config.idle_timeout = std::chrono::milliseconds{100};
  EpollReactor reactor =
      EpollReactor::start(config, {.requests = &requests, .responses = &responses}).value();
  const int slow = connect_client(reactor.bound_port());
  ASSERT_GE(slow, 0);
  ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());

  const auto hello_payload = encode_client_hello({}).value();
  const auto hello =
      encode_frame({.message_type = MessageType::kClientHello}, hello_payload).value();
  send_all(slow, common::ByteView{hello}.first(7U));
  ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());

  const int excess = connect_client(reactor.bound_port());
  ASSERT_GE(excess, 0);
  ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  EXPECT_EQ(reactor.metrics().rejected_connections, 1U);
  ::close(excess);

  std::this_thread::sleep_for(std::chrono::milliseconds{3});
  ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_EQ(reactor.metrics().timed_out_connections, 1U);
  EXPECT_EQ(reactor.metrics().active_connections, 0U);
  ::close(slow);
  EXPECT_TRUE(reactor.shutdown().is_ok());
}

TEST(EpollReactorTest, PortableClientSessionInteroperatesWithRealSocketServer) {
  SpscNetworkTaskQueue requests = SpscNetworkTaskQueue::create(4U).value();
  SpscNetworkTaskQueue responses = SpscNetworkTaskQueue::create(4U).value();
  EpollReactor reactor =
      EpollReactor::start({}, {.requests = &requests, .responses = &responses}).value();
  const int socket = connect_client(reactor.bound_port());
  ASSERT_GE(socket, 0);
  ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  NativeClientSession client = NativeClientSession::create().value();
  ASSERT_TRUE(client.queue_handshake().is_ok());
  send_client_pending(socket, client);
  for (std::size_t attempt = 0U; attempt < 8U; ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  ASSERT_TRUE(client.receive(receive_available(socket)).has_value());
  ASSERT_EQ(client.phase(), ClientSessionPhase::kActive);

  const std::uint64_t request_id = client.queue_query("SELECT 1").value();
  send_client_pending(socket, client);
  for (std::size_t attempt = 0U; attempt < 8U && requests.empty(); ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  const auto request = requests.try_pop();
  ASSERT_TRUE(request.has_value());
  const schema::LogicalType type =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  const std::array<QueryResultColumn, 1> columns{
      QueryResultColumn{.name = "value", .type = type, .nullable = false}};
  ASSERT_TRUE(
      responses.try_push({.connection_id = request->connection_id,
                          .frame = {.header = {.message_type = MessageType::kQueryResult,
                                               .flags = kFrameFlagEndStream,
                                               .request_id = request_id},
                                    .payload = *encode_query_result_batch(0U, columns, {})}}));
  ASSERT_TRUE(responses.try_push(
      {.connection_id = request->connection_id,
       .frame = {.header = {.message_type = MessageType::kQueryEnd, .request_id = request_id},
                 .payload = {}}}));
  std::size_t received_frames = 0U;
  for (std::size_t attempt = 0U; attempt < 64U && client.in_flight_requests() != 0U; ++attempt) {
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
    const std::vector<std::byte> available = receive_available(socket);
    if (!available.empty()) {
      const auto server_frames = client.receive(available);
      ASSERT_TRUE(server_frames.has_value());
      received_frames += server_frames->size();
    }
  }
  EXPECT_EQ(received_frames, 2U);
  EXPECT_EQ(client.in_flight_requests(), 0U);
  ::close(socket);
  EXPECT_TRUE(reactor.shutdown().is_ok());
}

TEST(EpollReactorTest, ResponseProducerWakeupInterruptsBlockedPoll) {
  SpscNetworkTaskQueue requests = SpscNetworkTaskQueue::create(4U).value();
  SpscNetworkTaskQueue responses = SpscNetworkTaskQueue::create(4U).value();
  EpollReactor reactor =
      EpollReactor::start({}, {.requests = &requests, .responses = &responses}).value();
  common::Status poll_status;
  const auto started = std::chrono::steady_clock::now();
  std::thread owner([&] { poll_status = reactor.poll_once(std::chrono::milliseconds{2000}); });
  std::this_thread::sleep_for(std::chrono::milliseconds{20});
  EXPECT_TRUE(reactor.notify_response_ready().is_ok());
  owner.join();
  EXPECT_TRUE(poll_status.is_ok());
  EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::milliseconds{500});
  EXPECT_EQ(reactor.metrics().response_wakeups, 1U);
  EXPECT_TRUE(reactor.shutdown().is_ok());
}

TEST(EpollReactorTest, ExplicitCancelAndHalfCloseDetachWorkDeterministically) {
  SpscNetworkTaskQueue requests = SpscNetworkTaskQueue::create(8U).value();
  SpscNetworkTaskQueue responses = SpscNetworkTaskQueue::create(8U).value();
  EpollReactor reactor =
      EpollReactor::start({}, {.requests = &requests, .responses = &responses}).value();

  int socket = connect_client(reactor.bound_port());
  ASSERT_GE(socket, 0);
  ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  NativeClientSession client = NativeClientSession::create().value();
  drive_handshake(reactor, socket, client);
  const std::uint64_t cancelled_id = client.queue_query("SELECT 1").value();
  send_client_pending(socket, client);
  for (std::size_t attempt = 0U; attempt < 128U && requests.empty(); ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  const auto original = requests.try_pop();
  ASSERT_TRUE(original.has_value());
  ASSERT_TRUE(client.queue_cancel(cancelled_id).is_ok());
  send_client_pending(socket, client);
  for (std::size_t attempt = 0U; attempt < 128U && requests.empty(); ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  const auto cancellation = requests.try_pop();
  ASSERT_TRUE(cancellation.has_value());
  EXPECT_EQ(cancellation->frame.header.message_type, MessageType::kCancel);
  EXPECT_EQ(cancellation->connection_id, original->connection_id);
  const schema::LogicalType type =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  const std::array<QueryResultColumn, 1> columns{
      QueryResultColumn{.name = "value", .type = type, .nullable = false}};
  ASSERT_TRUE(
      responses.try_push({.connection_id = original->connection_id,
                          .frame = {.header = {.message_type = MessageType::kQueryResult,
                                               .flags = kFrameFlagEndStream,
                                               .request_id = cancelled_id},
                                    .payload = *encode_query_result_batch(0U, columns, {})}}));
  ASSERT_TRUE(reactor.notify_response_ready().is_ok());
  ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  EXPECT_EQ(reactor.metrics().dropped_responses, 1U);
  EXPECT_TRUE(receive_available(socket).empty());
  ::close(socket);

  socket = connect_client(reactor.bound_port());
  ASSERT_GE(socket, 0);
  ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  NativeClientSession half_closed = NativeClientSession::create().value();
  drive_handshake(reactor, socket, half_closed);
  const std::uint64_t detached_id = half_closed.queue_query("SELECT 2").value();
  send_client_pending(socket, half_closed);
  ASSERT_EQ(::shutdown(socket, SHUT_WR), 0);
  for (std::size_t attempt = 0U; attempt < 32U && requests.empty(); ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  const auto dispatched = requests.try_pop();
  ASSERT_TRUE(dispatched.has_value());
  EXPECT_EQ(dispatched->frame.header.message_type, MessageType::kQueryRequest);
  for (std::size_t attempt = 0U; attempt < 32U && requests.empty(); ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  const auto detached = requests.try_pop();
  ASSERT_TRUE(detached.has_value());
  EXPECT_EQ(detached->frame.header.message_type, MessageType::kCancel);
  EXPECT_EQ(detached->frame.header.request_id, detached_id);
  EXPECT_EQ(detached->connection_id, dispatched->connection_id);
  ::close(socket);
  EXPECT_TRUE(reactor.shutdown().is_ok());
}

TEST(EpollReactorTest, ConnectionChurnReleasesEveryAdmittedSocket) {
  SpscNetworkTaskQueue requests = SpscNetworkTaskQueue::create(8U).value();
  SpscNetworkTaskQueue responses = SpscNetworkTaskQueue::create(8U).value();
  EpollServerConfig config;
  config.maximum_connections = 8U;
  EpollReactor reactor =
      EpollReactor::start(config, {.requests = &requests, .responses = &responses}).value();
  constexpr std::size_t kConnections = 128U;
  for (std::size_t index = 0U; index < kConnections; ++index) {
    const int socket = connect_client(reactor.bound_port());
    ASSERT_GE(socket, 0);
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
    ::close(socket);
    for (std::size_t attempt = 0U; attempt < 16U && reactor.metrics().active_connections != 0U;
         ++attempt)
      ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
    ASSERT_EQ(reactor.metrics().active_connections, 0U);
  }
  EXPECT_EQ(reactor.metrics().accepted_connections, kConnections);
  EXPECT_EQ(reactor.metrics().closed_connections, kConnections);
  EXPECT_TRUE(reactor.shutdown().is_ok());
}

TEST(EpollReactorTest, RealSocketShortWritesPreserveLargeResultAndTerminalOrder) {
  SpscNetworkTaskQueue requests = SpscNetworkTaskQueue::create(4U).value();
  SpscNetworkTaskQueue responses = SpscNetworkTaskQueue::create(4U).value();
  EpollServerConfig config;
  config.maximum_io_operations_per_event = 1U;
  EpollReactor reactor =
      EpollReactor::start(config, {.requests = &requests, .responses = &responses}).value();
  const int socket = connect_client(reactor.bound_port());
  ASSERT_GE(socket, 0);
  int receive_buffer = 65'536;
  ASSERT_EQ(::setsockopt(socket, SOL_SOCKET, SO_RCVBUF, &receive_buffer, sizeof(receive_buffer)),
            0);
  ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  NativeClientSession client = NativeClientSession::create().value();
  drive_handshake(reactor, socket, client);
  const std::uint64_t request_id = client.queue_query("SELECT payload").value();
  send_client_pending(socket, client);
  for (std::size_t attempt = 0U; attempt < 16U && requests.empty(); ++attempt)
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
  const auto request = requests.try_pop();
  ASSERT_TRUE(request.has_value());

  const schema::LogicalType binary =
      schema::LogicalType::create(schema::LogicalTypeKind::kBinary).value();
  const std::array<QueryResultColumn, 1> columns{
      QueryResultColumn{.name = "payload", .type = binary, .nullable = false}};
  const std::vector<std::byte> value(std::size_t{8U} * 1024U * 1024U, std::byte{0x5a});
  const std::array<QueryResultCell, 1> cells{QueryResultCell{.value = value}};
  const std::vector<std::byte> batch = *encode_query_result_batch(1U, columns, cells);
  ASSERT_TRUE(responses.try_push({.connection_id = request->connection_id,
                                  .frame = {.header = {.message_type = MessageType::kQueryResult,
                                                       .flags = kFrameFlagEndStream,
                                                       .request_id = request_id},
                                            .payload = batch}}));
  ASSERT_TRUE(responses.try_push(
      {.connection_id = request->connection_id,
       .frame = {.header = {.message_type = MessageType::kQueryEnd, .request_id = request_id},
                 .payload = {}}}));
  const std::uint64_t bytes_before = reactor.metrics().bytes_written;
  ASSERT_TRUE(reactor.notify_response_ready().is_ok());
  ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{0}).is_ok());
  const std::uint64_t first_write = reactor.metrics().bytes_written - bytes_before;
  EXPECT_GT(first_write, 0U);
  EXPECT_LT(first_write, batch.size() + 2U * kFrameHeaderSize);

  std::size_t received_frames = 0U;
  for (std::size_t attempt = 0U; attempt < 5'000U && client.in_flight_requests() != 0U; ++attempt) {
    ASSERT_TRUE(reactor.poll_once(std::chrono::milliseconds{1}).is_ok());
    const std::vector<std::byte> available = receive_available(socket);
    if (!available.empty()) {
      const auto decoded = client.receive(available);
      ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
      received_frames += decoded->size();
    }
  }
  EXPECT_EQ(received_frames, 2U);
  EXPECT_EQ(client.in_flight_requests(), 0U);
  ::close(socket);
  EXPECT_TRUE(reactor.shutdown().is_ok());
}
#endif

} // namespace
} // namespace chronos::network
