#include "chronos/network/security.hpp"
#include "chronos/network/tls_socket.hpp"

#include <algorithm>
#include <array>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <iterator>
#include <memory>
#include <openssl/ssl.h>
#include <optional>
#include <poll.h>
#include <span>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

namespace chronos::network {
namespace {

struct SocketPair {
  std::array<int, 2> sockets{-1, -1};
  ~SocketPair() {
    for (const int socket : sockets) {
      if (socket >= 0)
        ::close(socket);
    }
  }
};

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

using ClientContext = std::unique_ptr<SSL_CTX, SslContextDeleter>;
using ClientSession = std::unique_ptr<SSL, SslDeleter>;

volatile std::sig_atomic_t sigpipe_observed{};

void observe_sigpipe(int) {
  sigpipe_observed = 1;
}

class SigpipeObserver {
public:
  SigpipeObserver() {
    struct sigaction action {};
    action.sa_handler = &observe_sigpipe;
    if (sigemptyset(&action.sa_mask) == 0 &&
        ::sigaction(SIGPIPE, &action, &previous_action_) == 0) {
      installed_ = true;
    }
  }

  ~SigpipeObserver() {
    if (installed_)
      (void)::sigaction(SIGPIPE, &previous_action_, nullptr);
  }

  SigpipeObserver(const SigpipeObserver&) = delete;
  SigpipeObserver& operator=(const SigpipeObserver&) = delete;

  [[nodiscard]] bool installed() const noexcept {
    return installed_;
  }

private:
  struct sigaction previous_action_ {};
  bool installed_{};
};

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] TlsServerConfig server_config() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
}

[[nodiscard]] TlsClientConfig client_config() {
  return {.certificate_chain_file = fixture("client.pem").string(),
          .private_key_file = fixture("client-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

[[nodiscard]] std::string fixture_bytes(const char* name) {
  std::ifstream stream{fixture(name), std::ios::binary};
  return {std::istreambuf_iterator<char>{stream}, std::istreambuf_iterator<char>{}};
}

[[nodiscard]] std::shared_ptr<const TlsPemCredentials> pem_credentials(const char* certificate,
                                                                       const char* private_key) {
  return std::make_shared<const TlsPemCredentials>(
      TlsPemCredentials{.certificate_chain = fixture_bytes(certificate),
                        .private_key = fixture_bytes(private_key),
                        .trust_store = fixture_bytes("ca.pem")});
}

[[nodiscard]] SocketPair nonblocking_socket_pair() {
  SocketPair pair;
  EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, pair.sockets.data()), 0);
  for (const int socket : pair.sockets) {
    const int flags = ::fcntl(socket, F_GETFL, 0);
    EXPECT_GE(flags, 0);
    EXPECT_EQ(::fcntl(socket, F_SETFL, flags | O_NONBLOCK), 0);
  }
  return pair;
}

[[nodiscard]] ClientContext client_context(const bool provide_certificate) {
  ClientContext context{SSL_CTX_new(TLS_client_method())};
  EXPECT_NE(context, nullptr);
  if (!context)
    return context;
  EXPECT_EQ(SSL_CTX_set_min_proto_version(context.get(), TLS1_2_VERSION), 1);
  SSL_CTX_set_verify(context.get(), SSL_VERIFY_PEER, nullptr);
  EXPECT_EQ(SSL_CTX_load_verify_locations(context.get(), fixture("ca.pem").c_str(), nullptr), 1);
  if (provide_certificate) {
    EXPECT_EQ(SSL_CTX_use_certificate_chain_file(context.get(), fixture("client.pem").c_str()), 1);
    EXPECT_EQ(SSL_CTX_use_PrivateKey_file(context.get(), fixture("client-key.pem").c_str(),
                                          SSL_FILETYPE_PEM),
              1);
    EXPECT_EQ(SSL_CTX_check_private_key(context.get()), 1);
  }
  return context;
}

[[nodiscard]] ClientSession client_session(SSL_CTX* context, const int socket) {
  ClientSession session{SSL_new(context)};
  EXPECT_NE(session, nullptr);
  if (!session)
    return session;
  EXPECT_EQ(SSL_set_fd(session.get(), socket), 1);
  SSL_set_connect_state(session.get());
  return session;
}

void complete_handshake(TlsSocket& server, TlsSocket& client) {
  for (std::size_t attempt = 0U; attempt < 1024U; ++attempt) {
    if (!server.handshake_complete()) {
      auto progress = server.handshake();
      ASSERT_TRUE(progress.has_value()) << progress.error().message();
    }
    if (!client.handshake_complete()) {
      auto progress = client.handshake();
      ASSERT_TRUE(progress.has_value()) << progress.error().message();
    }
    if (server.handshake_complete() && client.handshake_complete())
      return;
  }
  FAIL() << "nonblocking mutual TLS handshake did not converge";
}

class CapturingAuthenticator final : public ConnectionAuthenticator {
public:
  common::Result<PeerAuthenticationResult>
  authenticate(const PeerAuthenticationRequest& request) override {
    captured = request;
    return PeerAuthenticationResult{.authorized = true, .principal_id = 91U};
  }

  PeerAuthenticationRequest captured;
};

TEST(TlsSocketTest, InvalidCredentialConfigurationFailsClosed) {
  const auto empty_fingerprint = TlsSocket{}.peer_certificate_sha256();
  ASSERT_FALSE(empty_fingerprint.has_value());
  EXPECT_EQ(empty_fingerprint.error().code(), common::StatusCode::kInvalidArgument);

  const auto context = TlsServerContext::create({});
  ASSERT_FALSE(context.has_value());
  EXPECT_EQ(context.error().code(), common::StatusCode::kInvalidArgument);

  TlsServerConfig missing = server_config();
  missing.trust_store_file = fixture("missing-ca.pem").string();
  const auto missing_context = TlsServerContext::create(missing);
  ASSERT_FALSE(missing_context.has_value());
  EXPECT_EQ(missing_context.error().code(), common::StatusCode::kUnauthenticated);

  const auto empty_client = TlsClientContext::create({});
  ASSERT_FALSE(empty_client.has_value());
  EXPECT_EQ(empty_client.error().code(), common::StatusCode::kInvalidArgument);

  TlsClientConfig missing_client = client_config();
  missing_client.certificate_chain_file = fixture("missing-client.pem").string();
  const auto missing_client_context = TlsClientContext::create(missing_client);
  ASSERT_FALSE(missing_client_context.has_value());
  EXPECT_EQ(missing_client_context.error().code(), common::StatusCode::kUnauthenticated);

  TlsServerConfig ambiguous = server_config();
  ambiguous.pem_credentials = pem_credentials("server.pem", "server-key.pem");
  const auto ambiguous_context = TlsServerContext::create(ambiguous);
  ASSERT_FALSE(ambiguous_context.has_value());
  EXPECT_EQ(ambiguous_context.error().code(), common::StatusCode::kInvalidArgument);

  auto invalid_pem =
      std::make_shared<TlsPemCredentials>(*pem_credentials("server.pem", "server-key.pem"));
  invalid_pem->certificate_chain = "not a certificate";
  const auto invalid_pem_context =
      TlsServerContext::create({.pem_credentials = std::move(invalid_pem)});
  ASSERT_FALSE(invalid_pem_context.has_value());
  EXPECT_EQ(invalid_pem_context.error().code(), common::StatusCode::kUnauthenticated);
}

TEST(TlsSocketTest, InMemoryPemCredentialsCompleteMutualHandshakeWithoutPaths) {
  const auto server_credentials = pem_credentials("server.pem", "server-key.pem");
  auto client_credentials =
      std::make_shared<TlsPemCredentials>(*pem_credentials("client.pem", "client-key.pem"));
  client_credentials->trust_store += client_credentials->trust_store;
  ASSERT_FALSE(server_credentials->certificate_chain.empty());
  ASSERT_FALSE(server_credentials->private_key.empty());
  ASSERT_FALSE(server_credentials->trust_store.empty());
  auto server_context = TlsServerContext::create(
      {.require_client_certificate = true, .pem_credentials = server_credentials});
  ASSERT_TRUE(server_context.has_value()) << server_context.error().message();
  auto client_context_owner = TlsClientContext::create(
      {.expected_server_identity = "127.0.0.1", .pem_credentials = client_credentials});
  ASSERT_TRUE(client_context_owner.has_value()) << client_context_owner.error().message();
  CapturingAuthenticator authenticator;
  NetworkSecurityConfig security{.mode = TransportSecurityMode::kTlsRequired,
                                 .authenticator = &authenticator,
                                 .tls = TlsServerConfig{.require_client_certificate = true,
                                                        .pem_credentials = server_credentials}};
  EXPECT_TRUE(validate_network_security_config(security, {10U, 0U, 0U, 1U}).is_ok());

  SocketPair sockets = nonblocking_socket_pair();
  auto server = TlsSocket::accept(*server_context, sockets.sockets[0]);
  ASSERT_TRUE(server.has_value()) << server.error().message();
  auto client = TlsSocket::connect(*client_context_owner, sockets.sockets[1]);
  ASSERT_TRUE(client.has_value()) << client.error().message();
  complete_handshake(*server, *client);
  EXPECT_TRUE(server->peer_certificate_sha256().has_value());
  EXPECT_TRUE(client->peer_certificate_sha256().has_value());
}

TEST(TlsSocketTest, SessionsRetainOpenSslContextReferencesAfterFactoryOwnersAreDestroyed) {
  SocketPair sockets = nonblocking_socket_pair();
  std::optional<TlsSocket> server;
  std::optional<TlsSocket> client;
  {
    auto server_context = TlsServerContext::create(server_config());
    auto client_context_owner = TlsClientContext::create(client_config());
    ASSERT_TRUE(server_context.has_value()) << server_context.error().message();
    ASSERT_TRUE(client_context_owner.has_value()) << client_context_owner.error().message();
    auto accepted = TlsSocket::accept(*server_context, sockets.sockets[0]);
    auto connected = TlsSocket::connect(*client_context_owner, sockets.sockets[1]);
    ASSERT_TRUE(accepted.has_value()) << accepted.error().message();
    ASSERT_TRUE(connected.has_value()) << connected.error().message();
    server.emplace(std::move(*accepted));
    client.emplace(std::move(*connected));
  }

  complete_handshake(*server, *client);
  EXPECT_TRUE(server->peer_certificate_sha256().has_value());
  EXPECT_TRUE(client->peer_certificate_sha256().has_value());
}

TEST(TlsSocketTest, ReusesSignalSafeBioMethodAcrossSessionLifetimes) {
  auto server_context = TlsServerContext::create(server_config());
  ASSERT_TRUE(server_context.has_value()) << server_context.error().message();
  auto client_context_owner = TlsClientContext::create(client_config());
  ASSERT_TRUE(client_context_owner.has_value()) << client_context_owner.error().message();

  for (std::size_t session = 0U; session < 256U; ++session) {
    SocketPair sockets = nonblocking_socket_pair();
    auto server = TlsSocket::accept(*server_context, sockets.sockets[0]);
    ASSERT_TRUE(server.has_value())
        << "server session " << session << ": " << server.error().message();
    auto client = TlsSocket::connect(*client_context_owner, sockets.sockets[1]);
    ASSERT_TRUE(client.has_value())
        << "client session " << session << ": " << client.error().message();
  }
}

TEST(TlsSocketTest, MutualHandshakeCarriesVerifiedIdentityAndPlaintext) {
  auto server_context = TlsServerContext::create(server_config());
  ASSERT_TRUE(server_context.has_value()) << server_context.error().message();
  auto client_context_owner = TlsClientContext::create(client_config());
  ASSERT_TRUE(client_context_owner.has_value()) << client_context_owner.error().message();
  SocketPair sockets = nonblocking_socket_pair();
  auto server = TlsSocket::accept(*server_context, sockets.sockets[0]);
  ASSERT_TRUE(server.has_value()) << server.error().message();
  auto client = TlsSocket::connect(*client_context_owner, sockets.sockets[1]);
  ASSERT_TRUE(client.has_value()) << client.error().message();
  const auto fingerprint_before_handshake = server->peer_certificate_sha256();
  ASSERT_FALSE(fingerprint_before_handshake.has_value());
  EXPECT_EQ(fingerprint_before_handshake.error().code(), common::StatusCode::kInvalidArgument);

  complete_handshake(*server, *client);
  auto client_fingerprint = server->peer_certificate_sha256();
  ASSERT_TRUE(client_fingerprint.has_value());
  EXPECT_NE(*client_fingerprint, PeerCertificateSha256{});
  auto server_fingerprint = client->peer_certificate_sha256();
  ASSERT_TRUE(server_fingerprint.has_value());
  EXPECT_NE(*server_fingerprint, PeerCertificateSha256{});
  EXPECT_NE(*server_fingerprint, *client_fingerprint);

  CapturingAuthenticator authenticator;
  NetworkSecurityConfig security{.mode = TransportSecurityMode::kTlsRequired,
                                 .authenticator = &authenticator,
                                 .tls = server_config()};
  const auto authentication =
      authenticate_peer(security, {.ipv4_address = {10U, 0U, 0U, 7U},
                                   .transport_authenticated = true,
                                   .peer_certificate_sha256 = *client_fingerprint});
  ASSERT_TRUE(authentication.has_value());
  EXPECT_EQ(authentication->principal_id, 91U);
  EXPECT_EQ(authenticator.captured.peer_certificate_sha256, client_fingerprint);

  constexpr std::array<std::byte, 5> request{std::byte{'h'}, std::byte{'e'}, std::byte{'l'},
                                             std::byte{'l'}, std::byte{'o'}};
  auto client_write = client->write(request);
  ASSERT_TRUE(client_write.has_value()) << client_write.error().message();
  ASSERT_EQ(client_write->state, TlsIoState::kComplete);
  ASSERT_EQ(client_write->bytes_transferred, request.size());
  std::array<std::byte, 2> received{};
  auto read = server->read(received);
  ASSERT_TRUE(read.has_value()) << read.error().message();
  ASSERT_EQ(read->state, TlsIoState::kComplete);
  EXPECT_EQ(read->bytes_transferred, received.size());
  EXPECT_TRUE(std::ranges::equal(std::span{received}, std::span{request}.first(received.size())));
  EXPECT_EQ(server->pending_plaintext_bytes(), request.size() - received.size());
  std::array<std::byte, 16> remaining{};
  read = server->read(remaining);
  ASSERT_TRUE(read.has_value()) << read.error().message();
  ASSERT_EQ(read->state, TlsIoState::kComplete);
  EXPECT_TRUE(std::ranges::equal(std::span{remaining}.first(read->bytes_transferred),
                                 std::span{request}.subspan(received.size())));
  EXPECT_EQ(server->pending_plaintext_bytes(), 0U);

  constexpr std::array<std::byte, 2> response{std::byte{'o'}, std::byte{'k'}};
  auto write = server->write(response);
  ASSERT_TRUE(write.has_value()) << write.error().message();
  ASSERT_EQ(write->state, TlsIoState::kComplete);
  EXPECT_EQ(write->bytes_transferred, response.size());
  std::array<std::byte, 8> client_received{};
  const auto client_read = client->read(client_received);
  ASSERT_TRUE(client_read.has_value()) << client_read.error().message();
  EXPECT_EQ(client_read->state, TlsIoState::kComplete);
  EXPECT_TRUE(std::ranges::equal(std::span{client_received}.first(client_read->bytes_transferred),
                                 std::span{response}));
}

TEST(TlsSocketTest, AbruptPeerCloseNeverRaisesSigpipe) {
  auto server_context = TlsServerContext::create(server_config());
  ASSERT_TRUE(server_context.has_value()) << server_context.error().message();
  auto client_context_owner = TlsClientContext::create(client_config());
  ASSERT_TRUE(client_context_owner.has_value()) << client_context_owner.error().message();
  SocketPair sockets = nonblocking_socket_pair();
  auto server = TlsSocket::accept(*server_context, sockets.sockets[0]);
  ASSERT_TRUE(server.has_value()) << server.error().message();
  auto client = TlsSocket::connect(*client_context_owner, sockets.sockets[1]);
  ASSERT_TRUE(client.has_value()) << client.error().message();
  complete_handshake(*server, *client);

  constexpr std::array<std::byte, 1U> initial_byte{std::byte{0x5a}};
  const auto initial_write = server->write(initial_byte);
  ASSERT_TRUE(initial_write.has_value()) << initial_write.error().message();
  ASSERT_EQ(initial_write->state, TlsIoState::kComplete);
  std::array<std::byte, 1U> initial_read_buffer{};
  const auto initial_read = client->read(initial_read_buffer);
  ASSERT_TRUE(initial_read.has_value()) << initial_read.error().message();
  ASSERT_EQ(initial_read->state, TlsIoState::kComplete);
  ASSERT_EQ(initial_read->bytes_transferred, initial_read_buffer.size());

  SigpipeObserver observer;
  ASSERT_TRUE(observer.installed());
  sigpipe_observed = 0;
  const linger reset_on_close{.l_onoff = 1, .l_linger = 0};
  ASSERT_EQ(::setsockopt(sockets.sockets[1], SOL_SOCKET, SO_LINGER, &reset_on_close,
                         sizeof(reset_on_close)),
            0);
  *client = TlsSocket{};
  ASSERT_EQ(::close(sockets.sockets[1]), 0);
  sockets.sockets[1] = -1;
  pollfd peer_close{.fd = sockets.sockets[0], .events = POLLIN, .revents = 0};
  ASSERT_GT(::poll(&peer_close, 1U, 1000), 0);
  ASSERT_NE(peer_close.revents & (POLLIN | POLLERR | POLLHUP), 0);

  const std::array<std::byte, std::size_t{16U} * 1024U> bytes{};
  common::Status write_failure;
  bool write_terminated = false;
  bool write_failed = false;
  for (std::size_t attempt = 0U; attempt < 1024U && !write_terminated; ++attempt) {
    const auto written = server->write(bytes);
    if (!written.has_value()) {
      write_failure = written.error();
      write_failed = true;
      write_terminated = true;
    } else if (written->state == TlsIoState::kClosed) {
      write_terminated = true;
    }
  }
  ASSERT_TRUE(write_terminated);
  if (write_failed) {
    EXPECT_EQ(write_failure.code(), common::StatusCode::kIoError);
  }
  EXPECT_EQ(sigpipe_observed, 0);
}

TEST(TlsSocketTest, ClientRejectsServerCertificateForDifferentIdentity) {
  auto server_context = TlsServerContext::create(server_config());
  ASSERT_TRUE(server_context.has_value());
  TlsClientConfig wrong_identity = client_config();
  wrong_identity.expected_server_identity = "127.0.0.2";
  auto client_context_owner = TlsClientContext::create(wrong_identity);
  ASSERT_TRUE(client_context_owner.has_value());
  SocketPair sockets = nonblocking_socket_pair();
  auto server = TlsSocket::accept(*server_context, sockets.sockets[0]);
  ASSERT_TRUE(server.has_value());
  auto client = TlsSocket::connect(*client_context_owner, sockets.sockets[1]);
  ASSERT_TRUE(client.has_value());

  common::Status client_error;
  bool client_failed = false;
  for (std::size_t attempt = 0U; attempt < 1024U && !client_failed; ++attempt) {
    if (!server->handshake_complete()) {
      const auto server_progress = server->handshake();
      ASSERT_TRUE(server_progress.has_value()) << server_progress.error().message();
    }
    auto progress = client->handshake();
    if (!progress.has_value()) {
      client_error = progress.error();
      client_failed = true;
    }
  }
  ASSERT_TRUE(client_failed);
  EXPECT_EQ(client_error.code(), common::StatusCode::kUnauthenticated);
  EXPECT_FALSE(client->handshake_complete());
}

TEST(TlsSocketTest, MissingClientCertificateCannotCompleteServerHandshake) {
  auto context = TlsServerContext::create(server_config());
  ASSERT_TRUE(context.has_value());
  SocketPair sockets = nonblocking_socket_pair();
  auto server = TlsSocket::accept(*context, sockets.sockets[0]);
  ASSERT_TRUE(server.has_value());
  ClientContext client_context_owner = client_context(false);
  ASSERT_NE(client_context_owner, nullptr);
  ClientSession client = client_session(client_context_owner.get(), sockets.sockets[1]);
  ASSERT_NE(client, nullptr);

  bool rejected = false;
  for (std::size_t attempt = 0U; attempt < 1024U && !rejected; ++attempt) {
    const auto server_progress = server->handshake();
    if (!server_progress.has_value()) {
      rejected = true;
      break;
    }
    const int client_result = SSL_do_handshake(client.get());
    if (client_result != 1) {
      const int error = SSL_get_error(client.get(), client_result);
      if (error != SSL_ERROR_WANT_READ && error != SSL_ERROR_WANT_WRITE)
        rejected = true;
    }
  }
  EXPECT_TRUE(rejected);
  EXPECT_FALSE(server->handshake_complete());
}

} // namespace
} // namespace chronos::network
