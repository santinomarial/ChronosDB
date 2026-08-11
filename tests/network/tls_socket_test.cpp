#include "chronos/network/security.hpp"
#include "chronos/network/tls_socket.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <openssl/ssl.h>
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

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] TlsServerConfig server_config() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
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

[[nodiscard]] bool advance_client_handshake(SSL* session) {
  const int result = SSL_do_handshake(session);
  if (result == 1)
    return true;
  const int error = SSL_get_error(session, result);
  EXPECT_TRUE(error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE);
  return false;
}

void complete_handshake(TlsSocket& server, SSL* client) {
  bool client_complete = false;
  for (std::size_t attempt = 0U; attempt < 1024U; ++attempt) {
    if (!server.handshake_complete()) {
      auto progress = server.handshake();
      ASSERT_TRUE(progress.has_value()) << progress.error().message();
    }
    if (!client_complete)
      client_complete = advance_client_handshake(client);
    if (server.handshake_complete() && client_complete)
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
  const auto context = TlsServerContext::create({});
  ASSERT_FALSE(context.has_value());
  EXPECT_EQ(context.error().code(), common::StatusCode::kInvalidArgument);

  TlsServerConfig missing = server_config();
  missing.trust_store_file = fixture("missing-ca.pem").string();
  const auto missing_context = TlsServerContext::create(missing);
  ASSERT_FALSE(missing_context.has_value());
  EXPECT_EQ(missing_context.error().code(), common::StatusCode::kUnauthenticated);
}

TEST(TlsSocketTest, MutualHandshakeCarriesVerifiedIdentityAndPlaintext) {
  auto context = TlsServerContext::create(server_config());
  ASSERT_TRUE(context.has_value()) << context.error().message();
  SocketPair sockets = nonblocking_socket_pair();
  auto server = TlsSocket::accept(*context, sockets.sockets[0]);
  ASSERT_TRUE(server.has_value()) << server.error().message();
  ClientContext client_context_owner = client_context(true);
  ASSERT_NE(client_context_owner, nullptr);
  ClientSession client = client_session(client_context_owner.get(), sockets.sockets[1]);
  ASSERT_NE(client, nullptr);

  complete_handshake(*server, client.get());
  auto fingerprint = server->peer_certificate_sha256();
  ASSERT_TRUE(fingerprint.has_value());
  EXPECT_NE(*fingerprint, PeerCertificateSha256{});

  CapturingAuthenticator authenticator;
  NetworkSecurityConfig security{.mode = TransportSecurityMode::kTlsRequired,
                                 .authenticator = &authenticator,
                                 .tls = server_config()};
  const auto authentication =
      authenticate_peer(security, {.ipv4_address = {10U, 0U, 0U, 7U},
                                   .transport_authenticated = true,
                                   .peer_certificate_sha256 = *fingerprint});
  ASSERT_TRUE(authentication.has_value());
  EXPECT_EQ(authentication->principal_id, 91U);
  EXPECT_EQ(authenticator.captured.peer_certificate_sha256, fingerprint);

  constexpr std::array<std::byte, 5> request{std::byte{'h'}, std::byte{'e'}, std::byte{'l'},
                                             std::byte{'l'}, std::byte{'o'}};
  std::size_t client_written{};
  ASSERT_EQ(SSL_write_ex(client.get(), request.data(), request.size(), &client_written), 1);
  ASSERT_EQ(client_written, request.size());
  std::array<std::byte, 16> received{};
  auto read = server->read(received);
  ASSERT_TRUE(read.has_value()) << read.error().message();
  ASSERT_EQ(read->state, TlsIoState::kComplete);
  EXPECT_TRUE(
      std::ranges::equal(std::span{received}.first(read->bytes_transferred), std::span{request}));

  constexpr std::array<std::byte, 2> response{std::byte{'o'}, std::byte{'k'}};
  auto write = server->write(response);
  ASSERT_TRUE(write.has_value()) << write.error().message();
  ASSERT_EQ(write->state, TlsIoState::kComplete);
  EXPECT_EQ(write->bytes_transferred, response.size());
  std::array<std::byte, 8> client_received{};
  std::size_t client_read{};
  ASSERT_EQ(SSL_read_ex(client.get(), client_received.data(), client_received.size(), &client_read),
            1);
  EXPECT_TRUE(
      std::ranges::equal(std::span{client_received}.first(client_read), std::span{response}));
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
