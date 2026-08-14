#include "chronos/network/tls_socket.hpp"

#include <arpa/inet.h>
#include <array>
#include <cerrno>
#include <cstddef>
#include <memory>
#include <mutex>
#include <new>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <optional>
#include <string>
#include <sys/socket.h>
#include <utility>

namespace chronos::network {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status unauthenticated(std::string message) {
  return {common::StatusCode::kUnauthenticated, std::move(message)};
}

[[nodiscard]] common::Status io_error(std::string message) {
  return {common::StatusCode::kIoError, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}

struct SocketBioState {
  int descriptor{-1};
};

[[nodiscard]] SocketBioState* socket_bio_state(BIO* bio) noexcept {
  return static_cast<SocketBioState*>(BIO_get_data(bio));
}

int socket_bio_create(BIO* bio) noexcept {
  auto* state = new (std::nothrow) SocketBioState;
  if (state == nullptr)
    return 0;
  BIO_set_data(bio, state);
  BIO_set_init(bio, 1);
  BIO_set_shutdown(bio, BIO_NOCLOSE);
  return 1;
}

int socket_bio_destroy(BIO* bio) noexcept {
  if (bio == nullptr)
    return 0;
  delete socket_bio_state(bio);
  BIO_set_data(bio, nullptr);
  BIO_set_init(bio, 0);
  return 1;
}

int socket_bio_read(BIO* bio, char* destination, const std::size_t size,
                    std::size_t* transferred) noexcept {
  *transferred = 0U;
  BIO_clear_retry_flags(bio);
  const SocketBioState* state = socket_bio_state(bio);
  if (state == nullptr || state->descriptor < 0 || destination == nullptr || size == 0U) {
    errno = EINVAL;
    return 0;
  }
  const ssize_t result = ::recv(state->descriptor, destination, size, 0);
  if (result <= 0) {
    if (result < 0 && BIO_sock_should_retry(-1) != 0)
      BIO_set_retry_read(bio);
    return 0;
  }
  *transferred = static_cast<std::size_t>(result);
  return 1;
}

int socket_bio_write(BIO* bio, const char* source, const std::size_t size,
                     std::size_t* transferred) noexcept {
  *transferred = 0U;
  BIO_clear_retry_flags(bio);
  const SocketBioState* state = socket_bio_state(bio);
  if (state == nullptr || state->descriptor < 0 || source == nullptr || size == 0U) {
    errno = EINVAL;
    return 0;
  }
#if defined(MSG_NOSIGNAL)
  constexpr int flags = MSG_NOSIGNAL;
#else
  constexpr int flags = 0;
#endif
  const ssize_t result = ::send(state->descriptor, source, size, flags);
  if (result <= 0) {
    if (result < 0 && BIO_sock_should_retry(-1) != 0)
      BIO_set_retry_write(bio);
    return 0;
  }
  *transferred = static_cast<std::size_t>(result);
  return 1;
}

long socket_bio_control(BIO* bio, const int command, long, void* argument) noexcept {
  if (command == BIO_CTRL_FLUSH)
    return 1L;
  if (command != BIO_C_GET_FD)
    return 0L;
  const SocketBioState* state = socket_bio_state(bio);
  const int descriptor = state == nullptr ? -1 : state->descriptor;
  if (argument != nullptr)
    *static_cast<int*>(argument) = descriptor;
  return descriptor;
}

[[nodiscard]] common::Result<BIO_METHOD*> create_socket_bio_method() {
  const int type = BIO_get_new_index();
  if (type < 0)
    return common::make_unexpected(exhausted("OpenSSL socket BIO type limit is exhausted"));
  BIO_METHOD* method =
      BIO_meth_new(type | BIO_TYPE_SOURCE_SINK | BIO_TYPE_DESCRIPTOR, "ChronosDB socket");
  if (method == nullptr)
    return common::make_unexpected(exhausted("OpenSSL socket BIO method allocation failed"));
  if (BIO_meth_set_create(method, &socket_bio_create) != 1 ||
      BIO_meth_set_destroy(method, &socket_bio_destroy) != 1 ||
      BIO_meth_set_read_ex(method, &socket_bio_read) != 1 ||
      BIO_meth_set_write_ex(method, &socket_bio_write) != 1 ||
      BIO_meth_set_ctrl(method, &socket_bio_control) != 1) {
    BIO_meth_free(method);
    return common::make_unexpected(io_error("OpenSSL socket BIO method configuration failed"));
  }
  return method;
}

class SocketBioMethod {
public:
  explicit SocketBioMethod(BIO_METHOD* method) noexcept : method_(method) {}
  ~SocketBioMethod() {
    BIO_meth_free(method_);
  }

  SocketBioMethod(const SocketBioMethod&) = delete;
  SocketBioMethod& operator=(const SocketBioMethod&) = delete;

  [[nodiscard]] BIO_METHOD* get() const noexcept {
    return method_;
  }

private:
  BIO_METHOD* method_{};
};

struct SocketBioMethodRegistry {
  std::mutex mutex;
  std::shared_ptr<SocketBioMethod> method;
};

[[nodiscard]] common::Result<std::shared_ptr<SocketBioMethod>> acquire_socket_bio_method() {
  static SocketBioMethodRegistry registry;
  const std::lock_guard lock(registry.mutex);
  if (registry.method)
    return registry.method;
  auto created = create_socket_bio_method();
  if (!created.has_value())
    return common::make_unexpected(created.error());
  try {
    registry.method = std::make_shared<SocketBioMethod>(*created);
  } catch (const std::bad_alloc&) {
    BIO_meth_free(*created);
    return common::make_unexpected(exhausted("OpenSSL socket BIO method owner allocation failed"));
  }
  return registry.method;
}

[[nodiscard]] common::Result<std::shared_ptr<SocketBioMethod>> attach_socket(SSL* session,
                                                                             const int descriptor) {
#if defined(SO_NOSIGPIPE)
  const int enabled = 1;
  if (::setsockopt(descriptor, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled)) != 0)
    return common::make_unexpected(io_error("TLS socket could not suppress SIGPIPE"));
#elif !defined(MSG_NOSIGNAL)
#error "ChronosDB TLS requires MSG_NOSIGNAL or SO_NOSIGPIPE"
#endif
  auto method = acquire_socket_bio_method();
  if (!method.has_value())
    return common::make_unexpected(method.error());
  BIO* bio = BIO_new((*method)->get());
  if (bio == nullptr)
    return common::make_unexpected(exhausted("OpenSSL socket BIO allocation failed"));
  SocketBioState* state = socket_bio_state(bio);
  if (state == nullptr) {
    BIO_free(bio);
    return common::make_unexpected(io_error("OpenSSL socket BIO state is unavailable"));
  }
  state->descriptor = descriptor;
  if (BIO_up_ref(bio) != 1) {
    BIO_free(bio);
    return common::make_unexpected(exhausted("OpenSSL socket BIO reference allocation failed"));
  }
  SSL_set0_rbio(session, bio);
  SSL_set0_wbio(session, bio);
  return *method;
}

[[nodiscard]] common::Result<TlsIoResult> classify_io(SSL* ssl, const int result,
                                                      const char* operation) {
  const int error = SSL_get_error(ssl, result);
  if (error == SSL_ERROR_WANT_READ)
    return TlsIoResult{.state = TlsIoState::kWantRead};
  if (error == SSL_ERROR_WANT_WRITE)
    return TlsIoResult{.state = TlsIoState::kWantWrite};
  if (error == SSL_ERROR_ZERO_RETURN)
    return TlsIoResult{.state = TlsIoState::kClosed};
  if (error == SSL_ERROR_SYSCALL && result == 0)
    return TlsIoResult{.state = TlsIoState::kClosed};
  return common::make_unexpected(io_error(std::string(operation) + " failed"));
}

} // namespace

class TlsServerContext::Impl {
public:
  explicit Impl(SSL_CTX* context) noexcept : context_(context) {}
  ~Impl() {
    SSL_CTX_free(context_);
  }

  SSL_CTX* context_{};
};

class TlsClientContext::Impl {
public:
  Impl(SSL_CTX* context, std::string expected_server_identity) noexcept
      : context_(context), expected_server_identity_(std::move(expected_server_identity)) {}
  ~Impl() {
    SSL_CTX_free(context_);
  }

  SSL_CTX* context_{};
  std::string expected_server_identity_;
};

class TlsSocket::Impl {
public:
  Impl(SSL* session, std::shared_ptr<SocketBioMethod> socket_bio_method) noexcept
      : session_(session), socket_bio_method_(std::move(socket_bio_method)) {}
  ~Impl() {
    SSL_free(session_);
  }

  SSL* session_{};
  std::shared_ptr<SocketBioMethod> socket_bio_method_;
  std::optional<PeerCertificateSha256> peer_certificate_sha256_;
};

TlsServerContext::TlsServerContext() noexcept = default;
TlsServerContext::TlsServerContext(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
TlsServerContext::~TlsServerContext() = default;
TlsServerContext::TlsServerContext(TlsServerContext&&) noexcept = default;
TlsServerContext& TlsServerContext::operator=(TlsServerContext&&) noexcept = default;

common::Result<TlsServerContext> TlsServerContext::create(const TlsServerConfig& config) {
  if (config.certificate_chain_file.empty() || config.private_key_file.empty() ||
      config.trust_store_file.empty())
    return common::make_unexpected(invalid("TLS credential paths must not be empty"));
  if (!config.require_client_certificate)
    return common::make_unexpected(
        invalid("ChronosDB TLS server connections require a client certificate"));

  ERR_clear_error();
  SSL_CTX* context = SSL_CTX_new(TLS_server_method());
  if (context == nullptr)
    return common::make_unexpected(exhausted("OpenSSL server context allocation failed"));

  const auto release_on_error =
      [&context](common::Status status) -> common::Result<TlsServerContext> {
    SSL_CTX_free(context);
    context = nullptr;
    return common::make_unexpected(std::move(status));
  };
  if (SSL_CTX_set_min_proto_version(context, TLS1_2_VERSION) != 1)
    return release_on_error(io_error("OpenSSL could not require TLS 1.2 or newer"));
  SSL_CTX_set_options(context, SSL_OP_NO_COMPRESSION | SSL_OP_NO_RENEGOTIATION);
  SSL_CTX_set_mode(context, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
  SSL_CTX_set_verify(context, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
  if (SSL_CTX_load_verify_locations(context, config.trust_store_file.c_str(), nullptr) != 1)
    return release_on_error(unauthenticated("TLS trust store could not be loaded"));
  if (SSL_CTX_use_certificate_chain_file(context, config.certificate_chain_file.c_str()) != 1)
    return release_on_error(unauthenticated("TLS certificate chain could not be loaded"));
  if (SSL_CTX_use_PrivateKey_file(context, config.private_key_file.c_str(), SSL_FILETYPE_PEM) != 1)
    return release_on_error(unauthenticated("TLS private key could not be loaded"));
  if (SSL_CTX_check_private_key(context) != 1)
    return release_on_error(unauthenticated("TLS private key does not match certificate"));

  try {
    return TlsServerContext{std::make_unique<Impl>(context)};
  } catch (const std::bad_alloc&) {
    return release_on_error(exhausted("TLS server context owner allocation failed"));
  }
}

TlsClientContext::TlsClientContext() noexcept = default;
TlsClientContext::TlsClientContext(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
TlsClientContext::~TlsClientContext() = default;
TlsClientContext::TlsClientContext(TlsClientContext&&) noexcept = default;
TlsClientContext& TlsClientContext::operator=(TlsClientContext&&) noexcept = default;

common::Result<TlsClientContext> TlsClientContext::create(const TlsClientConfig& config) {
  if (config.certificate_chain_file.empty() || config.private_key_file.empty() ||
      config.trust_store_file.empty() || config.expected_server_identity.empty())
    return common::make_unexpected(
        invalid("TLS client credential paths and expected server identity must not be empty"));
  if (config.expected_server_identity.size() > 253U ||
      config.expected_server_identity.find('\0') != std::string::npos)
    return common::make_unexpected(invalid("TLS expected server identity is invalid"));

  ERR_clear_error();
  SSL_CTX* context = SSL_CTX_new(TLS_client_method());
  if (context == nullptr)
    return common::make_unexpected(exhausted("OpenSSL client context allocation failed"));

  const auto release_on_error =
      [&context](common::Status status) -> common::Result<TlsClientContext> {
    SSL_CTX_free(context);
    context = nullptr;
    return common::make_unexpected(std::move(status));
  };
  if (SSL_CTX_set_min_proto_version(context, TLS1_2_VERSION) != 1)
    return release_on_error(io_error("OpenSSL could not require TLS 1.2 or newer"));
  SSL_CTX_set_options(context, SSL_OP_NO_COMPRESSION | SSL_OP_NO_RENEGOTIATION);
  SSL_CTX_set_mode(context, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
  SSL_CTX_set_verify(context, SSL_VERIFY_PEER, nullptr);
  if (SSL_CTX_load_verify_locations(context, config.trust_store_file.c_str(), nullptr) != 1)
    return release_on_error(unauthenticated("TLS trust store could not be loaded"));
  if (SSL_CTX_use_certificate_chain_file(context, config.certificate_chain_file.c_str()) != 1)
    return release_on_error(unauthenticated("TLS certificate chain could not be loaded"));
  if (SSL_CTX_use_PrivateKey_file(context, config.private_key_file.c_str(), SSL_FILETYPE_PEM) != 1)
    return release_on_error(unauthenticated("TLS private key could not be loaded"));
  if (SSL_CTX_check_private_key(context) != 1)
    return release_on_error(unauthenticated("TLS private key does not match certificate"));

  try {
    return TlsClientContext{std::make_unique<Impl>(context, config.expected_server_identity)};
  } catch (const std::bad_alloc&) {
    return release_on_error(exhausted("TLS client context owner allocation failed"));
  }
}

TlsSocket::TlsSocket() noexcept = default;
TlsSocket::TlsSocket(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
TlsSocket::~TlsSocket() = default;
TlsSocket::TlsSocket(TlsSocket&&) noexcept = default;
TlsSocket& TlsSocket::operator=(TlsSocket&&) noexcept = default;

common::Result<TlsSocket> TlsSocket::accept(const TlsServerContext& context,
                                            const int connected_socket) {
  if (!context.implementation_ || connected_socket < 0)
    return common::make_unexpected(invalid("TLS socket creation arguments are invalid"));
  ERR_clear_error();
  SSL* session = SSL_new(context.implementation_->context_);
  if (session == nullptr)
    return common::make_unexpected(exhausted("OpenSSL session allocation failed"));
  auto socket_bio_method = attach_socket(session, connected_socket);
  if (!socket_bio_method.has_value()) {
    SSL_free(session);
    return common::make_unexpected(socket_bio_method.error());
  }
  SSL_set_accept_state(session);
  try {
    return TlsSocket{std::make_unique<Impl>(session, *socket_bio_method)};
  } catch (const std::bad_alloc&) {
    SSL_free(session);
    return common::make_unexpected(exhausted("TLS socket owner allocation failed"));
  }
}

common::Result<TlsSocket> TlsSocket::connect(const TlsClientContext& context,
                                             const int connected_socket) {
  if (!context.implementation_ || connected_socket < 0)
    return common::make_unexpected(invalid("TLS socket creation arguments are invalid"));
  ERR_clear_error();
  SSL* session = SSL_new(context.implementation_->context_);
  if (session == nullptr)
    return common::make_unexpected(exhausted("OpenSSL session allocation failed"));
  const auto release_on_error = [&session](common::Status status) -> common::Result<TlsSocket> {
    SSL_free(session);
    session = nullptr;
    return common::make_unexpected(std::move(status));
  };
  const std::string& identity = context.implementation_->expected_server_identity_;
  std::array<unsigned char, 16> address{};
  const bool is_ip_address = ::inet_pton(AF_INET, identity.c_str(), address.data()) == 1 ||
                             ::inet_pton(AF_INET6, identity.c_str(), address.data()) == 1;
  X509_VERIFY_PARAM* verification = SSL_get0_param(session);
  if (verification == nullptr)
    return release_on_error(io_error("OpenSSL server identity verifier is unavailable"));
  X509_VERIFY_PARAM_set_hostflags(verification, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
  if (is_ip_address) {
    if (X509_VERIFY_PARAM_set1_ip_asc(verification, identity.c_str()) != 1)
      return release_on_error(invalid("TLS expected IP identity is invalid"));
  } else {
    if (SSL_set1_host(session, identity.c_str()) != 1 ||
        SSL_set_tlsext_host_name(session, identity.c_str()) != 1)
      return release_on_error(io_error("OpenSSL could not configure the expected DNS identity"));
  }
  SSL_set_connect_state(session);
  auto socket_bio_method = attach_socket(session, connected_socket);
  if (!socket_bio_method.has_value())
    return release_on_error(socket_bio_method.error());
  try {
    return TlsSocket{std::make_unique<Impl>(session, *socket_bio_method)};
  } catch (const std::bad_alloc&) {
    SSL_free(session);
    session = nullptr;
    return common::make_unexpected(exhausted("TLS socket owner allocation failed"));
  }
}

common::Result<TlsIoResult> TlsSocket::handshake() {
  if (!implementation_)
    return common::make_unexpected(invalid("TLS socket is empty"));
  if (implementation_->peer_certificate_sha256_.has_value())
    return TlsIoResult{};

  ERR_clear_error();
  const int result = SSL_do_handshake(implementation_->session_);
  if (result != 1) {
    auto classified = classify_io(implementation_->session_, result, "TLS handshake");
    if (classified.has_value())
      return classified;
    if (SSL_get_verify_result(implementation_->session_) != X509_V_OK)
      return common::make_unexpected(unauthenticated("TLS peer certificate verification failed"));
    return classified;
  }
  if (SSL_get_verify_result(implementation_->session_) != X509_V_OK)
    return common::make_unexpected(unauthenticated("TLS peer certificate verification failed"));
  X509* certificate = SSL_get1_peer_certificate(implementation_->session_);
  if (certificate == nullptr)
    return common::make_unexpected(unauthenticated("TLS peer certificate is required"));
  PeerCertificateSha256 fingerprint{};
  unsigned int digest_size{};
  const int digested = X509_digest(certificate, EVP_sha256(), fingerprint.data(), &digest_size);
  X509_free(certificate);
  if (digested != 1 || digest_size != fingerprint.size())
    return common::make_unexpected(io_error("TLS peer certificate fingerprint failed"));
  implementation_->peer_certificate_sha256_ = fingerprint;
  return TlsIoResult{};
}

common::Result<TlsIoResult> TlsSocket::read(const common::MutableByteView destination) {
  if (!handshake_complete())
    return common::make_unexpected(invalid("TLS read requires a completed handshake"));
  if (destination.empty())
    return common::make_unexpected(invalid("TLS read destination must not be empty"));
  ERR_clear_error();
  std::size_t transferred{};
  const int result =
      SSL_read_ex(implementation_->session_, destination.data(), destination.size(), &transferred);
  if (result == 1)
    return TlsIoResult{.bytes_transferred = transferred};
  return classify_io(implementation_->session_, result, "TLS read");
}

common::Result<TlsIoResult> TlsSocket::write(const common::ByteView source) {
  if (!handshake_complete())
    return common::make_unexpected(invalid("TLS write requires a completed handshake"));
  if (source.empty())
    return common::make_unexpected(invalid("TLS write source must not be empty"));
  ERR_clear_error();
  std::size_t transferred{};
  const int result =
      SSL_write_ex(implementation_->session_, source.data(), source.size(), &transferred);
  if (result == 1)
    return TlsIoResult{.bytes_transferred = transferred};
  return classify_io(implementation_->session_, result, "TLS write");
}

bool TlsSocket::handshake_complete() const noexcept {
  return implementation_ && implementation_->peer_certificate_sha256_.has_value();
}

std::size_t TlsSocket::pending_plaintext_bytes() const noexcept {
  if (!implementation_)
    return 0U;
  return static_cast<std::size_t>(SSL_pending(implementation_->session_));
}

common::Result<PeerCertificateSha256> TlsSocket::peer_certificate_sha256() const {
  if (!handshake_complete())
    return common::make_unexpected(invalid("TLS peer identity is unavailable before handshake"));
  return *implementation_->peer_certificate_sha256_;
}

} // namespace chronos::network
