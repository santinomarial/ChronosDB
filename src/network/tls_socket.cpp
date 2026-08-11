#include "chronos/network/tls_socket.hpp"

#include <cerrno>
#include <limits>
#include <new>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <optional>
#include <string>
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
  if (error == SSL_ERROR_SYSCALL && (errno == EAGAIN || errno == EWOULDBLOCK))
    return TlsIoResult{.state = TlsIoState::kWantRead};
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

class TlsSocket::Impl {
public:
  explicit Impl(SSL* session) noexcept : session_(session) {}
  ~Impl() {
    SSL_free(session_);
  }

  SSL* session_{};
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
  if (SSL_set_fd(session, connected_socket) != 1) {
    SSL_free(session);
    return common::make_unexpected(io_error("OpenSSL could not attach the connected socket"));
  }
  SSL_set_accept_state(session);
  try {
    return TlsSocket{std::make_unique<Impl>(session)};
  } catch (const std::bad_alloc&) {
    SSL_free(session);
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
    if (!classified.has_value() || classified->state != TlsIoState::kComplete)
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

common::Result<PeerCertificateSha256> TlsSocket::peer_certificate_sha256() const {
  if (!handshake_complete())
    return common::make_unexpected(invalid("TLS peer identity is unavailable before handshake"));
  return *implementation_->peer_certificate_sha256_;
}

} // namespace chronos::network
