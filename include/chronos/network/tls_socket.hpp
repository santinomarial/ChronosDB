#ifndef CHRONOS_NETWORK_TLS_SOCKET_HPP_
#define CHRONOS_NETWORK_TLS_SOCKET_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace chronos::network {

using PeerCertificateSha256 = std::array<std::uint8_t, 32>;

struct TlsServerConfig {
  std::string certificate_chain_file;
  std::string private_key_file;
  std::string trust_store_file;
  bool require_client_certificate{true};
};

struct TlsClientConfig {
  std::string certificate_chain_file;
  std::string private_key_file;
  std::string trust_store_file;
  // Required DNS name or IP address matched against the server certificate SAN.
  std::string expected_server_identity;
};

enum class TlsIoState : std::uint8_t { kComplete, kWantRead, kWantWrite, kClosed };

struct TlsIoResult {
  TlsIoState state{TlsIoState::kComplete};
  std::size_t bytes_transferred{};
};

// Owns an OpenSSL server context. It is immutable after creation and may be shared by socket
// sessions on one reactor owner thread. OpenSSL types remain private to the implementation.
class TlsServerContext {
public:
  TlsServerContext() noexcept;
  ~TlsServerContext();
  TlsServerContext(const TlsServerContext&) = delete;
  TlsServerContext& operator=(const TlsServerContext&) = delete;
  TlsServerContext(TlsServerContext&&) noexcept;
  TlsServerContext& operator=(TlsServerContext&&) noexcept;

  [[nodiscard]] static common::Result<TlsServerContext> create(const TlsServerConfig& config);

private:
  class Impl;
  explicit TlsServerContext(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;

  friend class TlsSocket;
};

// Owns an immutable OpenSSL client context and its required server identity. It may be shared by
// socket sessions on one reactor owner thread. OpenSSL types remain private to the implementation.
class TlsClientContext {
public:
  TlsClientContext() noexcept;
  ~TlsClientContext();
  TlsClientContext(const TlsClientContext&) = delete;
  TlsClientContext& operator=(const TlsClientContext&) = delete;
  TlsClientContext(TlsClientContext&&) noexcept;
  TlsClientContext& operator=(TlsClientContext&&) noexcept;

  [[nodiscard]] static common::Result<TlsClientContext> create(const TlsClientConfig& config);

private:
  class Impl;
  explicit TlsClientContext(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;

  friend class TlsSocket;
};

// A nonblocking TLS session over a borrowed connected socket. The caller owns the socket
// descriptor and must keep it open until this object is destroyed. One reactor thread must own all
// calls. Plaintext is available only after a mutually authenticated handshake completes.
class TlsSocket {
public:
  TlsSocket() noexcept;
  ~TlsSocket();
  TlsSocket(const TlsSocket&) = delete;
  TlsSocket& operator=(const TlsSocket&) = delete;
  TlsSocket(TlsSocket&&) noexcept;
  TlsSocket& operator=(TlsSocket&&) noexcept;

  [[nodiscard]] static common::Result<TlsSocket> accept(const TlsServerContext& context,
                                                        int connected_socket);
  [[nodiscard]] static common::Result<TlsSocket> connect(const TlsClientContext& context,
                                                         int connected_socket);
  [[nodiscard]] common::Result<TlsIoResult> handshake();
  [[nodiscard]] common::Result<TlsIoResult> read(common::MutableByteView destination);
  [[nodiscard]] common::Result<TlsIoResult> write(common::ByteView source);
  [[nodiscard]] bool handshake_complete() const noexcept;
  [[nodiscard]] std::size_t pending_plaintext_bytes() const noexcept;
  [[nodiscard]] common::Result<PeerCertificateSha256> peer_certificate_sha256() const;

private:
  class Impl;
  explicit TlsSocket(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::network

#endif // CHRONOS_NETWORK_TLS_SOCKET_HPP_
