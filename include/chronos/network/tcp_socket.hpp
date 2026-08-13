#ifndef CHRONOS_NETWORK_TCP_SOCKET_HPP_
#define CHRONOS_NETWORK_TCP_SOCKET_HPP_

#include "chronos/common/result.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace chronos::network {

struct Ipv4Endpoint {
  std::array<std::uint8_t, 4> address{};
  std::uint16_t port{};

  friend bool operator==(const Ipv4Endpoint&, const Ipv4Endpoint&) = default;
};

// Parses canonical dotted-decimal IPv4 plus a nonzero decimal port. Leading zeroes, signs,
// whitespace, names, extra separators, and zero addresses are rejected.
[[nodiscard]] common::Result<Ipv4Endpoint> parse_ipv4_endpoint(std::string_view text);

struct Ipv4EndpointResolutionLimits {
  std::size_t maximum_addresses{16U};
  std::size_t maximum_hostname_bytes{253U};
};

// Resolves one strict lowercase DNS-name:port or canonical IPv4 endpoint into an ordered, unique,
// bounded IPv4 candidate set. DNS resolution is a blocking system operation and must run before an
// event-loop owner starts; no answer is cached, so a later whole-operation rebind acquires a fresh
// set. Numeric IPv4 endpoints do not enter the system resolver.
[[nodiscard]] common::Result<std::vector<Ipv4Endpoint>>
resolve_ipv4_endpoints(std::string_view text, Ipv4EndpointResolutionLimits limits = {});

enum class TcpConnectState : std::uint8_t { kInProgress = 1, kConnected = 2 };

// Owns one nonblocking, close-on-exec IPv4 TCP descriptor with TCP_NODELAY. TlsSocket may borrow
// descriptor(), but must be destroyed before this owner closes or moves over the descriptor.
class TcpSocket {
public:
  TcpSocket() noexcept;
  ~TcpSocket();
  TcpSocket(const TcpSocket&) = delete;
  TcpSocket& operator=(const TcpSocket&) = delete;
  TcpSocket(TcpSocket&&) noexcept;
  TcpSocket& operator=(TcpSocket&&) noexcept;

  [[nodiscard]] static common::Result<TcpSocket> begin_connect(Ipv4Endpoint remote);
  // Call after writable/error readiness while connect is in progress. Completion is idempotent.
  [[nodiscard]] common::Result<TcpConnectState> finish_connect();
  [[nodiscard]] TcpConnectState connect_state() const noexcept;
  [[nodiscard]] common::Result<Ipv4Endpoint> local_endpoint() const;
  [[nodiscard]] common::Result<Ipv4Endpoint> peer_endpoint() const;
  [[nodiscard]] int descriptor() const noexcept;
  [[nodiscard]] bool valid() const noexcept;
  [[nodiscard]] common::Status close();

private:
  class Impl;
  explicit TcpSocket(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
  friend class TcpListener;
};

struct TcpListenerConfig {
  Ipv4Endpoint bind_endpoint{{127U, 0U, 0U, 1U}, 0U};
  int backlog{128};
  bool reuse_address{true};
};

// Single-owner nonblocking listener. accept_one performs at most one accept and returns nullopt on
// would-block; admission limits and readiness polling remain with the caller.
class TcpListener {
public:
  TcpListener() noexcept;
  ~TcpListener();
  TcpListener(const TcpListener&) = delete;
  TcpListener& operator=(const TcpListener&) = delete;
  TcpListener(TcpListener&&) noexcept;
  TcpListener& operator=(TcpListener&&) noexcept;

  [[nodiscard]] static common::Result<TcpListener> bind(TcpListenerConfig config = {});
  [[nodiscard]] common::Result<std::optional<TcpSocket>> accept_one();
  [[nodiscard]] Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] int descriptor() const noexcept;
  [[nodiscard]] bool valid() const noexcept;
  [[nodiscard]] common::Status close();

private:
  class Impl;
  explicit TcpListener(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::network

#endif // CHRONOS_NETWORK_TCP_SOCKET_HPP_
