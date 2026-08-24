#ifndef CHRONOS_NETWORK_NATIVE_QUERY_TCP_CLIENT_HPP_
#define CHRONOS_NETWORK_NATIVE_QUERY_TCP_CLIENT_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/native_node_principal_authorizer.hpp"
#include "chronos/network/native_query_retry.hpp"
#include "chronos/network/security.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace chronos::network {

struct NativeQueryTcpClientLimits {
  std::chrono::milliseconds connect_timeout{5000};
  std::chrono::milliseconds handshake_timeout{5000};
  std::chrono::milliseconds exchange_timeout{30000};
  std::size_t maximum_io_chunk_bytes{std::size_t{64U} * 1024U};
};

struct NativeQueryTcpClientConfig {
  NativeQueryRetryConfig retry;
  ConnectionAuthenticator* authenticator{};
  const NativeNodePrincipalAuthorizer* node_authorizer{};
  NativeQueryTcpClientLimits limits;
};

enum class NativeQueryTcpClientState : std::uint8_t {
  kConnecting = 1,
  kHandshaking = 2,
  kExchanging = 3,
  kComplete = 4,
  kFailed = 5,
};

struct NativeQueryTcpInterest {
  bool want_read{};
  bool want_write{};
};

// Owns the nonblocking TCP/TLS carrier for one exact redirected finite query. One event-loop thread
// serializes calls. Route TLS contexts, the principal authenticator, and node authorizer are
// borrowed and must outlive the client. Only an authenticated protocol redirect can start another
// connection; ambiguous transport failures are terminal and partial results are never published.
class NativeQueryTcpClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  NativeQueryTcpClient() = delete;
  ~NativeQueryTcpClient();
  NativeQueryTcpClient(const NativeQueryTcpClient&) = delete;
  NativeQueryTcpClient& operator=(const NativeQueryTcpClient&) = delete;
  NativeQueryTcpClient(NativeQueryTcpClient&&) noexcept;
  NativeQueryTcpClient& operator=(NativeQueryTcpClient&&) noexcept;

  [[nodiscard]] static common::Result<NativeQueryTcpClient> begin(NativeQueryTcpClientConfig config,
                                                                  std::string sql, TimePoint now);

  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);

  [[nodiscard]] NativeQueryTcpClientState state() const noexcept;
  [[nodiscard]] NativeQueryTcpInterest interest() const noexcept;
  [[nodiscard]] int descriptor() const noexcept;
  [[nodiscard]] std::optional<TimePoint> deadline() const noexcept;
  [[nodiscard]] NativeLeaderRoute current_route() const noexcept;
  [[nodiscard]] std::size_t attempts_started() const noexcept;
  [[nodiscard]] const std::optional<NativeQueryResult>& result() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit NativeQueryTcpClient(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::network

#endif // CHRONOS_NETWORK_NATIVE_QUERY_TCP_CLIENT_HPP_
