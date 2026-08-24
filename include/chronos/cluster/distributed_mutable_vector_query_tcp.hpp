#ifndef CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_QUERY_TCP_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_QUERY_TCP_HPP_

#include "chronos/cluster/distributed_mutable_vector_query_tls.hpp"
#include "chronos/cluster/distributed_vector_query_tcp_client_v2.hpp"
#include "chronos/cluster/distributed_vector_query_tcp_server_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <chrono>
#include <cstddef>
#include <memory>
#include <span>

namespace chronos::cluster {

struct DistributedMutableVectorQueryTcpClientConfig {
  network::Ipv4Endpoint remote_endpoint;
  const network::TlsClientContext* tls_context{};
  DistributedMutableVectorQueryTlsClientConfig carrier;
  std::chrono::milliseconds connect_timeout{5000};
};

using DistributedMutableVectorQueryTcpClientState = DistributedVectorQueryTcpClientStateV2;

class DistributedMutableVectorQueryTcpClient {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedMutableVectorQueryTcpClient() = delete;
  ~DistributedMutableVectorQueryTcpClient();
  DistributedMutableVectorQueryTcpClient(const DistributedMutableVectorQueryTcpClient&) = delete;
  DistributedMutableVectorQueryTcpClient&
  operator=(const DistributedMutableVectorQueryTcpClient&) = delete;
  DistributedMutableVectorQueryTcpClient(DistributedMutableVectorQueryTcpClient&&) noexcept;
  DistributedMutableVectorQueryTcpClient&
  operator=(DistributedMutableVectorQueryTcpClient&&) noexcept;

  [[nodiscard]] static common::Result<DistributedMutableVectorQueryTcpClient>
  begin(DistributedMutableVectorQueryAttempt attempt,
        DistributedMutableVectorQueryTcpClientConfig config, TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedMutableVectorQueryTcpClientState state() const noexcept;
  [[nodiscard]] DistributedMutableVectorQueryTlsInterest interest() const noexcept;
  [[nodiscard]] int descriptor() const noexcept;
  [[nodiscard]] common::Result<std::span<const DistributedVectorQueryResponseV2>> responses() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedMutableVectorQueryTcpClient(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

struct DistributedMutableVectorQueryTcpServerConfig {
  network::TcpListenerConfig listener;
  network::TlsServerConfig tls;
  network::ConnectionAuthenticator* authenticator{};
  DistributedMutableVectorQueryReceiver* receiver{};
  DistributedMutableVectorQueryTlsLimits carrier_limits;
  std::size_t maximum_connections{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

using DistributedMutableVectorQueryTcpServerMetrics = DistributedVectorQueryTcpServerMetricsV2;

// Portable POSIX poll owner for one authenticated mutable request per connection. One thread owns
// all calls. Authentication/receiver dependencies are borrowed and must outlive the server.
class DistributedMutableVectorQueryTcpServer {
public:
  DistributedMutableVectorQueryTcpServer() noexcept;
  ~DistributedMutableVectorQueryTcpServer();
  DistributedMutableVectorQueryTcpServer(const DistributedMutableVectorQueryTcpServer&) = delete;
  DistributedMutableVectorQueryTcpServer&
  operator=(const DistributedMutableVectorQueryTcpServer&) = delete;
  DistributedMutableVectorQueryTcpServer(DistributedMutableVectorQueryTcpServer&&) noexcept;
  DistributedMutableVectorQueryTcpServer&
  operator=(DistributedMutableVectorQueryTcpServer&&) noexcept;

  [[nodiscard]] static common::Result<DistributedMutableVectorQueryTcpServer>
  start(DistributedMutableVectorQueryTcpServerConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] DistributedMutableVectorQueryTcpServerMetrics metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;

private:
  class Impl;
  explicit DistributedMutableVectorQueryTcpServer(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_QUERY_TCP_HPP_
