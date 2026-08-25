#ifndef CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TCP_ACQUISITION_HPP_
#define CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TCP_ACQUISITION_HPP_

#include "chronos/cluster/raft_read_authority_tcp_client.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::cluster {

struct RaftReadAuthorityTcpRoute {
  raft::NodeId node_id{};
  std::vector<network::Ipv4Endpoint> endpoints;
  const network::TlsClientContext* tls_context{};
};

struct RaftReadAuthorityTcpRetryLimits {
  std::size_t maximum_attempts{5U};
  std::chrono::milliseconds initial_backoff{50};
  std::chrono::milliseconds maximum_backoff{5000};
};

struct RaftReadAuthorityTcpAcquisitionConfig {
  RaftReadAuthorityTcpRoute route;
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  RaftReadAuthorityRequest request;
  RaftReadAuthorityTlsClientLimits carrier_limits;
  std::chrono::milliseconds connect_timeout{5000};
  RaftReadAuthorityTcpRetryLimits retry;
};

struct RaftReadAuthorityTcpAcquisitionMetrics {
  std::uint64_t attempts_started{};
  std::uint64_t retries_started{};
  std::uint64_t completed_attempts{};
  std::uint64_t failed_attempts{};
  std::size_t active_attempts{};
};

enum class RaftReadAuthorityTcpAcquisitionState : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
  kCancelled = 4,
};

// Single-threaded poll owner for one immutable remote authority request. Retries rotate only the
// finite route's addresses and never rebind node, group, correlation, TLS policy, or attempt
// budget.
class RaftReadAuthorityTcpAcquisition {
public:
  RaftReadAuthorityTcpAcquisition() noexcept;
  ~RaftReadAuthorityTcpAcquisition();
  RaftReadAuthorityTcpAcquisition(const RaftReadAuthorityTcpAcquisition&) = delete;
  RaftReadAuthorityTcpAcquisition& operator=(const RaftReadAuthorityTcpAcquisition&) = delete;
  RaftReadAuthorityTcpAcquisition(RaftReadAuthorityTcpAcquisition&&) noexcept;
  RaftReadAuthorityTcpAcquisition& operator=(RaftReadAuthorityTcpAcquisition&&) noexcept;

  [[nodiscard]] static common::Result<RaftReadAuthorityTcpAcquisition>
  create(RaftReadAuthorityTcpAcquisitionConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();
  [[nodiscard]] RaftReadAuthorityTcpAcquisitionState state() const noexcept;
  [[nodiscard]] RaftReadAuthorityTcpAcquisitionMetrics metrics() const noexcept;
  [[nodiscard]] int descriptor() const noexcept;
  [[nodiscard]] RaftReadAuthorityTlsInterest interest() const noexcept;
  [[nodiscard]] std::optional<RaftReadAuthorityTcpClient::TimePoint> wake_deadline() const noexcept;
  [[nodiscard]] common::Result<RaftReadAuthority> result() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit RaftReadAuthorityTcpAcquisition(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TCP_ACQUISITION_HPP_
