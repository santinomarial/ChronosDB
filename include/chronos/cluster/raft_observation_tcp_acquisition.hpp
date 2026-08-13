#ifndef CHRONOS_CLUSTER_RAFT_OBSERVATION_TCP_ACQUISITION_HPP_
#define CHRONOS_CLUSTER_RAFT_OBSERVATION_TCP_ACQUISITION_HPP_

#include "chronos/cluster/raft_observation_tcp_client.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::cluster {

struct RaftObservationTcpRoute {
  raft::NodeId node_id{};
  std::vector<network::Ipv4Endpoint> endpoints;
  const network::TlsClientContext* tls_context{};
};

struct RaftObservationTcpRetryLimits {
  std::size_t maximum_attempts{5U};
  std::chrono::milliseconds initial_backoff{50};
  std::chrono::milliseconds maximum_backoff{5000};
};

struct RaftObservationTcpAcquisitionConfig {
  RaftObservationTcpRoute route;
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  RaftObservationRequest request;
  RaftObservationTlsClientLimits carrier_limits;
  std::chrono::milliseconds connect_timeout{5000};
  RaftObservationTcpRetryLimits retry;
};

struct RaftObservationTcpAcquisitionMetrics {
  std::uint64_t attempts_started{};
  std::uint64_t retries_started{};
  std::uint64_t completed_attempts{};
  std::uint64_t failed_attempts{};
  std::size_t active_attempts{};
};

enum class RaftObservationTcpAcquisitionState : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
  kCancelled = 4,
};

// Single-threaded poll owner for one exact remote observation. The route retains a finite ordered
// address snapshot for one target node; retries rotate addresses without changing the request
// authority or expanding the configured attempt budget. Borrowed TLS and authentication objects
// must outlive the acquisition.
class RaftObservationTcpAcquisition {
public:
  RaftObservationTcpAcquisition() noexcept;
  ~RaftObservationTcpAcquisition();
  RaftObservationTcpAcquisition(const RaftObservationTcpAcquisition&) = delete;
  RaftObservationTcpAcquisition& operator=(const RaftObservationTcpAcquisition&) = delete;
  RaftObservationTcpAcquisition(RaftObservationTcpAcquisition&&) noexcept;
  RaftObservationTcpAcquisition& operator=(RaftObservationTcpAcquisition&&) noexcept;

  [[nodiscard]] static common::Result<RaftObservationTcpAcquisition>
  create(RaftObservationTcpAcquisitionConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();

  [[nodiscard]] RaftObservationTcpAcquisitionState state() const noexcept;
  [[nodiscard]] RaftObservationTcpAcquisitionMetrics metrics() const noexcept;
  [[nodiscard]] common::Result<raft::RaftGroupObservation> result() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit RaftObservationTcpAcquisition(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_OBSERVATION_TCP_ACQUISITION_HPP_
