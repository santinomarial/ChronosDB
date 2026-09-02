#ifndef CHRONOS_SERVICE_REPLICATED_RAFT_TRANSPORT_RUNTIME_HPP_
#define CHRONOS_SERVICE_REPLICATED_RAFT_TRANSPORT_RUNTIME_HPP_

#include "chronos/cluster/raft_transport_runtime.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tls_socket.hpp"
#include "chronos/raft/async_durable_runtime.hpp"
#include "chronos/service/replicated_peer_config.hpp"

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace chronos::service {

struct ReplicatedRaftTransportTlsCredentials {
  std::string certificate_chain_file{}; // NOLINT(readability-redundant-member-init)
  std::string private_key_file{};       // NOLINT(readability-redundant-member-init)
  std::string trust_store_file{};       // NOLINT(readability-redundant-member-init)
  std::shared_ptr<const network::TlsPemCredentials>
      pem_credentials{}; // NOLINT(readability-redundant-member-init)
};

struct ReplicatedRaftGroupElectionTimeout {
  raft::GroupId group_id;
  std::chrono::milliseconds timeout{};

  friend bool operator==(const ReplicatedRaftGroupElectionTimeout&,
                         const ReplicatedRaftGroupElectionTimeout&) = default;
};

struct ReplicatedRaftTransportLimits {
  std::chrono::milliseconds minimum_election_timeout{300};
  std::chrono::milliseconds maximum_election_timeout{600};
  std::chrono::milliseconds connect_timeout{5000};
  raft::RaftTimerDriverLimits timers{};
  cluster::RaftTransportTlsServerLimits inbound_carrier{};
  cluster::RaftTransportTlsClientLimits outbound_carrier{};
  cluster::RaftTransportPeerReconnectLimits reconnect{};
  // This codec limit set is applied to receiver validation and both transport directions.
  cluster::RaftTransportPeerPoolLimits peer_pool{};
  cluster::RaftTransportRuntimeLimits runtime{};
  std::size_t maximum_inbound_connections{1024U};
  std::size_t maximum_accepts_per_poll{32U};
};

struct ReplicatedRaftTransportRuntimeConfig {
  raft::NodeId local_node_id{};
  raft::AsyncDurableMultiRaftRuntime* durable_runtime{};
  std::vector<ReplicatedPeer> peers{};          // NOLINT(readability-redundant-member-init)
  std::vector<raft::GroupId> resident_groups{}; // NOLINT(readability-redundant-member-init)
  // Optional exact local overrides. Creation canonicalizes by group, rejects duplicates and
  // nonresident groups, and applies the ordinary randomized limits to every omitted group.
  std::vector<ReplicatedRaftGroupElectionTimeout>
      group_election_timeouts{}; // NOLINT(readability-redundant-member-init)
  ReplicatedRaftTransportTlsCredentials tls{};
  ReplicatedRaftTransportLimits limits{};
};

// Address-stable owner for one node's authenticated inbound/outbound Raft carriers, randomized
// election driver, and unified poll runtime. Creation synchronously obtains each initial group
// observation from the borrowed durable runtime before arming it. The caller must stop and destroy
// this owner before shutting down that durable runtime. Completed transitions remain caller-owned.
class ReplicatedRaftTransportRuntime {
public:
  ReplicatedRaftTransportRuntime() = delete;
  ~ReplicatedRaftTransportRuntime();
  ReplicatedRaftTransportRuntime(const ReplicatedRaftTransportRuntime&) = delete;
  ReplicatedRaftTransportRuntime& operator=(const ReplicatedRaftTransportRuntime&) = delete;
  ReplicatedRaftTransportRuntime(ReplicatedRaftTransportRuntime&&) noexcept;
  ReplicatedRaftTransportRuntime& operator=(ReplicatedRaftTransportRuntime&&) noexcept;

  [[nodiscard]] static common::Result<ReplicatedRaftTransportRuntime>
  create(ReplicatedRaftTransportRuntimeConfig config);
  // Poll-owner-only application admission. The returned sequence identifies the eventual
  // kApplication completion observed through take_completed().
  [[nodiscard]] common::Result<std::uint64_t>
  try_submit_application(raft::DurableRaftRequest request);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Result<cluster::RaftTransportRuntimeResult> take_completed();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] cluster::RaftTransportRuntimeMetrics metrics() const noexcept;
  [[nodiscard]] bool is_running() const noexcept;
  [[nodiscard]] common::Status shutdown();

private:
  class Impl;
  explicit ReplicatedRaftTransportRuntime(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_RAFT_TRANSPORT_RUNTIME_HPP_
