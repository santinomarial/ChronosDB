#ifndef CHRONOS_CLUSTER_RAFT_TRANSPORT_RUNTIME_HPP_
#define CHRONOS_CLUSTER_RAFT_TRANSPORT_RUNTIME_HPP_

#include "chronos/cluster/raft_transport_peer_manager.hpp"
#include "chronos/cluster/raft_transport_tcp_server.hpp"
#include "chronos/raft/runtime_timer_driver.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

namespace chronos::cluster {

struct RaftTransportRuntimeLimits {
  std::size_t maximum_pending_results{1024U};
  std::size_t maximum_pending_application_requests{1024U};
  std::size_t maximum_results_per_poll{256U};
  std::size_t maximum_poll_descriptors{8192U};
};

enum class RaftTransportRuntimeResultOrigin : std::uint8_t {
  kInbound = 1,
  kTimer = 2,
  kApplication = 3
};

struct RaftTransportRuntimeResult {
  std::uint64_t submission_sequence{};
  RaftTransportRuntimeResultOrigin origin{RaftTransportRuntimeResultOrigin::kInbound};
  raft::GroupId group_id;
  std::optional<raft::NodeId> remote_source_node_id;
  std::optional<raft::RaftTimerAction> timer_action;
  raft::DurableRaftResult result;
  std::optional<raft::RaftGroupObservation> observation;
};

struct RaftTransportRuntimeMetrics {
  std::uint64_t polls{};
  std::uint64_t durable_wakeups{};
  std::uint64_t inbound_results{};
  std::uint64_t timer_results{};
  std::uint64_t application_results{};
  std::uint64_t routed_results{};
  std::uint64_t routing_backpressure{};
  std::uint64_t completed_results{};
  std::size_t pending_results{};
  std::size_t pending_application_requests{};
  bool failed{};
};

// One single-thread-affine production poll owner. The durable runtime and every authenticator,
// authorizer, receiver, TLS context, and deadline source borrowed by its owned components must
// outlive it. Results remain FIFO-owned until outbound routing succeeds and the embedding takes
// their application/snapshot/read-barrier work.
class RaftTransportRuntime {
public:
  using TimePoint = std::chrono::steady_clock::time_point;
  RaftTransportRuntime() = delete;
  ~RaftTransportRuntime();
  RaftTransportRuntime(const RaftTransportRuntime&) = delete;
  RaftTransportRuntime& operator=(const RaftTransportRuntime&) = delete;
  RaftTransportRuntime(RaftTransportRuntime&&) noexcept;
  RaftTransportRuntime& operator=(RaftTransportRuntime&&) noexcept;

  [[nodiscard]] static common::Result<RaftTransportRuntime>
  create(raft::AsyncDurableMultiRaftRuntime* durable_runtime, raft::RaftTimerDriver&& timer_driver,
         RaftTransportTcpServer&& inbound, RaftTransportPeerManager&& outbound,
         RaftTransportRuntimeLimits limits = {});
  [[nodiscard]] common::Status add_group(const raft::RaftGroupObservation& observation,
                                         TimePoint now);
  [[nodiscard]] common::Status remove_group(const raft::GroupId& group_id);
  // Poll-owner-only admission for application work whose transition must share this runtime's
  // exact outbound-routing FIFO. One ordered group observation is appended automatically. The
  // returned durable submission sequence identifies the eventual kApplication result.
  [[nodiscard]] common::Result<std::uint64_t>
  try_submit_application(raft::DurableRaftRequest request);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Result<RaftTransportRuntimeResult> take_completed();
  [[nodiscard]] network::Ipv4Endpoint bound_endpoint() const noexcept;
  [[nodiscard]] RaftTransportRuntimeMetrics metrics() const noexcept;
  [[nodiscard]] bool failed() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit RaftTransportRuntime(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_TRANSPORT_RUNTIME_HPP_
