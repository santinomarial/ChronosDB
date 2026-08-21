#ifndef CHRONOS_RAFT_DETERMINISTIC_SIMULATOR_HPP_
#define CHRONOS_RAFT_DETERMINISTIC_SIMULATOR_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/node.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <variant>
#include <vector>

namespace chronos::raft {

struct RaftSimulationStartElection {
  NodeId node_id{};
  friend bool operator==(const RaftSimulationStartElection&,
                         const RaftSimulationStartElection&) = default;
};
struct RaftSimulationPropose {
  NodeId node_id{};
  std::uint8_t type{1U};
  std::vector<std::byte> payload;
  friend bool operator==(const RaftSimulationPropose&, const RaftSimulationPropose&) = default;
};
struct RaftSimulationHeartbeat {
  NodeId node_id{};
  friend bool operator==(const RaftSimulationHeartbeat&, const RaftSimulationHeartbeat&) = default;
};
struct RaftSimulationDeliver {
  std::uint64_t message_id{};
  friend bool operator==(const RaftSimulationDeliver&, const RaftSimulationDeliver&) = default;
};
struct RaftSimulationDrop {
  std::uint64_t message_id{};
  friend bool operator==(const RaftSimulationDrop&, const RaftSimulationDrop&) = default;
};
struct RaftSimulationDuplicate {
  std::uint64_t message_id{};
  friend bool operator==(const RaftSimulationDuplicate&, const RaftSimulationDuplicate&) = default;
};
struct RaftSimulationSetLink {
  NodeId source{};
  NodeId destination{};
  bool enabled{};
  friend bool operator==(const RaftSimulationSetLink&, const RaftSimulationSetLink&) = default;
};
struct RaftSimulationCrash {
  NodeId node_id{};
  friend bool operator==(const RaftSimulationCrash&, const RaftSimulationCrash&) = default;
};
struct RaftSimulationRestart {
  NodeId node_id{};
  friend bool operator==(const RaftSimulationRestart&, const RaftSimulationRestart&) = default;
};
struct RaftSimulationFailNextPersistence {
  NodeId node_id{};
  friend bool operator==(const RaftSimulationFailNextPersistence&,
                         const RaftSimulationFailNextPersistence&) = default;
};
struct RaftSimulationMarkApplied {
  NodeId node_id{};
  LogIndex index{};
  friend bool operator==(const RaftSimulationMarkApplied&,
                         const RaftSimulationMarkApplied&) = default;
};
struct RaftSimulationBeginMembershipChange {
  NodeId node_id{};
  std::vector<NodeId> voters;
  friend bool operator==(const RaftSimulationBeginMembershipChange&,
                         const RaftSimulationBeginMembershipChange&) = default;
};
struct RaftSimulationFinalizeMembershipChange {
  NodeId node_id{};
  friend bool operator==(const RaftSimulationFinalizeMembershipChange&,
                         const RaftSimulationFinalizeMembershipChange&) = default;
};
struct RaftSimulationBeginReadBarrier {
  NodeId node_id{};
  friend bool operator==(const RaftSimulationBeginReadBarrier&,
                         const RaftSimulationBeginReadBarrier&) = default;
};
struct RaftSimulationCompleteSnapshotInstall {
  NodeId node_id{};
  bool installed{};
  friend bool operator==(const RaftSimulationCompleteSnapshotInstall&,
                         const RaftSimulationCompleteSnapshotInstall&) = default;
};
struct RaftSimulationCompactSnapshot {
  NodeId node_id{};
  SnapshotMetadata snapshot;
  friend bool operator==(const RaftSimulationCompactSnapshot&,
                         const RaftSimulationCompactSnapshot&) = default;
};

using RaftSimulationAction =
    std::variant<RaftSimulationStartElection, RaftSimulationPropose, RaftSimulationHeartbeat,
                 RaftSimulationDeliver, RaftSimulationDrop, RaftSimulationDuplicate,
                 RaftSimulationSetLink, RaftSimulationCrash, RaftSimulationRestart,
                 RaftSimulationFailNextPersistence, RaftSimulationMarkApplied,
                 RaftSimulationBeginMembershipChange, RaftSimulationFinalizeMembershipChange,
                 RaftSimulationBeginReadBarrier, RaftSimulationCompleteSnapshotInstall,
                 RaftSimulationCompactSnapshot>;

struct RaftSimulationLimits {
  std::size_t maximum_pending_messages{65'536U};
  std::size_t maximum_trace_actions{1U << 20U};
  std::size_t maximum_shrink_replays{100'000U};
  RaftLimits raft;
};

struct RaftSimulationConfig {
  std::vector<NodeId> node_ids;
  std::vector<NodeId> initial_voters;
  // Empty starts every node from canonical term-zero state. Otherwise this vector contains one
  // complete durable image per node, in the exact order of `node_ids`.
  std::vector<PersistentState> initial_persistent_states;
  RaftSimulationLimits limits;
};

struct RaftSimulationMessageRoute {
  std::uint64_t message_id{};
  NodeId source{};
  NodeId destination{};
  friend bool operator==(const RaftSimulationMessageRoute&,
                         const RaftSimulationMessageRoute&) = default;
};

struct RaftSimulationStats {
  std::uint64_t actions{};
  std::uint64_t delivered{};
  std::uint64_t dropped{};
  std::uint64_t duplicated{};
  std::uint64_t crashes{};
  std::uint64_t restarts{};
  std::uint64_t persistence_failures{};
  std::uint64_t completed_read_barriers{};
  std::uint64_t safety_checks{};
  friend bool operator==(const RaftSimulationStats&, const RaftSimulationStats&) = default;
};

struct RaftSeededSimulationSchedule {
  std::uint64_t seed{};
  std::size_t actions{};
};

struct RaftExhaustiveFaultSchedule {
  std::size_t maximum_depth{};
  std::size_t maximum_replays{};
  bool include_duplication{};
  bool include_link_changes{};
  bool include_persistence_failures{};
  bool include_elections{};
  bool include_heartbeats{};
  bool include_read_barriers{};
  bool include_application_advancement{};
  bool include_node_lifecycle{};
};

struct RaftExhaustiveFaultResult {
  std::size_t replayed_prefixes{};
  bool search_complete{};
  std::optional<common::Status> failure;
  std::vector<RaftSimulationAction> failing_trace;
};

// Deterministic single-threaded Raft laboratory. Explicit delivery order is virtual network time;
// links, duplication, loss, crashes, and atomic full-state persistence are controlled by actions.
// Every step checks election safety, log matching, leader completeness, and committed-prefix truth.
class DeterministicRaftSimulator {
public:
  DeterministicRaftSimulator() = delete;
  ~DeterministicRaftSimulator();
  DeterministicRaftSimulator(const DeterministicRaftSimulator&) = delete;
  DeterministicRaftSimulator& operator=(const DeterministicRaftSimulator&) = delete;
  DeterministicRaftSimulator(DeterministicRaftSimulator&&) noexcept;
  DeterministicRaftSimulator& operator=(DeterministicRaftSimulator&&) noexcept;

  [[nodiscard]] static common::Result<DeterministicRaftSimulator>
  create(RaftSimulationConfig config);
  [[nodiscard]] common::Status step(RaftSimulationAction action);
  [[nodiscard]] common::Status replay(std::span<const RaftSimulationAction> actions);
  [[nodiscard]] common::Status run_seeded(RaftSeededSimulationSchedule schedule);
  // Exhaustively branches queued-message outcomes after a valid setup trace. Schedules may opt
  // into bounded network, persistence, leader/read, application, and lifecycle branches.
  // `search_complete` is false when the replay bound truncates the frontier or a failure is found.
  [[nodiscard]] static common::Result<RaftExhaustiveFaultResult>
  explore_fault_schedules(const RaftSimulationConfig& config,
                          std::span<const RaftSimulationAction> setup_trace,
                          RaftExhaustiveFaultSchedule schedule);
  [[nodiscard]] static common::Result<std::vector<RaftSimulationAction>>
  shrink_failing_trace(const RaftSimulationConfig& config,
                       std::span<const RaftSimulationAction> failing_trace);

  [[nodiscard]] const RaftNode* active_node(NodeId node_id) const noexcept;
  [[nodiscard]] const PersistentState* durable_state(NodeId node_id) const noexcept;
  [[nodiscard]] common::Result<std::vector<RaftSimulationMessageRoute>> pending_messages() const;
  [[nodiscard]] std::span<const RaftSimulationAction> trace() const noexcept;
  [[nodiscard]] const common::Status& status() const noexcept;
  [[nodiscard]] const RaftSimulationStats& stats() const noexcept;

private:
  class Impl;
  explicit DeterministicRaftSimulator(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_DETERMINISTIC_SIMULATOR_HPP_
