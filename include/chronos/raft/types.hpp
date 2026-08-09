#ifndef CHRONOS_RAFT_TYPES_HPP_
#define CHRONOS_RAFT_TYPES_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

namespace chronos::raft {

using NodeId = std::uint64_t;
using Term = std::uint64_t;
using LogIndex = std::uint64_t;

enum class Role : std::uint8_t { kFollower = 1, kCandidate = 2, kLeader = 3 };

struct LogEntry {
  LogIndex index{};
  Term term{};
  std::uint8_t type{1U};
  std::vector<std::byte> payload;

  friend bool operator==(const LogEntry&, const LogEntry&) = default;
};

struct SnapshotMetadata {
  LogIndex last_included_index{};
  Term last_included_term{};
  std::uint64_t manifest_generation{};
  std::array<std::byte, 32U> part_set_checksum{};

  friend bool operator==(const SnapshotMetadata&, const SnapshotMetadata&) = default;
};

struct PersistentState {
  Term current_term{};
  std::optional<NodeId> voted_for;
  std::vector<LogEntry> log;
  LogIndex commit_index{};
  LogIndex applied_index{};
  SnapshotMetadata snapshot;

  friend bool operator==(const PersistentState&, const PersistentState&) = default;
};

struct RequestVoteRequest {
  Term term{};
  NodeId candidate_id{};
  LogIndex last_log_index{};
  Term last_log_term{};
};

struct RequestVoteResponse {
  Term term{};
  bool granted{};
};

struct AppendEntriesRequest {
  Term term{};
  NodeId leader_id{};
  LogIndex previous_log_index{};
  Term previous_log_term{};
  std::vector<LogEntry> entries;
  LogIndex leader_commit{};
};

struct AppendEntriesResponse {
  Term term{};
  bool success{};
  LogIndex match_index{};
  std::optional<Term> conflict_term;
  LogIndex conflict_index{};
};

using Message = std::variant<RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest,
                             AppendEntriesResponse>;

struct OutboundMessage {
  NodeId destination{};
  Message message;
};

// When persistent_state is present, the runtime must durably install it before sending any
// outbound message in the same transition. Committed entries are still invisible until the
// application state machine advances applied_index through mark_applied().
struct Transition {
  std::optional<PersistentState> persistent_state;
  std::vector<OutboundMessage> outbound;
  std::optional<LogIndex> advanced_commit_index;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_TYPES_HPP_
