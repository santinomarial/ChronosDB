#ifndef CHRONOS_RAFT_METADATA_HPP_
#define CHRONOS_RAFT_METADATA_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/types.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace chronos::raft {

struct ClusterNodeMetadata {
  NodeId node_id{};
  std::string endpoint;

  friend bool operator==(const ClusterNodeMetadata&, const ClusterNodeMetadata&) = default;
};

struct SchemaMetadata {
  schema::TableId table_id;
  schema::SchemaId schema_id;
  schema::SchemaVersion schema_version;

  friend bool operator==(const SchemaMetadata&, const SchemaMetadata&) = default;
};

struct TabletPlacementMetadata {
  schema::TableId table_id;
  schema::TabletId tablet_id;
  std::uint64_t placement_epoch{};
  std::vector<NodeId> replicas;
  std::optional<NodeId> leader_hint;

  friend bool operator==(const TabletPlacementMetadata&, const TabletPlacementMetadata&) = default;
};

struct RetentionMetadata {
  schema::TableId table_id;
  std::int64_t system_history_ns{};
  std::uint64_t retry_retention_positions{};

  friend bool operator==(const RetentionMetadata&, const RetentionMetadata&) = default;
};

using MetadataCommand =
    std::variant<ClusterNodeMetadata, SchemaMetadata, TabletPlacementMetadata, RetentionMetadata>;

struct MetadataLimits {
  std::size_t maximum_nodes{1024U};
  std::size_t maximum_schemas{65'536U};
  std::size_t maximum_tablets{1U << 20U};
  std::size_t maximum_replicas_per_tablet{9U};
  std::size_t maximum_endpoint_bytes{4096U};
};

// Deterministic application state for the dedicated metadata Raft group. Only committed commands
// in exact log-index order may call apply_committed; this class supplies no alternative consensus
// or last-writer-wins path.
class MetadataStateMachine {
public:
  MetadataStateMachine() = delete;
  ~MetadataStateMachine();
  MetadataStateMachine(const MetadataStateMachine&) = delete;
  MetadataStateMachine& operator=(const MetadataStateMachine&) = delete;
  MetadataStateMachine(MetadataStateMachine&&) noexcept;
  MetadataStateMachine& operator=(MetadataStateMachine&&) noexcept;

  [[nodiscard]] static common::Result<MetadataStateMachine> create(MetadataLimits limits = {});
  [[nodiscard]] common::Status apply_committed(LogIndex index, MetadataCommand command);
  [[nodiscard]] common::Status apply_internal_noop(LogIndex index);

  [[nodiscard]] LogIndex applied_index() const noexcept;
  [[nodiscard]] const ClusterNodeMetadata* find_node(NodeId node_id) const noexcept;
  [[nodiscard]] const SchemaMetadata* find_schema(const schema::SchemaId& schema_id) const noexcept;
  [[nodiscard]] const TabletPlacementMetadata*
  find_tablet(const schema::TabletId& tablet_id) const noexcept;
  [[nodiscard]] const RetentionMetadata*
  find_retention(const schema::TableId& table_id) const noexcept;

private:
  class Impl;
  explicit MetadataStateMachine(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_METADATA_HPP_
