#ifndef CHRONOS_RAFT_METADATA_HPP_
#define CHRONOS_RAFT_METADATA_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/types.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
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

// One complete immutable catalog definition applied through the dedicated schema-definition
// command format. The shared schema remains stable for every state reader until owner teardown.
struct CatalogTableDefinition {
  std::string name;
  bool quoted{};
  std::shared_ptr<const schema::TableSchema> schema;

  friend bool operator==(const CatalogTableDefinition& left, const CatalogTableDefinition& right) {
    return left.name == right.name && left.quoted == right.quoted && left.schema != nullptr &&
           right.schema != nullptr && *left.schema == *right.schema;
  }
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

// Complete SQL table behavior. RetentionMetadata remains decodable as the legacy partial policy;
// new table creation must publish this command so partitioning, event retention, history,
// lateness, and retry retention share one committed authority.
struct TablePolicyMetadata {
  schema::TableId table_id;
  std::int64_t partition_interval_ns{};
  std::int64_t retention_ns{};
  std::int64_t system_history_ns{};
  std::int64_t allowed_lateness_ns{};
  std::uint64_t retry_retention_positions{};

  friend bool operator==(const TablePolicyMetadata&, const TablePolicyMetadata&) = default;
};

using MetadataCommand = std::variant<ClusterNodeMetadata, SchemaMetadata, TabletPlacementMetadata,
                                     RetentionMetadata, TablePolicyMetadata>;

struct MetadataLimits {
  std::size_t maximum_nodes{1024U};
  std::size_t maximum_schemas{65'536U};
  std::size_t maximum_tablets{1U << 20U};
  std::size_t maximum_replicas_per_tablet{9U};
  std::size_t maximum_endpoint_bytes{4096U};
  std::size_t maximum_table_name_bytes{1024U};
  std::size_t maximum_column_name_bytes{1024U};
};

struct ActiveSchemaMetadata {
  schema::TableId table_id;
  schema::SchemaId schema_id;

  friend bool operator==(const ActiveSchemaMetadata&, const ActiveSchemaMetadata&) = default;
};

// One immutable owning projection used to reconstruct runtime catalogs and tablet owners after
// committed metadata replay. Vectors retain deterministic map-key order; complete definitions keep
// their immutable shared schemas pinned independently of the state machine lifetime.
struct MetadataCatalogSnapshot {
  LogIndex applied_index{};
  std::vector<CatalogTableDefinition> schema_definitions;
  std::vector<ActiveSchemaMetadata> active_schemas;
  std::vector<TabletPlacementMetadata> tablet_placements;
  std::vector<TablePolicyMetadata> table_policies;
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
  [[nodiscard]] common::Status apply_committed_schema_definition(LogIndex index,
                                                                 CatalogTableDefinition definition);
  [[nodiscard]] common::Status apply_internal_noop(LogIndex index);
  // Efficient equivalent of applying only internal Raft entries through index. The caller must
  // have independently proved that the skipped range contains no application entry.
  [[nodiscard]] common::Status apply_internal_noops_through(LogIndex index);

  [[nodiscard]] LogIndex applied_index() const noexcept;
  [[nodiscard]] const ClusterNodeMetadata* find_node(NodeId node_id) const noexcept;
  [[nodiscard]] const SchemaMetadata* find_schema(const schema::SchemaId& schema_id) const noexcept;
  [[nodiscard]] const CatalogTableDefinition*
  find_schema_definition(const schema::SchemaId& schema_id) const noexcept;
  [[nodiscard]] const CatalogTableDefinition*
  find_active_table_definition(const schema::TableId& table_id) const noexcept;
  [[nodiscard]] const CatalogTableDefinition*
  find_active_table_definition(std::string_view name, bool quoted) const noexcept;
  [[nodiscard]] const TabletPlacementMetadata*
  find_tablet(const schema::TabletId& tablet_id) const noexcept;
  [[nodiscard]] const RetentionMetadata*
  find_retention(const schema::TableId& table_id) const noexcept;
  [[nodiscard]] const TablePolicyMetadata*
  find_table_policy(const schema::TableId& table_id) const noexcept;
  [[nodiscard]] common::Result<MetadataCatalogSnapshot> catalog_snapshot() const;

private:
  class Impl;
  explicit MetadataStateMachine(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_METADATA_HPP_
