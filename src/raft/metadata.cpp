#include "chronos/raft/metadata.hpp"

#include "chronos/schema/utf8.hpp"

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <new>
#include <ranges>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

namespace chronos::raft {
namespace {
[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return common::Status{common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] bool valid_unquoted_name(const std::string& name) noexcept {
  if (name.empty())
    return false;
  const auto first = [](const char value) {
    return (value >= 'a' && value <= 'z') || value == '_';
  };
  const auto rest = [&](const char value) {
    return first(value) || (value >= '0' && value <= '9');
  };
  return first(name.front()) && std::ranges::all_of(name.begin() + 1, name.end(), rest);
}
} // namespace

class MetadataStateMachine::Impl {
public:
  explicit Impl(const MetadataLimits configured) : limits(configured) {}
  MetadataLimits limits;
  LogIndex applied{};
  std::map<NodeId, ClusterNodeMetadata> nodes;
  std::map<schema::SchemaId, SchemaMetadata> schemas;
  std::map<schema::SchemaId, CatalogTableDefinition> definitions;
  std::map<schema::TableId, schema::SchemaId> active_schemas;
  std::map<schema::TabletId, TabletPlacementMetadata> tablets;
  std::map<schema::TabletId, TabletGroupBindingMetadata> tablet_groups;
  std::map<GroupId, schema::TabletId> group_tablets;
  std::map<schema::TableId, RetentionMetadata> retention;
  std::map<schema::TableId, TablePolicyMetadata> policies;
};

MetadataStateMachine::MetadataStateMachine(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
MetadataStateMachine::~MetadataStateMachine() = default;
MetadataStateMachine::MetadataStateMachine(MetadataStateMachine&&) noexcept = default;
MetadataStateMachine& MetadataStateMachine::operator=(MetadataStateMachine&&) noexcept = default;

common::Result<MetadataStateMachine> MetadataStateMachine::create(const MetadataLimits limits) {
  if (limits.maximum_nodes == 0U || limits.maximum_schemas == 0U || limits.maximum_tablets == 0U ||
      limits.maximum_replicas_per_tablet == 0U || limits.maximum_endpoint_bytes == 0U ||
      limits.maximum_table_name_bytes == 0U || limits.maximum_column_name_bytes == 0U) {
    return common::make_unexpected(invalid("metadata state limits must be nonzero"));
  }
  return MetadataStateMachine{std::make_unique<Impl>(limits)};
}

common::Status
MetadataStateMachine::apply_committed_schema_definition(const LogIndex index,
                                                        CatalogTableDefinition definition) {
  if (impl_ == nullptr || index == 0U || index != impl_->applied + 1U)
    return invalid("schema definition must follow committed Raft log order");
  if (definition.schema == nullptr || definition.name.empty() ||
      definition.name.size() > impl_->limits.maximum_table_name_bytes ||
      !schema::is_valid_utf8(definition.name) || definition.name.find('\0') != std::string::npos ||
      (!definition.quoted && !valid_unquoted_name(definition.name))) {
    return invalid("catalog schema definition has an invalid name or schema");
  }
  const schema::TableSchema& value = *definition.schema;
  const schema::TableId table_id = value.table_id();
  const schema::SchemaId schema_id = value.schema_id();
  const schema::SchemaVersion schema_version = value.version();
  if (std::ranges::any_of(value.columns(), [&](const schema::ColumnDefinition& column) {
        return column.name().size() > impl_->limits.maximum_column_name_bytes;
      })) {
    return invalid("catalog schema definition has an oversized column name");
  }
  const auto identity = impl_->schemas.find(schema_id);
  if (identity != impl_->schemas.end() && (identity->second.table_id != table_id ||
                                           identity->second.schema_version != schema_version)) {
    return invalid("catalog schema definition disagrees with schema identity metadata");
  }
  const auto existing = impl_->definitions.find(schema_id);
  if (existing != impl_->definitions.end()) {
    if (!(existing->second == definition))
      return invalid("catalog schema identity cannot be redefined");
    impl_->applied = index;
    return common::Status::ok();
  }
  if (impl_->definitions.size() >= impl_->limits.maximum_schemas)
    return invalid("catalog schema definition state is full");
  if (identity == impl_->schemas.end() && impl_->schemas.size() >= impl_->limits.maximum_schemas)
    return invalid("catalog schema identity state is full");
  const auto active = impl_->active_schemas.find(table_id);
  if (active == impl_->active_schemas.end()) {
    if (schema_version != schema::SchemaVersion::initial())
      return invalid("first catalog schema definition must be version one");
  } else {
    const CatalogTableDefinition& predecessor = impl_->definitions.at(active->second);
    if (predecessor.name != definition.name || predecessor.quoted != definition.quoted)
      return invalid("catalog table rename requires a dedicated metadata command");
    if (!schema::validate_v1_successor(*predecessor.schema, value).is_ok())
      return invalid("catalog schema definition is not the direct v1 successor");
  }
  for (const auto& [candidate_schema_id, candidate] : impl_->definitions) {
    static_cast<void>(candidate_schema_id);
    if (candidate.schema->table_id() != table_id && candidate.name == definition.name &&
        candidate.quoted == definition.quoted) {
      return invalid("catalog table name is already owned by another table");
    }
  }
  const bool identity_was_present = identity != impl_->schemas.end();
  const bool active_was_present = active != impl_->active_schemas.end();
  const std::optional<schema::SchemaId> previous_active =
      active_was_present ? std::optional<schema::SchemaId>{active->second} : std::nullopt;
  try {
    impl_->definitions.emplace(schema_id, std::move(definition));
    impl_->active_schemas.insert_or_assign(table_id, schema_id);
    if (!identity_was_present) {
      impl_->schemas.emplace(schema_id, SchemaMetadata{table_id, schema_id, schema_version});
    }
  } catch (const std::bad_alloc&) {
    if (active_was_present) {
      impl_->active_schemas.insert_or_assign(table_id, *previous_active);
    } else {
      impl_->active_schemas.erase(table_id);
    }
    impl_->definitions.erase(schema_id);
    if (!identity_was_present)
      impl_->schemas.erase(schema_id);
    return {common::StatusCode::kResourceExhausted, "catalog schema definition allocation failed"};
  }
  impl_->applied = index;
  return common::Status::ok();
}

common::Status MetadataStateMachine::apply_committed(const LogIndex index,
                                                     MetadataCommand command) {
  if (impl_ == nullptr || index == 0U || index != impl_->applied + 1U) {
    return invalid("metadata command must follow committed Raft log order");
  }
  common::Status status = std::visit(
      [&](auto&& value) -> common::Status {
        using T = std::remove_cvref_t<decltype(value)>;
        if constexpr (std::is_same_v<T, ClusterNodeMetadata>) {
          if (value.node_id == 0U || value.endpoint.empty() ||
              value.endpoint.size() > impl_->limits.maximum_endpoint_bytes ||
              (!impl_->nodes.contains(value.node_id) &&
               impl_->nodes.size() >= impl_->limits.maximum_nodes)) {
            return invalid("cluster node metadata is invalid or full");
          }
          impl_->nodes.insert_or_assign(value.node_id, std::move(value));
        } else if constexpr (std::is_same_v<T, SchemaMetadata>) {
          if (value.table_id.uuid().is_nil() || value.schema_id.uuid().is_nil() ||
              (!impl_->schemas.contains(value.schema_id) &&
               impl_->schemas.size() >= impl_->limits.maximum_schemas)) {
            return invalid("schema metadata is invalid or full");
          }
          const auto existing = impl_->schemas.find(value.schema_id);
          if (existing != impl_->schemas.end() && existing->second.table_id != value.table_id) {
            return invalid("schema identity cannot move between tables");
          }
          impl_->schemas.insert_or_assign(value.schema_id, value);
        } else if constexpr (std::is_same_v<T, TabletPlacementMetadata>) {
          std::ranges::sort(value.replicas);
          if (value.table_id.uuid().is_nil() || value.tablet_id.uuid().is_nil() ||
              value.placement_epoch == 0U || value.replicas.empty() ||
              value.replicas.size() > impl_->limits.maximum_replicas_per_tablet ||
              value.replicas.front() == 0U ||
              std::adjacent_find(value.replicas.begin(), value.replicas.end()) !=
                  value.replicas.end() ||
              (value.leader_hint.has_value() &&
               !std::binary_search(value.replicas.begin(), value.replicas.end(),
                                   *value.leader_hint)) ||
              (!impl_->tablets.contains(value.tablet_id) &&
               impl_->tablets.size() >= impl_->limits.maximum_tablets)) {
            return invalid("tablet placement metadata is invalid or full");
          }
          const auto existing = impl_->tablets.find(value.tablet_id);
          if (existing != impl_->tablets.end() &&
              (existing->second.table_id != value.table_id ||
               value.placement_epoch <= existing->second.placement_epoch)) {
            return invalid("tablet placement must preserve table and advance epoch");
          }
          impl_->tablets.insert_or_assign(value.tablet_id, std::move(value));
        } else if constexpr (std::is_same_v<T, RetentionMetadata>) {
          if (value.table_id.uuid().is_nil() || value.system_history_ns < 0 ||
              value.retry_retention_positions == 0U) {
            return invalid("retention metadata is invalid");
          }
          const auto complete = impl_->policies.find(value.table_id);
          if (complete != impl_->policies.end() &&
              (complete->second.system_history_ns != value.system_history_ns ||
               complete->second.retry_retention_positions != value.retry_retention_positions)) {
            return invalid("legacy retention metadata conflicts with complete table policy");
          }
          impl_->retention.insert_or_assign(value.table_id, value);
        } else {
          if (value.table_id.uuid().is_nil() || value.partition_interval_ns <= 0 ||
              value.retention_ns <= 0 || value.system_history_ns <= 0 ||
              value.allowed_lateness_ns < 0 || value.retry_retention_positions == 0U ||
              !impl_->active_schemas.contains(value.table_id)) {
            return invalid("complete table policy metadata is invalid or has no table schema");
          }
          const auto existing_policy = impl_->policies.find(value.table_id);
          const std::optional<TablePolicyMetadata> previous_policy =
              existing_policy == impl_->policies.end()
                  ? std::nullopt
                  : std::optional<TablePolicyMetadata>{existing_policy->second};
          try {
            impl_->policies.insert_or_assign(value.table_id, value);
            impl_->retention.insert_or_assign(
                value.table_id, RetentionMetadata{value.table_id, value.system_history_ns,
                                                  value.retry_retention_positions});
          } catch (const std::bad_alloc&) {
            if (previous_policy.has_value()) {
              impl_->policies.insert_or_assign(value.table_id, *previous_policy);
            } else {
              impl_->policies.erase(value.table_id);
            }
            return {common::StatusCode::kResourceExhausted,
                    "complete table policy allocation failed"};
          }
        }
        return common::Status::ok();
      },
      std::move(command));
  if (status.is_ok())
    impl_->applied = index;
  return status;
}

common::Status MetadataStateMachine::apply_internal_noop(const LogIndex index) {
  if (impl_ == nullptr || index == 0U || index != impl_->applied + 1U)
    return invalid("metadata internal entry must follow committed Raft log order");
  impl_->applied = index;
  return common::Status::ok();
}

common::Status
MetadataStateMachine::apply_committed_tablet_group_binding(const LogIndex index,
                                                           TabletGroupBindingMetadata binding) {
  if (impl_ == nullptr || index == 0U || index != impl_->applied + 1U)
    return invalid("tablet group binding must follow committed Raft log order");
  if (binding.tablet_id.uuid().is_nil() || binding.group_id.is_nil() ||
      !impl_->tablets.contains(binding.tablet_id)) {
    return invalid("tablet group binding is invalid or has no committed placement");
  }
  const auto existing = impl_->tablet_groups.find(binding.tablet_id);
  if (existing != impl_->tablet_groups.end() && existing->second.group_id != binding.group_id)
    return invalid("tablet group binding is immutable");
  const auto group_owner = impl_->group_tablets.find(binding.group_id);
  if (group_owner != impl_->group_tablets.end() && group_owner->second != binding.tablet_id)
    return invalid("Raft group identity is already bound to another tablet");
  if (existing != impl_->tablet_groups.end()) {
    impl_->applied = index;
    return common::Status::ok();
  }
  try {
    auto [owner, owner_inserted] =
        impl_->group_tablets.emplace(binding.group_id, binding.tablet_id);
    if (!owner_inserted)
      return invalid("Raft group identity is already bound");
    try {
      impl_->tablet_groups.emplace(binding.tablet_id, std::move(binding));
    } catch (...) {
      impl_->group_tablets.erase(owner);
      throw;
    }
  } catch (const std::bad_alloc&) {
    return {common::StatusCode::kResourceExhausted, "tablet group binding allocation failed"};
  }
  impl_->applied = index;
  return common::Status::ok();
}

common::Status MetadataStateMachine::apply_internal_noops_through(const LogIndex index) {
  if (impl_ == nullptr || index < impl_->applied)
    return invalid("metadata internal range cannot move application backward");
  impl_->applied = index;
  return common::Status::ok();
}

LogIndex MetadataStateMachine::applied_index() const noexcept {
  return impl_->applied;
}
const ClusterNodeMetadata* MetadataStateMachine::find_node(const NodeId node_id) const noexcept {
  const auto found = impl_->nodes.find(node_id);
  return found == impl_->nodes.end() ? nullptr : &found->second;
}
const SchemaMetadata*
MetadataStateMachine::find_schema(const schema::SchemaId& schema_id) const noexcept {
  const auto found = impl_->schemas.find(schema_id);
  return found == impl_->schemas.end() ? nullptr : &found->second;
}
const CatalogTableDefinition*
MetadataStateMachine::find_schema_definition(const schema::SchemaId& schema_id) const noexcept {
  const auto found = impl_->definitions.find(schema_id);
  return found == impl_->definitions.end() ? nullptr : &found->second;
}
const CatalogTableDefinition*
MetadataStateMachine::find_active_table_definition(const schema::TableId& table_id) const noexcept {
  const auto active = impl_->active_schemas.find(table_id);
  return active == impl_->active_schemas.end() ? nullptr : find_schema_definition(active->second);
}
const CatalogTableDefinition*
MetadataStateMachine::find_active_table_definition(const std::string_view name,
                                                   const bool quoted) const noexcept {
  for (const auto& [table_id, schema_id] : impl_->active_schemas) {
    static_cast<void>(table_id);
    const auto definition = impl_->definitions.find(schema_id);
    if (definition != impl_->definitions.end() && definition->second.name == name &&
        definition->second.quoted == quoted) {
      return &definition->second;
    }
  }
  return nullptr;
}
const TabletPlacementMetadata*
MetadataStateMachine::find_tablet(const schema::TabletId& tablet_id) const noexcept {
  const auto found = impl_->tablets.find(tablet_id);
  return found == impl_->tablets.end() ? nullptr : &found->second;
}
const TabletGroupBindingMetadata*
MetadataStateMachine::find_tablet_group_binding(const schema::TabletId& tablet_id) const noexcept {
  const auto found = impl_->tablet_groups.find(tablet_id);
  return found == impl_->tablet_groups.end() ? nullptr : &found->second;
}
const RetentionMetadata*
MetadataStateMachine::find_retention(const schema::TableId& table_id) const noexcept {
  const auto found = impl_->retention.find(table_id);
  return found == impl_->retention.end() ? nullptr : &found->second;
}
const TablePolicyMetadata*
MetadataStateMachine::find_table_policy(const schema::TableId& table_id) const noexcept {
  const auto found = impl_->policies.find(table_id);
  return found == impl_->policies.end() ? nullptr : &found->second;
}

common::Result<MetadataCatalogSnapshot> MetadataStateMachine::catalog_snapshot() const {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("metadata state machine was moved from"));
  try {
    MetadataCatalogSnapshot snapshot;
    snapshot.applied_index = impl_->applied;
    snapshot.cluster_nodes.reserve(impl_->nodes.size());
    snapshot.schema_definitions.reserve(impl_->definitions.size());
    snapshot.active_schemas.reserve(impl_->active_schemas.size());
    snapshot.tablet_placements.reserve(impl_->tablets.size());
    snapshot.tablet_group_bindings.reserve(impl_->tablet_groups.size());
    snapshot.table_policies.reserve(impl_->policies.size());
    for (const auto& [node_id, node] : impl_->nodes) {
      static_cast<void>(node_id);
      snapshot.cluster_nodes.push_back(node);
    }
    for (const auto& [schema_id, definition] : impl_->definitions) {
      static_cast<void>(schema_id);
      snapshot.schema_definitions.push_back(definition);
    }
    for (const auto& [table_id, schema_id] : impl_->active_schemas)
      snapshot.active_schemas.push_back({table_id, schema_id});
    for (const auto& [tablet_id, placement] : impl_->tablets) {
      static_cast<void>(tablet_id);
      snapshot.tablet_placements.push_back(placement);
    }
    for (const auto& [tablet_id, binding] : impl_->tablet_groups) {
      static_cast<void>(tablet_id);
      snapshot.tablet_group_bindings.push_back(binding);
    }
    for (const auto& [table_id, policy] : impl_->policies) {
      static_cast<void>(table_id);
      snapshot.table_policies.push_back(policy);
    }
    return snapshot;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("metadata catalog snapshot allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("metadata catalog snapshot exceeds container limits"));
  }
}

} // namespace chronos::raft
