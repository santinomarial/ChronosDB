#include "chronos/raft/metadata.hpp"

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <type_traits>
#include <utility>
#include <variant>

namespace chronos::raft {
namespace {
[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}
} // namespace

class MetadataStateMachine::Impl {
public:
  explicit Impl(const MetadataLimits configured) : limits(configured) {}
  MetadataLimits limits;
  LogIndex applied{};
  std::map<NodeId, ClusterNodeMetadata> nodes;
  std::map<schema::SchemaId, SchemaMetadata> schemas;
  std::map<schema::TabletId, TabletPlacementMetadata> tablets;
  std::map<schema::TableId, RetentionMetadata> retention;
};

MetadataStateMachine::MetadataStateMachine(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
MetadataStateMachine::~MetadataStateMachine() = default;
MetadataStateMachine::MetadataStateMachine(MetadataStateMachine&&) noexcept = default;
MetadataStateMachine& MetadataStateMachine::operator=(MetadataStateMachine&&) noexcept = default;

common::Result<MetadataStateMachine> MetadataStateMachine::create(const MetadataLimits limits) {
  if (limits.maximum_nodes == 0U || limits.maximum_schemas == 0U || limits.maximum_tablets == 0U ||
      limits.maximum_replicas_per_tablet == 0U || limits.maximum_endpoint_bytes == 0U) {
    return common::make_unexpected(invalid("metadata state limits must be nonzero"));
  }
  return MetadataStateMachine{std::make_unique<Impl>(limits)};
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
        } else {
          if (value.table_id.uuid().is_nil() || value.system_history_ns < 0 ||
              value.retry_retention_positions == 0U) {
            return invalid("retention metadata is invalid");
          }
          impl_->retention.insert_or_assign(value.table_id, value);
        }
        return common::Status::ok();
      },
      std::move(command));
  if (status.is_ok())
    impl_->applied = index;
  return status;
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
const TabletPlacementMetadata*
MetadataStateMachine::find_tablet(const schema::TabletId& tablet_id) const noexcept {
  const auto found = impl_->tablets.find(tablet_id);
  return found == impl_->tablets.end() ? nullptr : &found->second;
}
const RetentionMetadata*
MetadataStateMachine::find_retention(const schema::TableId& table_id) const noexcept {
  const auto found = impl_->retention.find(table_id);
  return found == impl_->retention.end() ? nullptr : &found->second;
}

} // namespace chronos::raft
