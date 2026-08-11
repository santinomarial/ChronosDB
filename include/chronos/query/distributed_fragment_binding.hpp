#ifndef CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_BINDING_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_BINDING_HPP_

#include "chronos/common/result.hpp"
#include "chronos/cseg/pruning.hpp"
#include "chronos/manifest/temporal_publication.hpp"
#include "chronos/query/distributed_fragment_dispatch.hpp"
#include "chronos/raft/metadata.hpp"
#include "chronos/raft/types.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstdint>
#include <functional>
#include <optional>
#include <span>

namespace chronos::query {

// Borrows one acquire-loaded storage epoch and one committed placement view only for the duration
// of binding. The returned dispatch owns every field needed by the authenticated worker boundary.
struct DistributedAggregateFragmentBinding {
  std::reference_wrapper<const DistributedAggregatePlan> plan;
  std::reference_wrapper<const DistributedReadAdmission> admission;
  std::reference_wrapper<const manifest::TemporalDatabaseStorageSnapshot> snapshot;
  std::reference_wrapper<const schema::TableSchema> destination_schema;
  common::Uuid raft_group_id;
  std::reference_wrapper<const raft::TabletPlacementMetadata> placement;
  std::span<const std::uint32_t> destination_column_ordinals;
  std::uint32_t aggregate_input_index{};
  std::optional<cseg::EventTimePredicate> event_time_predicate;
};

// Reproves admission, committed placement, Raft source identity, exact durable boundary, and
// destination schema/projection against one immutable Manifest v2 epoch before constructing a
// group-scoped executable request. It performs no I/O and publishes no state.
[[nodiscard]] common::Result<DistributedAggregateFragmentDispatch>
bind_distributed_aggregate_fragment(const DistributedAggregateFragmentBinding& binding);

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_BINDING_HPP_
