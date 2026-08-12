#ifndef CHRONOS_RAFT_TABLET_GROUP_BINDING_HPP_
#define CHRONOS_RAFT_TABLET_GROUP_BINDING_HPP_

#include "chronos/raft/multi_raft.hpp"
#include "chronos/schema/identity.hpp"

namespace chronos::raft {

// Immutable committed identity bridge between one logical tablet and its independent Raft group.
// Placement epochs may change replicas and leader hints, but they never reinterpret this binding.
struct TabletGroupBindingMetadata {
  schema::TabletId tablet_id;
  GroupId group_id;

  friend bool operator==(const TabletGroupBindingMetadata&,
                         const TabletGroupBindingMetadata&) = default;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_TABLET_GROUP_BINDING_HPP_
