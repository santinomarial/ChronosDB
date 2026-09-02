#ifndef CHRONOS_QUERY_DISTRIBUTED_MUTABLE_VECTOR_FRAGMENT_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_MUTABLE_VECTOR_FRAGMENT_HPP_

#include "chronos/common/result.hpp"
#include "chronos/cseg/pruning.hpp"
#include "chronos/ingest/tablet_state.hpp"
#include "chronos/manifest/types.hpp"
#include "chronos/query/distributed_vector_fragment.hpp"
#include "chronos/query/distributed_vector_pre_group_program.hpp"
#include "chronos/query/distributed_vector_result_schema.hpp"
#include "chronos/raft/metadata.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <vector>

namespace chronos::query {

namespace distributed_mutable_vector_fragment_format {
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::uint16_t kPreGroupMajor = 2U;
inline constexpr std::size_t kHeaderLength = 248U;
inline constexpr std::size_t kTrailerLength = 4U;
inline constexpr std::uint32_t kMaximumProjectionColumns = 4096U;
inline constexpr std::size_t kMaximumFrameLength =
    kHeaderLength + static_cast<std::size_t>(kMaximumProjectionColumns) * 4U +
    distributed_vector_plan_format::kMaximumFrameLength +
    distributed_vector_result_schema_format::kMaximumFrameLength +
    distributed_vector_pre_group_program_format::kMaximumFrameLength + kTrailerLength;
} // namespace distributed_mutable_vector_fragment_format

// Distinct authority value for one immutable TabletState publication. It deliberately has no
// Manifest generation: applied_position names the exact committed mutable publication instead of
// reinterpreting Distributed Vector Fragment v1's exact durable-Manifest boundary.
struct DistributedMutableVectorFragment {
  common::Uuid query_id;
  manifest::DatabaseId database_id;
  schema::TableId table_id;
  schema::TabletId tablet_id;
  schema::SchemaId destination_schema_id;
  common::Uuid raft_group_id;
  std::uint64_t serving_node{};
  std::uint64_t applied_position{};
  std::uint64_t observed_leader_commit_position{};
  std::uint64_t placement_epoch{};
  DistributedReadPolicy read_policy{};
  std::optional<raft::ReadBarrier>
      linearizable_barrier{}; // NOLINT(readability-redundant-member-init)
  std::vector<std::uint32_t>
      destination_column_ordinals{}; // NOLINT(readability-redundant-member-init)
  std::optional<cseg::EventTimePredicate> event_time_predicate{std::nullopt};
  DistributedVectorPlanIntent plan{};
  DistributedVectorResultSchema result_schema{};
  std::optional<DistributedVectorPreGroupProgram> pre_group_program{std::nullopt};

  friend bool operator==(const DistributedMutableVectorFragment&,
                         const DistributedMutableVectorFragment&) = default;
};

struct DistributedMutableVectorFragmentBinding {
  std::reference_wrapper<const DistributedVectorQueryPlan> plan;
  std::reference_wrapper<const DistributedReadAdmission> admission;
  manifest::DatabaseId database_id;
  std::reference_wrapper<const ingest::TabletSnapshot> snapshot;
  std::reference_wrapper<const schema::SchemaLineage> lineage;
  common::Uuid raft_group_id;
  std::reference_wrapper<const raft::TabletPlacementMetadata> placement;
  std::span<const std::uint32_t> destination_column_ordinals;
  std::optional<cseg::EventTimePredicate> event_time_predicate;
  std::reference_wrapper<const DistributedVectorResultSchema> result_schema;
  const DistributedVectorPreGroupProgram* pre_group_program{};
};

struct DistributedMutableVectorFragmentDecodeLimits {
  std::size_t maximum_frame_length{distributed_mutable_vector_fragment_format::kMaximumFrameLength};
  std::uint32_t maximum_projection_columns{
      distributed_mutable_vector_fragment_format::kMaximumProjectionColumns};
  DistributedVectorPlanDecodeLimits plan;
  DistributedVectorResultSchemaDecodeLimits result_schema;
  DistributedVectorPreGroupProgramDecodeLimits pre_group_program;
};

class EncodedDistributedMutableVectorFragment {
public:
  EncodedDistributedMutableVectorFragment() = delete;
  EncodedDistributedMutableVectorFragment(const EncodedDistributedMutableVectorFragment&) = delete;
  EncodedDistributedMutableVectorFragment&
  operator=(const EncodedDistributedMutableVectorFragment&) = delete;
  EncodedDistributedMutableVectorFragment(EncodedDistributedMutableVectorFragment&&) noexcept =
      default;
  EncodedDistributedMutableVectorFragment&
  operator=(EncodedDistributedMutableVectorFragment&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedDistributedMutableVectorFragment(std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;

  friend common::Result<EncodedDistributedMutableVectorFragment>
  encode_distributed_mutable_vector_fragment(const DistributedMutableVectorFragment&);
};

[[nodiscard]] common::Result<EncodedDistributedMutableVectorFragment>
encode_distributed_mutable_vector_fragment(const DistributedMutableVectorFragment& fragment);
[[nodiscard]] common::Result<DistributedMutableVectorFragment>
decode_distributed_mutable_vector_fragment_exact(
    common::ByteView bytes, DistributedMutableVectorFragmentDecodeLimits limits = {});

// Binds one plan fragment to one exact committed/applied immutable TabletState publication. The
// returned value owns every field needed by a later authenticated carrier and worker revalidation.
[[nodiscard]] common::Result<DistributedMutableVectorFragment>
bind_distributed_mutable_vector_fragment(const DistributedMutableVectorFragmentBinding& binding);

// Structural validation is independent of local storage authority and performs no I/O. A worker
// must additionally exact-match current placement, group, node, schema, barrier, and publication.
[[nodiscard]] common::Status
validate_distributed_mutable_vector_fragment(const DistributedMutableVectorFragment& fragment);

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_MUTABLE_VECTOR_FRAGMENT_HPP_
