#ifndef CHRONOS_LIVE_MATERIALIZED_VIEW_CHECKPOINT_HPP_
#define CHRONOS_LIVE_MATERIALIZED_VIEW_CHECKPOINT_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/live/materialized_view.hpp"

#include <cstddef>
#include <vector>

namespace chronos::live {

inline constexpr std::size_t kMaterializedViewCheckpointHeaderSize = 160U;
inline constexpr std::size_t kMaterializedViewCheckpointRowSize = 40U;
inline constexpr std::size_t kMaterializedViewCheckpointWindowSize = 88U;
inline constexpr std::size_t kMaterializedViewCheckpointTrailerSize = 4U;
inline constexpr std::size_t kMaximumMaterializedViewCheckpointSize =
    std::size_t{1024U} * 1024U * 1024U;
inline constexpr std::size_t kBoundMaterializedViewCheckpointHeaderSize = 160U;
inline constexpr std::size_t kBoundMaterializedViewCheckpointTrailerSize = 4U;

struct MaterializedViewCheckpointCodecLimits {
  std::size_t maximum_checkpoint_bytes{std::size_t{256U} * 1024U * 1024U};
  std::size_t maximum_rows{65'536U};
  std::size_t maximum_windows{4096U};
  std::size_t maximum_window_contributions{1U << 20U};
};

struct MaterializedViewCheckpointIdentity {
  common::Uuid database_id;
  common::Uuid view_id;
  schema::TableId table_id;
  schema::SchemaId schema_id;
  schema::SchemaVersion schema_version;
  PlanFingerprint plan_fingerprint{};

  friend bool operator==(const MaterializedViewCheckpointIdentity&,
                         const MaterializedViewCheckpointIdentity&) = default;
};

struct BoundMaterializedViewCheckpoint {
  MaterializedViewCheckpointIdentity identity;
  std::uint64_t checkpoint_generation{};
  WindowedMaterializedViewCheckpoint state;

  friend bool operator==(const BoundMaterializedViewCheckpoint&,
                         const BoundMaterializedViewCheckpoint&) = default;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_windowed_materialized_view_checkpoint_v1(
    const WindowedMaterializedViewCheckpoint& checkpoint,
    MaterializedViewCheckpointCodecLimits limits = {});

[[nodiscard]] common::Result<WindowedMaterializedViewCheckpoint>
decode_windowed_materialized_view_checkpoint_v1(common::ByteView bytes,
                                                MaterializedViewCheckpointCodecLimits limits = {});

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_bound_materialized_view_checkpoint_v1(const BoundMaterializedViewCheckpoint& checkpoint,
                                             MaterializedViewCheckpointCodecLimits limits = {});

[[nodiscard]] common::Result<BoundMaterializedViewCheckpoint>
decode_bound_materialized_view_checkpoint_v1(common::ByteView bytes,
                                             MaterializedViewCheckpointCodecLimits limits = {});

} // namespace chronos::live

#endif // CHRONOS_LIVE_MATERIALIZED_VIEW_CHECKPOINT_HPP_
