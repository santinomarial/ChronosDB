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
inline constexpr std::size_t kMaximumMaterializedViewCheckpointSize = 1024U * 1024U * 1024U;

struct MaterializedViewCheckpointCodecLimits {
  std::size_t maximum_checkpoint_bytes{256U * 1024U * 1024U};
  std::size_t maximum_rows{65'536U};
  std::size_t maximum_windows{4096U};
  std::size_t maximum_window_contributions{1U << 20U};
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_windowed_materialized_view_checkpoint_v1(
    const WindowedMaterializedViewCheckpoint& checkpoint,
    MaterializedViewCheckpointCodecLimits limits = {});

[[nodiscard]] common::Result<WindowedMaterializedViewCheckpoint>
decode_windowed_materialized_view_checkpoint_v1(common::ByteView bytes,
                                                MaterializedViewCheckpointCodecLimits limits = {});

} // namespace chronos::live

#endif // CHRONOS_LIVE_MATERIALIZED_VIEW_CHECKPOINT_HPP_
