#ifndef CHRONOS_RUNTIME_THREAD_PLACEMENT_HPP_
#define CHRONOS_RUNTIME_THREAD_PLACEMENT_HPP_

#include "chronos/common/status.hpp"

#include <cstdint>
#include <optional>

namespace chronos::runtime {

struct ThreadPlacement {
  std::optional<std::uint32_t> cpu;
  std::optional<std::uint32_t> numa_node;
};

// Optional optimization hook only. An empty placement is always successful. Unsupported CPU/NUMA
// requests return NOT_SUPPORTED and never affect correctness or silently choose another CPU.
[[nodiscard]] common::Status apply_current_thread_placement(const ThreadPlacement& placement);

} // namespace chronos::runtime

#endif // CHRONOS_RUNTIME_THREAD_PLACEMENT_HPP_
