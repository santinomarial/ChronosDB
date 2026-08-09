#include "chronos/runtime/thread_placement.hpp"

#include <cstdint>

#if defined(__linux__)
#include <cerrno>
#include <sched.h>
#endif

namespace chronos::runtime {

common::Status apply_current_thread_placement(const ThreadPlacement& placement) {
  if (!placement.cpu.has_value() && !placement.numa_node.has_value())
    return common::Status::ok();
  if (placement.numa_node.has_value()) {
    return common::Status{common::StatusCode::kNotSupported,
                          "NUMA memory placement provider is not configured"};
  }
#if defined(__linux__)
  if (*placement.cpu >= static_cast<std::uint32_t>(CPU_SETSIZE)) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "CPU affinity index exceeds CPU_SETSIZE"};
  }
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(*placement.cpu, &set);
  if (::sched_setaffinity(0, sizeof(set), &set) != 0) {
    return common::Status{common::StatusCode::kIoError,
                          errno == EINVAL ? "CPU affinity is unavailable" : "CPU affinity failed"};
  }
  return common::Status::ok();
#else
  return common::Status{common::StatusCode::kNotSupported, "CPU affinity hook requires Linux"};
#endif
}

} // namespace chronos::runtime
