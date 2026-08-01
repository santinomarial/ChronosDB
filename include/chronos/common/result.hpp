#ifndef CHRONOS_COMMON_RESULT_HPP_
#define CHRONOS_COMMON_RESULT_HPP_

#include "chronos/common/status.hpp"

#include <expected>
#include <utility>

namespace chronos::common {

template <typename T> using Result = std::expected<T, Status>;

[[nodiscard]] inline std::unexpected<Status> make_unexpected(Status status) {
  return std::unexpected<Status>{std::move(status)};
}

} // namespace chronos::common

#endif // CHRONOS_COMMON_RESULT_HPP_
