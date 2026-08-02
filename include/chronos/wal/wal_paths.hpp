#ifndef CHRONOS_WAL_WAL_PATHS_HPP_
#define CHRONOS_WAL_WAL_PATHS_HPP_

#include "chronos/common/result.hpp"
#include "chronos/wal/types.hpp"

#include <cstdint>
#include <string>
#include <string_view>

namespace chronos::wal {

inline constexpr std::string_view kWalLockFileName = "LOCK";

[[nodiscard]] common::Result<std::string> wal_segment_file_name(std::uint64_t segment_number);
[[nodiscard]] common::Result<std::string>
wal_temporary_segment_file_name(std::uint64_t segment_number, const WalId& nonce);

} // namespace chronos::wal

#endif // CHRONOS_WAL_WAL_PATHS_HPP_
