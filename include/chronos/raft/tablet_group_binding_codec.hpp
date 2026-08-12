#ifndef CHRONOS_RAFT_TABLET_GROUP_BINDING_CODEC_HPP_
#define CHRONOS_RAFT_TABLET_GROUP_BINDING_CODEC_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/raft/tablet_group_binding.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::raft {

inline constexpr std::uint8_t kRaftTabletGroupBindingEntryType = 4U;
inline constexpr std::size_t kTabletGroupBindingHeaderSize = 48U;
inline constexpr std::size_t kTabletGroupBindingPayloadSize = 32U;
inline constexpr std::size_t kTabletGroupBindingTrailerSize = 4U;
inline constexpr std::size_t kTabletGroupBindingSize =
    kTabletGroupBindingHeaderSize + kTabletGroupBindingPayloadSize + kTabletGroupBindingTrailerSize;

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_tablet_group_binding_v1(const TabletGroupBindingMetadata& binding);

[[nodiscard]] common::Result<TabletGroupBindingMetadata>
decode_tablet_group_binding_v1(common::ByteView bytes);

} // namespace chronos::raft

#endif // CHRONOS_RAFT_TABLET_GROUP_BINDING_CODEC_HPP_
