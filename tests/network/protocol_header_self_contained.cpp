#include "chronos/network/protocol.hpp"

#include <cstdint>
#include <type_traits>

static_assert(std::is_same_v<std::underlying_type_t<chronos::network::MessageType>, std::uint16_t>);
