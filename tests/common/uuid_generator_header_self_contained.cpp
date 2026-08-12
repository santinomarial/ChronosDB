#include "chronos/common/uuid_generator.hpp"

#include <type_traits>

static_assert(
    std::is_base_of_v<chronos::common::UuidGenerator, chronos::common::SystemUuidGenerator>);
