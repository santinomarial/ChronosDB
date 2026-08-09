#include "chronos/tiering/object_store.hpp"

namespace {
[[maybe_unused]] constexpr auto kHeaderIsSelfContained = &chronos::tiering::ObjectMetadata::size;
}
