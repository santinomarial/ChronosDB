#include "chronos/columnar/columnar_batch_codec.hpp"

#include <type_traits>

namespace {
static_assert(!std::is_default_constructible_v<chronos::columnar::EncodedColumnarBatch>);
static_assert(!std::is_copy_constructible_v<chronos::columnar::EncodedColumnarBatch>);
} // namespace
