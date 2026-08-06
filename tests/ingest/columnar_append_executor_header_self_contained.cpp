#include "chronos/ingest/columnar_append_executor.hpp"

#include <type_traits>

namespace {

static_assert(std::is_move_constructible_v<chronos::ingest::ColumnarAppendExecutionResult>);

} // namespace
