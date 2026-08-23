#include "chronos/network/native_quorum_ingest_tcp_execution.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::network::NativeQuorumIngestTcpExecution>);
static_assert(!std::is_copy_constructible_v<chronos::network::NativeQuorumIngestTcpExecution>);
static_assert(
    std::is_nothrow_move_constructible_v<chronos::network::NativeQuorumIngestTcpExecution>);
