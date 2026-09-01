#include "chronos/cluster/raft_read_authority_tcp_batch_acquisition.hpp"

#include <chrono>
#include <type_traits>

static_assert(
    !std::is_copy_constructible_v<chronos::cluster::RaftReadAuthorityTcpBatchAcquisition>);
static_assert(
    std::is_nothrow_move_constructible_v<chronos::cluster::RaftReadAuthorityTcpBatchAcquisition>);

void use_raft_read_authority_tcp_batch_acquisition_header() {
  chronos::cluster::RaftReadAuthorityTcpBatchAcquisition acquisition;
  (void)acquisition.state();
  (void)acquisition.metrics();
  (void)acquisition.poll_once(std::chrono::milliseconds{0});
  [[maybe_unused]] const auto& result = acquisition.result();
}
