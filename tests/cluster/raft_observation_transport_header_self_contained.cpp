#include "chronos/cluster/raft_observation_transport.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::cluster::RaftObservationRequest>);
static_assert(std::is_aggregate_v<chronos::cluster::RaftObservationResponse>);
static_assert(std::is_abstract_v<chronos::cluster::RaftObservationService>);

namespace {
[[maybe_unused]] const auto kEncodeRequest = &chronos::cluster::encode_raft_observation_request_v1;
[[maybe_unused]] const auto kDecodeResponse =
    &chronos::cluster::decode_raft_observation_response_v1;
} // namespace
