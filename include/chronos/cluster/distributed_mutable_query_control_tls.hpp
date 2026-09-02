#ifndef CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_QUERY_CONTROL_TLS_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_QUERY_CONTROL_TLS_HPP_

#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tls.hpp"
#include "chronos/cluster/distributed_mutable_vector_query_tls.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_transport.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_service.hpp"
#include "chronos/cluster/raft_read_authority_tls_server.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tls_socket.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace chronos::cluster {

struct DistributedMutableQueryControlTlsServerLimits {
  std::chrono::milliseconds handshake_timeout{5000};
  std::chrono::milliseconds exchange_timeout{30000};
  std::size_t maximum_mutable_response_frames{1024U};
  std::size_t maximum_mutable_response_bytes{kDefaultDistributedVectorQueryV2ResponseBytes};
  std::size_t maximum_mutable_grouped_response_frames{
      query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups};
  std::size_t maximum_mutable_grouped_response_bytes{
      kDefaultDistributedVectorGroupedAggregateQueryV2ResponseBytes};
  std::size_t maximum_mutable_grouped_decode_memory_bytes{
      kDefaultDistributedVectorGroupedAggregateQueryV2DecodeMemoryBytes};
  query::DistributedVectorGroupedAggregateExchangeDecodeLimits mutable_grouped_payload{};
  RaftReadAuthorityTransportLimits read_authority_transport{};
  DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits grouped_shuffle_job_control{};
};

struct DistributedMutableQueryControlTlsServerConfig {
  network::ConnectionAuthenticator* authenticator{};
  DistributedMutableVectorQueryReceiver* mutable_receiver{};
  DistributedMutableVectorGroupedAggregateQueryReceiver* mutable_grouped_receiver{};
  RaftReadAuthorityReceiver* read_authority_receiver{};
  DistributedVectorGroupedAggregateShuffleJobService* grouped_shuffle_job_service{};
  std::array<std::uint8_t, 4U> peer_ipv4_address{};
  DistributedMutableQueryControlTlsServerLimits limits;
};

enum class DistributedMutableQueryControlProtocol : std::uint8_t {
  kUndetermined = 0,
  kMutableVectorQuery = 1,
  kRaftReadAuthority = 2,
  kMutableVectorGroupedAggregateQuery = 3,
  kGroupedShuffleJobControl = 4,
};

enum class DistributedMutableQueryControlTlsServerState : std::uint8_t {
  kHandshaking = 1,
  kReadingProtocol = 2,
  kReadingRequest = 3,
  kWritingResponse = 4,
  kComplete = 5,
  kFailed = 6,
};

struct DistributedMutableQueryControlTlsInterest {
  bool want_read{};
  bool want_write{};
};

// Authenticates before reading the exact application magic, then serves one mutable row,
// mutable grouped sufficient-state, Raft read-authority, or grouped reducer-job request on the
// shared private query-control TLS endpoint. The two mutable requests share CHDMREQ1 and are
// distinguished only after exact request decoding by the bound plan mode. One event-loop thread
// serializes calls; all authentication, receiver, and job-service dependencies are borrowed.
class DistributedMutableQueryControlTlsServer {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedMutableQueryControlTlsServer() = delete;
  ~DistributedMutableQueryControlTlsServer();
  DistributedMutableQueryControlTlsServer(const DistributedMutableQueryControlTlsServer&) = delete;
  DistributedMutableQueryControlTlsServer&
  operator=(const DistributedMutableQueryControlTlsServer&) = delete;
  DistributedMutableQueryControlTlsServer(DistributedMutableQueryControlTlsServer&&) noexcept;
  DistributedMutableQueryControlTlsServer&
  operator=(DistributedMutableQueryControlTlsServer&&) noexcept;

  [[nodiscard]] static common::Result<DistributedMutableQueryControlTlsServer>
  create(network::TlsSocket socket, DistributedMutableQueryControlTlsServerConfig config,
         TimePoint now);
  [[nodiscard]] common::Status on_ready(bool readable, bool writable, TimePoint now);
  [[nodiscard]] DistributedMutableQueryControlTlsServerState state() const noexcept;
  [[nodiscard]] DistributedMutableQueryControlProtocol protocol() const noexcept;
  [[nodiscard]] DistributedMutableQueryControlTlsInterest interest() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit DistributedMutableQueryControlTlsServer(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_QUERY_CONTROL_TLS_HPP_
