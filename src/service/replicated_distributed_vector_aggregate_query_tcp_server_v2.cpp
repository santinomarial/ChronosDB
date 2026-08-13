#include "chronos/service/replicated_distributed_vector_aggregate_query_tcp_server_v2.hpp"

#include <memory>
#include <new>
#include <optional>
#include <utility>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status empty_server() {
  return {common::StatusCode::kInvalidArgument,
          "replicated distributed vector aggregate query TCP server v2 is empty"};
}

} // namespace

class ReplicatedDistributedVectorAggregateQueryTcpServerV2::Impl {
public:
  explicit Impl(ReplicatedDistributedVectorAggregateQueryWorkerV2 owned_worker) noexcept
      : worker(std::move(owned_worker)) {}

  ReplicatedDistributedVectorAggregateQueryWorkerV2 worker;
  std::optional<cluster::DistributedVectorAggregateQueryReceiverV2> receiver;
  std::optional<cluster::DistributedVectorAggregateQueryTcpServerV2> server;

  [[nodiscard]] cluster::DistributedVectorAggregateQueryTcpServerV2* active_server() noexcept {
    if (!server.has_value())
      return nullptr;
    // Guarded by the presence check above.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    return std::addressof(*server);
  }

  [[nodiscard]] const cluster::DistributedVectorAggregateQueryTcpServerV2*
  active_server() const noexcept {
    if (!server.has_value())
      return nullptr;
    // Guarded by the presence check above.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    return std::addressof(*server);
  }
};

ReplicatedDistributedVectorAggregateQueryTcpServerV2::
    ReplicatedDistributedVectorAggregateQueryTcpServerV2(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
ReplicatedDistributedVectorAggregateQueryTcpServerV2::
    ~ReplicatedDistributedVectorAggregateQueryTcpServerV2() = default;
ReplicatedDistributedVectorAggregateQueryTcpServerV2::
    ReplicatedDistributedVectorAggregateQueryTcpServerV2(
        ReplicatedDistributedVectorAggregateQueryTcpServerV2&&) noexcept = default;
ReplicatedDistributedVectorAggregateQueryTcpServerV2&
ReplicatedDistributedVectorAggregateQueryTcpServerV2::operator=(
    ReplicatedDistributedVectorAggregateQueryTcpServerV2&&) noexcept = default;

common::Result<ReplicatedDistributedVectorAggregateQueryTcpServerV2>
ReplicatedDistributedVectorAggregateQueryTcpServerV2::start(
    ReplicatedDistributedVectorAggregateQueryTcpServerConfigV2 config) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInvalidArgument,
        "replicated vector aggregate query TCP server v2 authentication configuration is invalid"});
  }
  auto worker = ReplicatedDistributedVectorAggregateQueryWorkerV2::create(config.worker);
  if (!worker.has_value())
    return common::make_unexpected(worker.error());
  try {
    auto implementation = std::make_unique<Impl>(std::move(*worker));
    auto receiver = cluster::DistributedVectorAggregateQueryReceiverV2::create(
        {.local_node_id = config.worker.local_node_id,
         .authorizer = config.node_authorizer,
         .worker = std::addressof(implementation->worker),
         .leader_hint_provider = config.leader_hint_provider,
         .maximum_response_frames = config.carrier_limits.maximum_response_frames,
         .maximum_response_bytes = config.carrier_limits.maximum_response_bytes});
    if (!receiver.has_value())
      return common::make_unexpected(receiver.error());
    cluster::DistributedVectorAggregateQueryReceiverV2& owned_receiver =
        implementation->receiver.emplace(*receiver);
    auto server = cluster::DistributedVectorAggregateQueryTcpServerV2::start(
        {.listener = config.listener,
         .tls = std::move(config.tls),
         .authenticator = config.authenticator,
         .receiver = std::addressof(owned_receiver),
         .carrier_limits = config.carrier_limits,
         .maximum_connections = config.maximum_connections,
         .maximum_accepts_per_poll = config.maximum_accepts_per_poll});
    if (!server.has_value())
      return common::make_unexpected(server.error());
    implementation->server.emplace(std::move(*server));
    return ReplicatedDistributedVectorAggregateQueryTcpServerV2{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kResourceExhausted,
        "replicated distributed vector aggregate query TCP server v2 allocation failed"});
  }
}

common::Status ReplicatedDistributedVectorAggregateQueryTcpServerV2::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  cluster::DistributedVectorAggregateQueryTcpServerV2* server =
      implementation_ ? implementation_->active_server() : nullptr;
  if (server == nullptr)
    return empty_server();
  return server->poll_once(maximum_wait);
}

common::Status ReplicatedDistributedVectorAggregateQueryTcpServerV2::shutdown() {
  cluster::DistributedVectorAggregateQueryTcpServerV2* server =
      implementation_ ? implementation_->active_server() : nullptr;
  if (server == nullptr)
    return empty_server();
  return server->shutdown();
}

network::Ipv4Endpoint
ReplicatedDistributedVectorAggregateQueryTcpServerV2::bound_endpoint() const noexcept {
  const cluster::DistributedVectorAggregateQueryTcpServerV2* server =
      implementation_ ? implementation_->active_server() : nullptr;
  return server == nullptr ? network::Ipv4Endpoint{} : server->bound_endpoint();
}

cluster::DistributedVectorAggregateQueryTcpServerMetricsV2
ReplicatedDistributedVectorAggregateQueryTcpServerV2::metrics() const noexcept {
  const cluster::DistributedVectorAggregateQueryTcpServerV2* server =
      implementation_ ? implementation_->active_server() : nullptr;
  return server == nullptr ? cluster::DistributedVectorAggregateQueryTcpServerMetricsV2{}
                           : server->metrics();
}

bool ReplicatedDistributedVectorAggregateQueryTcpServerV2::is_running() const noexcept {
  const cluster::DistributedVectorAggregateQueryTcpServerV2* server =
      implementation_ ? implementation_->active_server() : nullptr;
  return server != nullptr && server->is_running();
}

} // namespace chronos::service
