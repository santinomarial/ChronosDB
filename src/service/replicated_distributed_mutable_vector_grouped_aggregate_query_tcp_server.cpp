#include "chronos/service/replicated_distributed_mutable_vector_grouped_aggregate_query_tcp_server.hpp"

#include <memory>
#include <new>
#include <optional>
#include <utility>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status empty_server() {
  return {common::StatusCode::kInvalidArgument,
          "replicated distributed mutable vector grouped aggregate query TCP server is empty"};
}

} // namespace

class ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer::Impl {
public:
  explicit Impl(ReplicatedDistributedMutableVectorGroupedAggregateQueryWorker owned_worker) noexcept
      : worker(std::move(owned_worker)) {}

  ReplicatedDistributedMutableVectorGroupedAggregateQueryWorker worker;
  std::optional<cluster::DistributedMutableVectorGroupedAggregateQueryReceiver> receiver;
  std::optional<cluster::DistributedMutableVectorGroupedAggregateQueryTcpServer> server;

  [[nodiscard]] cluster::DistributedMutableVectorGroupedAggregateQueryTcpServer*
  active_server() noexcept {
    if (!server.has_value())
      return nullptr;
    // Guarded by the presence check above.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    return std::addressof(*server);
  }

  [[nodiscard]] const cluster::DistributedMutableVectorGroupedAggregateQueryTcpServer*
  active_server() const noexcept {
    if (!server.has_value())
      return nullptr;
    // Guarded by the presence check above.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    return std::addressof(*server);
  }
};

ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer::
    ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer::
    ~ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer() = default;
ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer::
    ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer(
        ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer&&) noexcept = default;
ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer&
ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer::operator=(
    ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer&&) noexcept = default;

common::Result<ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer>
ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer::start(
    ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServerConfig config) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInvalidArgument,
        "replicated mutable vector grouped aggregate query TCP server authentication "
        "configuration is invalid"});
  }
  auto worker =
      ReplicatedDistributedMutableVectorGroupedAggregateQueryWorker::create(config.worker);
  if (!worker.has_value())
    return common::make_unexpected(worker.error());
  try {
    auto implementation = std::make_unique<Impl>(std::move(*worker));
    auto receiver = cluster::DistributedMutableVectorGroupedAggregateQueryReceiver::create(
        {.local_node_id = config.worker.local_node_id,
         .authorizer = config.node_authorizer,
         .worker = std::addressof(implementation->worker),
         .leader_hint_provider = config.leader_hint_provider,
         .maximum_response_frames = config.carrier_limits.maximum_response_frames,
         .maximum_response_bytes = config.carrier_limits.maximum_response_bytes});
    if (!receiver.has_value())
      return common::make_unexpected(receiver.error());
    cluster::DistributedMutableVectorGroupedAggregateQueryReceiver& owned_receiver =
        implementation->receiver.emplace(*receiver);
    auto server = cluster::DistributedMutableVectorGroupedAggregateQueryTcpServer::start(
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
    return ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer{
        std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kResourceExhausted,
        "replicated distributed mutable vector grouped aggregate query TCP server allocation "
        "failed"});
  }
}

common::Status ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  cluster::DistributedMutableVectorGroupedAggregateQueryTcpServer* server =
      implementation_ ? implementation_->active_server() : nullptr;
  if (server == nullptr)
    return empty_server();
  return server->poll_once(maximum_wait);
}

common::Status ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer::shutdown() {
  cluster::DistributedMutableVectorGroupedAggregateQueryTcpServer* server =
      implementation_ ? implementation_->active_server() : nullptr;
  if (server == nullptr)
    return empty_server();
  return server->shutdown();
}

network::Ipv4Endpoint
ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer::bound_endpoint() const noexcept {
  const cluster::DistributedMutableVectorGroupedAggregateQueryTcpServer* server =
      implementation_ ? implementation_->active_server() : nullptr;
  return server == nullptr ? network::Ipv4Endpoint{} : server->bound_endpoint();
}

cluster::DistributedMutableVectorGroupedAggregateQueryTcpServerMetrics
ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer::metrics() const noexcept {
  const cluster::DistributedMutableVectorGroupedAggregateQueryTcpServer* server =
      implementation_ ? implementation_->active_server() : nullptr;
  return server == nullptr
             ? cluster::DistributedMutableVectorGroupedAggregateQueryTcpServerMetrics{}
             : server->metrics();
}

bool ReplicatedDistributedMutableVectorGroupedAggregateQueryTcpServer::is_running() const noexcept {
  const cluster::DistributedMutableVectorGroupedAggregateQueryTcpServer* server =
      implementation_ ? implementation_->active_server() : nullptr;
  return server != nullptr && server->is_running();
}

} // namespace chronos::service
