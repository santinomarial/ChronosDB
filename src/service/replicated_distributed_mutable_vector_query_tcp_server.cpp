#include "chronos/service/replicated_distributed_mutable_vector_query_tcp_server.hpp"

#include <memory>
#include <new>
#include <optional>
#include <utility>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status empty_server() {
  return {common::StatusCode::kInvalidArgument,
          "replicated distributed mutable vector query TCP server is empty"};
}

} // namespace

class ReplicatedDistributedMutableVectorQueryTcpServer::Impl {
public:
  explicit Impl(ReplicatedDistributedMutableVectorQueryWorker owned_worker) noexcept
      : worker(std::move(owned_worker)) {}

  ReplicatedDistributedMutableVectorQueryWorker worker;
  std::optional<cluster::DistributedMutableVectorQueryReceiver> receiver;
  std::optional<cluster::DistributedMutableVectorQueryTcpServer> server;

  [[nodiscard]] cluster::DistributedMutableVectorQueryTcpServer* active_server() noexcept {
    if (!server.has_value())
      return nullptr;
    // Guarded by the presence check above.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    return std::addressof(*server);
  }

  [[nodiscard]] const cluster::DistributedMutableVectorQueryTcpServer*
  active_server() const noexcept {
    if (!server.has_value())
      return nullptr;
    // Guarded by the presence check above.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    return std::addressof(*server);
  }
};

ReplicatedDistributedMutableVectorQueryTcpServer::ReplicatedDistributedMutableVectorQueryTcpServer(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
ReplicatedDistributedMutableVectorQueryTcpServer::
    ~ReplicatedDistributedMutableVectorQueryTcpServer() = default;
ReplicatedDistributedMutableVectorQueryTcpServer::ReplicatedDistributedMutableVectorQueryTcpServer(
    ReplicatedDistributedMutableVectorQueryTcpServer&&) noexcept = default;
ReplicatedDistributedMutableVectorQueryTcpServer&
ReplicatedDistributedMutableVectorQueryTcpServer::operator=(
    ReplicatedDistributedMutableVectorQueryTcpServer&&) noexcept = default;

common::Result<ReplicatedDistributedMutableVectorQueryTcpServer>
ReplicatedDistributedMutableVectorQueryTcpServer::start(
    ReplicatedDistributedMutableVectorQueryTcpServerConfig config) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInvalidArgument,
        "replicated mutable vector query TCP server authentication configuration is invalid"});
  }
  auto worker = ReplicatedDistributedMutableVectorQueryWorker::create(config.worker);
  if (!worker.has_value())
    return common::make_unexpected(worker.error());
  try {
    auto implementation = std::make_unique<Impl>(std::move(*worker));
    auto receiver = cluster::DistributedMutableVectorQueryReceiver::create(
        {.local_node_id = config.worker.local_node_id,
         .authorizer = config.node_authorizer,
         .worker = std::addressof(implementation->worker),
         .leader_hint_provider = config.leader_hint_provider,
         .maximum_response_frames = config.carrier_limits.maximum_response_frames,
         .maximum_response_bytes = config.carrier_limits.maximum_response_bytes});
    if (!receiver.has_value())
      return common::make_unexpected(receiver.error());
    cluster::DistributedMutableVectorQueryReceiver& owned_receiver =
        implementation->receiver.emplace(*receiver);
    auto server = cluster::DistributedMutableVectorQueryTcpServer::start(
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
    return ReplicatedDistributedMutableVectorQueryTcpServer{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "replicated distributed mutable vector query TCP server allocation failed"});
  }
}

common::Status ReplicatedDistributedMutableVectorQueryTcpServer::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  cluster::DistributedMutableVectorQueryTcpServer* server =
      implementation_ ? implementation_->active_server() : nullptr;
  if (server == nullptr)
    return empty_server();
  return server->poll_once(maximum_wait);
}

common::Status ReplicatedDistributedMutableVectorQueryTcpServer::shutdown() {
  cluster::DistributedMutableVectorQueryTcpServer* server =
      implementation_ ? implementation_->active_server() : nullptr;
  if (server == nullptr)
    return empty_server();
  return server->shutdown();
}

network::Ipv4Endpoint
ReplicatedDistributedMutableVectorQueryTcpServer::bound_endpoint() const noexcept {
  const cluster::DistributedMutableVectorQueryTcpServer* server =
      implementation_ ? implementation_->active_server() : nullptr;
  return server == nullptr ? network::Ipv4Endpoint{} : server->bound_endpoint();
}

cluster::DistributedMutableVectorQueryTcpServerMetrics
ReplicatedDistributedMutableVectorQueryTcpServer::metrics() const noexcept {
  const cluster::DistributedMutableVectorQueryTcpServer* server =
      implementation_ ? implementation_->active_server() : nullptr;
  return server == nullptr ? cluster::DistributedMutableVectorQueryTcpServerMetrics{}
                           : server->metrics();
}

bool ReplicatedDistributedMutableVectorQueryTcpServer::is_running() const noexcept {
  const cluster::DistributedMutableVectorQueryTcpServer* server =
      implementation_ ? implementation_->active_server() : nullptr;
  return server != nullptr && server->is_running();
}

} // namespace chronos::service
