#include "chronos/service/replicated_distributed_grouped_query_tcp_server.hpp"

#include <new>
#include <optional>
#include <utility>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status empty_server() {
  return {common::StatusCode::kInvalidArgument, "replicated grouped query TCP server is empty"};
}

} // namespace

class ReplicatedDistributedGroupedQueryTcpServer::Impl {
public:
  explicit Impl(ReplicatedDistributedGroupedQueryWorker owned_worker) noexcept
      : worker(std::move(owned_worker)) {}

  ReplicatedDistributedGroupedQueryWorker worker;
  std::optional<cluster::DistributedGroupedQueryReceiver> receiver;
  std::optional<cluster::DistributedGroupedQueryTcpServer> server;
};

ReplicatedDistributedGroupedQueryTcpServer::ReplicatedDistributedGroupedQueryTcpServer(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
ReplicatedDistributedGroupedQueryTcpServer::~ReplicatedDistributedGroupedQueryTcpServer() = default;
ReplicatedDistributedGroupedQueryTcpServer::ReplicatedDistributedGroupedQueryTcpServer(
    ReplicatedDistributedGroupedQueryTcpServer&&) noexcept = default;
ReplicatedDistributedGroupedQueryTcpServer& ReplicatedDistributedGroupedQueryTcpServer::operator=(
    ReplicatedDistributedGroupedQueryTcpServer&&) noexcept = default;

common::Result<ReplicatedDistributedGroupedQueryTcpServer>
ReplicatedDistributedGroupedQueryTcpServer::start(
    ReplicatedDistributedGroupedQueryTcpServerConfig config) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInvalidArgument,
        "replicated grouped query TCP server authentication configuration is invalid"});
  }
  auto worker = ReplicatedDistributedGroupedQueryWorker::create(config.worker);
  if (!worker.has_value())
    return common::make_unexpected(worker.error());
  try {
    auto implementation = std::make_unique<Impl>(std::move(*worker));
    auto receiver = cluster::DistributedGroupedQueryReceiver::create(
        {.local_node_id = config.worker.local_node_id,
         .authorizer = config.node_authorizer,
         .worker = std::addressof(implementation->worker),
         .leader_hint_provider = config.leader_hint_provider,
         .maximum_response_frames = config.carrier_limits.maximum_response_frames});
    if (!receiver.has_value())
      return common::make_unexpected(receiver.error());
    implementation->receiver.emplace(std::move(*receiver));
    auto server = cluster::DistributedGroupedQueryTcpServer::start(
        {.listener = config.listener,
         .tls = std::move(config.tls),
         .authenticator = config.authenticator,
         .receiver = std::addressof(*implementation->receiver),
         .carrier_limits = config.carrier_limits,
         .maximum_connections = config.maximum_connections,
         .maximum_accepts_per_poll = config.maximum_accepts_per_poll});
    if (!server.has_value())
      return common::make_unexpected(server.error());
    implementation->server.emplace(std::move(*server));
    return ReplicatedDistributedGroupedQueryTcpServer{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "replicated grouped query TCP server allocation failed"});
  }
}

common::Status ReplicatedDistributedGroupedQueryTcpServer::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  return implementation_ ? implementation_->server->poll_once(maximum_wait) : empty_server();
}

common::Status ReplicatedDistributedGroupedQueryTcpServer::shutdown() {
  return implementation_ ? implementation_->server->shutdown() : empty_server();
}

network::Ipv4Endpoint ReplicatedDistributedGroupedQueryTcpServer::bound_endpoint() const noexcept {
  return implementation_ ? implementation_->server->bound_endpoint() : network::Ipv4Endpoint{};
}

cluster::DistributedGroupedQueryTcpServerMetrics
ReplicatedDistributedGroupedQueryTcpServer::metrics() const noexcept {
  return implementation_ ? implementation_->server->metrics()
                         : cluster::DistributedGroupedQueryTcpServerMetrics{};
}

bool ReplicatedDistributedGroupedQueryTcpServer::is_running() const noexcept {
  return implementation_ && implementation_->server->is_running();
}

} // namespace chronos::service
