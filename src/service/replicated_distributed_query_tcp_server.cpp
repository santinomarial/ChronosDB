#include "chronos/service/replicated_distributed_query_tcp_server.hpp"

#include <new>
#include <optional>
#include <utility>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status empty_server() {
  return {common::StatusCode::kInvalidArgument, "replicated distributed query TCP server is empty"};
}

} // namespace

class ReplicatedDistributedQueryTcpServer::Impl {
public:
  explicit Impl(ReplicatedDistributedQueryWorker owned_worker) noexcept
      : worker(std::move(owned_worker)) {}

  ReplicatedDistributedQueryWorker worker;
  std::optional<cluster::DistributedQueryReceiver> receiver;
  std::optional<cluster::DistributedQueryTcpServer> server;
};

ReplicatedDistributedQueryTcpServer::ReplicatedDistributedQueryTcpServer(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
ReplicatedDistributedQueryTcpServer::~ReplicatedDistributedQueryTcpServer() = default;
ReplicatedDistributedQueryTcpServer::ReplicatedDistributedQueryTcpServer(
    ReplicatedDistributedQueryTcpServer&&) noexcept = default;
ReplicatedDistributedQueryTcpServer& ReplicatedDistributedQueryTcpServer::operator=(
    ReplicatedDistributedQueryTcpServer&&) noexcept = default;

common::Result<ReplicatedDistributedQueryTcpServer>
ReplicatedDistributedQueryTcpServer::start(ReplicatedDistributedQueryTcpServerConfig config) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInvalidArgument,
        "replicated distributed query TCP server authentication configuration is invalid"});
  }
  auto worker = ReplicatedDistributedQueryWorker::create(config.worker);
  if (!worker.has_value())
    return common::make_unexpected(worker.error());
  try {
    auto implementation = std::make_unique<Impl>(std::move(*worker));
    auto receiver = cluster::DistributedQueryReceiver::create(
        {.local_node_id = config.worker.local_node_id,
         .authorizer = config.node_authorizer,
         .worker = std::addressof(implementation->worker),
         .leader_hint_provider = config.leader_hint_provider});
    if (!receiver.has_value())
      return common::make_unexpected(receiver.error());
    implementation->receiver.emplace(std::move(*receiver));
    auto server = cluster::DistributedQueryTcpServer::start(
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
    return ReplicatedDistributedQueryTcpServer{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "replicated distributed query TCP server allocation failed"});
  }
}

common::Status
ReplicatedDistributedQueryTcpServer::poll_once(const std::chrono::milliseconds maximum_wait) {
  return implementation_ ? implementation_->server->poll_once(maximum_wait) : empty_server();
}

common::Status ReplicatedDistributedQueryTcpServer::shutdown() {
  return implementation_ ? implementation_->server->shutdown() : empty_server();
}

network::Ipv4Endpoint ReplicatedDistributedQueryTcpServer::bound_endpoint() const noexcept {
  return implementation_ ? implementation_->server->bound_endpoint() : network::Ipv4Endpoint{};
}

cluster::DistributedQueryTcpServerMetrics
ReplicatedDistributedQueryTcpServer::metrics() const noexcept {
  return implementation_ ? implementation_->server->metrics()
                         : cluster::DistributedQueryTcpServerMetrics{};
}

bool ReplicatedDistributedQueryTcpServer::is_running() const noexcept {
  return implementation_ && implementation_->server->is_running();
}

} // namespace chronos::service
