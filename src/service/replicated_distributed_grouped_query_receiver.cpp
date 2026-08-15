#include "chronos/service/replicated_distributed_grouped_query_receiver.hpp"

#include <memory>
#include <new>
#include <optional>
#include <utility>

namespace chronos::service {

class ReplicatedDistributedGroupedQueryReceiver::Impl {
public:
  explicit Impl(ReplicatedDistributedGroupedQueryWorker owned_worker) noexcept
      : worker(std::move(owned_worker)) {}

  ReplicatedDistributedGroupedQueryWorker worker;
  std::optional<cluster::DistributedGroupedQueryReceiver> receiver;
};

ReplicatedDistributedGroupedQueryReceiver::ReplicatedDistributedGroupedQueryReceiver(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
ReplicatedDistributedGroupedQueryReceiver::~ReplicatedDistributedGroupedQueryReceiver() = default;
ReplicatedDistributedGroupedQueryReceiver::ReplicatedDistributedGroupedQueryReceiver(
    ReplicatedDistributedGroupedQueryReceiver&&) noexcept = default;
ReplicatedDistributedGroupedQueryReceiver& ReplicatedDistributedGroupedQueryReceiver::operator=(
    ReplicatedDistributedGroupedQueryReceiver&&) noexcept = default;

common::Result<ReplicatedDistributedGroupedQueryReceiver>
ReplicatedDistributedGroupedQueryReceiver::create(
    ReplicatedDistributedGroupedQueryReceiverConfig config) {
  if (config.node_authorizer == nullptr) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument,
                       "replicated grouped query receiver authorization configuration is invalid"});
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
         .maximum_response_frames = config.maximum_response_frames});
    if (!receiver.has_value())
      return common::make_unexpected(receiver.error());
    implementation->receiver.emplace(*receiver);
    return ReplicatedDistributedGroupedQueryReceiver{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "replicated grouped query receiver allocation failed"});
  }
}

common::Result<std::vector<std::vector<std::byte>>>
ReplicatedDistributedGroupedQueryReceiver::receive(
    const common::ByteView request_bytes,
    const network::PeerAuthenticationResult& authenticated_peer) {
  if (!implementation_) {
    return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                  "replicated grouped query receiver is empty"});
  }
  auto& receiver = implementation_->receiver;
  if (!receiver.has_value()) {
    return common::make_unexpected(common::Status{common::StatusCode::kInternal,
                                                  "replicated grouped query receiver is absent"});
  }
  return receiver->receive(request_bytes, authenticated_peer);
}

} // namespace chronos::service
