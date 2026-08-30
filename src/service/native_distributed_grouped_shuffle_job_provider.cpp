#include "chronos/service/native_distributed_grouped_shuffle_job_provider.hpp"

#include <algorithm>
#include <chrono>
#include <memory>
#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] bool
valid_config(const NativeDistributedGroupedShuffleJobProviderConfig& config) noexcept {
  return config.coordinator_node_id != 0U && config.authenticator != nullptr &&
         config.node_authorizer != nullptr && config.connect_timeout.count() > 0 &&
         config.reducer_execution_timeout.count() > 0 &&
         config.reducer_execution_timeout <=
             cluster::distributed_vector_grouped_aggregate_shuffle_job_control_format::
                 kMaximumExecutionTimeout &&
         config.maximum_reducer_nodes > 0U && config.maximum_reducer_nodes <= 4096U &&
         config.maximum_retained_result_streams > 0U &&
         config.maximum_result_accepts_per_poll > 0U &&
         config.maximum_collected_encoded_bytes > 0U && config.maximum_batch_working_bytes > 0U &&
         config.maximum_working_memory_bytes > 0U;
}

} // namespace

class NativeDistributedGroupedShuffleJobProvider::Impl {
public:
  explicit Impl(NativeDistributedGroupedShuffleJobProviderConfig configured) noexcept
      : config(std::move(configured)) {}

  NativeDistributedGroupedShuffleJobProviderConfig config;
};

NativeDistributedGroupedShuffleJobProvider::NativeDistributedGroupedShuffleJobProvider() noexcept =
    default;
NativeDistributedGroupedShuffleJobProvider::~NativeDistributedGroupedShuffleJobProvider() = default;
NativeDistributedGroupedShuffleJobProvider::NativeDistributedGroupedShuffleJobProvider(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
NativeDistributedGroupedShuffleJobProvider::NativeDistributedGroupedShuffleJobProvider(
    NativeDistributedGroupedShuffleJobProvider&&) noexcept = default;
NativeDistributedGroupedShuffleJobProvider& NativeDistributedGroupedShuffleJobProvider::operator=(
    NativeDistributedGroupedShuffleJobProvider&&) noexcept = default;

// A by-value configuration is the ownership-transfer boundary into the provider.
// NOLINTBEGIN(performance-unnecessary-value-param,readability-convert-member-functions-to-static)
common::Result<NativeDistributedGroupedShuffleJobProvider>
NativeDistributedGroupedShuffleJobProvider::create(
    NativeDistributedGroupedShuffleJobProviderConfig config) {
  // NOLINTEND(performance-unnecessary-value-param,readability-convert-member-functions-to-static)
  if (!valid_config(config)) {
    return common::make_unexpected(
        invalid("Native grouped shuffle job provider configuration is invalid"));
  }
  try {
    return NativeDistributedGroupedShuffleJobProvider{std::make_unique<Impl>(std::move(config))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("Native grouped shuffle job provider allocation failed"));
  }
}

// These spans have distinct element types; the macOS 26/LLVM 18 parser fallback reports them as
// identical integers only after failing to parse the installed libc++ headers.
// NOLINTBEGIN(bugprone-easily-swappable-parameters)
common::Result<NativeDistributedGroupedShufflePlan>
NativeDistributedGroupedShuffleJobProvider::prepare(
    const std::span<const query::DistributedMutableVectorFragment> fragments,
    const std::span<const query::VectorGroupKeyDefinition> keys,
    const std::span<const query::VectorAggregateDefinition> aggregates,
    const std::span<const cluster::DistributedQueryNodeRoute> routes,
    const std::chrono::steady_clock::time_point execution_deadline) {
  // NOLINTEND(bugprone-easily-swappable-parameters)
  if (!implementation_)
    return common::make_unexpected(invalid("Native grouped shuffle job provider is empty"));
  const auto now = std::chrono::steady_clock::now();
  if (fragments.empty() || execution_deadline <= now) {
    return common::make_unexpected(
        invalid("Native grouped shuffle job request is invalid or expired"));
  }
  const NativeDistributedGroupedShuffleJobProviderConfig& config = implementation_->config;
  if (std::ranges::any_of(fragments, [&](const auto& fragment) {
        return fragment.serving_node == config.coordinator_node_id;
      })) {
    return NativeDistributedGroupedShufflePlan{.selected = false};
  }

  try {
    auto authority =
        cluster::DistributedVectorGroupedAggregateShuffleAuthority::create_from_mutable_fragments(
            fragments, keys, aggregates, config.authority);
    if (!authority.has_value())
      return common::make_unexpected(authority.error());
    if (authority->destinations().size() > config.maximum_reducer_nodes) {
      return common::make_unexpected(
          exhausted("Native grouped shuffle reducer-node limit exceeded"));
    }
    if (!std::ranges::is_sorted(routes, {}, &cluster::DistributedQueryNodeRoute::node_id) ||
        std::ranges::adjacent_find(routes, {}, &cluster::DistributedQueryNodeRoute::node_id) !=
            routes.end()) {
      return common::make_unexpected(invalid("Native grouped shuffle routes are not canonical"));
    }

    std::vector<cluster::DistributedQueryNodeRoute> selected_routes;
    selected_routes.reserve(authority->destinations().size());
    for (const auto& destination : authority->destinations()) {
      const auto found = std::ranges::lower_bound(routes, destination.node_id, {},
                                                  &cluster::DistributedQueryNodeRoute::node_id);
      if (found == routes.end() || found->node_id != destination.node_id) {
        return common::make_unexpected(
            invalid("Native grouped shuffle reducer route coverage is incomplete"));
      }
      selected_routes.push_back(*found);
    }

    auto remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(execution_deadline - now);
    if (remaining.count() <= 0)
      remaining = std::chrono::milliseconds{1};
    const auto reducer_timeout = std::min(config.reducer_execution_timeout, remaining);
    cluster::DistributedVectorGroupedAggregateShuffleJobCoordinatorExecutionConfig reducers{
        .coordinator_node_id = config.coordinator_node_id,
        .reducer_control_routes = std::move(selected_routes),
        .authenticator = config.authenticator,
        .node_authorizer = config.node_authorizer,
        .carrier_limits = config.carrier_limits,
        .connect_timeout = config.connect_timeout,
        .prepare_retry = config.prepare_retry,
        .route_install_retry = config.route_install_retry,
        .seal_retry = config.seal_retry,
        .reducer_execution_timeout = reducer_timeout,
        .execution_deadline = execution_deadline,
        .maximum_reducer_nodes = config.maximum_reducer_nodes,
        .result = {.listener = config.result_listener,
                   .tls = config.result_tls,
                   .authenticator = config.authenticator,
                   .node_authorizer = config.node_authorizer,
                   .coordinator_node_id = config.coordinator_node_id,
                   .maximum_retained_server_streams = config.maximum_retained_result_streams,
                   .maximum_accepts_per_poll = config.maximum_result_accepts_per_poll,
                   .maximum_collected_encoded_bytes = config.maximum_collected_encoded_bytes,
                   .maximum_batch_working_bytes = config.maximum_batch_working_bytes,
                   .maximum_working_memory_bytes = config.maximum_working_memory_bytes,
                   .execution_deadline = execution_deadline}};
    return NativeDistributedGroupedShufflePlan{
        .selected = true, .reducer_jobs = std::move(reducers), .authority = config.authority};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Native grouped shuffle job plan allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("Native grouped shuffle job plan exceeds container limits"));
  }
}

} // namespace chronos::service
