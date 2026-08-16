#include "chronos/live/raft_subscription_reclaimer.hpp"

#include <algorithm>
#include <cstddef>
#include <new>
#include <optional>
#include <ranges>
#include <utility>

namespace chronos::live {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

} // namespace

class RaftSubscriptionSourceReclaimer::Impl {
public:
  explicit Impl(RaftSubscriptionSourceReclaimerConfig configured) noexcept
      : config(std::move(configured)) {}

  RaftSubscriptionSourceReclaimerConfig config;
};

RaftSubscriptionSourceReclaimer::RaftSubscriptionSourceReclaimer(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

RaftSubscriptionSourceReclaimer::~RaftSubscriptionSourceReclaimer() = default;
RaftSubscriptionSourceReclaimer::RaftSubscriptionSourceReclaimer(
    RaftSubscriptionSourceReclaimer&&) noexcept = default;
RaftSubscriptionSourceReclaimer&
RaftSubscriptionSourceReclaimer::operator=(RaftSubscriptionSourceReclaimer&&) noexcept = default;

common::Result<RaftSubscriptionSourceReclaimer>
RaftSubscriptionSourceReclaimer::create(RaftSubscriptionSourceReclaimerConfig config) {
  if (config.sources.empty() || config.maximum_sources == 0U ||
      config.sources.size() > config.maximum_sources || config.runtime == nullptr ||
      config.application == nullptr || !config.application->initialized() ||
      config.application->failed() || !config.runtime->owns_worker_extension(*config.application))
    return common::make_unexpected(invalid("Raft subscription reclaimer configuration is invalid"));
  std::ranges::sort(config.sources, {}, &RaftSubscriptionReclamationSource::tablet_id);
  for (std::size_t index = 0U; index < config.sources.size(); ++index) {
    const RaftSubscriptionReclamationSource& source = config.sources[index];
    if (source.tablet_id.uuid().is_nil() || source.group_id.is_nil() ||
        source.placement_epoch == 0U ||
        (index != 0U && config.sources[index - 1U].tablet_id >= source.tablet_id))
      return common::make_unexpected(invalid("Raft subscription reclaimer source is invalid"));
    if (std::ranges::find(config.sources.begin(),
                          config.sources.begin() + static_cast<std::ptrdiff_t>(index),
                          source.group_id, &RaftSubscriptionReclamationSource::group_id) !=
        config.sources.begin() + static_cast<std::ptrdiff_t>(index))
      return common::make_unexpected(invalid("Raft subscription reclaimer group is duplicated"));
  }
  try {
    return RaftSubscriptionSourceReclaimer{std::make_unique<Impl>(std::move(config))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft subscription reclaimer allocation failed"));
  }
}

common::Status RaftSubscriptionSourceReclaimer::reclaim(
    const std::span<const SubscriptionSourceReclamation> requests) {
  if (requests.size() != impl_->config.sources.size())
    return invalid("Raft subscription reclamation request count is invalid");
  const raft::LogIndex metadata_index = requests.empty() ? 0U : requests.front().metadata_index;
  if (metadata_index == 0U)
    return invalid("Raft subscription reclamation metadata index is invalid");

  for (std::size_t index = 0U; index < requests.size(); ++index) {
    const RaftSubscriptionReclamationSource& source = impl_->config.sources[index];
    const SubscriptionSourceReclamation& request = requests[index];
    if (!request.reclaim_through.same_source(
            SourcePosition::raft(source.tablet_id, source.group_id, 0U)) ||
        request.reclaim_through.record_sequence == 0U ||
        request.placement_epoch != source.placement_epoch ||
        request.metadata_index != metadata_index)
      return invalid("Raft subscription reclamation identity or topology is invalid");
    auto tablet = impl_->config.application->snapshot(source.group_id);
    if (!tablet.has_value())
      return tablet.error();
    const std::optional<head::HeadCommitPosition>& applied = tablet->applied_position();
    if (tablet->tablet_id() != source.tablet_id || !applied.has_value() ||
        applied->source != head::CommitSource::kRaft || applied->raft_group_id != source.group_id ||
        applied->record_sequence < request.reclaim_through.record_sequence)
      return unavailable("Raft tablet publication does not cover reclamation frontier");
  }

  // Observations are read-only and complete before the first physical mutation. A compacted Raft
  // prefix is authoritative only because the hosted tablet application exact-loaded its matching
  // durable application snapshot during worker initialization.
  for (std::size_t index = 0U; index < requests.size(); ++index) {
    const RaftSubscriptionReclamationSource& source = impl_->config.sources[index];
    auto completion = impl_->config.runtime->try_observe_group(source.group_id);
    if (!completion.has_value())
      return completion.error();
    auto result = completion->wait();
    if (!result.has_value())
      return result.error();
    if (result->size() != 1U || !result->front().status.is_ok())
      return result->empty() ? unavailable("Raft reclamation observation returned no result")
                             : result->front().status;
    const std::optional<raft::RaftGroupObservation>& observation = result->front().observation;
    if (!observation.has_value() || observation->group_id != source.group_id ||
        observation->applied_index < requests[index].reclaim_through.record_sequence ||
        observation->snapshot_index < requests[index].reclaim_through.record_sequence)
      return unavailable("durable Raft snapshot does not cover reclamation frontier");
  }

  auto completion = impl_->config.runtime->try_checkpoint_and_reclaim();
  if (!completion.has_value())
    return completion.error();
  auto reclaimed = completion->wait();
  return reclaimed.has_value() ? common::Status::ok() : reclaimed.error();
}

} // namespace chronos::live
