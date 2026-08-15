#include "chronos/live/wal_subscription_reclaimer.hpp"

#include <algorithm>
#include <cstddef>
#include <new>
#include <optional>
#include <ranges>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

struct PendingWalReclamation {
  wal::WalWriter* writer{};
  std::uint64_t record_sequence{};
  std::optional<wal::WalReplayCheckpoint> checkpoint;
};

} // namespace

class WalSubscriptionSourceReclaimer::Impl {
public:
  explicit Impl(WalSubscriptionSourceReclaimerConfig configured) noexcept
      : config(std::move(configured)) {}

  WalSubscriptionSourceReclaimerConfig config;
};

WalSubscriptionSourceReclaimer::WalSubscriptionSourceReclaimer(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

WalSubscriptionSourceReclaimer::~WalSubscriptionSourceReclaimer() = default;
WalSubscriptionSourceReclaimer::WalSubscriptionSourceReclaimer(
    WalSubscriptionSourceReclaimer&&) noexcept = default;
WalSubscriptionSourceReclaimer&
WalSubscriptionSourceReclaimer::operator=(WalSubscriptionSourceReclaimer&&) noexcept = default;

common::Result<WalSubscriptionSourceReclaimer>
WalSubscriptionSourceReclaimer::create(WalSubscriptionSourceReclaimerConfig config) {
  if (config.sources.empty() || config.maximum_sources == 0U ||
      config.sources.size() > config.maximum_sources) {
    return common::make_unexpected(invalid("WAL subscription reclaimer source count is invalid"));
  }
  std::ranges::sort(config.sources, {}, &WalSubscriptionReclamationSource::tablet_id);
  for (std::size_t index = 0U; index < config.sources.size(); ++index) {
    const WalSubscriptionReclamationSource& source = config.sources[index];
    if (source.tablet_id.uuid().is_nil() || source.placement_epoch == 0U ||
        source.writer == nullptr || !source.writer->is_open() || source.writer->is_failed() ||
        !source.writer->wal_id().is_valid() ||
        (index != 0U && config.sources[index - 1U].tablet_id >= source.tablet_id)) {
      return common::make_unexpected(invalid("WAL subscription reclaimer source is invalid"));
    }
    for (std::size_t previous = 0U; previous < index; ++previous) {
      const WalSubscriptionReclamationSource& other = config.sources[previous];
      if (source.writer != other.writer && source.writer->wal_id() == other.writer->wal_id()) {
        return common::make_unexpected(
            invalid("WAL subscription reclaimer lineage has multiple writer owners"));
      }
    }
  }
  try {
    return WalSubscriptionSourceReclaimer{std::make_unique<Impl>(std::move(config))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("WAL subscription reclaimer allocation failed"));
  }
}

common::Status WalSubscriptionSourceReclaimer::reclaim(
    const std::span<const SubscriptionSourceReclamation> requests) {
  if (requests.size() != impl_->config.sources.size()) {
    return invalid("WAL subscription reclamation request count is invalid");
  }
  const raft::LogIndex metadata_index = requests.empty() ? 0U : requests.front().metadata_index;
  if (metadata_index == 0U) {
    return invalid("WAL subscription reclamation metadata index is invalid");
  }

  try {
    std::vector<PendingWalReclamation> pending;
    pending.reserve(requests.size());
    for (std::size_t index = 0U; index < requests.size(); ++index) {
      const WalSubscriptionReclamationSource& source = impl_->config.sources[index];
      const SubscriptionSourceReclamation& request = requests[index];
      if (!request.reclaim_through.same_source(
              SourcePosition::wal(source.tablet_id, source.writer->wal_id(), 0U)) ||
          request.placement_epoch != source.placement_epoch ||
          request.metadata_index != metadata_index) {
        return invalid("WAL subscription reclamation identity or topology is invalid");
      }
      const auto existing =
          std::ranges::find(pending, source.writer, &PendingWalReclamation::writer);
      if (existing == pending.end()) {
        pending.push_back({source.writer, request.reclaim_through.record_sequence, std::nullopt});
      } else {
        existing->record_sequence =
            std::min(existing->record_sequence, request.reclaim_through.record_sequence);
      }
    }

    // No physical mutation is allowed until every independent WAL has proved its complete current
    // namespace and mapped the conservative logical boundary to an exact record end.
    for (PendingWalReclamation& item : pending) {
      auto resolved = item.writer->resolve_replay_checkpoint(item.record_sequence);
      if (!resolved.has_value()) {
        return resolved.error();
      }
      item.checkpoint = *resolved;
    }
    for (PendingWalReclamation& item : pending) {
      if (!item.checkpoint.has_value()) {
        continue;
      }
      auto reclaimed = item.writer->reclaim_checkpointed_segments(*item.checkpoint);
      if (!reclaimed.has_value()) {
        return reclaimed.error();
      }
    }
    return common::Status::ok();
  } catch (const std::bad_alloc&) {
    return exhausted("WAL subscription reclamation allocation failed");
  } catch (const std::length_error&) {
    return exhausted("WAL subscription reclamation state exceeds limits");
  }
}

} // namespace chronos::live
