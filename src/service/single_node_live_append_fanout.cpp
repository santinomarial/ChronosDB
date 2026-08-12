#include "chronos/service/single_node_live_append_fanout.hpp"

#include "chronos/common/status.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <new>
#include <ranges>
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
[[nodiscard]] bool source_contains(const live::MultiTabletSubscriptionSource& source,
                                   const schema::TabletId tablet_id,
                                   const wal::WalId& wal_id) noexcept {
  return std::ranges::any_of(source.members,
                             [&](const live::MultiTabletSubscriptionMember& member) {
                               return member.tablet_id == tablet_id && member.wal_id == wal_id;
                             });
}

} // namespace

SingleNodeLiveAppendFanout::SingleNodeLiveAppendFanout(std::vector<Entry> entries) noexcept
    : entries_(std::move(entries)) {
  metrics_.configured_plans = entries_.size();
}

common::Result<std::unique_ptr<SingleNodeLiveAppendFanout>>
SingleNodeLiveAppendFanout::create(std::vector<SingleNodeLiveAppendBinding> bindings,
                                   const std::size_t maximum_bindings) {
  if (bindings.empty() || maximum_bindings == 0U || bindings.size() > maximum_bindings)
    return common::make_unexpected(invalid("live append fan-out binding count is invalid"));
  try {
    std::vector<Entry> entries;
    entries.reserve(bindings.size());
    for (SingleNodeLiveAppendBinding& binding : bindings) {
      if (binding.plan == nullptr || binding.coordinator == nullptr || binding.resources == nullptr)
        return common::make_unexpected(invalid("live append fan-out binding owner is null"));
      const common::Status plan_status = live::validate_committed_batch_plan(*binding.plan);
      if (!plan_status.is_ok())
        return common::make_unexpected(plan_status);
      const live::MultiTabletSubscriptionSource& source = binding.coordinator->source();
      if (source.table_id != binding.plan->schema_ptr()->table_id() ||
          source.plan_fingerprint != binding.plan->fingerprint() ||
          source.schema_id != binding.plan->schema_ptr()->schema_id() ||
          source.schema_version != binding.plan->schema_ptr()->version())
        return common::make_unexpected(
            invalid("live append fan-out plan and coordinator disagree"));
      if (std::ranges::any_of(entries, [&](const Entry& entry) {
            return entry.binding.plan->fingerprint() == binding.plan->fingerprint();
          }))
        return common::make_unexpected(invalid("live append fan-out plan is duplicated"));
      entries.push_back({std::move(binding), true});
    }
    return std::unique_ptr<SingleNodeLiveAppendFanout>{
        new SingleNodeLiveAppendFanout{std::move(entries)}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("live append fan-out allocation failed"));
  }
}

void SingleNodeLiveAppendFanout::on_applied(AppliedSingleNodeColumnarAppend append) noexcept {
  ++metrics_.observed_appends;
  if (append.batch == nullptr || !append.position.is_valid()) {
    ++metrics_.containment_failures;
    return;
  }
  for (Entry& entry : entries_) {
    if (!entry.enabled)
      continue;
    const live::MultiTabletSubscriptionSource& source = entry.binding.coordinator->source();
    if (source.table_id != append.batch->schema().table_id() ||
        !source_contains(source, append.tablet_id, append.position.wal_id))
      continue;
    const live::SourcePosition position{append.tablet_id, append.position.wal_id,
                                        append.position.record_sequence};
    const auto contain = [&]() noexcept {
      try {
        const common::Status contained = entry.binding.coordinator->mark_continuity_lost(position);
        if (contained.is_ok()) {
          ++metrics_.continuity_losses;
          return;
        }
      } catch (...) {
      }
      entry.enabled = false;
      ++metrics_.disabled_plans;
      ++metrics_.containment_failures;
    };
    if (append.batch->schema().schema_id() != source.schema_id ||
        append.batch->schema().version() != source.schema_version) {
      try {
        live::CommittedChange incompatible{position,
                                           append.batch->schema().schema_id(),
                                           append.batch->schema().version(),
                                           live::LogicalChangeOperation::kUpsert,
                                           {std::byte{1U}},
                                           {}};
        const common::Status published =
            entry.binding.coordinator->publish_committed(std::move(incompatible));
        if (published.is_ok()) {
          ++metrics_.schema_invalidations;
          continue;
        }
      } catch (...) {
      }
      ++metrics_.publication_failures;
      contain();
    } else {
      ++metrics_.evaluated_plans;
      try {
        auto change =
            live::evaluate_committed_batch(*entry.binding.plan, position, append.batch,
                                           *entry.binding.resources, entry.binding.evaluator);
        if (change.has_value()) {
          const common::Status published =
              entry.binding.coordinator->publish_committed(std::move(*change));
          if (published.is_ok()) {
            ++metrics_.published_changes;
            continue;
          }
          ++metrics_.publication_failures;
        } else {
          ++metrics_.evaluation_failures;
        }
      } catch (...) {
        ++metrics_.evaluation_failures;
      }
      contain();
    }
  }
}

SingleNodeLiveAppendFanoutMetrics SingleNodeLiveAppendFanout::metrics() const noexcept {
  return metrics_;
}

} // namespace chronos::service
