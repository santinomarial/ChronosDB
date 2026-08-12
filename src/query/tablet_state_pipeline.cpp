#include "chronos/query/tablet_state_pipeline.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"
#include "chronos/query/snapshot_shape.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}

class SerialHeadSources final : public PhysicalOperator {
public:
  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(const QueryResourceContext& resources,
         std::vector<std::unique_ptr<PhysicalOperator>> sources,
         const std::size_t maximum_configuration_bytes) {
    if (sources.empty())
      return common::make_unexpected(invalid("tablet-state pipeline requires a head source"));
    auto slots = common::checked_multiply(sources.capacity(), sizeof(sources.front()) + 256U);
    auto charge = slots.has_value()
                      ? common::checked_add(sizeof(SerialHeadSources) + std::size_t{256U}, *slots)
                      : std::nullopt;
    if (!charge.has_value() || *charge > maximum_configuration_bytes)
      return common::make_unexpected(exhausted("tablet-state source configuration exceeds limit"));
    auto reservation = resources.reserve(*charge);
    if (!reservation.has_value())
      return common::make_unexpected(reservation.error());
    try {
      return std::unique_ptr<PhysicalOperator>{
          new SerialHeadSources{std::move(sources), std::move(*reservation)}};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("tablet-state source allocation failed"));
    }
  }

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override {
    if (ended_)
      return PhysicalOperatorStep::end();
    if (!resources.owns(reservation_)) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(invalid("tablet-state source belongs to another query"));
    }
    auto active = resources.check_cancelled();
    if (!active.has_value())
      return common::make_unexpected(active.error());
    while (next_source_ < sources_.size()) {
      auto step = sources_[next_source_]->next(resources);
      if (!step.has_value()) {
        static_cast<void>(resources.request_cancel());
        sources_.clear();
        reservation_.release();
        ended_ = true;
        return common::make_unexpected(step.error());
      }
      if (step->kind() == PhysicalOperatorStepKind::kEnd) {
        sources_[next_source_].reset();
        ++next_source_;
        continue;
      }
      if (step->chunk() == nullptr || !step->chunk()->belongs_to(resources)) {
        static_cast<void>(resources.request_cancel());
        return common::make_unexpected(
            invalid("tablet-state source returned a foreign or missing chunk"));
      }
      return step;
    }
    std::vector<std::unique_ptr<PhysicalOperator>>{}.swap(sources_);
    reservation_.release();
    ended_ = true;
    return PhysicalOperatorStep::end();
  }

private:
  SerialHeadSources(std::vector<std::unique_ptr<PhysicalOperator>> sources,
                    QueryMemoryReservation reservation) noexcept
      : sources_(std::move(sources)), reservation_(std::move(reservation)) {}

  std::vector<std::unique_ptr<PhysicalOperator>> sources_;
  QueryMemoryReservation reservation_;
  std::size_t next_source_{};
  bool ended_{};
};

} // namespace

common::Result<std::unique_ptr<PhysicalOperator>> instantiate_tablet_states_pipeline(
    const QueryResourceContext& resources, const std::span<const ingest::TabletSnapshot> snapshots,
    const schema::SchemaLineage& lineage, const schema::SchemaId destination_schema_id,
    const PhysicalPipelinePlan& pipeline, TabletStatePipelineLimits limits) {
  if (limits.maximum_source_configuration_bytes == 0U || snapshots.empty())
    return common::make_unexpected(invalid("tablet-state pipeline limits are invalid"));
  const std::shared_ptr<const schema::TableSchema> destination =
      lineage.find(destination_schema_id);
  if (destination == nullptr || destination->table_id() != lineage.table_id()) {
    return common::make_unexpected(
        invalid("tablet-state snapshot, lineage, and destination schema disagree"));
  }
  auto row_version_mode =
      validate_snapshot_pipeline_input_shape(pipeline.input_columns(), *destination);
  if (!row_version_mode.has_value())
    return common::make_unexpected(row_version_mode.error());
  limits.scan.row_version_columns = *row_version_mode;

  try {
    std::vector<schema::TabletId> tablet_ids;
    tablet_ids.reserve(snapshots.size());
    std::size_t source_count{};
    for (const ingest::TabletSnapshot& snapshot : snapshots) {
      if (snapshot.table_id() != lineage.table_id() || snapshot.tablet_id().uuid().is_nil()) {
        return common::make_unexpected(
            invalid("tablet-state snapshots contain a mismatched or nil tablet"));
      }
      tablet_ids.push_back(snapshot.tablet_id());
      const auto generations =
          common::checked_add(snapshot.sealed_generations().size(), std::size_t{1U});
      if (!generations.has_value())
        return common::make_unexpected(exhausted("tablet-state source count exceeds limits"));
      const auto total = common::checked_add(source_count, *generations);
      if (!total.has_value())
        return common::make_unexpected(exhausted("tablet-state source count exceeds limits"));
      source_count = *total;
    }
    std::ranges::sort(tablet_ids);
    if (std::ranges::adjacent_find(tablet_ids) != tablet_ids.end())
      return common::make_unexpected(invalid("tablet-state snapshots contain a duplicate tablet"));

    std::vector<std::uint32_t> ordinals;
    ordinals.reserve(destination->columns().size());
    for (std::size_t ordinal = 0U; ordinal < destination->columns().size(); ++ordinal)
      ordinals.push_back(static_cast<std::uint32_t>(ordinal));

    std::vector<std::unique_ptr<PhysicalOperator>> sources;
    sources.reserve(source_count);
    const auto add = [&](head::HeadSnapshot generation,
                         const schema::TabletId& tablet_id) -> common::Result<void> {
      auto source =
          HeadScanOperator::create(resources, std::move(generation), lineage, destination_schema_id,
                                   tablet_id, ordinals, limits.scan);
      if (!source.has_value())
        return common::make_unexpected(source.error());
      sources.push_back(std::move(*source));
      return {};
    };
    for (const ingest::TabletSnapshot& snapshot : snapshots) {
      for (const head::HeadSnapshot& sealed : snapshot.sealed_generations()) {
        auto added = add(sealed, snapshot.tablet_id());
        if (!added.has_value())
          return common::make_unexpected(added.error());
      }
      auto active = add(snapshot.active_generation(), snapshot.tablet_id());
      if (!active.has_value())
        return common::make_unexpected(active.error());
    }

    std::unique_ptr<PhysicalOperator> source;
    if (sources.size() == 1U) {
      source = std::move(sources.front());
    } else {
      auto merged = SerialHeadSources::create(resources, std::move(sources),
                                              limits.maximum_source_configuration_bytes);
      if (!merged.has_value())
        return common::make_unexpected(merged.error());
      source = std::move(*merged);
    }
    return pipeline.instantiate(std::move(source));
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("tablet-state pipeline allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("tablet-state pipeline exceeds container limits"));
  }
}

common::Result<std::unique_ptr<PhysicalOperator>> instantiate_tablet_state_pipeline(
    const QueryResourceContext& resources, ingest::TabletSnapshot snapshot,
    const schema::SchemaLineage& lineage, const schema::SchemaId destination_schema_id,
    const PhysicalPipelinePlan& pipeline, TabletStatePipelineLimits limits) {
  return instantiate_tablet_states_pipeline(resources, std::span{&snapshot, 1U}, lineage,
                                            destination_schema_id, pipeline, limits);
}

} // namespace chronos::query
