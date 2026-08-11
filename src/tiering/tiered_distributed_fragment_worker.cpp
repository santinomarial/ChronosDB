#include "chronos/tiering/tiered_distributed_fragment_worker.hpp"

#include <cstddef>
#include <memory>
#include <new>
#include <span>
#include <stdexcept>
#include <vector>

namespace chronos::tiering {
namespace {

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

class TieredPartBatchLoader final : public query::DistributedTemporalPartBatchLoader {
public:
  TieredPartBatchLoader(const TieredDatabaseStorageSnapshot& tiered_snapshot,
                        const manifest::ManifestStorage& local_storage,
                        const ObjectStore& remote_store,
                        const TieredTemporalPartLoadLimits limits) noexcept
      : tiered_snapshot_(tiered_snapshot), local_storage_(local_storage),
        remote_store_(remote_store), limits_(limits) {}

  common::Status load(const manifest::TemporalDatabaseStorageSnapshot& snapshot,
                      const std::span<const cseg::PartId> part_ids,
                      const std::span<const manifest::TabletSchemaBinding> schema_bindings,
                      const manifest::TemporalPartValidationLimits validation_limits,
                      query::DistributedTemporalPartBatchConsumer& consumer) const override {
    if (snapshot.selected_manifest() !=
        tiered_snapshot_.get().manifest_snapshot().selected_manifest()) {
      return unavailable("tiered worker Manifest owner differs from aggregate snapshot");
    }
    TieredTemporalPartLoadLimits effective = limits_;
    effective.validation = validation_limits;
    auto images = load_tiered_temporal_part_images(tiered_snapshot_, local_storage_, remote_store_,
                                                   part_ids, schema_bindings, effective);
    if (!images.has_value())
      return images.error();
    try {
      std::vector<query::TemporalManifestCsegPartView> views;
      views.reserve(images->size());
      for (const TieredTemporalPartImage& image : *images)
        views.push_back({.descriptor = &image.descriptor(), .bytes = image.bytes()});
      return consumer.consume(views);
    } catch (const std::bad_alloc&) {
      return {common::StatusCode::kResourceExhausted,
              "tiered distributed part view allocation failed"};
    } catch (const std::length_error&) {
      return {common::StatusCode::kResourceExhausted,
              "tiered distributed part view exceeds container limits"};
    }
  }

private:
  std::reference_wrapper<const TieredDatabaseStorageSnapshot> tiered_snapshot_;
  std::reference_wrapper<const manifest::ManifestStorage> local_storage_;
  std::reference_wrapper<const ObjectStore> remote_store_;
  TieredTemporalPartLoadLimits limits_;
};

} // namespace

common::Result<query::ExchangeMessage> execute_tiered_distributed_aggregate_fragment(
    const TieredDistributedAggregateWorkerRequest& request) {
  const query::DistributedAggregateWorkerRequest& worker = request.worker.get();
  const TieredDatabaseStorageSnapshot& tiered_snapshot = request.tiered_snapshot.get();
  if (worker.snapshot.get().selected_manifest() !=
      tiered_snapshot.manifest_snapshot().selected_manifest()) {
    return common::make_unexpected(
        unavailable("tiered worker request does not pin the aggregate Manifest owner"));
  }
  const TieredPartBatchLoader loader{tiered_snapshot, worker.storage.get(),
                                     request.remote_store.get(), request.load_limits};
  return query::execute_distributed_aggregate_fragment(worker, loader);
}

} // namespace chronos::tiering
