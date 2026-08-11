#include "chronos/cluster/tablet_physical_snapshot_ownership.hpp"

#include "chronos/manifest/temporal_codec.hpp"

#include <algorithm>
#include <memory>
#include <new>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}
[[nodiscard]] common::Status corruption(std::string message) {
  return {common::StatusCode::kCorruption, std::move(message)};
}
[[nodiscard]] common::Status unavailable(std::string message) {
  return {common::StatusCode::kUnavailable, std::move(message)};
}
[[nodiscard]] common::Status with_context(const std::string_view context,
                                          const common::Status& status) {
  std::string message{context};
  message.append(": ");
  message.append(status.message());
  return {status.code(), std::move(message)};
}

} // namespace

common::Result<PublishedTabletPhysicalSnapshot> install_and_publish_tablet_physical_snapshot(
    manifest::ManifestStorage& storage, manifest::TemporalDatabaseStoragePublisher& publisher,
    const TabletPhysicalSnapshotOwnershipRequest& request) {
  if (request.manifest_nonce.is_nil())
    return common::make_unexpected(invalid("destination Manifest nonce must be nonzero"));
  auto authority = manifest::validate_raft_tablet_physical_snapshot(
      request.physical_snapshot, request.group_id, request.table_id, request.tablet_id,
      request.raft_snapshot.get(), request.decode_limits);
  if (!authority.has_value())
    return common::make_unexpected(
        with_context("validate physical snapshot authority", authority.error()));
  auto current = publisher.snapshot();
  if (!current.has_value())
    return common::make_unexpected(current.error());
  auto current_view =
      manifest::decode_manifest_v2_temporal_exact(current->manifest_bytes(), request.decode_limits);
  if (!current_view.has_value()) {
    publisher.fail_closed_after_durable_successor();
    return common::make_unexpected(current_view.error().status());
  }
  auto candidate = manifest::build_raft_tablet_destination_manifest(
      *current_view, {.physical_snapshot = request.physical_snapshot,
                      .group_id = request.group_id,
                      .table_id = request.table_id,
                      .tablet_id = request.tablet_id,
                      .raft_snapshot = request.raft_snapshot,
                      .schema_bindings = request.schema_bindings,
                      .decode_limits = request.decode_limits});
  if (!candidate.has_value())
    return common::make_unexpected(with_context("build destination Manifest", candidate.error()));
  auto candidate_view =
      manifest::decode_manifest_v2_temporal_exact(candidate->bytes(), request.decode_limits);
  if (!candidate_view.has_value())
    return common::make_unexpected(candidate_view.error().status());

  auto namespace_snapshot = storage.scan_namespace();
  if (!namespace_snapshot.has_value())
    return common::make_unexpected(namespace_snapshot.error());
  const std::uint64_t highest = namespace_snapshot->generations.back();
  bool already_durable = false;
  if (highest == current->generation()) {
    auto installed = storage.install_temporal_manifest(
        {.encoded_manifest = std::cref(*candidate),
         .schema_bindings = request.schema_bindings,
         .nonce = request.manifest_nonce,
         .decode_limits = request.decode_limits,
         .part_validation_limits = request.part_validation_limits});
    if (!installed.has_value()) {
      if (!storage.is_usable())
        publisher.fail_closed_after_durable_successor();
      return common::make_unexpected(
          with_context("install destination Manifest", installed.error()));
    }
  } else if (highest == candidate_view->generation()) {
    already_durable = true;
  } else {
    publisher.fail_closed_after_durable_successor();
    return common::make_unexpected(
        highest > current->generation()
            ? unavailable("durable Manifest namespace is not the expected live successor")
            : corruption("live temporal publication is ahead of its durable Manifest namespace"));
  }

  auto loaded = storage.load_selected_temporal_manifest(
      {.expected_database_id = current->database_id(),
       .schema_bindings = request.schema_bindings,
       .source_bindings = request.source_bindings,
       .decode_limits = request.decode_limits,
       .part_validation_limits = request.part_validation_limits});
  if (!loaded.has_value()) {
    publisher.fail_closed_after_durable_successor();
    return common::make_unexpected(with_context("reload destination Manifest", loaded.error()));
  }
  if (!std::ranges::equal(loaded->encoded_bytes(), candidate->bytes())) {
    publisher.fail_closed_after_durable_successor();
    return common::make_unexpected(
        corruption("durable destination Manifest differs from the expected successor"));
  }
  std::shared_ptr<const manifest::LoadedTemporalManifestGeneration> selected;
  try {
    selected =
        std::make_shared<const manifest::LoadedTemporalManifestGeneration>(std::move(*loaded));
  } catch (const std::bad_alloc&) {
    publisher.fail_closed_after_durable_successor();
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "cannot allocate durable destination Manifest publication owner"});
  }
  auto published = publisher.publish_manifest({.selected_manifest = std::move(selected),
                                               .schema_bindings = request.schema_bindings,
                                               .decode_limits = request.decode_limits});
  if (!published.has_value())
    return common::make_unexpected(with_context("publish destination Manifest", published.error()));
  return PublishedTabletPhysicalSnapshot{.authority = std::move(*authority),
                                         .destination = std::move(*published),
                                         .manifest_already_durable = already_durable};
}

} // namespace chronos::cluster
