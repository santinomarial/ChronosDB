#include "chronos/cluster/tablet_physical_part_install.hpp"

#include "chronos/cseg/format.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/cseg/temporal_format.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return {common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status with_context(const std::string_view context,
                                          const common::Status& status) {
  std::string message{context};
  message.append(": ");
  message.append(status.message());
  return {status.code(), std::move(message)};
}

[[nodiscard]] common::Status validate_authority(const CompletedTabletPhysicalPartTransfer& transfer,
                                                const TabletPhysicalPartInstallRequest& request) {
  const TabletPhysicalPartTransferSession& session = transfer.session;
  const manifest::TemporalPartDescriptor& descriptor = request.descriptor;
  const manifest::TemporalTabletDescriptor& owner = request.owner;
  if (request.expected_manifest_generation == 0U)
    return invalid("expected physical snapshot Manifest generation must be nonzero");
  if (request.maximum_materialized_bytes == 0U ||
      request.maximum_materialized_bytes > cseg::format::kMaximumFileLength) {
    return invalid("physical part materialization limit is outside CSEG bounds");
  }
  if (session.manifest_generation != request.expected_manifest_generation)
    return invalid("physical part transfer Manifest generation does not match authority");
  if (session.table_id != descriptor.table_id || session.tablet_id != descriptor.tablet_id ||
      session.part_id != descriptor.part_id || session.total_bytes != descriptor.file_length ||
      session.content_sha256 != descriptor.content_sha256) {
    return corruption("physical part transfer does not match its Manifest descriptor");
  }
  if (owner.table_id != descriptor.table_id || owner.tablet_id != descriptor.tablet_id)
    return corruption("physical part descriptor does not belong to its tablet owner");
  if (session.group_id != owner.source_id || session.group_id != descriptor.source_id)
    return corruption("physical part transfer does not match its Raft source lineage");
  if (owner.commit_source != manifest::ManifestCommitSource::kRaft ||
      descriptor.commit_source != manifest::ManifestCommitSource::kRaft) {
    return corruption("physical part installation requires Raft-owned temporal state");
  }
  if (descriptor.cseg_format_major != cseg::temporal_format::kFormatMajor ||
      descriptor.cseg_format_minor != cseg::temporal_format::kFormatMinor) {
    return corruption("physical part descriptor does not name CSEG v2");
  }
  if (transfer.received_bytes != session.total_bytes)
    return corruption("completed physical part transfer length changed");
  return common::Status::ok();
}

} // namespace

common::Result<InstalledTabletPhysicalPart>
install_tablet_physical_part(const TabletPhysicalPartChunkStorage& transfer,
                             manifest::ManifestStorage& destination,
                             const TabletPhysicalPartInstallRequest& request) {
  auto completed = transfer.finalize();
  if (!completed.has_value())
    return common::make_unexpected(
        with_context("finalize received physical part", completed.error()));
  common::Status authority = validate_authority(*completed, request);
  if (!authority.is_ok())
    return common::make_unexpected(std::move(authority));
  if (completed->received_bytes > request.maximum_materialized_bytes)
    return common::make_unexpected(exhausted("physical part exceeds materialization limit"));
  if (completed->received_bytes > std::numeric_limits<std::size_t>::max())
    return common::make_unexpected(exhausted("physical part exceeds platform container limits"));

  std::vector<std::byte> image;
  try {
    image.reserve(static_cast<std::size_t>(completed->received_bytes));
    std::uint64_t offset = 0U;
    while (offset < completed->received_bytes) {
      auto loaded = transfer.load_chunk(offset);
      if (!loaded.has_value()) {
        return common::make_unexpected(
            with_context("reload received physical part chunk", loaded.error()));
      }
      if (loaded->chunk.session != completed->session || loaded->chunk.bytes.empty() ||
          loaded->chunk.bytes.size() > completed->received_bytes - offset) {
        return common::make_unexpected(
            corruption("received physical part chunk changed during materialization"));
      }
      image.insert(image.end(), loaded->chunk.bytes.begin(), loaded->chunk.bytes.end());
      offset += loaded->chunk.bytes.size();
    }
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("physical part materialization allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("physical part materialization exceeded limits"));
  }
  if (image.size() != completed->received_bytes)
    return common::make_unexpected(corruption("physical part materialization length changed"));

  auto encoded = cseg::adopt_cseg_v2_temporal_part(image, request.validation_limits.decode);
  if (!encoded.has_value())
    return common::make_unexpected(with_context("adopt received CSEG v2", encoded.error()));
  auto installed =
      destination.install_temporal_part({.encoded_part = std::cref(*encoded),
                                         .descriptor = request.descriptor,
                                         .owner = request.owner,
                                         .schema = request.schema,
                                         .nonce = request.nonce,
                                         .validation_limits = request.validation_limits});
  if (!installed.has_value())
    return common::make_unexpected(
        with_context("install received temporal CSEG", installed.error()));
  return InstalledTabletPhysicalPart{.transfer = *completed, .part = std::move(*installed)};
}

} // namespace chronos::cluster
