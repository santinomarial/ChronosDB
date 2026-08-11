#include "chronos/manifest/temporal_publication.hpp"

#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/manifest/temporal_validation.hpp"

#include <atomic>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <utility>

namespace chronos::manifest {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}
[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}
[[nodiscard]] common::Status unavailable(std::string message) {
  return {common::StatusCode::kUnavailable, std::move(message)};
}

[[nodiscard]] common::Status decode_status(const ManifestDecodeError& error) {
  return error.status();
}

[[nodiscard]] common::Status
validate_selected(const LoadedTemporalManifestGeneration& selected,
                  const std::span<const TabletSchemaBinding> schema_bindings,
                  const ManifestDecodeLimits limits) {
  auto decoded = decode_manifest_v2_temporal_exact(selected.encoded_bytes(), limits);
  if (!decoded.has_value())
    return decode_status(decoded.error());
  if (decoded->generation() != selected.generation() ||
      decoded->database_id() != selected.database_id()) {
    return invalid("loaded temporal Manifest owner disagrees with its encoded bytes");
  }
  return validate_manifest_v2_temporal_schema_binding(*decoded, schema_bindings);
}

} // namespace

class TemporalDatabaseStoragePublisher::Impl {
public:
  explicit Impl(std::shared_ptr<const LoadedTemporalManifestGeneration> selected) noexcept
      : selected_(std::move(selected)) {}

  [[nodiscard]] common::Result<TemporalDatabaseStorageSnapshot> snapshot() const {
    if (failed_.load(std::memory_order_acquire))
      return common::make_unexpected(unavailable("temporal storage publisher is failed closed"));
    return TemporalDatabaseStorageSnapshot{
        std::atomic_load_explicit(&selected_, std::memory_order_acquire)};
  }

  [[nodiscard]] common::Result<TemporalDatabaseStorageSnapshot>
  publish(const DurableTemporalManifestPublicationRequest& request) {
    if (failed_.load(std::memory_order_acquire))
      return common::make_unexpected(unavailable("temporal storage publisher is failed closed"));
    if (request.selected_manifest == nullptr)
      return common::make_unexpected(invalid("temporal Manifest publication requires an owner"));
    auto fail = [&](common::Status status) -> common::Result<TemporalDatabaseStorageSnapshot> {
      failed_.store(true, std::memory_order_release);
      return common::make_unexpected(std::move(status));
    };
    try {
      const std::shared_ptr<const LoadedTemporalManifestGeneration> current =
          std::atomic_load_explicit(&selected_, std::memory_order_acquire);
      auto predecessor =
          decode_manifest_v2_temporal_exact(current->encoded_bytes(), request.decode_limits);
      if (!predecessor.has_value())
        return fail(decode_status(predecessor.error()));
      auto successor = decode_manifest_v2_temporal_exact(request.selected_manifest->encoded_bytes(),
                                                         request.decode_limits);
      if (!successor.has_value())
        return fail(decode_status(successor.error()));
      common::Status transition = validate_manifest_v2_temporal_transition(*predecessor, *successor,
                                                                           request.schema_bindings);
      if (!transition.is_ok())
        return fail(std::move(transition));
      if (successor->generation() != request.selected_manifest->generation() ||
          successor->database_id() != request.selected_manifest->database_id()) {
        return fail(invalid("durable temporal Manifest owner disagrees with its encoded bytes"));
      }
      std::atomic_store_explicit(&selected_, request.selected_manifest, std::memory_order_release);
      return TemporalDatabaseStorageSnapshot{request.selected_manifest};
    } catch (const std::bad_alloc&) {
      return fail(exhausted("temporal Manifest publication allocation failed"));
    } catch (const std::length_error&) {
      return fail(exhausted("temporal Manifest publication exceeded limits"));
    }
  }

  [[nodiscard]] bool is_usable() const noexcept {
    return !failed_.load(std::memory_order_acquire);
  }

  void fail_closed() noexcept {
    failed_.store(true, std::memory_order_release);
  }

  [[nodiscard]] common::Status poison_status() const {
    return is_usable() ? common::Status::ok() : poison_status_;
  }

private:
  std::shared_ptr<const LoadedTemporalManifestGeneration> selected_;
  std::atomic<bool> failed_{false};
  common::Status poison_status_{
      unavailable("temporal storage publisher failed after durable successor")};
};

TemporalDatabaseStorageSnapshot::TemporalDatabaseStorageSnapshot(
    std::shared_ptr<const LoadedTemporalManifestGeneration> selected) noexcept
    : selected_(std::move(selected)) {}

std::uint64_t TemporalDatabaseStorageSnapshot::generation() const noexcept {
  return selected_->generation();
}
const DatabaseId& TemporalDatabaseStorageSnapshot::database_id() const noexcept {
  return selected_->database_id();
}
common::ByteView TemporalDatabaseStorageSnapshot::manifest_bytes() const noexcept {
  return selected_->encoded_bytes();
}
std::span<const TemporalTabletDescriptor>
TemporalDatabaseStorageSnapshot::tablets() const noexcept {
  return selected_->tablets();
}
std::span<const TemporalPartDescriptor> TemporalDatabaseStorageSnapshot::parts() const noexcept {
  return selected_->parts();
}
std::span<const TemporalRetryDescriptor> TemporalDatabaseStorageSnapshot::retries() const noexcept {
  return selected_->retries();
}
std::shared_ptr<const LoadedTemporalManifestGeneration>
TemporalDatabaseStorageSnapshot::selected_manifest() const noexcept {
  return selected_;
}

TemporalDatabaseStoragePublisher::TemporalDatabaseStoragePublisher(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
TemporalDatabaseStoragePublisher::~TemporalDatabaseStoragePublisher() = default;
TemporalDatabaseStoragePublisher::TemporalDatabaseStoragePublisher(
    TemporalDatabaseStoragePublisher&&) noexcept = default;
TemporalDatabaseStoragePublisher&
TemporalDatabaseStoragePublisher::operator=(TemporalDatabaseStoragePublisher&&) noexcept = default;

common::Result<TemporalDatabaseStoragePublisher> TemporalDatabaseStoragePublisher::create(
    std::shared_ptr<const LoadedTemporalManifestGeneration> selected,
    const std::span<const TabletSchemaBinding> schema_bindings, const ManifestDecodeLimits limits) {
  if (selected == nullptr)
    return common::make_unexpected(invalid("temporal storage publisher requires an owner"));
  common::Status validation = validate_selected(*selected, schema_bindings, limits);
  if (!validation.is_ok())
    return common::make_unexpected(std::move(validation));
  try {
    return TemporalDatabaseStoragePublisher{std::make_unique<Impl>(std::move(selected))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("temporal storage publisher allocation failed"));
  }
}

common::Result<TemporalDatabaseStorageSnapshot> TemporalDatabaseStoragePublisher::snapshot() const {
  if (implementation_ == nullptr)
    return common::make_unexpected(invalid("temporal storage publisher was moved from"));
  return implementation_->snapshot();
}
common::Result<TemporalDatabaseStorageSnapshot> TemporalDatabaseStoragePublisher::publish_manifest(
    const DurableTemporalManifestPublicationRequest& request) {
  if (implementation_ == nullptr)
    return common::make_unexpected(invalid("temporal storage publisher was moved from"));
  return implementation_->publish(request);
}
void TemporalDatabaseStoragePublisher::fail_closed_after_durable_successor() noexcept {
  if (implementation_ != nullptr)
    implementation_->fail_closed();
}
bool TemporalDatabaseStoragePublisher::is_usable() const noexcept {
  return implementation_ != nullptr && implementation_->is_usable();
}
common::Status TemporalDatabaseStoragePublisher::poison_status() const {
  return implementation_ == nullptr ? invalid("temporal storage publisher was moved from")
                                    : implementation_->poison_status();
}

} // namespace chronos::manifest
