#include "chronos/tiering/object_store.hpp"

#include "chronos/ingest/sha256.hpp"

#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::tiering {
namespace {
[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}
} // namespace

class MemoryObjectStore::Impl {
public:
  struct Object {
    std::vector<std::byte> bytes;
    ingest::Sha256Digest checksum;
  };
  mutable std::mutex mutex;
  std::map<std::string, Object, std::less<>> objects;
};

MemoryObjectStore::MemoryObjectStore() : impl_(std::make_unique<Impl>()) {}
MemoryObjectStore::~MemoryObjectStore() = default;

common::Result<ObjectMetadata>
MemoryObjectStore::put_if_absent(const std::string_view key, const common::ByteView bytes,
                                 const ingest::Sha256Digest& checksum) {
  if (key.empty())
    return common::make_unexpected(invalid("object key must be nonempty"));
  auto actual = ingest::sha256(bytes);
  if (!actual.has_value())
    return common::make_unexpected(actual.error());
  if (*actual != checksum) {
    return common::make_unexpected(invalid("object checksum does not match upload bytes"));
  }
  std::scoped_lock lock{impl_->mutex};
  const auto existing = impl_->objects.find(key);
  if (existing != impl_->objects.end()) {
    if (existing->second.checksum != checksum || existing->second.bytes.size() != bytes.size()) {
      return common::make_unexpected(common::Status{common::StatusCode::kAlreadyExists,
                                                    "immutable object key has different content"});
    }
    return ObjectMetadata{std::string{key}, existing->second.bytes.size(), checksum};
  }
  impl_->objects.emplace(std::string{key}, Impl::Object{{bytes.begin(), bytes.end()}, checksum});
  return ObjectMetadata{std::string{key}, bytes.size(), checksum};
}

common::Result<ObjectMetadata> MemoryObjectStore::stat(const std::string_view key) const {
  std::scoped_lock lock{impl_->mutex};
  const auto found = impl_->objects.find(key);
  if (found == impl_->objects.end()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "object does not exist"});
  }
  return ObjectMetadata{found->first, found->second.bytes.size(), found->second.checksum};
}

common::Result<std::vector<std::byte>>
MemoryObjectStore::get_range(const std::string_view key, const std::size_t offset,
                             const std::size_t length) const {
  std::scoped_lock lock{impl_->mutex};
  const auto found = impl_->objects.find(key);
  if (found == impl_->objects.end()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "object does not exist"});
  }
  if (offset > found->second.bytes.size() || length > found->second.bytes.size() - offset) {
    return common::make_unexpected(invalid("object range is outside immutable content"));
  }
  return std::vector<std::byte>{found->second.bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                                found->second.bytes.begin() +
                                    static_cast<std::ptrdiff_t>(offset + length)};
}

std::size_t MemoryObjectStore::object_count() const noexcept {
  std::scoped_lock lock{impl_->mutex};
  return impl_->objects.size();
}

} // namespace chronos::tiering
