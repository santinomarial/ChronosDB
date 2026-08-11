#ifndef CHRONOS_TIERING_OBJECT_STORE_HPP_
#define CHRONOS_TIERING_OBJECT_STORE_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/ingest/sha256.hpp"

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace chronos::tiering {

struct ObjectMetadata {
  std::string key;
  std::size_t size{};
  ingest::Sha256Digest checksum;

  friend bool operator==(const ObjectMetadata&, const ObjectMetadata&) = default;
};

// S3-compatible semantic boundary. Implementations must make put_if_absent idempotent for one
// immutable key and must never report success for a different existing body.
class ObjectStore {
public:
  ObjectStore() = default;
  ObjectStore(const ObjectStore&) = delete;
  ObjectStore& operator=(const ObjectStore&) = delete;
  ObjectStore(ObjectStore&&) = delete;
  ObjectStore& operator=(ObjectStore&&) = delete;
  virtual ~ObjectStore() = default;

  [[nodiscard]] virtual common::Result<ObjectMetadata>
  put_if_absent(std::string_view key, common::ByteView bytes,
                const ingest::Sha256Digest& checksum) = 0;
  [[nodiscard]] virtual common::Result<ObjectMetadata> stat(std::string_view key) const = 0;
  [[nodiscard]] virtual common::Result<std::vector<std::byte>>
  get_range(std::string_view key, std::size_t offset, std::size_t length) const = 0;
};

// Deterministic reference backend for focused tests and embedded deployments. It follows the same
// immutable-key contract as a remote S3 implementation but does not claim remote durability.
class MemoryObjectStore final : public ObjectStore {
public:
  MemoryObjectStore();
  ~MemoryObjectStore() override;
  MemoryObjectStore(const MemoryObjectStore&) = delete;
  MemoryObjectStore& operator=(const MemoryObjectStore&) = delete;
  MemoryObjectStore(MemoryObjectStore&&) = delete;
  MemoryObjectStore& operator=(MemoryObjectStore&&) = delete;

  [[nodiscard]] common::Result<ObjectMetadata>
  put_if_absent(std::string_view key, common::ByteView bytes,
                const ingest::Sha256Digest& checksum) override;
  [[nodiscard]] common::Result<ObjectMetadata> stat(std::string_view key) const override;
  [[nodiscard]] common::Result<std::vector<std::byte>>
  get_range(std::string_view key, std::size_t offset, std::size_t length) const override;
  [[nodiscard]] std::size_t object_count() const noexcept;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

struct S3ObjectStoreConfig {
  // A scheme and authority with an optional base path, for example
  // https://s3.us-east-1.amazonaws.com or https://minio.example/storage.
  std::string endpoint;
  std::string region;
  std::string bucket;
  std::string access_key_id;
  std::string secret_access_key;
  std::optional<std::string> session_token;
  std::optional<std::string> ca_bundle_path;
  std::chrono::milliseconds connect_timeout{5'000};
  std::chrono::milliseconds request_timeout{30'000};
  std::size_t maximum_response_bytes{std::size_t{4U} * 1024U * 1024U * 1024U};
  // Plain HTTP is rejected by default. This switch exists for explicitly isolated S3-compatible
  // deployments and local tests; credentials and object bytes are exposed to that network.
  bool require_tls{true};
};

// Synchronous S3-compatible HTTPS backend. Each operation owns an independent libcurl easy handle,
// so callers may invoke const and non-const operations concurrently. Requests use SigV4, never
// follow redirects, enforce finite timeouts and response bounds, and use If-None-Match for
// immutable object creation. The configuration (including credentials) is owned for the store
// lifetime.
class S3ObjectStore final : public ObjectStore {
public:
  S3ObjectStore() = delete;
  ~S3ObjectStore() override;
  S3ObjectStore(const S3ObjectStore&) = delete;
  S3ObjectStore& operator=(const S3ObjectStore&) = delete;
  S3ObjectStore(S3ObjectStore&&) = delete;
  S3ObjectStore& operator=(S3ObjectStore&&) = delete;

  [[nodiscard]] static common::Result<std::unique_ptr<S3ObjectStore>>
  create(S3ObjectStoreConfig config);

  [[nodiscard]] common::Result<ObjectMetadata>
  put_if_absent(std::string_view key, common::ByteView bytes,
                const ingest::Sha256Digest& checksum) override;
  [[nodiscard]] common::Result<ObjectMetadata> stat(std::string_view key) const override;
  [[nodiscard]] common::Result<std::vector<std::byte>>
  get_range(std::string_view key, std::size_t offset, std::size_t length) const override;

private:
  class Impl;
  explicit S3ObjectStore(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::tiering

#endif // CHRONOS_TIERING_OBJECT_STORE_HPP_
