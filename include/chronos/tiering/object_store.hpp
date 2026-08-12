#ifndef CHRONOS_TIERING_OBJECT_STORE_HPP_
#define CHRONOS_TIERING_OBJECT_STORE_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/ingest/sha256.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
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

struct ObjectDeletionReport {
  bool removed{};
  bool already_absent{};

  friend bool operator==(const ObjectDeletionReport&, const ObjectDeletionReport&) = default;
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
  // Idempotently removes only the exact immutable key/length/SHA-256 identity. A different current
  // object fails without deletion; an already absent key is successful and reported separately.
  [[nodiscard]] virtual common::Result<ObjectDeletionReport>
  remove_if_exact(std::string_view key, std::size_t expected_size,
                  const ingest::Sha256Digest& expected_checksum) = 0;
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
  [[nodiscard]] common::Result<ObjectDeletionReport>
  remove_if_exact(std::string_view key, std::size_t expected_size,
                  const ingest::Sha256Digest& expected_checksum) override;
  [[nodiscard]] std::size_t object_count() const noexcept;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

class S3CredentialProvider;

enum class S3ServerSideEncryption : std::uint8_t {
  kS3ManagedAes256 = 1U,
  kKms = 2U,
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
  std::shared_ptr<S3CredentialProvider> credential_provider;
  std::optional<std::string> ca_bundle_path;
  // Process proxy variables are never inherited. Supplying this explicitly enables one HTTP(S)
  // proxy for every request; TLS endpoint verification and redirect rejection remain unchanged.
  std::optional<std::string> proxy_url;
  std::chrono::milliseconds connect_timeout{5'000};
  std::chrono::milliseconds request_timeout{30'000};
  std::size_t maximum_attempts{3U};
  std::chrono::milliseconds initial_retry_backoff{50};
  std::chrono::milliseconds maximum_retry_backoff{1'000};
  // Adds a uniformly distributed nonnegative delay after exponential/provider floors, without
  // exceeding maximum_retry_backoff. A fixed seed is intended only for deterministic testing.
  std::chrono::milliseconds maximum_retry_jitter{50};
  std::optional<std::uint64_t> retry_jitter_seed;
  std::size_t multipart_threshold_bytes{64U * 1024U * 1024U};
  std::size_t multipart_part_bytes{16U * 1024U * 1024U};
  // Bounds simultaneously active UploadPart requests for one object. All workers are joined before
  // completion or abort, and completion retains ascending part-number order.
  std::size_t multipart_maximum_concurrency{4U};
  std::size_t maximum_response_bytes{std::size_t{4U} * 1024U * 1024U * 1024U};
  // When set, every object creation requests this encryption mode and every successful HEAD must
  // report it. SSE-KMS also requires kms_key_id; use the canonical identifier expected in the
  // provider's HEAD response (normally a key ARN) so exact verification remains deterministic.
  std::optional<S3ServerSideEncryption> server_side_encryption;
  std::optional<std::string> kms_key_id;
  // Plain HTTP is rejected by default. This switch exists for explicitly isolated S3-compatible
  // deployments and local tests; credentials and object bytes are exposed to that network.
  bool require_tls{true};
};

struct S3Credentials {
  std::string access_key_id;
  std::string secret_access_key;
  std::optional<std::string> session_token;
};

enum class S3CredentialRequest : std::uint8_t {
  kCurrent = 1U,
  kRefresh = 2U,
};

// A provider may implement environment, workload-identity, instance-role, or ordered-chain policy.
// acquire() may be called concurrently and must synchronize its own mutable cache. kRefresh follows
// an authenticated request rejected with HTTP 401/403 and must not return a knowingly expired
// cached value as successful refresh.
class S3CredentialProvider {
public:
  S3CredentialProvider() = default;
  S3CredentialProvider(const S3CredentialProvider&) = delete;
  S3CredentialProvider& operator=(const S3CredentialProvider&) = delete;
  S3CredentialProvider(S3CredentialProvider&&) = delete;
  S3CredentialProvider& operator=(S3CredentialProvider&&) = delete;
  virtual ~S3CredentialProvider() = default;

  [[nodiscard]] virtual common::Result<S3Credentials> acquire(S3CredentialRequest request) = 0;
};

// Explicit opt-in provider for the standard AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and optional
// AWS_SESSION_TOKEN variables. create() snapshots process environment once; acquire() is immutable
// and thread-safe. The caller must exclude process-environment mutation while create() runs. A
// forced refresh fails closed because concurrent environment mutation is not a supported
// credential-rotation mechanism.
class S3EnvironmentCredentialProvider final : public S3CredentialProvider {
public:
  S3EnvironmentCredentialProvider() = delete;
  ~S3EnvironmentCredentialProvider() override = default;
  S3EnvironmentCredentialProvider(const S3EnvironmentCredentialProvider&) = delete;
  S3EnvironmentCredentialProvider& operator=(const S3EnvironmentCredentialProvider&) = delete;
  S3EnvironmentCredentialProvider(S3EnvironmentCredentialProvider&&) = delete;
  S3EnvironmentCredentialProvider& operator=(S3EnvironmentCredentialProvider&&) = delete;

  [[nodiscard]] static common::Result<std::shared_ptr<S3EnvironmentCredentialProvider>> create();
  [[nodiscard]] common::Result<S3Credentials> acquire(S3CredentialRequest request) override;

private:
  explicit S3EnvironmentCredentialProvider(S3Credentials credentials) noexcept;
  S3Credentials credentials_;
};

// Explicit ordered precedence across caller-selected providers. During initial selection only,
// NOT_FOUND means "not configured" and advances to the next provider; every other failure stops.
// Once selected, one provider remains pinned for current and refresh requests so authorization
// rejection cannot silently downgrade to a lower-priority identity.
class S3CredentialProviderChain final : public S3CredentialProvider {
public:
  S3CredentialProviderChain() = delete;
  ~S3CredentialProviderChain() override;
  S3CredentialProviderChain(const S3CredentialProviderChain&) = delete;
  S3CredentialProviderChain& operator=(const S3CredentialProviderChain&) = delete;
  S3CredentialProviderChain(S3CredentialProviderChain&&) = delete;
  S3CredentialProviderChain& operator=(S3CredentialProviderChain&&) = delete;

  [[nodiscard]] static common::Result<std::shared_ptr<S3CredentialProviderChain>>
  create(std::vector<std::shared_ptr<S3CredentialProvider>> providers);
  [[nodiscard]] common::Result<S3Credentials> acquire(S3CredentialRequest request) override;

private:
  class Impl;
  explicit S3CredentialProviderChain(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

struct S3ContainerCredentialProviderConfig {
  // Explicit container-agent credential URL. Redirects and ambient proxies are disabled.
  std::string endpoint;
  std::optional<std::string> authorization_token;
  std::optional<std::string> ca_bundle_path;
  std::chrono::milliseconds connect_timeout{1'000};
  std::chrono::milliseconds request_timeout{2'000};
  std::chrono::seconds refresh_before_expiration{300};
  std::size_t maximum_response_bytes{64U * 1024U};
  bool require_tls{true};
};

// Explicit ECS/EKS-compatible container credential provider. It never reads process environment,
// token files, proxies, or metadata endpoints implicitly. Cached credentials are returned only
// before the configured expiration refresh window; kRefresh always contacts the configured agent.
class S3ContainerCredentialProvider final : public S3CredentialProvider {
public:
  S3ContainerCredentialProvider() = delete;
  ~S3ContainerCredentialProvider() override;
  S3ContainerCredentialProvider(const S3ContainerCredentialProvider&) = delete;
  S3ContainerCredentialProvider& operator=(const S3ContainerCredentialProvider&) = delete;
  S3ContainerCredentialProvider(S3ContainerCredentialProvider&&) = delete;
  S3ContainerCredentialProvider& operator=(S3ContainerCredentialProvider&&) = delete;

  [[nodiscard]] static common::Result<std::shared_ptr<S3ContainerCredentialProvider>>
  create(S3ContainerCredentialProviderConfig config);
  [[nodiscard]] common::Result<S3Credentials> acquire(S3CredentialRequest request) override;

private:
  class Impl;
  explicit S3ContainerCredentialProvider(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

// Synchronous S3-compatible HTTPS backend. Each operation owns an independent libcurl easy handle,
// so callers may invoke const and non-const operations concurrently. Requests use SigV4, never
// follow redirects, enforce finite timeouts and response bounds, and use If-None-Match for
// immutable object creation. Retryable requests use bounded capped backoff. Credentials are either
// static values owned for the store lifetime or are acquired per attempt from a caller-owned shared
// provider; the two modes are mutually exclusive.
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
  [[nodiscard]] common::Result<ObjectDeletionReport>
  remove_if_exact(std::string_view key, std::size_t expected_size,
                  const ingest::Sha256Digest& expected_checksum) override;

private:
  class Impl;
  explicit S3ObjectStore(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::tiering

#endif // CHRONOS_TIERING_OBJECT_STORE_HPP_
