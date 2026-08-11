#include "chronos/ingest/sha256.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <memory>
#include <openssl/evp.h>
#include <span>
#include <utility>

namespace chronos::ingest {
namespace {

using MessageDigest = std::unique_ptr<EVP_MD, decltype(&EVP_MD_free)>;
using MessageDigestContext = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>;

[[nodiscard]] common::Status crypto_error(const char* message) {
  return common::Status{common::StatusCode::kInternal, message};
}

} // namespace

class Sha256Hasher::Impl {
public:
  Impl(MessageDigest digest, MessageDigestContext context) noexcept
      : digest_(std::move(digest)), context_(std::move(context)) {}

  MessageDigest digest_;
  MessageDigestContext context_;
  bool finished_{false};
  common::Status failure_;
};

Sha256Hasher::Sha256Hasher(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
Sha256Hasher::~Sha256Hasher() = default;
Sha256Hasher::Sha256Hasher(Sha256Hasher&&) noexcept = default;
Sha256Hasher& Sha256Hasher::operator=(Sha256Hasher&&) noexcept = default;

common::Result<Sha256Hasher> Sha256Hasher::create() {
  MessageDigest digest{EVP_MD_fetch(nullptr, "SHA256", nullptr), EVP_MD_free};
  if (digest == nullptr) {
    return common::make_unexpected(crypto_error("OpenSSL SHA-256 provider is unavailable"));
  }
  MessageDigestContext context{EVP_MD_CTX_new(), EVP_MD_CTX_free};
  if (context == nullptr) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "OpenSSL SHA-256 context allocation failed"});
  }
  if (EVP_DigestInit_ex2(context.get(), digest.get(), nullptr) != 1) {
    return common::make_unexpected(crypto_error("OpenSSL SHA-256 initialization failed"));
  }
  try {
    return Sha256Hasher{std::make_unique<Impl>(std::move(digest), std::move(context))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted, "SHA-256 owner allocation failed"});
  }
}

common::Status Sha256Hasher::update(const common::ByteView bytes) {
  if (implementation_ == nullptr) {
    return {common::StatusCode::kInvalidArgument, "SHA-256 owner was moved from"};
  }
  if (implementation_->finished_) {
    return {common::StatusCode::kInvalidArgument, "SHA-256 digest is already finalized"};
  }
  if (!implementation_->failure_.is_ok()) {
    return implementation_->failure_;
  }
  if (!bytes.empty() &&
      EVP_DigestUpdate(implementation_->context_.get(), bytes.data(), bytes.size()) != 1) {
    implementation_->failure_ = crypto_error("OpenSSL SHA-256 update failed");
  }
  return implementation_->failure_;
}

common::Result<Sha256Digest> Sha256Hasher::finish() {
  if (implementation_ == nullptr) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument, "SHA-256 owner was moved from"});
  }
  if (implementation_->finished_) {
    return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                  "SHA-256 digest is already finalized"});
  }
  implementation_->finished_ = true;
  if (!implementation_->failure_.is_ok()) {
    return common::make_unexpected(implementation_->failure_);
  }
  std::array<unsigned char, Sha256Digest::kSize> output{};
  unsigned int digest_length = 0U;
  if (EVP_DigestFinal_ex(implementation_->context_.get(), output.data(), &digest_length) != 1 ||
      digest_length != output.size()) {
    implementation_->failure_ = crypto_error("OpenSSL SHA-256 finalization failed");
    return common::make_unexpected(implementation_->failure_);
  }
  Sha256Digest::Bytes bytes{};
  std::transform(output.begin(), output.end(), bytes.begin(),
                 [](const unsigned char value) { return static_cast<std::byte>(value); });
  return Sha256Digest{bytes};
}

common::Result<Sha256Digest> sha256(const std::span<const common::ByteView> fragments) {
  auto hasher = Sha256Hasher::create();
  if (!hasher.has_value())
    return common::make_unexpected(hasher.error());
  for (const common::ByteView fragment : fragments) {
    common::Status updated = hasher->update(fragment);
    if (!updated.is_ok())
      return common::make_unexpected(std::move(updated));
  }
  return hasher->finish();
}

common::Result<Sha256Digest> sha256(const common::ByteView bytes) {
  const std::array<common::ByteView, 1U> fragments{bytes};
  return sha256(fragments);
}

} // namespace chronos::ingest
