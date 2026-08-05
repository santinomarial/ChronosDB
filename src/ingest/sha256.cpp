#include "chronos/ingest/sha256.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <memory>
#include <openssl/evp.h>
#include <span>

namespace chronos::ingest {
namespace {

using MessageDigest = std::unique_ptr<EVP_MD, decltype(&EVP_MD_free)>;
using MessageDigestContext = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>;

[[nodiscard]] common::Status crypto_error(const char* message) {
  return common::Status{common::StatusCode::kInternal, message};
}

} // namespace

common::Result<Sha256Digest> sha256(const std::span<const common::ByteView> fragments) {
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
  for (const common::ByteView fragment : fragments) {
    if (!fragment.empty() &&
        EVP_DigestUpdate(context.get(), fragment.data(), fragment.size()) != 1) {
      return common::make_unexpected(crypto_error("OpenSSL SHA-256 update failed"));
    }
  }

  std::array<unsigned char, Sha256Digest::kSize> output{};
  unsigned int digest_length = 0U;
  if (EVP_DigestFinal_ex(context.get(), output.data(), &digest_length) != 1 ||
      digest_length != output.size()) {
    return common::make_unexpected(crypto_error("OpenSSL SHA-256 finalization failed"));
  }
  Sha256Digest::Bytes bytes{};
  std::transform(output.begin(), output.end(), bytes.begin(),
                 [](const unsigned char value) { return static_cast<std::byte>(value); });
  return Sha256Digest{bytes};
}

common::Result<Sha256Digest> sha256(const common::ByteView bytes) {
  const std::array<common::ByteView, 1U> fragments{bytes};
  return sha256(fragments);
}

} // namespace chronos::ingest
