#ifndef CHRONOS_INGEST_SHA256_HPP_
#define CHRONOS_INGEST_SHA256_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"

#include <array>
#include <compare>
#include <cstddef>
#include <span>

namespace chronos::ingest {

class Sha256Digest {
public:
  static constexpr std::size_t kSize = 32U;
  using Bytes = std::array<std::byte, kSize>;

  explicit constexpr Sha256Digest(Bytes bytes) noexcept : bytes_(bytes) {}

  [[nodiscard]] constexpr const Bytes& bytes() const noexcept {
    return bytes_;
  }

  friend constexpr auto operator<=>(const Sha256Digest&, const Sha256Digest&) = default;

private:
  Bytes bytes_;
};

// SHA-256 is provided by the configured OpenSSL 3 provider. Fragments are hashed in order without
// constructing a concatenated buffer. Every fragment's backing storage must remain alive during
// the call; no storage is borrowed by the result.
[[nodiscard]] common::Result<Sha256Digest>
sha256(std::span<const common::ByteView> fragments);
[[nodiscard]] common::Result<Sha256Digest> sha256(common::ByteView bytes);

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_SHA256_HPP_
