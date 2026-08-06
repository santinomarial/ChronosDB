#include "chronos/common/bytes.hpp"
#include "chronos/manifest/codec.hpp"
#include "manifest/manifest_test_support.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <span>
#include <vector>

namespace {

void exercise(const chronos::common::ByteView bytes) {
  using chronos::manifest::ManifestDecodeErrorKind;
  const auto prefix = chronos::manifest::decode_manifest_v1_prefix(bytes);
  if (prefix.has_value()) {
    if (prefix->encoded_bytes().data() != bytes.data() ||
        prefix->encoded_bytes().size() > bytes.size() || prefix->generation() == 0U) {
      std::abort();
    }
  } else if (prefix.error().kind() == ManifestDecodeErrorKind::kIncomplete &&
             (prefix.error().required_size() <= bytes.size() ||
              prefix.error().status().code() != chronos::common::StatusCode::kOutOfRange)) {
    std::abort();
  }

  const auto exact = chronos::manifest::decode_manifest_v1_exact(bytes);
  if (exact.has_value()) {
    if (!prefix.has_value() || exact->encoded_bytes().size() != bytes.size()) {
      std::abort();
    }
  } else if (prefix.has_value() && prefix->encoded_bytes().size() == bytes.size()) {
    std::abort();
  }
}

[[nodiscard]] std::vector<std::byte> structured_manifest() {
  const chronos::manifest::test::ManifestFixture fixture;
  const chronos::manifest::EncodedManifest encoded =
      chronos::manifest::test::encode_fixture(fixture);
  return {encoded.bytes().begin(), encoded.bytes().end()};
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  exercise(chronos::common::byte_view(std::span<const std::uint8_t>{data, size}));

  static const std::vector<std::byte> canonical = structured_manifest();
  std::vector<std::byte> mutated = canonical;
  if (size != 0U) {
    const std::size_t offset = static_cast<std::size_t>(data[0]) % mutated.size();
    const std::byte mask = size > 1U ? static_cast<std::byte>(data[1]) : std::byte{1U};
    mutated[offset] ^= mask;
    if (size > 2U) {
      const std::size_t new_size = static_cast<std::size_t>(data[2]) * mutated.size() / 255U;
      mutated.resize(new_size);
    }
  }
  exercise(mutated);
  return 0;
}
