#include "chronos/live/resume_token.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <openssl/evp.h>
#include <span>
#include <vector>

namespace {

[[nodiscard]] chronos::common::Uuid uuid(const std::uint8_t seed) {
  chronos::common::Uuid::Bytes bytes{};
  bytes.fill(static_cast<std::byte>(seed));
  bytes.front() = static_cast<std::byte>(seed | 1U);
  return chronos::common::Uuid{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier identifier(const std::uint8_t seed) {
  auto result = Identifier::from_uuid(uuid(seed));
  if (!result.has_value())
    std::abort();
  return *result;
}

[[nodiscard]] chronos::wal::WalId wal_id(const std::uint8_t seed) {
  chronos::wal::WalId result{};
  result.bytes = uuid(seed).bytes();
  return result;
}

[[nodiscard]] chronos::live::ResumeTokenMacKey mac_key() {
  chronos::live::ResumeTokenMacKey key{};
  key.fill(std::byte{0xa5});
  return key;
}

void refresh_mac(std::vector<std::byte>& bytes, const chronos::live::ResumeTokenMacKey& key) {
  if (bytes.size() < chronos::live::kResumeTokenMacSize)
    return;
  const std::size_t authenticated_size = bytes.size() - chronos::live::kResumeTokenMacSize;
  std::size_t output_size = 0U;
  // OpenSSL models octet buffers as unsigned char while ChronosDB uses std::byte.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  const auto* input = reinterpret_cast<const unsigned char*>(bytes.data());
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  auto* output = reinterpret_cast<unsigned char*>(bytes.data() + authenticated_size);
  if (EVP_Q_mac(nullptr, "HMAC", nullptr, "SHA256", nullptr, key.data(), key.size(), input,
                authenticated_size, output, chronos::live::kResumeTokenMacSize,
                &output_size) == nullptr ||
      output_size != chronos::live::kResumeTokenMacSize) {
    std::abort();
  }
}

void exercise_bytes(const chronos::common::ByteView bytes,
                    const chronos::live::ResumeTokenMacKey& key) {
  using namespace chronos::live;
  const auto v1 = decode_resume_token_v1(bytes, key);
  const auto v2 = decode_resume_token_v2(bytes, key);
  const auto compatible = decode_resume_token(bytes, key);

  if (v1.has_value() && v2.has_value())
    std::abort();
  if (compatible.has_value()) {
    if (v1.has_value() == v2.has_value())
      std::abort();
    const ResumeToken& explicit_token = v1.has_value() ? *v1 : *v2;
    if (*compatible != explicit_token)
      std::abort();
  } else if (v1.has_value() || v2.has_value()) {
    std::abort();
  }

  if (v1.has_value()) {
    const auto encoded = encode_resume_token_v1(*v1, key);
    if (!encoded.has_value() || !std::ranges::equal(*encoded, bytes))
      std::abort();
  }
  if (v2.has_value()) {
    const auto encoded = encode_resume_token_v2(*v2, key);
    if (!encoded.has_value() || !std::ranges::equal(*encoded, bytes))
      std::abort();
  }
}

[[nodiscard]] chronos::live::ResumeToken structured_token(const chronos::common::ByteView input,
                                                          const bool source_tagged) {
  using namespace chronos;
  const auto input_byte = [input](const std::size_t index, const std::uint8_t fallback) {
    return index < input.size() ? std::to_integer<std::uint8_t>(input[index]) : fallback;
  };

  live::PlanFingerprint fingerprint{};
  fingerprint.fill(static_cast<std::byte>(input_byte(0U, 0x37U)));
  const std::size_t source_count = 1U + (input_byte(1U, 1U) % 8U);
  std::vector<live::SourcePosition> positions;
  positions.reserve(source_count);
  for (std::size_t index = 0U; index < source_count; ++index) {
    const std::uint8_t seed = input_byte(index + 2U, static_cast<std::uint8_t>(index + 1U));
    const schema::TabletId tablet =
        identifier<schema::TabletId>(static_cast<std::uint8_t>(seed + 1U));
    const std::uint64_t sequence =
        (static_cast<std::uint64_t>(seed) << 56U) | static_cast<std::uint64_t>(index);
    if (source_tagged && (seed & 1U) != 0U) {
      positions.push_back(
          live::SourcePosition::raft(tablet, uuid(static_cast<std::uint8_t>(seed + 2U)), sequence));
    } else {
      positions.push_back(live::SourcePosition::wal(
          tablet, wal_id(static_cast<std::uint8_t>(seed + 2U)), sequence));
    }
  }

  return {.database_id = uuid(input_byte(10U, 1U)),
          .subscription_id = uuid(input_byte(11U, 2U)),
          .schema_id = identifier<schema::SchemaId>(input_byte(12U, 3U)),
          .schema_version = schema::SchemaVersion::initial(),
          .safe_delivery_sequence = input_byte(13U, 42U),
          .plan_fingerprint = fingerprint,
          .source_positions = std::move(positions)};
}

void exercise_structured(const chronos::common::ByteView input,
                         const chronos::live::ResumeTokenMacKey& key, const bool source_tagged) {
  using namespace chronos::live;
  const ResumeToken token = structured_token(input, source_tagged);
  auto encoded =
      source_tagged ? encode_resume_token_v2(token, key) : encode_resume_token_v1(token, key);
  if (!encoded.has_value())
    std::abort();
  exercise_bytes(*encoded, key);

  const std::size_t source_count = token.source_positions.size();
  if (source_count > 1U && decode_resume_token(*encoded, key, source_count - 1U).has_value()) {
    std::abort();
  }

  if (!input.empty()) {
    const std::size_t authenticated_size = encoded->size() - kResumeTokenMacSize;
    const std::size_t offset_selector =
        static_cast<std::size_t>(std::to_integer<std::uint8_t>(input.front())) |
        (input.size() > 1U
             ? static_cast<std::size_t>(std::to_integer<std::uint8_t>(input[1U])) << 8U
             : 0U);
    const std::size_t offset = offset_selector % encoded->size();
    const std::byte supplied_mask = input.size() > 2U ? input[2U] : std::byte{1U};
    (*encoded)[offset] ^= supplied_mask == std::byte{} ? std::byte{1U} : supplied_mask;
    if (offset < authenticated_size)
      refresh_mac(*encoded, key);
    exercise_bytes(*encoded, key);
  }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const chronos::common::ByteView bytes = std::as_bytes(std::span{data, size});
  const chronos::live::ResumeTokenMacKey key = mac_key();
  exercise_bytes(bytes, key);
  exercise_structured(bytes, key, false);
  exercise_structured(bytes, key, true);
  return 0;
}
