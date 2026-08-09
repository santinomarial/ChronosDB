#include "chronos/live/resume_token.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstring>
#include <limits>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace chronos::live {
namespace {

inline constexpr std::array<std::byte, 8U> kMagic{
    std::byte{0x43}, std::byte{0x48}, std::byte{0x52}, std::byte{0x4e},
    std::byte{0x52}, std::byte{0x53}, std::byte{0x4d}, std::byte{0x00},
};

[[nodiscard]] common::Status invalid(std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

[[nodiscard]] common::Result<ResumeTokenMacKey> compute_mac(const common::ByteView bytes,
                                                            const ResumeTokenMacKey& key) {
  ResumeTokenMacKey output{};
  std::size_t output_size = 0U;
  if (EVP_Q_mac(nullptr, "HMAC", nullptr, "SHA256", nullptr, key.data(), key.size(),
                reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size(),
                reinterpret_cast<unsigned char*>(output.data()), output.size(),
                &output_size) == nullptr ||
      output_size != output.size()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "OpenSSL HMAC-SHA256 failed"});
  }
  return output;
}

[[nodiscard]] bool key_is_zero(const ResumeTokenMacKey& key) noexcept {
  return std::ranges::all_of(key, [](const std::byte value) { return value == std::byte{0}; });
}

[[nodiscard]] common::Status write_identifier(common::ByteWriter& writer,
                                              const common::Uuid::Bytes& bytes) {
  return writer.write_exact(bytes);
}

} // namespace

common::Result<std::vector<std::byte>> encode_resume_token_v1(const ResumeToken& token,
                                                              const ResumeTokenMacKey& key) {
  if (token.database_id.is_nil() || token.subscription_id.is_nil() ||
      token.schema_id.uuid().is_nil()) {
    return common::make_unexpected(invalid("resume token identities must be nonzero"));
  }
  if (key_is_zero(key)) {
    return common::make_unexpected(invalid("resume token MAC key must be nonzero"));
  }
  if (token.source_positions.empty() ||
      token.source_positions.size() > kMaximumResumeTokenSources) {
    return common::make_unexpected(invalid("resume token source count is outside v1 bounds"));
  }
  for (const SourcePosition& position : token.source_positions) {
    if (position.tablet_id.uuid().is_nil() || !position.wal_id.is_valid()) {
      return common::make_unexpected(invalid("resume token contains an invalid source identity"));
    }
  }

  const auto positions_size = common::checked_multiply<std::size_t>(token.source_positions.size(),
                                                                    kResumeTokenPositionSize);
  if (!positions_size.has_value()) {
    return common::make_unexpected(invalid("resume token size overflows"));
  }
  const auto authenticated_size =
      common::checked_add<std::size_t>(kResumeTokenHeaderSize, *positions_size);
  if (!authenticated_size.has_value()) {
    return common::make_unexpected(invalid("resume token size overflows"));
  }
  const auto total_size =
      common::checked_add<std::size_t>(*authenticated_size, kResumeTokenMacSize);
  if (!total_size.has_value() || *total_size > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(invalid("resume token size exceeds v1 encoding"));
  }

  std::vector<std::byte> encoded(*total_size);
  common::ByteWriter writer{common::MutableByteView{encoded.data(), *authenticated_size}};
  auto status = writer.write_exact(kMagic);
  if (status.is_ok()) {
    status = writer.write_u16_le(kResumeTokenFormatMajor);
  }
  if (status.is_ok()) {
    status = writer.write_u16_le(kResumeTokenFormatMinor);
  }
  if (status.is_ok()) {
    status = writer.write_u32_le(static_cast<std::uint32_t>(kResumeTokenHeaderSize));
  }
  if (status.is_ok()) {
    status = writer.write_u32_le(static_cast<std::uint32_t>(*total_size));
  }
  if (status.is_ok()) {
    status = writer.write_u32_le(static_cast<std::uint32_t>(token.source_positions.size()));
  }
  if (status.is_ok()) {
    status = write_identifier(writer, token.database_id.bytes());
  }
  if (status.is_ok()) {
    status = write_identifier(writer, token.subscription_id.bytes());
  }
  if (status.is_ok()) {
    status = write_identifier(writer, token.schema_id.bytes());
  }
  if (status.is_ok()) {
    status = writer.write_u64_le(token.schema_version.value());
  }
  if (status.is_ok()) {
    status = writer.write_u64_le(token.safe_delivery_sequence);
  }
  if (status.is_ok()) {
    status = writer.write_exact(token.plan_fingerprint);
  }
  if (status.is_ok()) {
    status = writer.zero_fill(8U);
  }
  for (const SourcePosition& position : token.source_positions) {
    if (!status.is_ok()) {
      break;
    }
    status = write_identifier(writer, position.tablet_id.bytes());
    if (status.is_ok()) {
      status = writer.write_exact(position.wal_id.bytes);
    }
    if (status.is_ok()) {
      status = writer.write_u64_le(position.record_sequence);
    }
  }
  if (!status.is_ok() || !writer.full()) {
    return common::make_unexpected(
        status.is_ok()
            ? common::Status{common::StatusCode::kInternal, "resume token layout was not filled"}
            : std::move(status));
  }

  auto mac = compute_mac(common::ByteView{encoded.data(), *authenticated_size}, key);
  if (!mac.has_value()) {
    return common::make_unexpected(mac.error());
  }
  std::copy(mac->begin(), mac->end(),
            encoded.begin() + static_cast<std::ptrdiff_t>(*authenticated_size));
  return encoded;
}

common::Result<ResumeToken> decode_resume_token_v1(const common::ByteView encoded,
                                                   const ResumeTokenMacKey& key,
                                                   const std::size_t maximum_sources) {
  if (key_is_zero(key)) {
    return common::make_unexpected(invalid("resume token MAC key must be nonzero"));
  }
  if (maximum_sources == 0U || maximum_sources > kMaximumResumeTokenSources) {
    return common::make_unexpected(invalid("resume token decoder source bound is invalid"));
  }
  if (encoded.size() < kResumeTokenHeaderSize + kResumeTokenPositionSize + kResumeTokenMacSize) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "resume token is truncated"});
  }

  const std::size_t authenticated_size = encoded.size() - kResumeTokenMacSize;
  auto expected_mac = compute_mac(encoded.first(authenticated_size), key);
  if (!expected_mac.has_value()) {
    return common::make_unexpected(expected_mac.error());
  }
  if (CRYPTO_memcmp(expected_mac->data(), encoded.data() + authenticated_size,
                    kResumeTokenMacSize) != 0) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnauthenticated, "resume token authentication failed"});
  }

  common::ByteReader reader{encoded.first(authenticated_size)};
  auto magic = reader.read_exact(kMagic.size());
  if (!magic.has_value() || !std::ranges::equal(*magic, kMagic)) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "resume token magic is invalid"});
  }
  auto major = reader.read_u16_le();
  auto minor = reader.read_u16_le();
  auto header_size = reader.read_u32_le();
  auto total_size = reader.read_u32_le();
  auto source_count = reader.read_u32_le();
  if (!major || !minor || !header_size || !total_size || !source_count) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "resume token header is truncated"});
  }
  if (*major != kResumeTokenFormatMajor || *minor > kResumeTokenFormatMinor) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotSupported, "resume token version is unsupported"});
  }
  if (*header_size != kResumeTokenHeaderSize || *total_size != encoded.size() ||
      *source_count == 0U || *source_count > maximum_sources) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "resume token layout is invalid"});
  }
  const auto positions_size =
      common::checked_multiply<std::size_t>(*source_count, kResumeTokenPositionSize);
  const auto expected_authenticated =
      positions_size.has_value()
          ? common::checked_add<std::size_t>(kResumeTokenHeaderSize, *positions_size)
          : std::nullopt;
  if (!expected_authenticated.has_value() || *expected_authenticated != authenticated_size) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "resume token size is inconsistent"});
  }

  auto database_bytes = reader.read_exact(common::Uuid::kSize);
  auto subscription_bytes = reader.read_exact(common::Uuid::kSize);
  auto schema_bytes = reader.read_exact(common::Uuid::kSize);
  auto schema_version_value = reader.read_u64_le();
  auto safe_sequence = reader.read_u64_le();
  auto plan = reader.read_exact(PlanFingerprint{}.size());
  auto reserved = reader.read_exact(8U);
  if (!database_bytes || !subscription_bytes || !schema_bytes || !schema_version_value ||
      !safe_sequence || !plan || !reserved) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "resume token header is incomplete"});
  }
  if (std::ranges::any_of(*reserved, [](const std::byte value) { return value != std::byte{0}; })) {
    return common::make_unexpected(common::Status{common::StatusCode::kNotSupported,
                                                  "resume token required flags are unsupported"});
  }

  common::Uuid::Bytes database_array{};
  common::Uuid::Bytes subscription_array{};
  common::Uuid::Bytes schema_array{};
  std::copy(database_bytes->begin(), database_bytes->end(), database_array.begin());
  std::copy(subscription_bytes->begin(), subscription_bytes->end(), subscription_array.begin());
  std::copy(schema_bytes->begin(), schema_bytes->end(), schema_array.begin());
  auto schema_id = schema::SchemaId::from_bytes(schema_array);
  auto schema_version = schema::SchemaVersion::from_value(*schema_version_value);
  if (!schema_id || !schema_version) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "resume token schema identity is invalid"});
  }

  ResumeToken token{common::Uuid{database_array},
                    common::Uuid{subscription_array},
                    *schema_id,
                    *schema_version,
                    *safe_sequence,
                    {},
                    {}};
  std::copy(plan->begin(), plan->end(), token.plan_fingerprint.begin());
  token.source_positions.reserve(*source_count);
  for (std::uint32_t index = 0U; index < *source_count; ++index) {
    auto tablet_bytes = reader.read_exact(common::Uuid::kSize);
    auto wal_bytes = reader.read_exact(wal::kWalIdSize);
    auto record_sequence = reader.read_u64_le();
    if (!tablet_bytes || !wal_bytes || !record_sequence) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kCorruption, "resume token source is truncated"});
    }
    common::Uuid::Bytes tablet_array{};
    wal::WalId wal_id{};
    std::copy(tablet_bytes->begin(), tablet_bytes->end(), tablet_array.begin());
    std::copy(wal_bytes->begin(), wal_bytes->end(), wal_id.bytes.begin());
    auto tablet_id = schema::TabletId::from_bytes(tablet_array);
    if (!tablet_id || !wal_id.is_valid()) {
      return common::make_unexpected(common::Status{common::StatusCode::kCorruption,
                                                    "resume token source identity is invalid"});
    }
    token.source_positions.push_back(SourcePosition{*tablet_id, wal_id, *record_sequence});
  }
  if (!reader.empty() || token.database_id.is_nil() || token.subscription_id.is_nil()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kCorruption, "resume token identity or trailing bytes invalid"});
  }
  return token;
}

} // namespace chronos::live
