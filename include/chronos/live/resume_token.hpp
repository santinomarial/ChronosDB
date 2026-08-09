#ifndef CHRONOS_LIVE_RESUME_TOKEN_HPP_
#define CHRONOS_LIVE_RESUME_TOKEN_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/wal/types.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::live {

inline constexpr std::uint16_t kResumeTokenFormatMajor = 1U;
inline constexpr std::uint16_t kResumeTokenFormatMinor = 0U;
inline constexpr std::size_t kResumeTokenHeaderSize = 128U;
inline constexpr std::size_t kResumeTokenPositionSize = 40U;
inline constexpr std::size_t kResumeTokenMacSize = 32U;
inline constexpr std::size_t kMaximumResumeTokenSources = 4096U;

using PlanFingerprint = std::array<std::byte, 32U>;
using ResumeTokenMacKey = std::array<std::byte, 32U>;

struct SourcePosition {
  schema::TabletId tablet_id;
  wal::WalId wal_id;
  std::uint64_t record_sequence{};

  friend bool operator==(const SourcePosition&, const SourcePosition&) = default;
};

struct ResumeToken {
  common::Uuid database_id;
  common::Uuid subscription_id;
  schema::SchemaId schema_id;
  schema::SchemaVersion schema_version;
  std::uint64_t safe_delivery_sequence{};
  PlanFingerprint plan_fingerprint{};
  std::vector<SourcePosition> source_positions;

  friend bool operator==(const ResumeToken&, const ResumeToken&) = default;
};

// Tokens are opaque authenticated bytes. HMAC-SHA256 rejects modification, while semantic
// validation rejects a token bound to another database, plan, schema, or source lineage.
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_resume_token_v1(const ResumeToken& token, const ResumeTokenMacKey& key);

[[nodiscard]] common::Result<ResumeToken>
decode_resume_token_v1(common::ByteView encoded, const ResumeTokenMacKey& key,
                       std::size_t maximum_sources = kMaximumResumeTokenSources);

} // namespace chronos::live

#endif // CHRONOS_LIVE_RESUME_TOKEN_HPP_
