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

inline constexpr std::uint16_t kResumeTokenV1FormatMajor = 1U;
inline constexpr std::uint16_t kResumeTokenV1FormatMinor = 0U;
inline constexpr std::uint16_t kResumeTokenV2FormatMajor = 2U;
inline constexpr std::uint16_t kResumeTokenV2FormatMinor = 0U;
inline constexpr std::size_t kResumeTokenHeaderSize = 128U;
inline constexpr std::size_t kResumeTokenV1PositionSize = 40U;
inline constexpr std::size_t kResumeTokenV2PositionSize = 48U;
inline constexpr std::size_t kResumeTokenMacSize = 32U;
inline constexpr std::size_t kMaximumResumeTokenSources = 4096U;

using PlanFingerprint = std::array<std::byte, 32U>;
using ResumeTokenMacKey = std::array<std::byte, 32U>;

enum class SubscriptionSourceKind : std::uint8_t {
  kWal = 1,
  kRaft = 2,
};

struct SourcePosition {
  SourcePosition(schema::TabletId tablet, wal::WalId id, std::uint64_t sequence) noexcept
      : tablet_id(tablet), wal_id(id), record_sequence(sequence) {}

  schema::TabletId tablet_id;
  wal::WalId wal_id;
  std::uint64_t record_sequence{};
  SubscriptionSourceKind source_kind{SubscriptionSourceKind::kWal};
  common::Uuid raft_group_id;

  [[nodiscard]] static SourcePosition wal(schema::TabletId tablet, wal::WalId id,
                                          std::uint64_t sequence) noexcept {
    return SourcePosition{tablet, id, sequence};
  }

  [[nodiscard]] static SourcePosition raft(schema::TabletId tablet, common::Uuid group_id,
                                           std::uint64_t log_index) noexcept {
    return SourcePosition{tablet, {}, log_index, SubscriptionSourceKind::kRaft, group_id};
  }

  [[nodiscard]] bool is_valid() const noexcept {
    if (tablet_id.uuid().is_nil())
      return false;
    if (source_kind == SubscriptionSourceKind::kWal)
      return wal_id.is_valid() && raft_group_id.is_nil();
    return source_kind == SubscriptionSourceKind::kRaft && !raft_group_id.is_nil() &&
           !wal_id.is_valid();
  }

  [[nodiscard]] bool same_source(const SourcePosition& other) const noexcept {
    if (tablet_id != other.tablet_id || source_kind != other.source_kind)
      return false;
    if (source_kind == SubscriptionSourceKind::kWal)
      return wal_id == other.wal_id;
    return source_kind == SubscriptionSourceKind::kRaft && raft_group_id == other.raft_group_id;
  }

  friend bool operator==(const SourcePosition&, const SourcePosition&) = default;

private:
  SourcePosition(schema::TabletId tablet, wal::WalId wal, std::uint64_t sequence,
                 SubscriptionSourceKind kind, common::Uuid group_id) noexcept
      : tablet_id(tablet), wal_id(wal), record_sequence(sequence), source_kind(kind),
        raft_group_id(group_id) {}
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

// V2 retains the authenticated header but tags every position as WAL or Raft and carries the
// corresponding source-specific 16-byte identity. V1 remains decodable and WAL-only.
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_resume_token_v2(const ResumeToken& token, const ResumeTokenMacKey& key);

[[nodiscard]] common::Result<ResumeToken>
decode_resume_token_v2(common::ByteView encoded, const ResumeTokenMacKey& key,
                       std::size_t maximum_sources = kMaximumResumeTokenSources);

// Compatibility decoder for authenticated v1 and v2 tokens. New token issuance uses v2.
[[nodiscard]] common::Result<ResumeToken>
decode_resume_token(common::ByteView encoded, const ResumeTokenMacKey& key,
                    std::size_t maximum_sources = kMaximumResumeTokenSources);

} // namespace chronos::live

#endif // CHRONOS_LIVE_RESUME_TOKEN_HPP_
