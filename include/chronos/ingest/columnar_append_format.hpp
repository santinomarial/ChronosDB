#ifndef CHRONOS_INGEST_COLUMNAR_APPEND_FORMAT_HPP_
#define CHRONOS_INGEST_COLUMNAR_APPEND_FORMAT_HPP_

#include "chronos/columnar/columnar_batch_format.hpp"
#include "chronos/wal/types.hpp"

#include <array>
#include <cstddef>
#include <cstdint>

namespace chronos::ingest::columnar_append_v1 {

inline constexpr std::uint32_t kApplicationFormat = 1U;
inline constexpr std::uint32_t kApplicationKind = 2U;
inline constexpr std::uint64_t kApplicationFlags = 0U;
inline constexpr std::uint32_t kMutationKindAppendRows = 1U;
inline constexpr std::uint32_t kDigestAlgorithmSha256 = 1U;
inline constexpr std::uint32_t kOutcomeCodeApplied = 1U;

inline constexpr std::size_t kCommandHeaderLength = 160U;
inline constexpr std::size_t kCommandHeaderLengthOffset = 0U;
inline constexpr std::size_t kCommandFlagsOffset = 4U;
inline constexpr std::size_t kMutationKindOffset = 8U;
inline constexpr std::size_t kDigestAlgorithmOffset = 12U;
inline constexpr std::size_t kClientIdOffset = 16U;
inline constexpr std::size_t kClientBatchIdOffset = 32U;
inline constexpr std::size_t kTableIdOffset = 48U;
inline constexpr std::size_t kTabletIdOffset = 64U;
inline constexpr std::size_t kSchemaIdOffset = 80U;
inline constexpr std::size_t kSchemaVersionOffset = 96U;
inline constexpr std::size_t kRowCountOffset = 104U;
inline constexpr std::size_t kBatchLengthOffset = 108U;
inline constexpr std::size_t kRequestDigestOffset = 112U;
inline constexpr std::size_t kOutcomeCodeOffset = 144U;
inline constexpr std::size_t kOutcomeFlagsOffset = 148U;
inline constexpr std::size_t kOutcomeRowCountOffset = 152U;
inline constexpr std::size_t kReservedOffset = 156U;
inline constexpr std::size_t kBatchOffset = kCommandHeaderLength;

inline constexpr std::size_t kApplicationPayloadHeaderLength =
    wal::kApplicationEnvelopeSize + kCommandHeaderLength;
inline constexpr std::size_t kMaximumApplicationPayloadLength =
    kApplicationPayloadHeaderLength + columnar::format::kMaximumEmbeddedBatchLength;

inline constexpr std::array<std::byte, 28> kRequestDigestDomain{
    std::byte{'C'}, std::byte{'h'}, std::byte{'r'}, std::byte{'o'}, std::byte{'n'}, std::byte{'o'},
    std::byte{'s'}, std::byte{'D'}, std::byte{'B'}, std::byte{'.'}, std::byte{'C'}, std::byte{'o'},
    std::byte{'l'}, std::byte{'u'}, std::byte{'m'}, std::byte{'n'}, std::byte{'a'}, std::byte{'r'},
    std::byte{'A'}, std::byte{'p'}, std::byte{'p'}, std::byte{'e'}, std::byte{'n'}, std::byte{'d'},
    std::byte{'.'}, std::byte{'v'}, std::byte{'1'}, std::byte{0},
};

static_assert(kApplicationPayloadHeaderLength == 176U);
static_assert(kMaximumApplicationPayloadLength == 16'777'168U);
static_assert(kMaximumApplicationPayloadLength <= wal::kMaximumPayloadLength);

} // namespace chronos::ingest::columnar_append_v1

#endif // CHRONOS_INGEST_COLUMNAR_APPEND_FORMAT_HPP_
