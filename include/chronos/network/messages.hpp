#ifndef CHRONOS_NETWORK_MESSAGES_HPP_
#define CHRONOS_NETWORK_MESSAGES_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/network/protocol.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace chronos::network {

inline constexpr std::uint16_t kMessagePayloadFormat = 1U;
// Zero remains the Protocol 1.0 feature set. Minor-1 features are declared by their owning
// extension headers and validated against the complete supported mask in messages.cpp.
inline constexpr std::uint64_t kProtocolV1FeatureBits = 0U;
inline constexpr std::uint64_t kProtocolV2QuorumSyncFeature = std::uint64_t{1U} << 1U;
inline constexpr std::uint64_t kProtocolV2LeaderRedirectFeature = std::uint64_t{1U} << 2U;
inline constexpr std::uint64_t kProtocolV2SupportedFeatureBits = kProtocolV1SubscriptionFeature |
                                                                 kProtocolV2QuorumSyncFeature |
                                                                 kProtocolV2LeaderRedirectFeature;
inline constexpr std::size_t kHelloPayloadSize = 24U;
inline constexpr std::size_t kIngestEnvelopeSize = 8U;
inline constexpr std::size_t kIngestAcknowledgementSize = 32U;
inline constexpr std::size_t kQuorumSyncIngestAcknowledgementSize = 64U;
inline constexpr std::size_t kLeaderRedirectSize = 48U;
inline constexpr std::size_t kQueryEnvelopeSize = 8U;
inline constexpr std::size_t kErrorEnvelopeSize = 8U;
inline constexpr std::size_t kQueryResultEnvelopeSize = 16U;
inline constexpr std::size_t kQueryResultColumnEnvelopeSize = 16U;
inline constexpr std::uint32_t kQueryResultNullCellLength = 0xffff'ffffU;

enum class DurabilityMode : std::uint8_t { kAsync = 1, kLocalSync = 2, kQuorumSync = 3 };
enum class IngestOutcome : std::uint8_t { kApplied = 1, kMatchingRetry = 2 };
enum class ProtocolErrorCode : std::uint8_t {
  kMalformedFrame = 1,
  kUnsupportedVersion = 2,
  kInvalidState = 3,
  kDuplicateRequest = 4,
  kUnknownRequest = 5,
  kOverloaded = 6,
  kCancelled = 7,
  kInvalidRequest = 8,
  kExecutionFailure = 9,
  kUnauthorized = 10,
  kInternal = 11,
};

struct ClientHello {
  std::uint16_t minimum_major{kProtocolMajor};
  std::uint16_t maximum_major{kProtocolMajor};
  std::uint16_t maximum_minor{kProtocolMinor};
  std::uint64_t feature_bits{kProtocolV1FeatureBits};
  std::uint32_t maximum_payload_size{kDefaultMaximumPayloadSize};
};

struct ServerHello {
  std::uint16_t selected_major{kProtocolMajor};
  std::uint16_t selected_minor{kProtocolMinor};
  std::uint64_t feature_bits{kProtocolV1FeatureBits};
  std::uint32_t maximum_payload_size{kDefaultMaximumPayloadSize};
};

struct IngestProtocolContext {
  std::uint16_t protocol_major{kProtocolMajor};
  std::uint16_t protocol_minor{kProtocolMinor};
  std::uint64_t feature_bits{};
};

struct IngestRequestView {
  DurabilityMode durability{DurabilityMode::kAsync};
  common::ByteView encoded_columnar_append;
};

struct IngestAcknowledgement {
  DurabilityMode requested_durability{DurabilityMode::kAsync};
  DurabilityMode effective_durability{DurabilityMode::kAsync};
  IngestOutcome outcome{IngestOutcome::kApplied};
  std::uint64_t record_sequence{};
  std::uint64_t segment_number{};
  std::uint64_t byte_offset{};

  friend bool operator==(const IngestAcknowledgement&, const IngestAcknowledgement&) = default;
};

struct QuorumSyncIngestAcknowledgement {
  DurabilityMode requested_durability{DurabilityMode::kQuorumSync};
  DurabilityMode effective_durability{DurabilityMode::kQuorumSync};
  IngestOutcome outcome{IngestOutcome::kApplied};
  common::Uuid group_id;
  std::uint64_t leader_node_id{};
  std::uint64_t leader_term{};
  std::uint64_t log_index{};
  std::uint64_t entry_term{};
  std::uint64_t local_durable_physical_sequence{};

  friend bool operator==(const QuorumSyncIngestAcknowledgement&,
                         const QuorumSyncIngestAcknowledgement&) = default;
};

struct LeaderRedirect {
  common::Uuid group_id;
  std::uint64_t leader_node_id{};
  std::uint64_t leader_term{};
  std::uint64_t placement_epoch{};

  friend bool operator==(const LeaderRedirect&, const LeaderRedirect&) = default;
};

struct ErrorMessageView {
  ProtocolErrorCode code{ProtocolErrorCode::kInternal};
  common::ByteView message;
};

struct QueryResultLimits {
  ProtocolLimits protocol;
  std::uint32_t maximum_rows{1'048'576U};
  std::uint32_t maximum_columns{4096U};
  std::uint32_t maximum_column_name_bytes{1024U};
};

struct QueryResultColumn {
  std::string_view name;
  schema::LogicalType type;
  bool nullable{};
};

struct QueryResultCell {
  bool is_null{};
  common::ByteView value;
};

class QueryResultBatchView {
public:
  QueryResultBatchView() = delete;

  [[nodiscard]] std::uint32_t row_count() const noexcept;
  [[nodiscard]] std::span<const QueryResultColumn> columns() const noexcept;
  [[nodiscard]] const QueryResultCell* cell(std::uint32_t row, std::size_t column) const noexcept;

private:
  QueryResultBatchView(std::uint32_t rows, std::vector<QueryResultColumn> columns,
                       std::vector<QueryResultCell> cells) noexcept;

  std::uint32_t rows_{};
  std::vector<QueryResultColumn> columns_;
  std::vector<QueryResultCell> cells_;

  friend common::Result<QueryResultBatchView> decode_query_result_batch(common::ByteView,
                                                                        const QueryResultLimits&);
};

[[nodiscard]] common::Result<std::vector<std::byte>> encode_client_hello(const ClientHello& hello);
[[nodiscard]] common::Result<ClientHello> decode_client_hello(common::ByteView payload);
[[nodiscard]] common::Result<std::vector<std::byte>> encode_server_hello(const ServerHello& hello);
[[nodiscard]] common::Result<ServerHello> decode_server_hello(common::ByteView payload);
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_ingest_request(DurabilityMode durability, common::ByteView encoded_columnar_append,
                      const ProtocolLimits& limits = {});
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_ingest_request(DurabilityMode durability, common::ByteView encoded_columnar_append,
                      const IngestProtocolContext& context, const ProtocolLimits& limits = {});
[[nodiscard]] common::Result<IngestRequestView>
decode_ingest_request(common::ByteView payload, const ProtocolLimits& limits = {});
[[nodiscard]] common::Result<IngestRequestView>
decode_ingest_request(common::ByteView payload, const IngestProtocolContext& context,
                      const ProtocolLimits& limits = {});
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_ingest_acknowledgement(const IngestAcknowledgement& acknowledgement);
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_ingest_acknowledgement(const IngestAcknowledgement& acknowledgement,
                              const IngestProtocolContext& context);
[[nodiscard]] common::Result<IngestAcknowledgement>
decode_ingest_acknowledgement(common::ByteView payload);
[[nodiscard]] common::Result<IngestAcknowledgement>
decode_ingest_acknowledgement(common::ByteView payload, const IngestProtocolContext& context);
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_quorum_sync_ingest_acknowledgement(const QuorumSyncIngestAcknowledgement& acknowledgement);
[[nodiscard]] common::Result<QuorumSyncIngestAcknowledgement>
decode_quorum_sync_ingest_acknowledgement(common::ByteView payload);
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_leader_redirect(const LeaderRedirect& redirect);
[[nodiscard]] common::Result<LeaderRedirect> decode_leader_redirect(common::ByteView payload);
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_query_request(std::string_view sql, const ProtocolLimits& limits = {});
[[nodiscard]] common::Result<common::ByteView>
decode_query_request(common::ByteView payload, const ProtocolLimits& limits = {});
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_error_message(ProtocolErrorCode code, std::string_view message,
                     const ProtocolLimits& limits = {});
[[nodiscard]] common::Result<ErrorMessageView>
decode_error_message(common::ByteView payload, const ProtocolLimits& limits = {});
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_query_result_batch(std::uint32_t rows, std::span<const QueryResultColumn> columns,
                          std::span<const QueryResultCell> cells,
                          const QueryResultLimits& limits = {});
[[nodiscard]] common::Result<QueryResultBatchView>
decode_query_result_batch(common::ByteView payload, const QueryResultLimits& limits = {});

} // namespace chronos::network

#endif // CHRONOS_NETWORK_MESSAGES_HPP_
