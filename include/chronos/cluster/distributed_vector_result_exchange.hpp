#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_RESULT_EXCHANGE_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_RESULT_EXCHANGE_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/distributed.hpp"
#include "chronos/query/distributed_vector_result_schema.hpp"
#include "chronos/schema/identity.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::cluster {

namespace distributed_vector_result_exchange_v2_format {
inline constexpr std::uint16_t kMajor = 2U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderLength = 80U;
inline constexpr std::size_t kTrailerLength = 4U;
inline constexpr std::size_t kMaximumFrameLength =
    kHeaderLength + network::kDefaultMaximumPayloadSize + kTrailerLength;
} // namespace distributed_vector_result_exchange_v2_format

struct DistributedVectorResultExchangeMessage {
  common::Uuid query_id;
  schema::TabletId tablet_id;
  std::uint64_t sequence{};
  bool terminal{};
  // Empty only for a terminal-only empty stream. Otherwise exactly one Native Protocol v1
  // QUERY_RESULT payload whose descriptors equal the separately authorized result schema.
  std::vector<std::byte> encoded_result_batch{}; // NOLINT(readability-redundant-member-init)
};

struct DistributedVectorResultExchangeDecodeLimits {
  std::size_t maximum_frame_length{
      distributed_vector_result_exchange_v2_format::kMaximumFrameLength};
  network::QueryResultLimits result_batch{};
};

class EncodedDistributedVectorResultExchangeMessage {
public:
  EncodedDistributedVectorResultExchangeMessage() = delete;
  EncodedDistributedVectorResultExchangeMessage(
      const EncodedDistributedVectorResultExchangeMessage&) = delete;
  EncodedDistributedVectorResultExchangeMessage&
  operator=(const EncodedDistributedVectorResultExchangeMessage&) = delete;
  EncodedDistributedVectorResultExchangeMessage(
      EncodedDistributedVectorResultExchangeMessage&&) noexcept = default;
  EncodedDistributedVectorResultExchangeMessage&
  operator=(EncodedDistributedVectorResultExchangeMessage&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedDistributedVectorResultExchangeMessage(std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;

  friend common::Result<EncodedDistributedVectorResultExchangeMessage>
  encode_distributed_vector_result_exchange_message_v2(
      const DistributedVectorResultExchangeMessage&, const query::DistributedVectorResultSchema&);
};

[[nodiscard]] common::Result<EncodedDistributedVectorResultExchangeMessage>
encode_distributed_vector_result_exchange_message_v2(
    const DistributedVectorResultExchangeMessage& message,
    const query::DistributedVectorResultSchema& expected_schema);

[[nodiscard]] common::Result<DistributedVectorResultExchangeMessage>
decode_distributed_vector_result_exchange_message_v2_exact(
    common::ByteView bytes, const query::DistributedVectorResultSchema& expected_schema,
    DistributedVectorResultExchangeDecodeLimits limits = {});

struct DistributedVectorResultExchangeReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedVectorResultExchangeMessage> message;
};

// One owner supplies the fragment-bound schema at construction and serializes consume calls. The
// reader owns only the current frame; coalesced successor bytes remain caller-owned.
class DistributedVectorResultExchangeReader {
public:
  explicit DistributedVectorResultExchangeReader(
      query::DistributedVectorResultSchema&& expected_schema,
      DistributedVectorResultExchangeDecodeLimits limits = {}) noexcept;
  DistributedVectorResultExchangeReader(const DistributedVectorResultExchangeReader&) = delete;
  DistributedVectorResultExchangeReader&
  operator=(const DistributedVectorResultExchangeReader&) = delete;
  DistributedVectorResultExchangeReader(DistributedVectorResultExchangeReader&&) = delete;
  DistributedVectorResultExchangeReader&
  operator=(DistributedVectorResultExchangeReader&&) = delete;

  [[nodiscard]] common::Result<DistributedVectorResultExchangeReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  query::DistributedVectorResultSchema expected_schema_;
  DistributedVectorResultExchangeDecodeLimits limits_;
  std::array<std::byte, distributed_vector_result_exchange_v2_format::kHeaderLength> header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<common::Status> failure_;
};

class DistributedVectorResultExchangeWriteCursor {
public:
  DistributedVectorResultExchangeWriteCursor() = delete;
  DistributedVectorResultExchangeWriteCursor(const DistributedVectorResultExchangeWriteCursor&) =
      delete;
  DistributedVectorResultExchangeWriteCursor&
  operator=(const DistributedVectorResultExchangeWriteCursor&) = delete;
  DistributedVectorResultExchangeWriteCursor(
      DistributedVectorResultExchangeWriteCursor&& other) noexcept;
  DistributedVectorResultExchangeWriteCursor&
  operator=(DistributedVectorResultExchangeWriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorResultExchangeWriteCursor>
  create(const DistributedVectorResultExchangeMessage& message,
         const query::DistributedVectorResultSchema& expected_schema);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit DistributedVectorResultExchangeWriteCursor(
      EncodedDistributedVectorResultExchangeMessage encoded) noexcept;
  EncodedDistributedVectorResultExchangeMessage encoded_;
  std::size_t written_bytes_{};
};

inline constexpr std::size_t kDefaultDistributedVectorResultCoordinatorBytesV2 =
    std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorResultCoordinatorBytesV2 =
    std::size_t{1024U} * 1024U * 1024U;

struct DistributedVectorResultCoordinatorLimitsV2 {
  query::DistributedCoordinatorLimits messages{};
  std::size_t maximum_total_encoded_bytes{kDefaultDistributedVectorResultCoordinatorBytesV2};
};

struct DistributedVectorQueryResultV2 {
  query::DistributedVectorResultSchema result_schema;
  std::vector<DistributedVectorResultExchangeMessage> messages;
};

// Single-owner coordinator for schema-bound v2 result streams. Successful finish transfers the
// admitted schema and retained messages together in plan-tablet and sequence order.
class DistributedVectorResultCoordinatorV2 {
public:
  DistributedVectorResultCoordinatorV2() = delete;
  ~DistributedVectorResultCoordinatorV2();
  DistributedVectorResultCoordinatorV2(const DistributedVectorResultCoordinatorV2&) = delete;
  DistributedVectorResultCoordinatorV2&
  operator=(const DistributedVectorResultCoordinatorV2&) = delete;
  DistributedVectorResultCoordinatorV2(DistributedVectorResultCoordinatorV2&&) noexcept;
  DistributedVectorResultCoordinatorV2& operator=(DistributedVectorResultCoordinatorV2&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorResultCoordinatorV2>
  create(common::Uuid query_id, std::vector<schema::TabletId> tablets,
         query::DistributedVectorResultSchema result_schema,
         DistributedVectorResultCoordinatorLimitsV2 limits = {});
  [[nodiscard]] common::Status accept(const DistributedVectorResultExchangeMessage& message);
  [[nodiscard]] common::Status worker_failed(const schema::TabletId& tablet_id,
                                             common::Status failure);
  [[nodiscard]] common::Result<DistributedVectorQueryResultV2> finish() &&;

private:
  class Impl;
  explicit DistributedVectorResultCoordinatorV2(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_RESULT_EXCHANGE_HPP_
