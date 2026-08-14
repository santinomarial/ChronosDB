#ifndef CHRONOS_QUERY_DISTRIBUTED_VECTOR_EXCHANGE_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_VECTOR_EXCHANGE_HPP_

#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/query/distributed.hpp"
#include "chronos/schema/identity.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::query {

namespace distributed_vector_exchange_format {
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderLength = 80U;
inline constexpr std::size_t kTrailerLength = 4U;
inline constexpr std::size_t kMaximumFrameLength =
    kHeaderLength + columnar::format::kMaximumEmbeddedBatchLength + kTrailerLength;
} // namespace distributed_vector_exchange_format

struct DistributedVectorExchangeMessage {
  common::Uuid query_id;
  schema::TabletId tablet_id;
  std::uint64_t sequence{};
  bool terminal{};
  // Empty only for a terminal-only empty stream. Otherwise exactly one canonical Columnar Batch v1.
  std::vector<std::byte> encoded_batch;
};

struct DistributedVectorExchangeDecodeLimits {
  std::size_t maximum_frame_length{distributed_vector_exchange_format::kMaximumFrameLength};
  columnar::ColumnarBatchDecodeLimits batch;
};

class EncodedDistributedVectorExchangeMessage {
public:
  EncodedDistributedVectorExchangeMessage() = delete;
  EncodedDistributedVectorExchangeMessage(const EncodedDistributedVectorExchangeMessage&) = delete;
  EncodedDistributedVectorExchangeMessage&
  operator=(const EncodedDistributedVectorExchangeMessage&) = delete;
  EncodedDistributedVectorExchangeMessage(EncodedDistributedVectorExchangeMessage&&) noexcept =
      default;
  EncodedDistributedVectorExchangeMessage&
  operator=(EncodedDistributedVectorExchangeMessage&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedDistributedVectorExchangeMessage(std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;

  friend common::Result<EncodedDistributedVectorExchangeMessage>
  encode_distributed_vector_exchange_message(const DistributedVectorExchangeMessage&);
};

[[nodiscard]] common::Result<EncodedDistributedVectorExchangeMessage>
encode_distributed_vector_exchange_message(const DistributedVectorExchangeMessage& message);

[[nodiscard]] common::Result<DistributedVectorExchangeMessage>
decode_distributed_vector_exchange_message_exact(common::ByteView bytes,
                                                 DistributedVectorExchangeDecodeLimits limits = {});

struct DistributedVectorExchangeReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedVectorExchangeMessage> message;
};

class DistributedVectorExchangeReader {
public:
  explicit DistributedVectorExchangeReader(DistributedVectorExchangeDecodeLimits limits = {});
  DistributedVectorExchangeReader(const DistributedVectorExchangeReader&) = delete;
  DistributedVectorExchangeReader& operator=(const DistributedVectorExchangeReader&) = delete;
  DistributedVectorExchangeReader(DistributedVectorExchangeReader&&) = delete;
  DistributedVectorExchangeReader& operator=(DistributedVectorExchangeReader&&) = delete;

  [[nodiscard]] common::Result<DistributedVectorExchangeReadStep> consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  DistributedVectorExchangeDecodeLimits limits_;
  std::array<std::byte, distributed_vector_exchange_format::kHeaderLength> header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<common::Status> failure_;
};

class DistributedVectorExchangeWriteCursor {
public:
  DistributedVectorExchangeWriteCursor() = delete;
  DistributedVectorExchangeWriteCursor(const DistributedVectorExchangeWriteCursor&) = delete;
  DistributedVectorExchangeWriteCursor&
  operator=(const DistributedVectorExchangeWriteCursor&) = delete;
  DistributedVectorExchangeWriteCursor(DistributedVectorExchangeWriteCursor&& other) noexcept;
  DistributedVectorExchangeWriteCursor&
  operator=(DistributedVectorExchangeWriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorExchangeWriteCursor>
  create(const DistributedVectorExchangeMessage& message);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit DistributedVectorExchangeWriteCursor(
      EncodedDistributedVectorExchangeMessage encoded) noexcept;
  EncodedDistributedVectorExchangeMessage encoded_;
  std::size_t written_bytes_{};
};

inline constexpr std::size_t kDefaultDistributedVectorCoordinatorBytes =
    std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorCoordinatorBytes =
    std::size_t{1024U} * 1024U * 1024U;

struct DistributedVectorCoordinatorLimits {
  DistributedCoordinatorLimits messages;
  std::size_t maximum_total_batch_bytes{kDefaultDistributedVectorCoordinatorBytes};
};

// Single-owner coordinator for canonical vector-exchange streams. The owner serializes accept,
// worker_failed, and finish. Successful finish consumes the retained messages into plan-tablet and
// sequence order; no semantic output-schema contract is inferred from the nested batch schemas.
class DistributedVectorCoordinator {
public:
  DistributedVectorCoordinator() = delete;
  ~DistributedVectorCoordinator();
  DistributedVectorCoordinator(const DistributedVectorCoordinator&) = delete;
  DistributedVectorCoordinator& operator=(const DistributedVectorCoordinator&) = delete;
  DistributedVectorCoordinator(DistributedVectorCoordinator&&) noexcept;
  DistributedVectorCoordinator& operator=(DistributedVectorCoordinator&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorCoordinator>
  create(common::Uuid query_id, std::vector<schema::TabletId> tablets,
         DistributedVectorCoordinatorLimits limits = {});
  [[nodiscard]] common::Status accept(const DistributedVectorExchangeMessage& message);
  [[nodiscard]] common::Status worker_failed(const schema::TabletId& tablet_id,
                                             common::Status failure);
  [[nodiscard]] common::Result<std::vector<DistributedVectorExchangeMessage>> finish() &&;

private:
  class Impl;
  explicit DistributedVectorCoordinator(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_VECTOR_EXCHANGE_HPP_
