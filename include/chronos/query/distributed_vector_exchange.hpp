#ifndef CHRONOS_QUERY_DISTRIBUTED_VECTOR_EXCHANGE_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_VECTOR_EXCHANGE_HPP_

#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <cstdint>
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

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_VECTOR_EXCHANGE_HPP_
