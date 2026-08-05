#ifndef CHRONOS_TESTS_INGEST_INGEST_TEST_SUPPORT_HPP_
#define CHRONOS_TESTS_INGEST_INGEST_TEST_SUPPORT_HPP_

#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "columnar/columnar_test_support.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::ingest::test {

template <typename Identifier> [[nodiscard]] Identifier request_id(const std::uint8_t first) {
  common::Uuid::Bytes bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    bytes[index] = static_cast<std::byte>(first + index);
  }
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] inline columnar::EncodedColumnarBatch encoded_batch() {
  const columnar::OwnedColumnarBatch batch =
      columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                           columnar::test::batch_columns())
          .value();
  return columnar::encode_columnar_batch_v1(batch).value();
}

[[nodiscard]] inline wal::EncodedApplicationPayload encoded_command() {
  const columnar::EncodedColumnarBatch batch = encoded_batch();
  return encode_columnar_append_v1(
             ColumnarAppendEncodeInput{.client_id = request_id<ClientId>(0x10U),
                                       .client_batch_id = request_id<ClientBatchId>(0x20U),
                                       .tablet_id = columnar::test::id<schema::TabletId>(52U)},
             batch)
      .value();
}

[[nodiscard]] inline std::vector<std::byte> command_bytes() {
  const wal::EncodedApplicationPayload command = encoded_command();
  return {command.bytes().begin(), command.bytes().end()};
}

inline void store_u32_le(const common::MutableByteView bytes, const std::size_t offset,
                         const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
}

} // namespace chronos::ingest::test

#endif // CHRONOS_TESTS_INGEST_INGEST_TEST_SUPPORT_HPP_
