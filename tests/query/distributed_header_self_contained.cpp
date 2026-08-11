#include "chronos/query/distributed.hpp"

namespace {
[[maybe_unused]] constexpr auto kHeaderIsSelfContained =
    chronos::query::DistributedReadConsistency::kLeaderLinearizable;
[[maybe_unused]] auto* const kEncodeExchangeMessage = &chronos::query::encode_exchange_message;
[[maybe_unused]] auto* const kDecodeExchangeMessage =
    &chronos::query::decode_exchange_message_exact;
[[maybe_unused]] const auto kConsumeExchangeFrame = &chronos::query::ExchangeFrameReader::consume;
[[maybe_unused]] auto* const kCreateExchangeWriteCursor =
    &chronos::query::ExchangeFrameWriteCursor::create;
} // namespace
