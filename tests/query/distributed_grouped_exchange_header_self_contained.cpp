#include "chronos/query/distributed_grouped_exchange.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::GroupedFloat64ExchangeMessage>);
static_assert(std::is_aggregate_v<chronos::query::GroupedExchangeTerminalMessage>);
static_assert(
    !std::is_default_constructible_v<chronos::query::EncodedGroupedFloat64ExchangeMessage>);
static_assert(
    !std::is_default_constructible_v<chronos::query::EncodedGroupedExchangeTerminalMessage>);
static_assert(!std::is_move_constructible_v<chronos::query::GroupedFloat64ExchangeFrameReader>);
static_assert(std::is_move_constructible_v<chronos::query::GroupedFloat64ExchangeFrameWriteCursor>);
