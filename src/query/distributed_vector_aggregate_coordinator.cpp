#include "chronos/query/distributed_vector_aggregate_coordinator.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/query/resource_context.hpp"

#include <algorithm>
#include <map>
#include <new>
#include <optional>
#include <ranges>
#include <set>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status
validate_definitions_and_schema(const std::span<const VectorAggregateDefinition> definitions,
                                const DistributedVectorResultSchema& result_schema) {
  common::Status schema_status = validate_distributed_vector_result_schema_value(result_schema);
  if (!schema_status.is_ok())
    return schema_status;
  if (definitions.empty() || definitions.size() > kMaximumUngroupedAggregateWidth ||
      definitions.size() != result_schema.columns.size()) {
    return invalid("vector aggregate coordinator result width is invalid");
  }
  for (std::size_t ordinal = 0U; ordinal < definitions.size(); ++ordinal) {
    const auto shape = vector_aggregate_output_shape(definitions[ordinal]);
    if (!shape.has_value())
      return invalid("vector aggregate coordinator definition is invalid");
    const DistributedVectorResultColumn& column = result_schema.columns[ordinal];
    if (shape->type != column.type || shape->nullable != column.nullable)
      return invalid("vector aggregate coordinator result schema differs from its definitions");
    const bool input_ordinal_exceeds_wire_limit =
        definitions[ordinal]
            .input
            .transform([](const VectorAggregateInput& input) {
              return input.column_ordinal >
                     distributed_vector_aggregate_state_format::kMaximumInputColumnOrdinal;
            })
            .value_or(false);
    if (input_ordinal_exceeds_wire_limit) {
      return invalid("vector aggregate coordinator input ordinal exceeds the wire limit");
    }
  }
  return common::Status::ok();
}

[[nodiscard]] bool
valid_decode_limits(const DistributedVectorAggregateExchangeDecodeLimits& limits) noexcept {
  return limits.maximum_frame_length >=
             distributed_vector_aggregate_exchange_format::kMinimumFrameLength &&
         limits.maximum_frame_length <=
             distributed_vector_aggregate_exchange_format::kMaximumFrameLength &&
         limits.maximum_aggregates > 0U &&
         limits.maximum_aggregates <=
             distributed_vector_aggregate_exchange_format::kMaximumAggregates &&
         limits.state.maximum_frame_length >=
             distributed_vector_aggregate_state_format::kMinimumFrameLength &&
         limits.state.maximum_frame_length <=
             distributed_vector_aggregate_state_format::kMaximumFrameLength &&
         limits.state.maximum_variable_extremum_bytes > 0U &&
         limits.state.maximum_variable_extremum_bytes <=
             distributed_vector_aggregate_state_format::kMaximumExtremumBytes;
}

} // namespace

class DistributedVectorAggregateCoordinatorV2::Impl {
public:
  struct FragmentProgress {
    std::vector<EncodedDistributedVectorAggregateExchangeMessage> messages;
    bool terminal{};
  };

  Impl(common::Uuid id, std::vector<schema::TabletId> tablets,
       std::vector<VectorAggregateDefinition> aggregate_definitions,
       DistributedVectorResultSchema schema,
       const DistributedVectorAggregateCoordinatorLimitsV2 configured)
      : query_id(id), tablet_order(std::move(tablets)),
        definitions(std::move(aggregate_definitions)), result_schema(std::move(schema)),
        limits(configured) {
    for (const schema::TabletId& tablet_id : tablet_order)
      fragments.emplace(tablet_id, FragmentProgress{});
  }

  common::Uuid query_id;
  std::vector<schema::TabletId> tablet_order;
  std::vector<VectorAggregateDefinition> definitions;
  DistributedVectorResultSchema result_schema;
  DistributedVectorAggregateCoordinatorLimitsV2 limits;
  std::map<schema::TabletId, FragmentProgress> fragments;
  std::size_t retained_messages{};
  std::size_t retained_encoded_bytes{};
  std::optional<common::Status> failure;
  bool finished{};
};

DistributedVectorAggregateCoordinatorV2::DistributedVectorAggregateCoordinatorV2(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}

DistributedVectorAggregateCoordinatorV2::~DistributedVectorAggregateCoordinatorV2() = default;
DistributedVectorAggregateCoordinatorV2::DistributedVectorAggregateCoordinatorV2(
    DistributedVectorAggregateCoordinatorV2&&) noexcept = default;
DistributedVectorAggregateCoordinatorV2& DistributedVectorAggregateCoordinatorV2::operator=(
    DistributedVectorAggregateCoordinatorV2&&) noexcept = default;

common::Result<DistributedVectorAggregateCoordinatorV2>
DistributedVectorAggregateCoordinatorV2::create(
    const common::Uuid query_id, std::vector<schema::TabletId> tablets,
    std::vector<VectorAggregateDefinition> definitions, DistributedVectorResultSchema result_schema,
    const DistributedVectorAggregateCoordinatorLimitsV2 limits) {
  const common::Status shape_status = validate_definitions_and_schema(definitions, result_schema);
  if (!shape_status.is_ok())
    return common::make_unexpected(shape_status);
  if (query_id.is_nil() || tablets.empty())
    return common::make_unexpected(invalid("vector aggregate coordinator identity is invalid"));
  const auto required_messages = common::checked_multiply(tablets.size(), definitions.size());
  const auto minimum_encoded_bytes =
      required_messages.has_value()
          ? common::checked_multiply(
                *required_messages,
                distributed_vector_aggregate_exchange_format::kMinimumFrameLength)
          : std::nullopt;
  const auto fixed_state_bytes =
      common::checked_multiply(definitions.size(), sizeof(MergeableVectorAggregateState));
  if (limits.messages.maximum_messages_per_fragment < definitions.size() ||
      limits.messages.maximum_messages_per_fragment > limits.messages.maximum_total_messages ||
      limits.messages.maximum_total_messages > kMaximumDistributedCoordinatorMessages ||
      !required_messages.has_value() ||
      *required_messages > limits.messages.maximum_total_messages ||
      limits.maximum_total_encoded_bytes == 0U ||
      limits.maximum_total_encoded_bytes > kMaximumDistributedVectorAggregateCoordinatorBytesV2 ||
      !minimum_encoded_bytes.has_value() ||
      *minimum_encoded_bytes > limits.maximum_total_encoded_bytes ||
      limits.maximum_query_memory_bytes == 0U ||
      limits.maximum_query_memory_bytes >
          kMaximumDistributedVectorAggregateCoordinatorMemoryBytesV2 ||
      limits.maximum_retained_configuration_bytes == 0U || !fixed_state_bytes.has_value() ||
      *fixed_state_bytes > limits.maximum_retained_configuration_bytes ||
      !valid_decode_limits(limits.decode) ||
      definitions.size() > limits.decode.maximum_aggregates) {
    return common::make_unexpected(invalid("vector aggregate coordinator limits are invalid"));
  }
  try {
    std::set<schema::TabletId> unique_tablets;
    for (const schema::TabletId& tablet_id : tablets) {
      if (tablet_id.uuid().is_nil() || !unique_tablets.insert(tablet_id).second)
        return common::make_unexpected(invalid("vector aggregate coordinator tablets are invalid"));
    }
    return DistributedVectorAggregateCoordinatorV2{std::make_unique<Impl>(
        query_id, std::move(tablets), std::move(definitions), std::move(result_schema), limits)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("vector aggregate coordinator allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("vector aggregate coordinator exceeds limits"));
  }
}

common::Status DistributedVectorAggregateCoordinatorV2::accept(
    const DistributedVectorAggregateExchangeMessage& message) {
  Impl& impl = *implementation_;
  if (impl.finished)
    return invalid("vector aggregate coordinator is already finished");
  if (impl.failure.has_value())
    return *impl.failure;
  auto fragment = impl.fragments.find(message.tablet_id);
  if (message.query_id != impl.query_id || fragment == impl.fragments.end())
    return invalid("vector aggregate message does not belong to the coordinator");
  auto encoded = encode_distributed_vector_aggregate_exchange_message(message, impl.definitions);
  if (!encoded.has_value())
    return encoded.error();

  Impl::FragmentProgress& progress = fragment->second;
  if (message.sequence <= progress.messages.size()) {
    const common::ByteView retained = progress.messages[message.sequence - 1U].bytes();
    return std::ranges::equal(encoded->bytes(), retained)
               ? common::Status::ok()
               : common::Status{common::StatusCode::kAlreadyExists,
                                "vector aggregate sequence conflicts with retained state"};
  }
  if (progress.terminal)
    return invalid("vector aggregate fragment emitted after its terminal message");
  if (message.sequence != progress.messages.size() + 1U)
    return {common::StatusCode::kUnavailable, "vector aggregate fragment sequence has a gap"};
  if (progress.messages.size() == impl.limits.messages.maximum_messages_per_fragment ||
      impl.retained_messages == impl.limits.messages.maximum_total_messages ||
      encoded->bytes().size() >
          impl.limits.maximum_total_encoded_bytes - impl.retained_encoded_bytes) {
    return exhausted("vector aggregate coordinator retention is exhausted");
  }
  const std::size_t encoded_size = encoded->bytes().size();
  try {
    progress.messages.push_back(std::move(*encoded));
  } catch (const std::bad_alloc&) {
    return exhausted("vector aggregate coordinator retention allocation failed");
  } catch (const std::length_error&) {
    return exhausted("vector aggregate coordinator retention exceeds limits");
  }
  progress.terminal = message.terminal;
  ++impl.retained_messages;
  impl.retained_encoded_bytes += encoded_size;
  return common::Status::ok();
}

common::Status
DistributedVectorAggregateCoordinatorV2::worker_failed(const schema::TabletId& tablet_id,
                                                       common::Status failure) {
  Impl& impl = *implementation_;
  if (impl.finished)
    return invalid("vector aggregate coordinator is already finished");
  const auto fragment = impl.fragments.find(tablet_id);
  if (fragment == impl.fragments.end() || failure.is_ok())
    return invalid("vector aggregate worker failure is invalid or unplanned");
  if (fragment->second.terminal)
    return common::Status::ok();
  if (impl.failure.has_value())
    return *impl.failure;
  impl.failure = std::move(failure);
  return common::Status::ok();
}

common::Result<DistributedVectorAggregateQueryResultV2>
DistributedVectorAggregateCoordinatorV2::finish() && {
  Impl& impl = *implementation_;
  if (impl.finished)
    return common::make_unexpected(invalid("vector aggregate coordinator is already finished"));
  if (impl.failure.has_value())
    return common::make_unexpected(*impl.failure);
  for (const schema::TabletId& tablet_id : impl.tablet_order) {
    const Impl::FragmentProgress& progress = impl.fragments.at(tablet_id);
    if (!progress.terminal || progress.messages.size() != impl.definitions.size()) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kUnavailable, "vector aggregate query has incomplete fragments"});
    }
  }

  try {
    auto resources = QueryResourceContext::create(impl.limits.maximum_query_memory_bytes);
    if (!resources.has_value())
      return common::make_unexpected(resources.error());
    std::vector<MergeableVectorAggregateState> merged;
    merged.reserve(impl.definitions.size());
    const auto retained_capacity =
        common::checked_multiply(merged.capacity(), sizeof(MergeableVectorAggregateState));
    if (!retained_capacity.has_value() ||
        *retained_capacity > impl.limits.maximum_retained_configuration_bytes) {
      return common::make_unexpected(
          exhausted("vector aggregate merged state exceeds its configuration limit"));
    }
    for (const VectorAggregateDefinition& definition : impl.definitions) {
      auto state = MergeableVectorAggregateState::create(
          definition, impl.limits.decode.state.maximum_variable_extremum_bytes);
      if (!state.has_value())
        return common::make_unexpected(state.error());
      merged.push_back(std::move(*state));
    }

    for (const schema::TabletId& tablet_id : impl.tablet_order) {
      const Impl::FragmentProgress& progress = impl.fragments.at(tablet_id);
      for (std::size_t ordinal = 0U; ordinal < progress.messages.size(); ++ordinal) {
        auto decoded = decode_distributed_vector_aggregate_exchange_message_exact(
            progress.messages[ordinal].bytes(), impl.definitions, *resources, impl.limits.decode);
        if (!decoded.has_value())
          return common::make_unexpected(decoded.error());
        if (decoded->query_id != impl.query_id || decoded->tablet_id != tablet_id ||
            decoded->aggregate_ordinal != ordinal) {
          return common::make_unexpected(
              corruption("retained vector aggregate correlation changed"));
        }
        auto combined = merged[ordinal].merge(decoded->state, *resources);
        if (!combined.has_value())
          return common::make_unexpected(combined.error());
      }
    }

    std::vector<ScalarValue> values;
    values.reserve(merged.size());
    for (MergeableVectorAggregateState& state : merged) {
      auto value = std::move(state).take_result();
      if (!value.has_value())
        return common::make_unexpected(value.error());
      values.push_back(std::move(*value));
    }
    DistributedVectorAggregateQueryResultV2 result{.definitions = std::move(impl.definitions),
                                                   .result_schema = std::move(impl.result_schema),
                                                   .values = std::move(values),
                                                   .retained_encoded_bytes =
                                                       impl.retained_encoded_bytes};
    impl.finished = true;
    return result;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("vector aggregate finalization allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("vector aggregate finalization exceeds limits"));
  }
}

} // namespace chronos::query
