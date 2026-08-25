#include "chronos/query/distributed_vector_grouped_aggregate_coordinator.hpp"

#include "chronos/common/checked_math.hpp"

#include <algorithm>
#include <map>
#include <new>
#include <optional>
#include <ranges>
#include <set>
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

[[nodiscard]] bool
valid_decode_limits(const DistributedVectorGroupedAggregateExchangeDecodeLimits& limits) noexcept {
  return limits.maximum_frame_length >=
             distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength &&
         limits.maximum_frame_length <=
             distributed_vector_grouped_aggregate_exchange_format::kMaximumFrameLength &&
         limits.maximum_key_payload_bytes > 0U &&
         limits.maximum_key_payload_bytes <=
             distributed_vector_grouped_aggregate_exchange_format::kMaximumKeyPayloadBytes &&
         limits.maximum_groups > 0U &&
         limits.maximum_groups <=
             distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups &&
         limits.maximum_group_keys > 0U &&
         limits.maximum_group_keys <=
             distributed_vector_grouped_aggregate_exchange_format::kMaximumGroupKeys &&
         limits.maximum_aggregates <=
             distributed_vector_grouped_aggregate_exchange_format::kMaximumAggregates &&
         limits.state.maximum_frame_length >=
             distributed_vector_aggregate_state_format::kMinimumFrameLength &&
         limits.state.maximum_frame_length <=
             distributed_vector_aggregate_state_format::kMaximumFrameLength &&
         limits.state.maximum_variable_extremum_bytes > 0U &&
         limits.state.maximum_variable_extremum_bytes <=
             distributed_vector_aggregate_state_format::kMaximumExtremumBytes;
}

[[nodiscard]] common::Result<std::size_t>
retained_configuration_bytes(const std::size_t tablet_capacity, const std::size_t key_capacity,
                             const std::size_t definition_capacity) {
  constexpr std::size_t kConservativeTabletOwnerBytes =
      sizeof(schema::TabletId) * 2U +
      sizeof(std::vector<EncodedDistributedVectorGroupedAggregateExchangeMessage>) * 2U +
      sizeof(std::optional<std::uint32_t>) * 2U + 128U;
  const auto tablet_bytes =
      common::checked_multiply(tablet_capacity, kConservativeTabletOwnerBytes);
  const auto key_bytes =
      common::checked_multiply(key_capacity, sizeof(VectorGroupKeyDefinition) * 2U);
  const auto definition_bytes =
      common::checked_multiply(definition_capacity, sizeof(VectorAggregateDefinition) * 2U);
  const auto first = tablet_bytes.has_value() && key_bytes.has_value()
                         ? common::checked_add(*tablet_bytes, *key_bytes)
                         : std::nullopt;
  const auto total = first.has_value() && definition_bytes.has_value()
                         ? common::checked_add(*first, *definition_bytes)
                         : std::nullopt;
  return total.has_value() ? common::Result<std::size_t>{*total}
                           : common::Result<std::size_t>{common::make_unexpected(exhausted(
                                 "grouped aggregate coordinator configuration size overflowed"))};
}

} // namespace

class DistributedVectorGroupedAggregateCoordinator::Impl {
public:
  struct FragmentProgress {
    std::vector<EncodedDistributedVectorGroupedAggregateExchangeMessage> messages;
    std::optional<std::uint32_t> group_count;
    bool terminal{};
  };

  Impl(common::Uuid id, std::vector<schema::TabletId> tablets,
       std::vector<VectorGroupKeyDefinition> group_keys,
       std::vector<VectorAggregateDefinition> aggregate_definitions,
       const DistributedVectorGroupedAggregateCoordinatorLimits configured)
      : query_id(id), tablet_order(std::move(tablets)), keys(std::move(group_keys)),
        definitions(std::move(aggregate_definitions)), limits(configured) {
    for (const schema::TabletId& tablet_id : tablet_order)
      fragments.emplace(tablet_id, FragmentProgress{});
  }

  common::Uuid query_id;
  std::vector<schema::TabletId> tablet_order;
  std::vector<VectorGroupKeyDefinition> keys;
  std::vector<VectorAggregateDefinition> definitions;
  DistributedVectorGroupedAggregateCoordinatorLimits limits;
  std::map<schema::TabletId, FragmentProgress> fragments;
  std::size_t retained_messages{};
  std::size_t retained_encoded_bytes{};
  std::optional<common::Status> failure;
  std::optional<QueryResourceContext> resources;
  std::optional<MergeableVectorGroupedAggregateTable> table;
  std::size_t output_group{};
  bool ready{};
  bool output_complete{};
};

DistributedVectorGroupedAggregateCoordinator::DistributedVectorGroupedAggregateCoordinator(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

DistributedVectorGroupedAggregateCoordinator::~DistributedVectorGroupedAggregateCoordinator() =
    default;
DistributedVectorGroupedAggregateCoordinator::DistributedVectorGroupedAggregateCoordinator(
    DistributedVectorGroupedAggregateCoordinator&&) noexcept = default;
DistributedVectorGroupedAggregateCoordinator&
DistributedVectorGroupedAggregateCoordinator::operator=(
    DistributedVectorGroupedAggregateCoordinator&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateCoordinator>
DistributedVectorGroupedAggregateCoordinator::create(
    const common::Uuid query_id, std::vector<schema::TabletId> tablets,
    std::vector<VectorGroupKeyDefinition> keys, std::vector<VectorAggregateDefinition> definitions,
    const DistributedVectorGroupedAggregateCoordinatorLimits limits) {
  if (query_id.is_nil() || tablets.empty())
    return common::make_unexpected(invalid("grouped aggregate coordinator identity is invalid"));
  const common::Status authority = validate_distributed_vector_grouped_aggregate_authority(
      keys, definitions, limits.decode.maximum_group_keys, limits.decode.maximum_aggregates);
  if (!authority.is_ok())
    return common::make_unexpected(authority);
  const auto configuration =
      retained_configuration_bytes(tablets.size(), keys.size(), definitions.size());
  if (!configuration.has_value())
    return common::make_unexpected(configuration.error());
  const auto minimum_encoded_bytes = common::checked_multiply(
      tablets.size(), distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength);
  if (limits.messages.maximum_messages_per_fragment == 0U ||
      limits.messages.maximum_messages_per_fragment >
          distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups ||
      limits.messages.maximum_messages_per_fragment > limits.messages.maximum_total_messages ||
      limits.messages.maximum_total_messages > kMaximumDistributedCoordinatorMessages ||
      tablets.size() > limits.messages.maximum_total_messages ||
      limits.maximum_total_encoded_bytes == 0U ||
      limits.maximum_total_encoded_bytes >
          kMaximumDistributedVectorGroupedCoordinatorEncodedBytes ||
      !minimum_encoded_bytes.has_value() ||
      *minimum_encoded_bytes > limits.maximum_total_encoded_bytes ||
      limits.maximum_query_memory_bytes == 0U ||
      limits.maximum_query_memory_bytes > kMaximumDistributedVectorGroupedCoordinatorMemoryBytes ||
      limits.maximum_retained_configuration_bytes == 0U ||
      *configuration > limits.maximum_retained_configuration_bytes ||
      !valid_decode_limits(limits.decode) || keys.size() > limits.table.maximum_group_keys ||
      definitions.size() > limits.table.maximum_aggregates ||
      limits.table.maximum_groups > limits.decode.maximum_groups ||
      limits.messages.maximum_messages_per_fragment > limits.decode.maximum_groups) {
    return common::make_unexpected(invalid("grouped aggregate coordinator limits are invalid"));
  }
  auto table_validation =
      MergeableVectorGroupedAggregateTable::create(keys, definitions, limits.table);
  if (!table_validation.has_value())
    return common::make_unexpected(table_validation.error());
  try {
    std::set<schema::TabletId> unique_tablets;
    for (const schema::TabletId& tablet_id : tablets) {
      if (tablet_id.uuid().is_nil() || !unique_tablets.insert(tablet_id).second)
        return common::make_unexpected(
            invalid("grouped aggregate coordinator tablets are invalid"));
    }
    std::vector<schema::TabletId> retained_tablets{tablets.begin(), tablets.end()};
    std::vector<VectorGroupKeyDefinition> retained_keys{keys.begin(), keys.end()};
    std::vector<VectorAggregateDefinition> retained_definitions{definitions.begin(),
                                                                definitions.end()};
    const auto retained = retained_configuration_bytes(
        retained_tablets.capacity(), retained_keys.capacity(), retained_definitions.capacity());
    if (!retained.has_value())
      return common::make_unexpected(retained.error());
    if (*retained > limits.maximum_retained_configuration_bytes) {
      return common::make_unexpected(
          exhausted("grouped aggregate coordinator retained configuration exceeds its limit"));
    }
    return DistributedVectorGroupedAggregateCoordinator{
        std::make_unique<Impl>(query_id, std::move(retained_tablets), std::move(retained_keys),
                               std::move(retained_definitions), limits)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped aggregate coordinator allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped aggregate coordinator exceeds limits"));
  }
}

common::Status DistributedVectorGroupedAggregateCoordinator::accept(
    const DistributedVectorGroupedAggregateExchangeMessage& message) {
  Impl& impl = *impl_;
  if (impl.ready || impl.output_complete)
    return invalid("grouped aggregate coordinator input is already sealed");
  if (impl.failure.has_value())
    return *impl.failure;
  const auto fragment = impl.fragments.find(message.position().tablet_id);
  if (message.position().query_id != impl.query_id || fragment == impl.fragments.end())
    return invalid("grouped aggregate message does not belong to the coordinator");
  auto encoded = encode_distributed_vector_grouped_aggregate_exchange_message(message, impl.keys,
                                                                              impl.definitions);
  if (!encoded.has_value())
    return encoded.error();

  Impl::FragmentProgress& progress = fragment->second;
  const std::uint64_t sequence = message.position().sequence;
  if (sequence <= progress.messages.size()) {
    const common::ByteView retained = progress.messages[sequence - 1U].bytes();
    return std::ranges::equal(encoded->bytes(), retained)
               ? common::Status::ok()
               : common::Status{common::StatusCode::kAlreadyExists,
                                "grouped aggregate sequence conflicts with retained state"};
  }
  if (progress.terminal)
    return invalid("grouped aggregate fragment emitted after its terminal message");
  if (sequence != progress.messages.size() + 1U)
    return {common::StatusCode::kUnavailable, "grouped aggregate fragment sequence has a gap"};
  const bool first_message = progress.messages.empty();
  if (first_message) {
    progress.group_count = message.position().group_count;
  } else if (message.position().empty ||
             message.position().group_count != progress.group_count.value_or(0U)) {
    return invalid("grouped aggregate fragment shape changed during its stream");
  }
  if (progress.messages.size() == impl.limits.messages.maximum_messages_per_fragment ||
      impl.retained_messages == impl.limits.messages.maximum_total_messages ||
      encoded->bytes().size() >
          impl.limits.maximum_total_encoded_bytes - impl.retained_encoded_bytes) {
    if (first_message)
      progress.group_count.reset();
    return exhausted("grouped aggregate coordinator retention is exhausted");
  }
  const std::size_t encoded_size = encoded->bytes().size();
  try {
    progress.messages.push_back(std::move(*encoded));
  } catch (const std::bad_alloc&) {
    if (first_message)
      progress.group_count.reset();
    return exhausted("grouped aggregate coordinator retention allocation failed");
  } catch (const std::length_error&) {
    if (first_message)
      progress.group_count.reset();
    return exhausted("grouped aggregate coordinator retention exceeds limits");
  }
  progress.terminal = message.position().terminal;
  ++impl.retained_messages;
  impl.retained_encoded_bytes += encoded_size;
  return common::Status::ok();
}

common::Status
DistributedVectorGroupedAggregateCoordinator::worker_failed(const schema::TabletId& tablet_id,
                                                            common::Status failure) {
  Impl& impl = *impl_;
  if (impl.ready || impl.output_complete)
    return invalid("grouped aggregate coordinator input is already sealed");
  const auto fragment = impl.fragments.find(tablet_id);
  if (fragment == impl.fragments.end() || failure.is_ok())
    return invalid("grouped aggregate worker failure is invalid or unplanned");
  if (fragment->second.terminal)
    return common::Status::ok();
  if (impl.failure.has_value())
    return *impl.failure;
  impl.failure = std::move(failure);
  return common::Status::ok();
}

common::Status DistributedVectorGroupedAggregateCoordinator::finish() {
  Impl& impl = *impl_;
  if (impl.ready || impl.output_complete)
    return invalid("grouped aggregate coordinator is already finished");
  if (impl.failure.has_value())
    return *impl.failure;
  for (const schema::TabletId& tablet_id : impl.tablet_order) {
    const Impl::FragmentProgress& progress = impl.fragments.at(tablet_id);
    if (!progress.terminal || progress.messages.empty())
      return {common::StatusCode::kUnavailable, "grouped aggregate query has incomplete fragments"};
    const std::size_t expected_messages =
        progress.group_count.value_or(0U) == 0U ? 1U : progress.group_count.value_or(0U);
    if (progress.messages.size() != expected_messages)
      return corruption("grouped aggregate terminal fragment length changed");
  }

  try {
    auto resources = QueryResourceContext::create(impl.limits.maximum_query_memory_bytes);
    if (!resources.has_value())
      return resources.error();
    auto table = MergeableVectorGroupedAggregateTable::create(impl.keys, impl.definitions,
                                                              impl.limits.table);
    if (!table.has_value())
      return table.error();
    for (const schema::TabletId& tablet_id : impl.tablet_order) {
      const Impl::FragmentProgress& progress = impl.fragments.at(tablet_id);
      for (std::size_t ordinal = 0U; ordinal < progress.messages.size(); ++ordinal) {
        auto decoded = decode_distributed_vector_grouped_aggregate_exchange_message_exact(
            progress.messages[ordinal].bytes(), impl.keys, impl.definitions, *resources,
            impl.limits.decode);
        if (!decoded.has_value())
          return decoded.error();
        const auto& position = decoded->position();
        if (position.query_id != impl.query_id || position.tablet_id != tablet_id ||
            position.sequence != ordinal + 1U || position.group_ordinal != ordinal) {
          return corruption("retained grouped aggregate correlation changed");
        }
        if (position.empty)
          continue;
        auto merged = table->merge_group(decoded->keys(), decoded->states(), *resources);
        if (!merged.has_value())
          return merged.error();
      }
    }
    impl.resources.emplace(std::move(*resources));
    impl.table.emplace(std::move(*table));
    impl.ready = true;
    return common::Status::ok();
  } catch (const std::bad_alloc&) {
    return exhausted("grouped aggregate coordinator finalization allocation failed");
  } catch (const std::length_error&) {
    return exhausted("grouped aggregate coordinator finalization exceeds limits");
  }
}

common::Result<PhysicalOperatorStep> DistributedVectorGroupedAggregateCoordinator::next() {
  Impl& impl = *impl_;
  if (impl.failure.has_value())
    return common::make_unexpected(*impl.failure);
  if (impl.output_complete)
    return PhysicalOperatorStep::end();
  if (!impl.ready || !impl.resources.has_value() || !impl.table.has_value())
    return common::make_unexpected(
        invalid("grouped aggregate coordinator is not ready for output"));
  if (impl.output_group >= impl.table->group_count()) {
    impl.table.reset();
    impl.resources.reset();
    impl.output_complete = true;
    return PhysicalOperatorStep::end();
  }
  auto output = impl.table->materialize_group(impl.output_group, *impl.resources,
                                              impl.limits.table.output_limits);
  if (!output.has_value()) {
    impl.failure = output.error();
    impl.table.reset();
    impl.resources.reset();
    return common::make_unexpected(*impl.failure);
  }
  ++impl.output_group;
  return output;
}

std::size_t DistributedVectorGroupedAggregateCoordinator::retained_message_count() const noexcept {
  return impl_->retained_messages;
}

std::size_t DistributedVectorGroupedAggregateCoordinator::retained_encoded_bytes() const noexcept {
  return impl_->retained_encoded_bytes;
}

std::size_t DistributedVectorGroupedAggregateCoordinator::group_count() const noexcept {
  return impl_->table
      .transform(
          [](const MergeableVectorGroupedAggregateTable& table) { return table.group_count(); })
      .value_or(0U);
}

std::optional<QueryResourceContext>
DistributedVectorGroupedAggregateCoordinator::output_resources() const noexcept {
  return impl_->resources;
}

} // namespace chronos::query
