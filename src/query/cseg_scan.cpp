#include "chronos/query/cseg_scan.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"

#include <bitset>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

inline constexpr std::size_t kConservativeAllocationOverheadBytes = 64U;
// Three metadata arrays, two metadata-validation scratch arrays, the schema projection, the
// caller-owned ordinal allocation adopted by the source, and the state/operator objects.
inline constexpr std::size_t kSourceAllocationCount = 9U;
// Decoded-page storage is added separately because raw and compressed pages share one plan shape.
inline constexpr std::size_t kOutputBaseAllocationCount = 6U;

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Result<std::size_t> add(const std::size_t left, const std::size_t right,
                                              const char* const message) {
  const std::optional<std::size_t> total = common::checked_add(left, right);
  if (!total.has_value())
    return common::make_unexpected(exhausted(message));
  return *total;
}

[[nodiscard]] common::Result<std::size_t>
bytes_for(const std::size_t count, const std::size_t element_size, const char* const message) {
  const std::optional<std::size_t> bytes = common::checked_multiply(count, element_size);
  if (!bytes.has_value())
    return common::make_unexpected(exhausted(message));
  return *bytes;
}

struct AllocationOverheadInput {
  std::size_t bytes;
  std::size_t allocation_count;
};

[[nodiscard]] common::Result<std::size_t>
add_allocation_overhead(const AllocationOverheadInput input, const char* const message) {
  common::Result<std::size_t> overhead =
      bytes_for(input.allocation_count, kConservativeAllocationOverheadBytes, message);
  if (!overhead.has_value())
    return overhead;
  return add(input.bytes, *overhead, message);
}

[[nodiscard]] common::Result<std::size_t>
source_charge(const CsegPartPin& part, const schema::TableSchema& destination_schema,
              const std::vector<std::uint32_t>& destination_column_ordinals) {
  std::size_t total = part.retained_buffer_bytes();
  common::Result<std::size_t> next =
      add(total, part.bytes().size(), "CSEG scan source byte accounting overflowed");
  if (!next.has_value())
    return next;
  total = *next;
  common::Result<std::size_t> validation_scratch =
      bytes_for(cseg::format::kMaximumUserColumnCount, sizeof(schema::ColumnId) + sizeof(bool),
                "CSEG scan metadata-validation scratch accounting overflowed");
  if (!validation_scratch.has_value())
    return validation_scratch;
  next = add(total, *validation_scratch, "CSEG scan source byte accounting overflowed");
  if (!next.has_value())
    return next;
  total = *next;
  common::Result<std::size_t> projection =
      bytes_for(destination_schema.columns().size(), sizeof(schema::ProjectionEntry),
                "CSEG scan schema projection accounting overflowed");
  if (!projection.has_value())
    return projection;
  next = add(total, *projection, "CSEG scan source byte accounting overflowed");
  if (!next.has_value())
    return next;
  total = *next;
  common::Result<std::size_t> ordinals =
      bytes_for(destination_column_ordinals.capacity(), sizeof(std::uint32_t),
                "CSEG scan ordinal accounting overflowed");
  if (!ordinals.has_value())
    return ordinals;
  next = add(total, *ordinals, "CSEG scan source byte accounting overflowed");
  if (!next.has_value())
    return next;
  constexpr std::size_t source_objects =
      sizeof(CsegPartPin) + sizeof(cseg::CsegProjectedReaderView) +
      sizeof(std::vector<std::uint32_t>) + sizeof(CsegScanLimits) + sizeof(QueryMemoryReservation) +
      sizeof(CsegScanOperator);
  next = add(*next, source_objects, "CSEG scan source object accounting overflowed");
  if (!next.has_value())
    return next;
  return add_allocation_overhead({.bytes = *next, .allocation_count = kSourceAllocationCount},
                                 "CSEG scan source allocation accounting overflowed");
}

[[nodiscard]] common::Result<std::size_t>
output_charge(const CsegPartPin& part, const cseg::CsegProjectedGranuleReadPlan& plan) {
  if (plan.owned_buffer_bytes() > std::numeric_limits<std::size_t>::max())
    return common::make_unexpected(exhausted("CSEG scan decoded output does not fit size_t"));
  std::size_t total = part.retained_buffer_bytes();
  common::Result<std::size_t> next = add(total, static_cast<std::size_t>(plan.owned_buffer_bytes()),
                                         "CSEG scan output byte accounting overflowed");
  if (!next.has_value())
    return next;
  total = *next;

  for (const auto [count, element_size] :
       {std::pair{plan.decoded_page_count(), sizeof(cseg::DecodedCsegPage)},
        std::pair{plan.synthesized_column_count(), sizeof(columnar::OwnedColumnVector)},
        std::pair{plan.destination_column_ordinals().size(), sizeof(cseg::CsegProjectedColumnView)},
        std::pair{static_cast<std::size_t>(plan.row_count()), sizeof(std::uint32_t)},
        std::pair{plan.destination_column_ordinals().size(), sizeof(std::size_t)}}) {
    common::Result<std::size_t> container =
        bytes_for(count, element_size, "CSEG scan output container accounting overflowed");
    if (!container.has_value())
      return container;
    next = add(total, *container, "CSEG scan output byte accounting overflowed");
    if (!next.has_value())
      return next;
    total = *next;
  }
  constexpr std::size_t backing_objects =
      sizeof(CsegPartPin) + sizeof(cseg::ProjectedCsegGranule) + 2U * sizeof(std::size_t);
  next = add(total, backing_objects, "CSEG scan backing object accounting overflowed");
  if (!next.has_value())
    return next;
  total = *next;
  const std::optional<std::size_t> synthesis_allocations =
      common::checked_multiply(plan.synthesized_column_count(), std::size_t{3U});
  const std::optional<std::size_t> page_allocations =
      common::checked_add(kOutputBaseAllocationCount, plan.decoded_page_count());
  const std::optional<std::size_t> dynamic_allocations =
      synthesis_allocations.has_value() && page_allocations.has_value()
          ? common::checked_add(*page_allocations, *synthesis_allocations)
          : std::nullopt;
  if (!dynamic_allocations.has_value()) {
    return common::make_unexpected(
        exhausted("CSEG scan output allocation count accounting overflowed"));
  }
  return add_allocation_overhead({.bytes = total, .allocation_count = *dynamic_allocations},
                                 "CSEG scan output allocation accounting overflowed");
}

class CsegGranuleBacking final : public VectorChunkBacking {
public:
  CsegGranuleBacking(CsegPartPin part, cseg::ProjectedCsegGranule granule) noexcept
      : part_(std::move(part)), granule_(std::move(granule)) {
    buffer_bytes_ = granule_.buffer_bytes();
    const std::optional<std::size_t> with_granule =
        common::checked_add(part_.retained_buffer_bytes(), granule_.retained_buffer_bytes());
    const std::optional<std::size_t> with_object =
        with_granule.has_value()
            ? common::checked_add(*with_granule,
                                  sizeof(CsegGranuleBacking) + kConservativeAllocationOverheadBytes)
            : std::nullopt;
    retained_buffer_bytes_ = with_object.value_or(std::numeric_limits<std::size_t>::max());
  }

  [[nodiscard]] std::size_t column_count() const noexcept override {
    return granule_.columns().size();
  }

  [[nodiscard]] const columnar::PhysicalColumnView*
  column(const std::size_t ordinal) const noexcept override {
    const cseg::CsegProjectedColumnView* value = granule_.column(ordinal);
    return value == nullptr ? nullptr : std::addressof(value->physical());
  }

  [[nodiscard]] std::size_t buffer_bytes() const noexcept override {
    return buffer_bytes_;
  }

  [[nodiscard]] std::size_t retained_buffer_bytes() const noexcept override {
    return retained_buffer_bytes_;
  }

private:
  CsegPartPin part_;
  cseg::ProjectedCsegGranule granule_;
  std::size_t buffer_bytes_{};
  std::size_t retained_buffer_bytes_{};
};

} // namespace

class CsegScanOperator::State {
public:
  State(CsegPartPin part_value, cseg::CsegProjectedReaderView reader_value,
        std::vector<std::uint32_t> destination_column_ordinals_value, CsegScanLimits limits_value,
        QueryMemoryReservation source_reservation_value) noexcept
      : part(std::move(part_value)), reader(std::move(reader_value)),
        destination_column_ordinals(std::move(destination_column_ordinals_value)),
        limits(limits_value), source_reservation(std::move(source_reservation_value)) {}

  CsegPartPin part;
  cseg::CsegProjectedReaderView reader;
  std::vector<std::uint32_t> destination_column_ordinals;
  CsegScanLimits limits;
  QueryMemoryReservation source_reservation;
  std::size_t next_granule{};
};

CsegScanOperator::~CsegScanOperator() = default;

CsegPartPin::CsegPartPin(std::shared_ptr<const void> owner, const common::ByteView bytes,
                         const std::size_t retained_buffer_bytes) noexcept
    : owner_(std::move(owner)), bytes_(bytes), retained_buffer_bytes_(retained_buffer_bytes) {}

common::Result<CsegPartPin> CsegPartPin::create(std::shared_ptr<const void> owner,
                                                const common::ByteView bytes,
                                                const std::size_t retained_buffer_bytes) {
  if (owner == nullptr)
    return common::make_unexpected(invalid("CSEG part pin owner must be non-null"));
  if (bytes.empty())
    return common::make_unexpected(invalid("CSEG part pin bytes must be nonempty"));
  if (retained_buffer_bytes < bytes.size()) {
    return common::make_unexpected(
        invalid("CSEG part pin retained bytes are smaller than its encoded image"));
  }
  return CsegPartPin{std::move(owner), bytes, retained_buffer_bytes};
}

CsegScanOperator::CsegScanOperator(std::unique_ptr<State> state) noexcept
    : state_(std::move(state)) {}

common::Result<std::unique_ptr<PhysicalOperator>> CsegScanOperator::create(
    const QueryResourceContext& resources, CsegPartPin part, const schema::SchemaLineage& lineage,
    const schema::SchemaId destination_schema_id, const schema::TabletId& target_tablet,
    std::vector<std::uint32_t> destination_column_ordinals, const CsegScanLimits limits) {
  const std::shared_ptr<const schema::TableSchema> destination_schema =
      lineage.find(destination_schema_id);
  if (destination_schema == nullptr) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound,
                       "CSEG scan destination schema is not retained in the lineage"});
  }
  if (limits.chunk.maximum_rows == 0U || limits.chunk.maximum_columns == 0U ||
      limits.chunk.maximum_buffer_bytes == 0U || limits.chunk.maximum_retained_buffer_bytes == 0U) {
    return common::make_unexpected(invalid("CSEG scan chunk limits must be nonzero"));
  }
  common::Result<std::size_t> charge =
      source_charge(part, *destination_schema, destination_column_ordinals);
  if (!charge.has_value())
    return common::make_unexpected(charge.error());
  common::Result<QueryMemoryReservation> source_reservation = resources.reserve(*charge);
  if (!source_reservation.has_value())
    return common::make_unexpected(source_reservation.error());

  try {
    cseg::CsegProjectedReaderOpenResult reader = cseg::open_cseg_v1_projected_reader_exact(
        part.bytes(), lineage, destination_schema_id, target_tablet, limits.reader);
    if (!reader.has_value())
      return common::make_unexpected(reader.error().status());
    common::Result<cseg::CsegProjectedGranuleReadPlan> first_plan =
        reader->plan_granule(0U, destination_column_ordinals);
    if (!first_plan.has_value())
      return common::make_unexpected(first_plan.error());

    auto state = std::make_unique<State>(std::move(part), std::move(*reader),
                                         std::move(destination_column_ordinals), limits,
                                         std::move(*source_reservation));
    return std::unique_ptr<PhysicalOperator>{new CsegScanOperator{std::move(state)}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("CSEG scan source allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("CSEG scan source exceeds container limits"));
  }
}

common::Result<PhysicalOperatorStep> CsegScanOperator::next(const QueryResourceContext& resources) {
  if (ended_)
    return PhysicalOperatorStep::end();
  const common::Result<void> active = resources.check_cancelled();
  if (!active.has_value())
    return common::make_unexpected(active.error());
  if (!resources.owns(state_->source_reservation)) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(invalid("CSEG scan source belongs to another query"));
  }
  if (state_->next_granule >= state_->reader.metadata().granules().size()) {
    ended_ = true;
    state_.reset();
    return PhysicalOperatorStep::end();
  }

  common::Result<cseg::CsegProjectedGranuleReadPlan> plan =
      state_->reader.plan_granule(state_->next_granule, state_->destination_column_ordinals);
  if (!plan.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(plan.error());
  }
  if (plan->row_count() > state_->limits.chunk.maximum_rows) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(
        exhausted("CSEG scan granule exceeds the configured chunk row limit"));
  }
  if (plan->destination_column_ordinals().size() > state_->limits.chunk.maximum_columns) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(
        exhausted("CSEG scan projection exceeds the configured chunk column limit"));
  }
  if (plan->decoded_buffer_bytes() > std::numeric_limits<std::size_t>::max()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(exhausted("CSEG scan logical output does not fit size_t"));
  }
  const common::Result<std::size_t> selection_bytes = bytes_for(
      plan->row_count(), sizeof(std::uint32_t), "CSEG scan selection byte accounting overflowed");
  common::Result<std::size_t> logical_bytes =
      selection_bytes.has_value()
          ? add(static_cast<std::size_t>(plan->decoded_buffer_bytes()), *selection_bytes,
                "CSEG scan logical byte accounting overflowed")
          : selection_bytes;
  if (!logical_bytes.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(logical_bytes.error());
  }
  if (*logical_bytes > state_->limits.chunk.maximum_buffer_bytes) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(
        exhausted("CSEG scan planned output exceeds the chunk logical-byte limit"));
  }
  common::Result<std::size_t> charge = output_charge(state_->part, *plan);
  if (!charge.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(charge.error());
  }
  if (*charge > state_->limits.chunk.maximum_retained_buffer_bytes) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(
        exhausted("CSEG scan planned output exceeds the chunk retained-byte limit"));
  }
  common::Result<QueryMemoryReservation> reservation = resources.reserve(*charge);
  if (!reservation.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(reservation.error());
  }

  common::Result<cseg::ProjectedCsegGranule> granule = state_->reader.read_granule(*plan);
  if (!granule.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(granule.error());
  }
  const common::Result<void> still_active = resources.check_cancelled();
  if (!still_active.has_value())
    return common::make_unexpected(still_active.error());

  try {
    std::shared_ptr<const VectorChunkBacking> backing =
        std::make_shared<const CsegGranuleBacking>(state_->part, std::move(*granule));
    common::Result<VectorSelection> selection = VectorSelection::all(plan->row_count());
    if (!selection.has_value()) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(selection.error());
    }
    common::Result<VectorChunk> chunk =
        VectorChunk::create_backed(std::move(backing), std::move(*selection), state_->limits.chunk);
    if (!chunk.has_value()) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(chunk.error());
    }
    common::Result<AccountedVectorChunk> accounted =
        AccountedVectorChunk::create(std::move(*chunk), std::move(*reservation), resources);
    if (!accounted.has_value()) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(accounted.error());
    }

    ++state_->next_granule;
    if (state_->next_granule == state_->reader.metadata().granules().size()) {
      ended_ = true;
      state_.reset();
    }
    return PhysicalOperatorStep::chunk(std::move(*accounted));
  } catch (const std::bad_alloc&) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(exhausted("CSEG scan output allocation failed"));
  } catch (const std::length_error&) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(exhausted("CSEG scan output exceeds container limits"));
  }
}

static_assert(sizeof(cseg::CsegColumnDescriptor) <= cseg::format::kColumnDescriptorLength);
static_assert(sizeof(cseg::CsegGranuleDescriptor) <= cseg::format::kGranuleDescriptorLength);
static_assert(sizeof(cseg::CsegPageDescriptor) <= cseg::format::kPageDescriptorLength);

} // namespace chronos::query
