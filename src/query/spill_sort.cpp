#include "chronos/query/spill_sort.hpp"

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/common/status.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

inline constexpr std::array<std::byte, 8> kRunMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                                    std::byte{'S'}, std::byte{'R'}, std::byte{'U'},
                                                    std::byte{'N'}, std::byte{0}};
inline constexpr std::uint16_t kRunMajorVersion = 1U;
inline constexpr std::uint16_t kRunMinorVersion = 0U;
inline constexpr std::size_t kRunHeaderBytes = 32U;
inline constexpr std::size_t kRecordFramingBytes = 8U;
inline constexpr std::size_t kAllocationOverheadBytes = 64U;

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

[[nodiscard]] common::Status exhausted(const std::string_view message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::string{message}};
}

[[nodiscard]] common::Status out_of_range(const std::string_view message) {
  return common::Status{common::StatusCode::kOutOfRange, std::string{message}};
}

[[nodiscard]] common::Status corruption(const std::string_view message) {
  return common::Status{common::StatusCode::kCorruption, std::string{message}};
}

[[nodiscard]] common::Status internal(const std::string_view message) {
  return common::Status{common::StatusCode::kInternal, std::string{message}};
}

[[nodiscard]] common::Result<std::size_t> add_size(const std::size_t left, const std::size_t right,
                                                   const std::string_view message) {
  const std::optional<std::size_t> sum = common::checked_add(left, right);
  if (!sum.has_value())
    return common::make_unexpected(exhausted(message));
  return *sum;
}

[[nodiscard]] common::Result<std::size_t>
multiply_size(const std::size_t left, const std::size_t right, const std::string_view message) {
  const std::optional<std::size_t> product = common::checked_multiply(left, right);
  if (!product.has_value())
    return common::make_unexpected(exhausted(message));
  return *product;
}

[[nodiscard]] std::size_t fixed_width(const schema::LogicalTypeKind kind) noexcept {
  using schema::LogicalTypeKind;
  switch (kind) {
  case LogicalTypeKind::kInt8:
  case LogicalTypeKind::kUInt8:
    return 1U;
  case LogicalTypeKind::kInt16:
  case LogicalTypeKind::kUInt16:
    return 2U;
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kUInt32:
  case LogicalTypeKind::kFloat32:
  case LogicalTypeKind::kDate:
    return 4U;
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kUInt64:
  case LogicalTypeKind::kFloat64:
  case LogicalTypeKind::kTimestampNs:
    return 8U;
  case LogicalTypeKind::kDecimal:
  case LogicalTypeKind::kUuid:
    return 16U;
  case LogicalTypeKind::kBool:
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
  case LogicalTypeKind::kBinary:
    return 0U;
  }
  return 0U;
}

void store_u32_le(const std::span<std::byte, 4> bytes, const std::uint32_t value) noexcept {
  for (std::size_t index = 0U; index < bytes.size(); ++index)
    bytes[index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

void set_bit(std::vector<std::byte>& bytes, const std::uint32_t row) {
  bytes[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
}

[[nodiscard]] bool valid_prefix(const std::string_view prefix) noexcept {
  if (prefix.empty() || prefix.size() > 128U || prefix == "." || prefix == "..")
    return false;
  return std::ranges::none_of(prefix,
                              [](const char value) { return value == '/' || value == '\0'; });
}

struct SpillShape {
  schema::LogicalType type;
  bool nullable;
};

struct RowReference {
  std::size_t run_ordinal;
  std::uint64_t file_offset;
};

struct ParsedCell {
  bool is_null{};
  bool boolean{};
  common::ByteView bytes;
};

[[nodiscard]] std::uint32_t shape_crc(const std::span<const SpillShape> shape) noexcept {
  std::uint32_t checksum = common::crc32c({});
  for (const SpillShape& column : shape) {
    std::array<std::byte, 8> descriptor{};
    descriptor[0] = static_cast<std::byte>(column.type.code() & 0xffU);
    descriptor[1] = static_cast<std::byte>(column.type.code() >> 8U);
    descriptor[2] = static_cast<std::byte>(column.type.parameter_0() & 0xffU);
    descriptor[3] = static_cast<std::byte>(column.type.parameter_0() >> 8U);
    descriptor[4] = static_cast<std::byte>(column.type.parameter_1() & 0xffU);
    descriptor[5] = static_cast<std::byte>(column.type.parameter_1() >> 8U);
    descriptor[6] = column.nullable ? std::byte{1} : std::byte{0};
    checksum = common::extend_crc32c(checksum, descriptor);
  }
  return checksum;
}

[[nodiscard]] common::Result<std::size_t> configuration_charge(const SpillSortLimits& limits) {
  common::Result<std::size_t> scratch =
      add_size(limits.maximum_serialized_record_bytes, kRecordFramingBytes,
               "spill record scratch size overflowed");
  if (!scratch.has_value())
    return scratch;
  scratch = multiply_size(*scratch, 2U, "spill scratch size overflowed");
  if (!scratch.has_value())
    return scratch;
  common::Result<std::size_t> runs =
      multiply_size(limits.maximum_runs, 256U, "spill run state size overflowed");
  if (!runs.has_value())
    return runs;
  common::Result<std::size_t> shape =
      multiply_size(limits.merge_output_limits.output_limits.maximum_columns, 32U,
                    "spill shape state size overflowed");
  if (!shape.has_value())
    return shape;
  common::Result<std::size_t> selected =
      multiply_size(limits.merge_output_limits.maximum_rows, sizeof(RowReference) * 2U,
                    "spill merge-reference state size overflowed");
  if (!selected.has_value())
    return selected;
  common::Result<std::size_t> keys = multiply_size(
      std::max(limits.run_sort_limits.maximum_keys, limits.merge_output_limits.maximum_keys),
      sizeof(VectorSortKey) * 2U, "spill key state size overflowed");
  if (!keys.has_value())
    return keys;
  common::Result<std::size_t> total = add_size(*scratch, *runs, "spill state size overflowed");
  if (total.has_value())
    total = add_size(*total, *shape, "spill state size overflowed");
  if (total.has_value())
    total = add_size(*total, *selected, "spill state size overflowed");
  if (total.has_value())
    total = add_size(*total, *keys, "spill state size overflowed");
  if (total.has_value())
    total = add_size(*total, 8U * kAllocationOverheadBytes, "spill state size overflowed");
  return total;
}

[[nodiscard]] common::Result<void> validate_limits(const SpillSortLimits& limits) {
  if (limits.maximum_rows == 0U || limits.maximum_runs == 0U || limits.maximum_spill_bytes == 0U ||
      limits.maximum_serialized_record_bytes == 0U || limits.maximum_configuration_bytes == 0U) {
    return common::make_unexpected(invalid("spill sort limits must be nonzero"));
  }
  if (limits.maximum_serialized_record_bytes > std::numeric_limits<std::uint32_t>::max())
    return common::make_unexpected(exhausted("spill record limit exceeds its encoded domain"));
  const common::Result<std::size_t> run_state =
      sort_state_reservation_bytes(limits.run_sort_limits);
  if (!run_state.has_value())
    return common::make_unexpected(run_state.error());
  const common::Result<std::size_t> merge_state =
      sort_state_reservation_bytes(limits.merge_output_limits);
  if (!merge_state.has_value())
    return common::make_unexpected(merge_state.error());
  if (limits.run_sort_limits.output_limits.maximum_rows < limits.run_sort_limits.maximum_rows)
    return common::make_unexpected(invalid("spill run output row limit is smaller than a run"));
  if (limits.merge_output_limits.output_limits.maximum_rows <
      limits.merge_output_limits.maximum_rows) {
    return common::make_unexpected(
        invalid("spill merge output row limit is smaller than a merge batch"));
  }
  const common::Result<std::size_t> charge = configuration_charge(limits);
  if (!charge.has_value())
    return common::make_unexpected(charge.error());
  if (*charge > limits.maximum_configuration_bytes)
    return common::make_unexpected(exhausted("spill configuration exceeds its byte limit"));
  return {};
}

[[nodiscard]] common::Result<void> parse_record(const common::ByteView payload,
                                                const std::span<const SpillShape> shape,
                                                const auto& consume) {
  common::ByteReader reader{payload};
  for (std::size_t ordinal = 0U; ordinal < shape.size(); ++ordinal) {
    const common::Result<std::uint8_t> encoded_null = reader.read_u8();
    if (!encoded_null.has_value())
      return common::make_unexpected(corruption("spill row ended before its NULL marker"));
    if (*encoded_null > 1U)
      return common::make_unexpected(corruption("spill row has an invalid NULL marker"));
    ParsedCell cell{.is_null = *encoded_null == 1U, .boolean = false, .bytes = {}};
    if (cell.is_null) {
      if (!shape[ordinal].nullable)
        return common::make_unexpected(corruption("spill row has NULL in a non-null column"));
    } else if (shape[ordinal].type.kind() == schema::LogicalTypeKind::kBool) {
      const common::Result<std::uint8_t> value = reader.read_u8();
      if (!value.has_value() || *value > 1U)
        return common::make_unexpected(corruption("spill row has an invalid Boolean value"));
      cell.boolean = *value == 1U;
    } else {
      std::size_t width = fixed_width(shape[ordinal].type.kind());
      if (shape[ordinal].type.is_variable_width()) {
        const common::Result<std::uint32_t> length = reader.read_u32_le();
        if (!length.has_value())
          return common::make_unexpected(corruption("spill row ended before a value length"));
        width = *length;
      }
      const common::Result<common::ByteView> bytes = reader.read_exact(width);
      if (!bytes.has_value())
        return common::make_unexpected(corruption("spill row ended within a value"));
      cell.bytes = *bytes;
    }
    const common::Result<void> consumed = consume(ordinal, cell);
    if (!consumed.has_value())
      return consumed;
  }
  if (!reader.empty())
    return common::make_unexpected(corruption("spill row has trailing payload bytes"));
  return {};
}

class CellAdapter {
public:
  [[nodiscard]] common::Result<columnar::ColumnCellView> adapt(const SpillShape& shape,
                                                               const ParsedCell cell) {
    validity_.fill(std::byte{0});
    offsets_.fill(std::byte{0});
    boolean_.fill(std::byte{0});
    zeros_.fill(std::byte{0});
    columnar::ColumnVectorBufferView buffers;
    if (shape.nullable) {
      if (!cell.is_null)
        validity_[0] = std::byte{1};
      buffers.validity = validity_;
    }
    if (shape.type.kind() == schema::LogicalTypeKind::kBool) {
      if (!cell.is_null && cell.boolean)
        boolean_[0] = std::byte{1};
      buffers.values = boolean_;
    } else if (shape.type.is_variable_width()) {
      const std::uint32_t length =
          cell.is_null ? 0U : static_cast<std::uint32_t>(cell.bytes.size());
      store_u32_le(std::span{offsets_}.subspan<4U, 4U>(), length);
      buffers.offsets = offsets_;
      buffers.values = cell.is_null ? common::ByteView{} : cell.bytes;
    } else {
      buffers.values = cell.is_null ? common::ByteView{zeros_}.first(fixed_width(shape.type.kind()))
                                    : cell.bytes;
    }
    common::Result<columnar::PhysicalColumnView> column =
        columnar::PhysicalColumnView::create({.type = shape.type,
                                              .nullable = shape.nullable,
                                              .row_count = 1U,
                                              .null_count = cell.is_null ? 1U : 0U},
                                             buffers);
    if (!column.has_value())
      return common::make_unexpected(corruption("spill row cell is not canonical"));
    column_.emplace(*column);
    common::Result<columnar::ColumnCellView> adapted = column_->cell(0U);
    if (!adapted.has_value())
      return common::make_unexpected(corruption("spill row cell cannot be inspected"));
    return *adapted;
  }

private:
  std::array<std::byte, 1> validity_{};
  std::array<std::byte, 8> offsets_{};
  std::array<std::byte, 1> boolean_{};
  std::array<std::byte, 16> zeros_{};
  std::optional<columnar::PhysicalColumnView> column_;
};

} // namespace

class SpillSortOperator::State {
public:
  struct RunFile {
    std::string name;
    io::PosixFile file;
    std::uint64_t current_offset{kRunHeaderBytes};
    std::uint64_t file_size{};
    std::uint32_t remaining_rows{};
    std::size_t ordinal{};
  };

  class RunInput final : public PhysicalOperator {
  public:
    explicit RunInput(State& state) noexcept : state_(state) {}

    [[nodiscard]] common::Result<PhysicalOperatorStep>
    next(const QueryResourceContext& resources) override {
      if (ended_)
        return PhysicalOperatorStep::end();
      for (;;) {
        common::Result<AccountedVectorChunk> chunk = state_.take_input(resources);
        if (!chunk.has_value()) {
          if (chunk.error().code() == common::StatusCode::kNotFound) {
            ended_ = true;
            return PhysicalOperatorStep::end();
          }
          return common::make_unexpected(chunk.error());
        }
        const std::size_t rows = chunk->chunk().selected_row_count();
        if (rows == 0U)
          continue;
        if (rows > state_.limits.run_sort_limits.maximum_rows)
          return common::make_unexpected(exhausted("spill input chunk is larger than one run"));
        const std::size_t remaining = state_.limits.run_sort_limits.maximum_rows - emitted_;
        if (rows > remaining) {
          state_.pending.emplace(std::move(*chunk));
          ended_ = true;
          return PhysicalOperatorStep::end();
        }
        emitted_ += rows;
        return PhysicalOperatorStep::chunk(std::move(*chunk));
      }
    }

  private:
    State& state_;
    std::size_t emitted_{};
    bool ended_{};
  };

  State(std::unique_ptr<PhysicalOperator> input_value, io::PosixDirectory directory_value,
        std::string prefix_value, const SpillSortLimits limits_value) noexcept
      : input(std::move(input_value)), directory(std::move(directory_value)),
        prefix(std::move(prefix_value)), limits(limits_value) {}

  ~State() {
    cleanup_best_effort();
  }

  [[nodiscard]] common::Result<void> initialize(const QueryResourceContext& resources) {
    const common::Result<std::size_t> charge = configuration_charge(limits);
    if (!charge.has_value())
      return common::make_unexpected(charge.error());
    common::Result<QueryMemoryReservation> reserved = resources.reserve(*charge);
    if (!reserved.has_value())
      return common::make_unexpected(reserved.error());
    configuration_reservation.emplace(std::move(*reserved));
    try {
      runs.reserve(limits.maximum_runs);
      shape.reserve(limits.merge_output_limits.output_limits.maximum_columns);
      selected.reserve(limits.merge_output_limits.maximum_rows);
      scratch_a.reserve(limits.maximum_serialized_record_bytes + kRecordFramingBytes);
      scratch_b.reserve(limits.maximum_serialized_record_bytes + kRecordFramingBytes);
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("spill configuration allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("spill configuration exceeds container limits"));
    }
    return {};
  }

  [[nodiscard]] common::Result<AccountedVectorChunk>
  take_input(const QueryResourceContext& resources) {
    if (pending.has_value()) {
      AccountedVectorChunk result = std::move(*pending);
      pending.reset();
      return result;
    }
    if (input_ended)
      return common::make_unexpected(
          common::Status{common::StatusCode::kNotFound, "spill input ended"});
    common::Result<PhysicalOperatorStep> step = input->next(resources);
    if (!step.has_value())
      return common::make_unexpected(step.error());
    if (step->kind() == PhysicalOperatorStepKind::kEnd) {
      input_ended = true;
      input.reset();
      return common::make_unexpected(
          common::Status{common::StatusCode::kNotFound, "spill input ended"});
    }
    common::Result<AccountedVectorChunk> chunk = std::move(*step).take_chunk();
    if (!chunk.has_value())
      return common::make_unexpected(chunk.error());
    if (!chunk->belongs_to(resources))
      return common::make_unexpected(invalid("spill input belongs to another query"));
    const common::Result<void> validated = validate_input_chunk(chunk->chunk());
    if (!validated.has_value())
      return common::make_unexpected(validated.error());
    const std::size_t rows = chunk->chunk().selected_row_count();
    if (rows > limits.maximum_rows - total_rows)
      return common::make_unexpected(exhausted("spill input exceeds its total row limit"));
    total_rows += rows;
    return std::move(*chunk);
  }

  [[nodiscard]] common::Result<void> validate_input_chunk(const VectorChunk& chunk) {
    if (chunk.column_count() > limits.merge_output_limits.output_limits.maximum_columns)
      return common::make_unexpected(exhausted("spill input exceeds its column limit"));
    for (const VectorSortKey& key : keys_for_validation) {
      if (key.column_ordinal >= chunk.column_count())
        return common::make_unexpected(out_of_range("spill key ordinal is outside the input"));
    }
    if (shape.empty()) {
      try {
        for (std::size_t ordinal = 0U; ordinal < chunk.column_count(); ++ordinal) {
          const columnar::PhysicalColumnView* column = chunk.column(ordinal);
          if (column == nullptr)
            return common::make_unexpected(internal("spill input column is absent"));
          shape.push_back({.type = column->type(), .nullable = column->nullable()});
        }
      } catch (const std::bad_alloc&) {
        return common::make_unexpected(exhausted("spill shape allocation failed"));
      }
      return {};
    }
    if (chunk.column_count() != shape.size())
      return common::make_unexpected(invalid("spill input column count changed across chunks"));
    for (std::size_t ordinal = 0U; ordinal < shape.size(); ++ordinal) {
      const columnar::PhysicalColumnView* column = chunk.column(ordinal);
      if (column == nullptr || column->type() != shape[ordinal].type ||
          column->nullable() != shape[ordinal].nullable) {
        return common::make_unexpected(invalid("spill input shape changed across chunks"));
      }
    }
    return {};
  }

  [[nodiscard]] common::Result<void> generate_runs(const QueryResourceContext& resources,
                                                   const std::vector<VectorSortKey>& keys) {
    try {
      keys_for_validation = keys;
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("spill validation-key allocation failed"));
    }
    while (!input_ended || pending.has_value()) {
      const common::Result<void> active = resources.check_cancelled();
      if (!active.has_value())
        return active;
      if (runs.size() == limits.maximum_runs) {
        // A run that ends exactly at its row bound has not yet observed child end. Probe through
        // empty chunks so an exactly full final run succeeds, while any additional selected row is
        // rejected without creating an unowned file.
        for (;;) {
          common::Result<AccountedVectorChunk> extra = take_input(resources);
          if (!extra.has_value()) {
            if (extra.error().code() == common::StatusCode::kNotFound)
              break;
            return common::make_unexpected(extra.error());
          }
          if (extra->chunk().selected_row_count() != 0U)
            return common::make_unexpected(exhausted("spill sort exceeds its run limit"));
        }
        break;
      }
      std::vector<VectorSortKey> run_keys;
      try {
        run_keys = keys;
      } catch (const std::bad_alloc&) {
        return common::make_unexpected(exhausted("spill run key allocation failed"));
      }
      std::unique_ptr<PhysicalOperator> run_input;
      try {
        run_input = std::make_unique<RunInput>(*this);
      } catch (const std::bad_alloc&) {
        return common::make_unexpected(exhausted("spill run source allocation failed"));
      }
      common::Result<std::unique_ptr<PhysicalOperator>> sorter =
          SortOperator::create(std::move(run_input), std::move(run_keys), limits.run_sort_limits);
      if (!sorter.has_value())
        return common::make_unexpected(sorter.error());
      common::Result<PhysicalOperatorStep> step = (*sorter)->next(resources);
      if (!step.has_value())
        return common::make_unexpected(step.error());
      if (step->kind() == PhysicalOperatorStepKind::kEnd)
        continue;
      common::Result<AccountedVectorChunk> sorted = std::move(*step).take_chunk();
      if (!sorted.has_value())
        return common::make_unexpected(sorted.error());
      const common::Result<void> written = write_run(sorted->chunk(), resources);
      if (!written.has_value())
        return written;
    }
    input.reset();
    pending.reset();
    return initialize_readers();
  }

  [[nodiscard]] common::Result<void> write_run(const VectorChunk& chunk,
                                               const QueryResourceContext& resources) {
    const std::size_t ordinal = runs.size();
    std::string name;
    try {
      name = prefix + ".run-" + std::to_string(ordinal) + ".tmp";
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("spill run name allocation failed"));
    }
    common::Result<io::PosixFile> file = directory.create_exclusive_regular_file(name);
    if (!file.has_value())
      return common::make_unexpected(file.error());
    runs.push_back(RunFile{.name = std::move(name),
                           .file = std::move(*file),
                           .current_offset = kRunHeaderBytes,
                           .file_size = 0U,
                           .remaining_rows = static_cast<std::uint32_t>(chunk.selected_row_count()),
                           .ordinal = ordinal});
    RunFile& run = runs.back();

    std::array<std::byte, kRunHeaderBytes> header{};
    common::ByteWriter writer{header};
    if (!writer.write_exact(kRunMagic).is_ok() || !writer.write_u16_le(kRunMajorVersion).is_ok() ||
        !writer.write_u16_le(kRunMinorVersion).is_ok() ||
        !writer.write_u32_le(static_cast<std::uint32_t>(kRunHeaderBytes)).is_ok() ||
        !writer.write_u32_le(run.remaining_rows).is_ok() ||
        !writer.write_u32_le(static_cast<std::uint32_t>(shape.size())).is_ok() ||
        !writer.write_u32_le(shape_crc(shape)).is_ok()) {
      return common::make_unexpected(internal("spill run header construction failed"));
    }
    const std::uint32_t checksum = common::crc32c(common::ByteView{header}.first(28U));
    if (!writer.write_u32_le(checksum).is_ok())
      return common::make_unexpected(internal("spill run header checksum construction failed"));
    const common::Result<void> admitted = admit_spill_bytes(header.size());
    if (!admitted.has_value())
      return admitted;
    common::Status status = run.file.write_all_at(0U, header);
    if (!status.is_ok())
      return common::make_unexpected(std::move(status));
    run.file_size = header.size();
    metrics.spill_bytes_written += header.size();

    for (std::size_t row = 0U; row < chunk.selected_row_count(); ++row) {
      const common::Result<void> active = resources.check_cancelled();
      if (!active.has_value())
        return active;
      common::Result<std::size_t> payload_size = encoded_payload_size(chunk, row);
      if (!payload_size.has_value())
        return common::make_unexpected(payload_size.error());
      if (*payload_size > limits.maximum_serialized_record_bytes)
        return common::make_unexpected(exhausted("spill row exceeds its record-byte limit"));
      scratch_a.resize(*payload_size + kRecordFramingBytes);
      common::ByteWriter row_writer{scratch_a};
      if (!row_writer.write_u32_le(static_cast<std::uint32_t>(*payload_size)).is_ok())
        return common::make_unexpected(internal("spill record framing failed"));
      const common::Result<void> encoded = encode_payload(chunk, row, row_writer);
      if (!encoded.has_value())
        return encoded;
      const std::uint32_t row_checksum =
          common::crc32c(common::ByteView{scratch_a}.subspan(sizeof(std::uint32_t), *payload_size));
      if (!row_writer.write_u32_le(row_checksum).is_ok() || !row_writer.full())
        return common::make_unexpected(internal("spill record size planning disagreed"));
      const common::Result<void> record_admitted = admit_spill_bytes(scratch_a.size());
      if (!record_admitted.has_value())
        return record_admitted;
      status = run.file.write_all_at(run.file_size, scratch_a);
      if (!status.is_ok())
        return common::make_unexpected(std::move(status));
      run.file_size += scratch_a.size();
      metrics.spill_bytes_written += scratch_a.size();
      ++metrics.rows_spilled;
    }
    ++metrics.runs_written;
    return {};
  }

  [[nodiscard]] common::Result<std::size_t> encoded_payload_size(const VectorChunk& chunk,
                                                                 const std::size_t row) const {
    std::size_t total = shape.size();
    for (std::size_t ordinal = 0U; ordinal < shape.size(); ++ordinal) {
      common::Result<columnar::ColumnCellView> cell =
          chunk.cell({.column_ordinal = ordinal, .selected_row = row});
      if (!cell.has_value())
        return common::make_unexpected(cell.error());
      if (cell->is_null())
        continue;
      if (shape[ordinal].type.kind() == schema::LogicalTypeKind::kBool) {
        const common::Result<std::size_t> next = add_size(total, 1U, "spill row size overflowed");
        if (!next.has_value())
          return next;
        total = *next;
      } else {
        common::Result<common::ByteView> bytes = cell->bytes();
        if (!bytes.has_value())
          return common::make_unexpected(bytes.error());
        const std::size_t length_prefix = shape[ordinal].type.is_variable_width() ? 4U : 0U;
        common::Result<std::size_t> next =
            add_size(total, length_prefix, "spill row size overflowed");
        if (next.has_value())
          next = add_size(*next, bytes->size(), "spill row size overflowed");
        if (!next.has_value())
          return next;
        total = *next;
      }
    }
    return total;
  }

  [[nodiscard]] common::Result<void> encode_payload(const VectorChunk& chunk, const std::size_t row,
                                                    common::ByteWriter& writer) const {
    for (std::size_t ordinal = 0U; ordinal < shape.size(); ++ordinal) {
      common::Result<columnar::ColumnCellView> cell =
          chunk.cell({.column_ordinal = ordinal, .selected_row = row});
      if (!cell.has_value())
        return common::make_unexpected(cell.error());
      if (!writer.write_u8(cell->is_null() ? 1U : 0U).is_ok())
        return common::make_unexpected(internal("spill row NULL encoding failed"));
      if (cell->is_null())
        continue;
      if (shape[ordinal].type.kind() == schema::LogicalTypeKind::kBool) {
        common::Result<bool> value = cell->boolean();
        if (!value.has_value())
          return common::make_unexpected(value.error());
        if (!writer.write_u8(*value ? 1U : 0U).is_ok())
          return common::make_unexpected(internal("spill row Boolean encoding failed"));
      } else {
        common::Result<common::ByteView> bytes = cell->bytes();
        if (!bytes.has_value())
          return common::make_unexpected(bytes.error());
        if (shape[ordinal].type.is_variable_width() &&
            !writer.write_u32_le(static_cast<std::uint32_t>(bytes->size())).is_ok()) {
          return common::make_unexpected(internal("spill row length encoding failed"));
        }
        if (!writer.write_exact(*bytes).is_ok())
          return common::make_unexpected(internal("spill row value encoding failed"));
      }
    }
    return {};
  }

  [[nodiscard]] common::Result<void> admit_spill_bytes(const std::size_t bytes) const {
    if (bytes > limits.maximum_spill_bytes - metrics.spill_bytes_written)
      return common::make_unexpected(exhausted("spill sort exceeds its disk-byte limit"));
    return {};
  }

  [[nodiscard]] common::Result<void> initialize_readers() {
    for (RunFile& run : runs) {
      std::array<std::byte, kRunHeaderBytes> header{};
      const common::Result<std::size_t> count = run.file.read_at(0U, header);
      if (!count.has_value())
        return common::make_unexpected(count.error());
      metrics.spill_bytes_read += *count;
      if (*count != header.size())
        return common::make_unexpected(corruption("spill run header is truncated"));
      common::ByteReader reader{header};
      const common::Result<common::ByteView> magic = reader.read_exact(kRunMagic.size());
      const common::Result<std::uint16_t> major = reader.read_u16_le();
      const common::Result<std::uint16_t> minor = reader.read_u16_le();
      const common::Result<std::uint32_t> header_length = reader.read_u32_le();
      const common::Result<std::uint32_t> rows = reader.read_u32_le();
      const common::Result<std::uint32_t> columns = reader.read_u32_le();
      const common::Result<std::uint32_t> encoded_shape_crc = reader.read_u32_le();
      const common::Result<std::uint32_t> encoded_header_crc = reader.read_u32_le();
      if (!magic.has_value() || !major.has_value() || !minor.has_value() ||
          !header_length.has_value() || !rows.has_value() || !columns.has_value() ||
          !encoded_shape_crc.has_value() || !encoded_header_crc.has_value()) {
        return common::make_unexpected(corruption("spill run header cannot be decoded"));
      }
      if (!std::ranges::equal(*magic, kRunMagic) || *major != kRunMajorVersion ||
          *minor != kRunMinorVersion || *header_length != kRunHeaderBytes ||
          *rows != run.remaining_rows || *columns != shape.size() ||
          *encoded_shape_crc != shape_crc(shape) ||
          *encoded_header_crc != common::crc32c(common::ByteView{header}.first(28U))) {
        return common::make_unexpected(corruption("spill run header is invalid"));
      }
      const common::Result<std::uint64_t> actual_size = run.file.size();
      if (!actual_size.has_value())
        return common::make_unexpected(actual_size.error());
      if (*actual_size != run.file_size)
        return common::make_unexpected(corruption("spill run size changed unexpectedly"));
      run.current_offset = kRunHeaderBytes;
    }
    return {};
  }

  struct RecordView {
    common::ByteView payload;
    std::size_t framed_size;
  };

  [[nodiscard]] common::Result<RecordView>
  read_record(const RunFile& run, const std::uint64_t offset, std::vector<std::byte>& scratch) {
    std::array<std::byte, 4> length_bytes{};
    common::Result<std::size_t> count = run.file.read_at(offset, length_bytes);
    if (!count.has_value())
      return common::make_unexpected(count.error());
    metrics.spill_bytes_read += *count;
    if (*count != length_bytes.size())
      return common::make_unexpected(corruption("spill record length is truncated"));
    common::ByteReader length_reader{length_bytes};
    const common::Result<std::uint32_t> payload_size = length_reader.read_u32_le();
    if (!payload_size.has_value() || *payload_size > limits.maximum_serialized_record_bytes)
      return common::make_unexpected(corruption("spill record length exceeds its limit"));
    const std::size_t framed_size = static_cast<std::size_t>(*payload_size) + kRecordFramingBytes;
    if (offset > run.file_size || framed_size > run.file_size - offset)
      return common::make_unexpected(corruption("spill record extends beyond its run"));
    scratch.resize(framed_size);
    count = run.file.read_at(offset, scratch);
    if (!count.has_value())
      return common::make_unexpected(count.error());
    metrics.spill_bytes_read += *count;
    if (*count != framed_size)
      return common::make_unexpected(corruption("spill record is truncated"));
    common::ByteReader reader{scratch};
    const common::Result<std::uint32_t> repeated_size = reader.read_u32_le();
    const common::Result<common::ByteView> payload = reader.read_exact(*payload_size);
    const common::Result<std::uint32_t> checksum = reader.read_u32_le();
    if (!repeated_size.has_value() || *repeated_size != *payload_size || !payload.has_value() ||
        !checksum.has_value() || *checksum != common::crc32c(*payload)) {
      return common::make_unexpected(corruption("spill record checksum is invalid"));
    }
    return RecordView{.payload = *payload, .framed_size = framed_size};
  }

  [[nodiscard]] common::Result<ParsedCell> cell_at(const common::ByteView payload,
                                                   const std::size_t wanted) const {
    std::optional<ParsedCell> result;
    const common::Result<void> parsed =
        parse_record(payload, shape,
                     [&](const std::size_t ordinal, const ParsedCell cell) -> common::Result<void> {
                       if (ordinal == wanted)
                         result = cell;
                       return {};
                     });
    if (!parsed.has_value())
      return common::make_unexpected(parsed.error());
    if (!result.has_value())
      return common::make_unexpected(out_of_range("spill key ordinal is outside its row"));
    return *result;
  }

  [[nodiscard]] common::Result<int> compare_runs(const RunFile& left, const RunFile& right,
                                                 const std::vector<VectorSortKey>& keys) {
    common::Result<RecordView> left_record = read_record(left, left.current_offset, scratch_a);
    if (!left_record.has_value())
      return common::make_unexpected(left_record.error());
    common::Result<RecordView> right_record = read_record(right, right.current_offset, scratch_b);
    if (!right_record.has_value())
      return common::make_unexpected(right_record.error());
    for (const VectorSortKey& key : keys) {
      common::Result<ParsedCell> left_cell = cell_at(left_record->payload, key.column_ordinal);
      if (!left_cell.has_value())
        return common::make_unexpected(left_cell.error());
      common::Result<ParsedCell> right_cell = cell_at(right_record->payload, key.column_ordinal);
      if (!right_cell.has_value())
        return common::make_unexpected(right_cell.error());
      CellAdapter left_adapter;
      CellAdapter right_adapter;
      common::Result<columnar::ColumnCellView> adapted_left =
          left_adapter.adapt(shape[key.column_ordinal], *left_cell);
      if (!adapted_left.has_value())
        return common::make_unexpected(adapted_left.error());
      common::Result<columnar::ColumnCellView> adapted_right =
          right_adapter.adapt(shape[key.column_ordinal], *right_cell);
      if (!adapted_right.has_value())
        return common::make_unexpected(adapted_right.error());
      const bool includes_null = left_cell->is_null || right_cell->is_null;
      common::Result<int> compared = compare_physical_cells(
          shape[key.column_ordinal].type, *adapted_left, *adapted_right, key.null_placement);
      if (!compared.has_value())
        return common::make_unexpected(compared.error());
      if (!includes_null && key.direction == PhysicalSortDirection::kDescending)
        *compared = -*compared;
      if (*compared != 0)
        return *compared;
    }
    return 0;
  }

  [[nodiscard]] common::Result<std::size_t> choose_winner(const std::vector<VectorSortKey>& keys) {
    std::optional<std::size_t> winner;
    for (std::size_t ordinal = 0U; ordinal < runs.size(); ++ordinal) {
      if (runs[ordinal].remaining_rows == 0U)
        continue;
      if (!winner.has_value()) {
        winner = ordinal;
        continue;
      }
      const common::Result<int> order = compare_runs(runs[ordinal], runs[*winner], keys);
      if (!order.has_value())
        return common::make_unexpected(order.error());
      if (*order < 0)
        winner = ordinal;
      // Equal rows deliberately retain the lower contiguous run ordinal. Combined with stable
      // sorting inside each run, this is exactly the original logical input order.
    }
    if (!winner.has_value())
      return common::make_unexpected(
          common::Status{common::StatusCode::kNotFound, "spill merge ended"});
    return *winner;
  }

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next_output(const QueryResourceContext& resources, const std::vector<VectorSortKey>& keys) {
    selected.clear();
    while (selected.size() < limits.merge_output_limits.maximum_rows) {
      const common::Result<void> active = resources.check_cancelled();
      if (!active.has_value())
        return common::make_unexpected(active.error());
      common::Result<std::size_t> winner = choose_winner(keys);
      if (!winner.has_value()) {
        if (winner.error().code() == common::StatusCode::kNotFound)
          break;
        return common::make_unexpected(winner.error());
      }
      RunFile& run = runs[*winner];
      common::Result<RecordView> record = read_record(run, run.current_offset, scratch_a);
      if (!record.has_value())
        return common::make_unexpected(record.error());
      selected.push_back({.run_ordinal = *winner, .file_offset = run.current_offset});
      run.current_offset += record->framed_size;
      --run.remaining_rows;
      if (run.remaining_rows == 0U && run.current_offset != run.file_size)
        return common::make_unexpected(corruption("spill run has trailing bytes"));
    }
    if (selected.empty())
      return PhysicalOperatorStep::end();
    common::Result<AccountedVectorChunk> output = materialize_selected(resources);
    if (!output.has_value())
      return common::make_unexpected(output.error());
    ++metrics.output_chunks;
    return PhysicalOperatorStep::chunk(std::move(*output));
  }

  [[nodiscard]] common::Result<AccountedVectorChunk>
  materialize_selected(const QueryResourceContext& resources) {
    const std::uint32_t row_count = static_cast<std::uint32_t>(selected.size());
    std::vector<std::size_t> variable_bytes;
    try {
      variable_bytes.assign(shape.size(), 0U);
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("spill output planning allocation failed"));
    }
    for (const RowReference reference : selected) {
      common::Result<RecordView> record =
          read_record(runs[reference.run_ordinal], reference.file_offset, scratch_a);
      if (!record.has_value())
        return common::make_unexpected(record.error());
      const common::Result<void> measured = parse_record(
          record->payload, shape,
          [&](const std::size_t ordinal, const ParsedCell cell) -> common::Result<void> {
            if (!cell.is_null && shape[ordinal].type.is_variable_width()) {
              const common::Result<std::size_t> next =
                  add_size(variable_bytes[ordinal], cell.bytes.size(),
                           "spill output variable bytes overflowed");
              if (!next.has_value())
                return common::make_unexpected(next.error());
              if (*next > std::numeric_limits<std::uint32_t>::max())
                return common::make_unexpected(
                    exhausted("spill output variable bytes exceed the offset domain"));
              variable_bytes[ordinal] = *next;
            }
            return {};
          });
      if (!measured.has_value())
        return common::make_unexpected(measured.error());
    }

    common::Result<std::size_t> buffer_bytes =
        multiply_size(row_count, sizeof(std::uint32_t), "spill output selection overflowed");
    if (!buffer_bytes.has_value())
      return common::make_unexpected(buffer_bytes.error());
    for (std::size_t ordinal = 0U; ordinal < shape.size(); ++ordinal) {
      if (shape[ordinal].nullable)
        buffer_bytes = add_size(*buffer_bytes, columnar::bitmap_size(row_count),
                                "spill output validity size overflowed");
      if (!buffer_bytes.has_value())
        return common::make_unexpected(buffer_bytes.error());
      if (shape[ordinal].type.kind() == schema::LogicalTypeKind::kBool) {
        buffer_bytes = add_size(*buffer_bytes, columnar::bitmap_size(row_count),
                                "spill output Boolean size overflowed");
      } else if (shape[ordinal].type.is_variable_width()) {
        const common::Result<std::size_t> offsets =
            multiply_size(static_cast<std::size_t>(row_count) + 1U, sizeof(std::uint32_t),
                          "spill output offset size overflowed");
        if (!offsets.has_value())
          return common::make_unexpected(offsets.error());
        buffer_bytes = add_size(*buffer_bytes, *offsets, "spill output size overflowed");
        if (buffer_bytes.has_value())
          buffer_bytes =
              add_size(*buffer_bytes, variable_bytes[ordinal], "spill output size overflowed");
      } else {
        const common::Result<std::size_t> values =
            multiply_size(row_count, fixed_width(shape[ordinal].type.kind()),
                          "spill output fixed size overflowed");
        if (!values.has_value())
          return common::make_unexpected(values.error());
        buffer_bytes = add_size(*buffer_bytes, *values, "spill output size overflowed");
      }
      if (!buffer_bytes.has_value())
        return common::make_unexpected(buffer_bytes.error());
      if (*buffer_bytes > limits.merge_output_limits.output_limits.maximum_buffer_bytes)
        return common::make_unexpected(exhausted("spill output exceeds its buffer-byte limit"));
    }
    common::Result<std::size_t> objects = multiply_size(
        shape.size(), sizeof(columnar::OwnedPhysicalColumn), "spill output owner size overflowed");
    if (!objects.has_value())
      return common::make_unexpected(objects.error());
    common::Result<std::size_t> retained =
        add_size(*buffer_bytes, *objects, "spill output retained size overflowed");
    if (retained.has_value()) {
      retained = add_size(*retained, (shape.size() * 3U + 3U) * kAllocationOverheadBytes,
                          "spill output retained size overflowed");
    }
    if (!retained.has_value())
      return common::make_unexpected(retained.error());
    if (*retained > limits.merge_output_limits.output_limits.maximum_retained_buffer_bytes)
      return common::make_unexpected(exhausted("spill output exceeds its retained-byte limit"));
    common::Result<QueryMemoryReservation> reservation = resources.reserve(*retained);
    if (!reservation.has_value())
      return common::make_unexpected(reservation.error());

    try {
      std::vector<columnar::ColumnVectorBuffers> buffers(shape.size());
      std::vector<std::uint32_t> null_counts(shape.size(), 0U);
      for (std::size_t ordinal = 0U; ordinal < shape.size(); ++ordinal) {
        if (shape[ordinal].nullable)
          buffers[ordinal].validity.resize(columnar::bitmap_size(row_count));
        if (shape[ordinal].type.kind() == schema::LogicalTypeKind::kBool) {
          buffers[ordinal].values.resize(columnar::bitmap_size(row_count));
        } else if (shape[ordinal].type.is_variable_width()) {
          buffers[ordinal].offsets.resize((static_cast<std::size_t>(row_count) + 1U) * 4U);
          buffers[ordinal].values.reserve(variable_bytes[ordinal]);
        } else {
          buffers[ordinal].values.resize(static_cast<std::size_t>(row_count) *
                                         fixed_width(shape[ordinal].type.kind()));
        }
      }
      for (std::uint32_t output_row = 0U; output_row < row_count; ++output_row) {
        const RowReference reference = selected[output_row];
        common::Result<RecordView> record =
            read_record(runs[reference.run_ordinal], reference.file_offset, scratch_a);
        if (!record.has_value())
          return common::make_unexpected(record.error());
        const common::Result<void> copied = parse_record(
            record->payload, shape,
            [&](const std::size_t ordinal, const ParsedCell cell) -> common::Result<void> {
              if (cell.is_null) {
                ++null_counts[ordinal];
              } else {
                if (shape[ordinal].nullable)
                  set_bit(buffers[ordinal].validity, output_row);
                if (shape[ordinal].type.kind() == schema::LogicalTypeKind::kBool) {
                  if (cell.boolean)
                    set_bit(buffers[ordinal].values, output_row);
                } else if (shape[ordinal].type.is_variable_width()) {
                  buffers[ordinal].values.insert(buffers[ordinal].values.end(), cell.bytes.begin(),
                                                 cell.bytes.end());
                } else {
                  const std::size_t width = fixed_width(shape[ordinal].type.kind());
                  std::ranges::copy(cell.bytes,
                                    buffers[ordinal].values.begin() +
                                        static_cast<std::ptrdiff_t>(output_row * width));
                }
              }
              if (shape[ordinal].type.is_variable_width()) {
                std::array<std::byte, 4> offset{};
                store_u32_le(offset, static_cast<std::uint32_t>(buffers[ordinal].values.size()));
                std::ranges::copy(offset, buffers[ordinal].offsets.begin() +
                                              static_cast<std::ptrdiff_t>(output_row + 1U) * 4);
              }
              return {};
            });
        if (!copied.has_value())
          return common::make_unexpected(copied.error());
      }
      std::vector<columnar::OwnedPhysicalColumn> columns;
      columns.reserve(shape.size());
      for (std::size_t ordinal = 0U; ordinal < shape.size(); ++ordinal) {
        common::Result<columnar::OwnedPhysicalColumn> column =
            columnar::OwnedPhysicalColumn::create({.type = shape[ordinal].type,
                                                   .nullable = shape[ordinal].nullable,
                                                   .row_count = row_count,
                                                   .null_count = null_counts[ordinal]},
                                                  std::move(buffers[ordinal]));
        if (!column.has_value())
          return common::make_unexpected(column.error());
        columns.push_back(std::move(*column));
      }
      common::Result<VectorSelection> selection = VectorSelection::all(row_count);
      if (!selection.has_value())
        return common::make_unexpected(selection.error());
      common::Result<VectorChunk> chunk = VectorChunk::create(
          std::move(columns), std::move(*selection), limits.merge_output_limits.output_limits);
      if (!chunk.has_value())
        return common::make_unexpected(chunk.error());
      return AccountedVectorChunk::create(std::move(*chunk), std::move(*reservation), resources);
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("spill output allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("spill output exceeds container limits"));
    }
  }

  [[nodiscard]] common::Result<void> cleanup() {
    std::optional<common::Status> first_error;
    for (RunFile& run : runs) {
      common::Status status = run.file.close();
      if (!status.is_ok() && !first_error.has_value())
        first_error = std::move(status);
      status = directory.remove_file(run.name);
      if (!status.is_ok() && !first_error.has_value())
        first_error = std::move(status);
      run.name.clear();
    }
    runs.clear();
    if (first_error.has_value())
      return common::make_unexpected(std::move(*first_error));
    return {};
  }

  void cleanup_best_effort() noexcept {
    for (RunFile& run : runs) {
      static_cast<void>(run.file.close());
      if (!run.name.empty())
        static_cast<void>(directory.remove_file(run.name));
    }
  }

  std::unique_ptr<PhysicalOperator> input;
  io::PosixDirectory directory;
  std::string prefix;
  SpillSortLimits limits;
  std::optional<AccountedVectorChunk> pending;
  std::vector<SpillShape> shape;
  std::vector<RunFile> runs;
  std::vector<RowReference> selected;
  std::vector<std::byte> scratch_a;
  std::vector<std::byte> scratch_b;
  std::vector<VectorSortKey> keys_for_validation;
  std::optional<QueryMemoryReservation> configuration_reservation;
  SpillSortMetrics metrics;
  std::uint64_t total_rows{};
  bool input_ended{};
};

common::Result<std::size_t>
spill_sort_configuration_reservation_bytes(const SpillSortLimits& limits) {
  const common::Result<void> valid = validate_limits(limits);
  if (!valid.has_value())
    return common::make_unexpected(valid.error());
  return configuration_charge(limits);
}

SpillSortOperator::~SpillSortOperator() = default;

SpillSortOperator::SpillSortOperator(std::vector<VectorSortKey> keys, const SpillSortLimits limits,
                                     std::unique_ptr<State> state) noexcept
    : keys_(std::move(keys)), limits_(limits), state_(std::move(state)) {}

common::Result<std::unique_ptr<PhysicalOperator>>
SpillSortOperator::create(std::unique_ptr<PhysicalOperator> input, std::vector<VectorSortKey> keys,
                          io::PosixDirectory spill_directory, std::string file_prefix,
                          const SpillSortLimits limits) {
  if (input == nullptr)
    return common::make_unexpected(invalid("spill sort input must be non-null"));
  if (!spill_directory.is_open())
    return common::make_unexpected(invalid("spill directory must be open"));
  if (!valid_prefix(file_prefix))
    return common::make_unexpected(invalid("spill file prefix is invalid"));
  const common::Result<void> valid = validate_limits(limits);
  if (!valid.has_value())
    return common::make_unexpected(valid.error());
  if (keys.empty())
    return common::make_unexpected(invalid("spill sort requires at least one key"));
  if (keys.size() > limits.run_sort_limits.maximum_keys ||
      keys.size() > limits.merge_output_limits.maximum_keys ||
      keys.capacity() > limits.run_sort_limits.maximum_keys ||
      keys.capacity() > limits.merge_output_limits.maximum_keys) {
    return common::make_unexpected(exhausted("spill sort key configuration exceeds its limits"));
  }
  try {
    auto state = std::make_unique<State>(std::move(input), std::move(spill_directory),
                                         std::move(file_prefix), limits);
    return std::unique_ptr<PhysicalOperator>{
        new SpillSortOperator{std::move(keys), limits, std::move(state)}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("spill sort operator allocation failed"));
  }
}

common::Result<PhysicalOperatorStep>
SpillSortOperator::next(const QueryResourceContext& resources) {
  try {
    return next_impl(resources);
  } catch (const std::bad_alloc&) {
    static_cast<void>(resources.request_cancel());
    if (state_ != nullptr)
      metrics_ = state_->metrics;
    state_.reset();
    return common::make_unexpected(exhausted("spill sort allocation failed"));
  } catch (const std::length_error&) {
    static_cast<void>(resources.request_cancel());
    if (state_ != nullptr)
      metrics_ = state_->metrics;
    state_.reset();
    return common::make_unexpected(exhausted("spill sort allocation exceeds container limits"));
  }
}

common::Result<PhysicalOperatorStep>
SpillSortOperator::next_impl(const QueryResourceContext& resources) {
  if (ended_)
    return PhysicalOperatorStep::end();
  const common::Result<void> active = resources.check_cancelled();
  if (!active.has_value())
    return common::make_unexpected(active.error());
  const auto fail = [&](const common::Status& status) -> common::Result<PhysicalOperatorStep> {
    static_cast<void>(resources.request_cancel());
    if (state_ != nullptr)
      metrics_ = state_->metrics;
    state_.reset();
    return common::make_unexpected(status);
  };
  if (!initialized_) {
    const common::Result<void> initialized = state_->initialize(resources);
    if (!initialized.has_value())
      return fail(initialized.error());
    const common::Result<void> generated = state_->generate_runs(resources, keys_);
    if (!generated.has_value())
      return fail(generated.error());
    initialized_ = true;
  }
  common::Result<PhysicalOperatorStep> output = state_->next_output(resources, keys_);
  if (!output.has_value())
    return fail(output.error());
  if (output->kind() == PhysicalOperatorStepKind::kChunk)
    return output;
  metrics_ = state_->metrics;
  const common::Result<void> cleaned = state_->cleanup();
  if (!cleaned.has_value())
    return fail(cleaned.error());
  state_.reset();
  ended_ = true;
  return PhysicalOperatorStep::end();
}

SpillSortMetrics SpillSortOperator::metrics() const noexcept {
  return state_ == nullptr ? metrics_ : state_->metrics;
}

} // namespace chronos::query
