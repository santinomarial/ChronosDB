#include "chronos/query/spill_sort.hpp"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

class Source final : public chronos::query::PhysicalOperator {
public:
  explicit Source(std::vector<chronos::query::AccountedVectorChunk> chunks)
      : chunks_(std::move(chunks)) {}
  chronos::common::Result<chronos::query::PhysicalOperatorStep>
  next(const chronos::query::QueryResourceContext&) override {
    if (cursor_ == chunks_.size())
      return chronos::query::PhysicalOperatorStep::end();
    return chronos::query::PhysicalOperatorStep::chunk(std::move(chunks_[cursor_++]));
  }

private:
  std::vector<chronos::query::AccountedVectorChunk> chunks_;
  std::size_t cursor_{};
};

[[nodiscard]] chronos::query::AccountedVectorChunk
chunk(const chronos::query::QueryResourceContext& resources,
      const std::span<const std::int64_t> values) {
  chronos::columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(values.size() * sizeof(std::int64_t));
  for (std::size_t row = 0U; row < values.size(); ++row) {
    const std::uint64_t bits = std::bit_cast<std::uint64_t>(values[row]);
    for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
      buffers.values[row * sizeof(bits) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
    }
  }
  std::vector<chronos::columnar::OwnedPhysicalColumn> columns;
  columns.push_back(
      chronos::columnar::OwnedPhysicalColumn::create(
          {.type = chronos::schema::LogicalType::create(chronos::schema::LogicalTypeKind::kInt64)
                       .value(),
           .nullable = false,
           .row_count = static_cast<std::uint32_t>(values.size()),
           .null_count = 0U},
          std::move(buffers))
          .value());
  chronos::query::VectorChunk vector =
      chronos::query::VectorChunk::create(
          std::move(columns),
          chronos::query::VectorSelection::all(static_cast<std::uint32_t>(values.size())).value())
          .value();
  const std::size_t charge = vector.retained_buffer_bytes() + 512U;
  return chronos::query::AccountedVectorChunk::create(std::move(vector),
                                                      resources.reserve(charge).value(), resources)
      .value();
}

[[nodiscard]] std::int64_t read(const chronos::query::VectorChunk& chunk_value,
                                const std::size_t row) {
  const chronos::common::ByteView bytes = chunk_value.cell({0U, row}).value().bytes().value();
  std::uint64_t bits = 0U;
  for (std::size_t byte = 0U; byte < bytes.size(); ++byte)
    bits |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[byte])) << (byte * 8U);
  return std::bit_cast<std::int64_t>(bits);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  if (size < 3U)
    return 0;
  const std::span<const std::uint8_t> input{data, size};
  chronos::query::SpillSortLimits limits;
  limits.maximum_rows = 32U;
  limits.maximum_runs = 32U;
  limits.maximum_spill_bytes = (static_cast<std::uint64_t>(input[0]) + 1U) * 64U;
  limits.maximum_serialized_record_bytes = (static_cast<std::size_t>(input[1]) + 1U) * 8U;
  limits.maximum_configuration_bytes = 1U << 20U;
  limits.run_sort_limits.maximum_rows = static_cast<std::uint32_t>(input[2] % 4U) + 1U;
  limits.run_sort_limits.maximum_state_bytes = 1U << 20U;
  limits.run_sort_limits.output_limits.maximum_rows = limits.run_sort_limits.maximum_rows;
  limits.merge_output_limits.maximum_rows = static_cast<std::uint32_t>(input[1] % 5U) + 1U;
  limits.merge_output_limits.maximum_state_bytes = 1U << 20U;
  limits.merge_output_limits.output_limits.maximum_rows = limits.merge_output_limits.maximum_rows;
  static_cast<void>(chronos::query::spill_sort_configuration_reservation_bytes(limits));
  if ((input[0] & 3U) != 0U)
    return 0;

  const std::size_t row_count = std::min<std::size_t>(size - 3U, 16U);
  if (row_count == 0U)
    return 0;
  std::vector<std::int64_t> model;
  model.reserve(row_count);
  for (std::size_t row = 0U; row < row_count; ++row)
    model.push_back(static_cast<std::int8_t>(input[row + 3U]));
  std::vector<std::int64_t> expected = model;
  const bool descending = (input[1] & 1U) != 0U;
  std::stable_sort(expected.begin(), expected.end(),
                   [descending](const auto left, const auto right) {
                     return descending ? left > right : left < right;
                   });

  std::string pattern =
      (std::filesystem::temp_directory_path() / "chronos-spill-fuzz-XXXXXX").string();
  char* const created = ::mkdtemp(pattern.data());
  if (created == nullptr)
    return 0;
  const std::filesystem::path path{created};
  chronos::query::QueryResourceContext resources =
      chronos::query::QueryResourceContext::create(8U << 20U).value();
  std::vector<chronos::query::AccountedVectorChunk> chunks;
  const std::size_t run_rows = limits.run_sort_limits.maximum_rows;
  for (std::size_t first = 0U; first < model.size(); first += run_rows) {
    chunks.push_back(chunk(resources, std::span<const std::int64_t>{model}.subspan(
                                          first, std::min(run_rows, model.size() - first))));
  }
  limits.maximum_spill_bytes = 1U << 20U;
  limits.maximum_serialized_record_bytes = 64U;
  auto sorted = chronos::query::SpillSortOperator::create(
      std::make_unique<Source>(std::move(chunks)),
      std::vector<chronos::query::VectorSortKey>{
          {.column_ordinal = 0U,
           .direction = descending ? chronos::query::PhysicalSortDirection::kDescending
                                   : chronos::query::PhysicalSortDirection::kAscending}},
      chronos::io::PosixDirectory::open(path.string()).value(), "fuzz", limits);
  std::size_t output_row = 0U;
  if (sorted.has_value()) {
    for (;;) {
      auto step = (*sorted)->next(resources);
      if (!step.has_value() || step->kind() == chronos::query::PhysicalOperatorStepKind::kEnd)
        break;
      auto output = std::move(*step).take_chunk();
      if (!output.has_value())
        std::abort();
      for (std::size_t local = 0U; local < output->chunk().selected_row_count(); ++local) {
        if (output_row >= expected.size() || read(output->chunk(), local) != expected[output_row++])
          std::abort();
      }
    }
  }
  sorted = chronos::common::make_unexpected(
      chronos::common::Status{chronos::common::StatusCode::kInternal, "drop fuzz operator"});
  if (!resources.is_cancelled() && output_row != expected.size())
    std::abort();
  if (resources.reserved_memory_bytes() != 0U)
    std::abort();
  std::error_code ignored;
  std::filesystem::remove_all(path, ignored);
  return 0;
}
