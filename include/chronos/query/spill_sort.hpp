#ifndef CHRONOS_QUERY_SPILL_SORT_HPP_
#define CHRONOS_QUERY_SPILL_SORT_HPP_

#include "chronos/common/result.hpp"
#include "chronos/io/posix_io.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/sort.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace chronos::query {

inline constexpr std::uint64_t kDefaultSpillSortRowLimit = std::uint64_t{1U} << 20U;
inline constexpr std::size_t kDefaultSpillSortRunLimit = 1'024U;
inline constexpr std::uint64_t kDefaultSpillSortByteLimit = std::uint64_t{4U} << 30U;
inline constexpr std::size_t kDefaultSpillSortRecordByteLimit = std::size_t{8U} << 20U;
inline constexpr std::size_t kDefaultSpillSortConfigurationByteLimit = std::size_t{24U} << 20U;

struct SpillSortLimits {
  std::uint64_t maximum_rows{kDefaultSpillSortRowLimit};
  std::size_t maximum_runs{kDefaultSpillSortRunLimit};
  std::uint64_t maximum_spill_bytes{kDefaultSpillSortByteLimit};
  std::size_t maximum_serialized_record_bytes{kDefaultSpillSortRecordByteLimit};
  std::size_t maximum_configuration_bytes{kDefaultSpillSortConfigurationByteLimit};
  SortLimits run_sort_limits{};
  SortLimits merge_output_limits{};
};

struct SpillSortMetrics {
  std::size_t runs_written{};
  std::uint64_t rows_spilled{};
  std::uint64_t spill_bytes_written{};
  std::uint64_t spill_bytes_read{};
  std::size_t output_chunks{};

  friend constexpr bool operator==(const SpillSortMetrics&, const SpillSortMetrics&) = default;
};

// Returns the conservative query credit acquired before run metadata, merge references, and two
// bounded record scratch buffers are allocated. Run-sort and output-chunk credit is additional.
[[nodiscard]] common::Result<std::size_t>
spill_sort_configuration_reservation_bytes(const SpillSortLimits& limits);

// Blocking run generation followed by a pull-based stable external merge. Input chunks may not be
// larger than one configured run; callers that need chunk splitting must place that explicit stage
// upstream. Runs are ephemeral, versioned, and checksum protected. The operator owns only files
// whose exclusive names it creates and removes them on completion or destruction.
class SpillSortOperator final : public PhysicalOperator {
public:
  ~SpillSortOperator() override;

  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(std::unique_ptr<PhysicalOperator> input, std::vector<VectorSortKey> keys,
         io::PosixDirectory spill_directory, std::string file_prefix, SpillSortLimits limits = {});

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override;

  [[nodiscard]] SpillSortMetrics metrics() const noexcept;

private:
  class State;

  SpillSortOperator(std::vector<VectorSortKey> keys, SpillSortLimits limits,
                    std::unique_ptr<State> state) noexcept;
  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next_impl(const QueryResourceContext& resources);

  std::vector<VectorSortKey> keys_;
  SpillSortLimits limits_;
  std::unique_ptr<State> state_;
  SpillSortMetrics metrics_;
  bool initialized_{};
  bool ended_{};
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_SPILL_SORT_HPP_
