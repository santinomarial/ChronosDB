#ifndef CHRONOS_QUERY_EXECUTOR_HPP_
#define CHRONOS_QUERY_EXECUTOR_HPP_

#include "chronos/query/binder.hpp"
#include "chronos/query/diagnostic.hpp"
#include "chronos/query/snapshot.hpp"
#include "chronos/query/value.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstddef>
#include <span>
#include <string>
#include <vector>

namespace chronos::query {

struct ScalarQueryLimits {
  std::size_t maximum_rows_per_source{1'000'000U};
  std::size_t maximum_intermediate_rows{1'000'000U};
  std::size_t maximum_output_rows{1'000'000U};
  std::size_t maximum_groups{100'000U};
};

struct ScalarResultColumn {
  std::string name;
  bool name_quoted{};
  schema::LogicalType type;
  bool nullable{};
};

class ScalarQueryResult {
public:
  ScalarQueryResult() = delete;
  ScalarQueryResult(std::vector<ScalarResultColumn> columns,
                    std::vector<std::vector<ScalarValue>> rows) noexcept;

  [[nodiscard]] std::span<const ScalarResultColumn> columns() const noexcept;
  [[nodiscard]] std::span<const std::vector<ScalarValue>> rows() const noexcept;

private:
  std::vector<ScalarResultColumn> columns_;
  std::vector<std::vector<ScalarValue>> rows_;
};

// Executes one already-bound SELECT against provider-owned stable snapshots. The scalar reference
// path is intentionally bounded and deterministic where SQL v1 promises order; it is an oracle,
// not the Phase 9 vectorized product path.
[[nodiscard]] SqlResult<ScalarQueryResult>
execute_sql_v1_select(const BoundSqlSelect& plan, const ScalarSnapshotProvider& provider,
                      ScalarQueryLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_EXECUTOR_HPP_
