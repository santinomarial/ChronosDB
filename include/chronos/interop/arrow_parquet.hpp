#ifndef CHRONOS_INTEROP_ARROW_PARQUET_HPP_
#define CHRONOS_INTEROP_ARROW_PARQUET_HPP_

#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <filesystem>
#include <memory>

namespace chronos::interop {

// Import limits bound the external file and the canonical ChronosDB batch retained from it.
// Parquet decoding may transiently allocate more than the compressed input size inside Arrow.
struct ImportLimits {
  std::size_t max_file_bytes{256U * 1024U * 1024U};
  columnar::ColumnarBatchLimits batch{};
};

// The caller supplies the exact target schema on import. External field names, types, nullability,
// and order must match; durable ChronosDB identities and roles are never inferred from a file.
[[nodiscard]] common::Status write_arrow_ipc_file(const columnar::OwnedColumnarBatch& batch,
                                                  const std::filesystem::path& path);
[[nodiscard]] common::Result<columnar::OwnedColumnarBatch>
read_arrow_ipc_file(const std::filesystem::path& path,
                    std::shared_ptr<const schema::TableSchema> target_schema,
                    ImportLimits limits = {});

[[nodiscard]] common::Status write_parquet_file(const columnar::OwnedColumnarBatch& batch,
                                                const std::filesystem::path& path);
[[nodiscard]] common::Result<columnar::OwnedColumnarBatch>
read_parquet_file(const std::filesystem::path& path,
                  std::shared_ptr<const schema::TableSchema> target_schema,
                  ImportLimits limits = {});

} // namespace chronos::interop

#endif // CHRONOS_INTEROP_ARROW_PARQUET_HPP_
