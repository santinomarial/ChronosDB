#include "chronos/interop/arrow_parquet.hpp"

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/bytes.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/byte_size.h>
#include <atomic>
#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::interop {
namespace {

static_assert(std::endian::native == std::endian::little,
              "Arrow interoperability currently requires a little-endian host");

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status io_error(std::string message) {
  return common::Status{common::StatusCode::kIoError, std::move(message)};
}

[[nodiscard]] common::Status
arrow_error(const std::string_view operation, const ::arrow::Status& status,
            const common::StatusCode code = common::StatusCode::kInvalidArgument) {
  return common::Status{code, std::string{operation} + ": " + status.ToString()};
}

template <typename T>
[[nodiscard]] common::Result<T>
from_arrow(const std::string_view operation, ::arrow::Result<T> result,
           const common::StatusCode code = common::StatusCode::kInvalidArgument) {
  if (!result.ok()) {
    return common::make_unexpected(arrow_error(operation, result.status(), code));
  }
  return std::move(result).ValueUnsafe();
}

[[nodiscard]] common::Status
from_arrow_status(const std::string_view operation, const ::arrow::Status& status,
                  const common::StatusCode code = common::StatusCode::kInvalidArgument) {
  return status.ok() ? common::Status::ok() : arrow_error(operation, status, code);
}

[[nodiscard]] common::Result<std::int64_t> checked_arrow_size(const std::size_t size,
                                                              const std::string_view label) {
  if (size > static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max())) {
    return common::make_unexpected(invalid(std::string{label} + " exceeds Arrow's size domain"));
  }
  return static_cast<std::int64_t>(size);
}

[[nodiscard]] common::Result<std::shared_ptr<::arrow::Buffer>>
copy_buffer(const common::ByteView bytes) {
  const common::Result<std::int64_t> size = checked_arrow_size(bytes.size(), "column buffer");
  if (!size.has_value()) {
    return common::make_unexpected(size.error());
  }
  common::Result<std::unique_ptr<::arrow::Buffer>> allocated =
      from_arrow("allocate Arrow column buffer", ::arrow::AllocateBuffer(*size),
                 common::StatusCode::kResourceExhausted);
  if (!allocated.has_value()) {
    return common::make_unexpected(allocated.error());
  }
  if (!bytes.empty()) {
    std::memcpy((*allocated)->mutable_data(), bytes.data(), bytes.size());
  }
  return std::shared_ptr<::arrow::Buffer>{std::move(*allocated)};
}

[[nodiscard]] std::shared_ptr<::arrow::DataType> arrow_type(const schema::LogicalType& type) {
  using schema::LogicalTypeKind;
  switch (type.kind()) {
  case LogicalTypeKind::kBool:
    return ::arrow::boolean();
  case LogicalTypeKind::kInt8:
    return ::arrow::int8();
  case LogicalTypeKind::kInt16:
    return ::arrow::int16();
  case LogicalTypeKind::kInt32:
    return ::arrow::int32();
  case LogicalTypeKind::kInt64:
    return ::arrow::int64();
  case LogicalTypeKind::kUInt8:
    return ::arrow::uint8();
  case LogicalTypeKind::kUInt16:
    return ::arrow::uint16();
  case LogicalTypeKind::kUInt32:
    return ::arrow::uint32();
  case LogicalTypeKind::kUInt64:
    return ::arrow::uint64();
  case LogicalTypeKind::kFloat32:
    return ::arrow::float32();
  case LogicalTypeKind::kFloat64:
    return ::arrow::float64();
  case LogicalTypeKind::kDecimal:
    return ::arrow::decimal128(type.parameter_0(), type.parameter_1());
  case LogicalTypeKind::kTimestampNs:
    return ::arrow::timestamp(::arrow::TimeUnit::NANO);
  case LogicalTypeKind::kDate:
    return ::arrow::date32();
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
    return ::arrow::utf8();
  case LogicalTypeKind::kBinary:
    return ::arrow::binary();
  case LogicalTypeKind::kUuid:
    return ::arrow::fixed_size_binary(16);
  }
  return nullptr;
}

[[nodiscard]] common::Result<std::shared_ptr<::arrow::Array>>
to_arrow_array(const columnar::OwnedColumnVector& column) {
  const columnar::ColumnVectorView view = column.view();
  std::vector<std::shared_ptr<::arrow::Buffer>> buffers;
  if (view.nullable()) {
    common::Result<std::shared_ptr<::arrow::Buffer>> validity = copy_buffer(view.validity());
    if (!validity.has_value()) {
      return common::make_unexpected(validity.error());
    }
    buffers.push_back(std::move(*validity));
  } else {
    buffers.push_back(nullptr);
  }
  if (view.type().is_variable_width()) {
    common::Result<std::shared_ptr<::arrow::Buffer>> offsets = copy_buffer(view.offsets());
    common::Result<std::shared_ptr<::arrow::Buffer>> values = copy_buffer(view.values());
    if (!offsets.has_value()) {
      return common::make_unexpected(offsets.error());
    }
    if (!values.has_value()) {
      return common::make_unexpected(values.error());
    }
    buffers.push_back(std::move(*offsets));
    buffers.push_back(std::move(*values));
  } else {
    common::Result<std::shared_ptr<::arrow::Buffer>> values = copy_buffer(view.values());
    if (!values.has_value()) {
      return common::make_unexpected(values.error());
    }
    buffers.push_back(std::move(*values));
  }

  std::shared_ptr<::arrow::Array> array = ::arrow::MakeArray(::arrow::ArrayData::Make(
      arrow_type(view.type()), view.row_count(), std::move(buffers), view.null_count()));
  const common::Status validated =
      from_arrow_status("validate exported Arrow array", array->ValidateFull());
  if (!validated.is_ok()) {
    return common::make_unexpected(validated);
  }
  return array;
}

[[nodiscard]] common::Result<std::shared_ptr<::arrow::RecordBatch>>
to_record_batch(const columnar::OwnedColumnarBatch& batch) {
  std::vector<std::shared_ptr<::arrow::Field>> fields;
  std::vector<std::shared_ptr<::arrow::Array>> arrays;
  fields.reserve(batch.columns().size());
  arrays.reserve(batch.columns().size());
  for (std::size_t ordinal = 0; ordinal < batch.columns().size(); ++ordinal) {
    const schema::ColumnDefinition& definition = batch.schema().columns()[ordinal];
    std::shared_ptr<::arrow::KeyValueMetadata> metadata;
    if (definition.type().kind() == schema::LogicalTypeKind::kSymbol) {
      metadata = ::arrow::key_value_metadata({"chronos.logical_type"}, {"symbol"});
    } else if (definition.type().kind() == schema::LogicalTypeKind::kUuid) {
      metadata = ::arrow::key_value_metadata({"chronos.logical_type"}, {"uuid"});
    }
    fields.push_back(::arrow::field(definition.name(), arrow_type(definition.type()),
                                    definition.nullable(), std::move(metadata)));
    common::Result<std::shared_ptr<::arrow::Array>> array =
        to_arrow_array(batch.columns()[ordinal]);
    if (!array.has_value()) {
      return common::make_unexpected(array.error());
    }
    arrays.push_back(std::move(*array));
  }
  std::shared_ptr<::arrow::RecordBatch> result = ::arrow::RecordBatch::Make(
      ::arrow::schema(std::move(fields)), batch.row_count(), std::move(arrays));
  const common::Status validated =
      from_arrow_status("validate exported Arrow record batch", result->ValidateFull());
  if (!validated.is_ok()) {
    return common::make_unexpected(validated);
  }
  return result;
}

void set_bit(std::vector<std::byte>& bytes, const std::uint32_t row) noexcept {
  const std::size_t index = static_cast<std::size_t>(row) / 8U;
  const auto mask = static_cast<std::uint8_t>(1U << (row % 8U));
  bytes[index] |= static_cast<std::byte>(mask);
}

void append_u32_le(std::vector<std::byte>& bytes, const std::uint32_t value) {
  for (std::size_t index = 0; index < sizeof(value); ++index) {
    bytes.push_back(static_cast<std::byte>((value >> (index * 8U)) & 0xffU));
  }
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

[[nodiscard]] common::Status validate_external_schema(const ::arrow::Schema& external,
                                                      const schema::TableSchema& target) {
  if (external.num_fields() != static_cast<int>(target.columns().size())) {
    return invalid("external column count does not match the target schema");
  }
  for (std::size_t ordinal = 0; ordinal < target.columns().size(); ++ordinal) {
    const schema::ColumnDefinition& definition = target.columns()[ordinal];
    const std::shared_ptr<::arrow::Field>& field = external.field(static_cast<int>(ordinal));
    if (field->name() != definition.name()) {
      return invalid("external field name does not match target column at ordinal " +
                     std::to_string(ordinal));
    }
    if (field->nullable() != definition.nullable()) {
      return invalid("external nullability does not match target column " + definition.name());
    }
    if (!field->type()->Equals(*arrow_type(definition.type()))) {
      return invalid("external logical type does not match target column " + definition.name());
    }
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<columnar::OwnedColumnVector>
from_arrow_array(const std::shared_ptr<::arrow::Array>& array,
                 const schema::ColumnDefinition& definition) {
  const std::uint32_t rows = static_cast<std::uint32_t>(array->length());
  const std::uint32_t null_count = static_cast<std::uint32_t>(array->null_count());
  columnar::ColumnVectorBuffers buffers;
  if (definition.nullable()) {
    buffers.validity.resize(columnar::bitmap_size(rows), std::byte{0});
    for (std::uint32_t row = 0; row < rows; ++row) {
      if (!array->IsNull(row)) {
        set_bit(buffers.validity, row);
      }
    }
  }

  if (definition.type().kind() == schema::LogicalTypeKind::kBool) {
    buffers.values.resize(columnar::bitmap_size(rows), std::byte{0});
    const auto& values = static_cast<const ::arrow::BooleanArray&>(*array);
    for (std::uint32_t row = 0; row < rows; ++row) {
      if (!array->IsNull(row) && values.Value(row)) {
        set_bit(buffers.values, row);
      }
    }
  } else if (definition.type().is_variable_width()) {
    append_u32_le(buffers.offsets, 0U);
    const auto& values = static_cast<const ::arrow::BinaryArray&>(*array);
    for (std::uint32_t row = 0; row < rows; ++row) {
      if (!array->IsNull(row)) {
        const std::string_view value = values.GetView(row);
        if (value.size() > std::numeric_limits<std::uint32_t>::max() - buffers.values.size()) {
          return common::make_unexpected(
              invalid("external variable-width values exceed ChronosDB's UINT32 offset domain"));
        }
        const auto* begin = reinterpret_cast<const std::byte*>(value.data());
        buffers.values.insert(buffers.values.end(), begin, begin + value.size());
      }
      append_u32_le(buffers.offsets, static_cast<std::uint32_t>(buffers.values.size()));
    }
  } else {
    const std::size_t width = fixed_width(definition.type().kind());
    if (width > 0U && rows > std::numeric_limits<std::size_t>::max() / width) {
      return common::make_unexpected(invalid("external fixed-width column size overflows"));
    }
    buffers.values.resize(static_cast<std::size_t>(rows) * width, std::byte{0});
    const std::shared_ptr<::arrow::Buffer>& values = array->data()->buffers[1];
    for (std::uint32_t row = 0; row < rows; ++row) {
      if (!array->IsNull(row)) {
        const std::int64_t source_row = array->offset() + row;
        const std::size_t source_offset = static_cast<std::size_t>(source_row) * width;
        std::memcpy(buffers.values.data() + static_cast<std::size_t>(row) * width,
                    values->data() + source_offset, width);
      }
    }
  }

  return columnar::OwnedColumnVector::create(
      columnar::ColumnVectorMetadata{.column_id = definition.id(),
                                     .type = definition.type(),
                                     .nullable = definition.nullable(),
                                     .row_count = rows,
                                     .null_count = null_count},
      std::move(buffers));
}

[[nodiscard]] common::Result<columnar::OwnedColumnarBatch>
from_table(std::shared_ptr<::arrow::Table> table,
           std::shared_ptr<const schema::TableSchema> target_schema,
           const columnar::ColumnarBatchLimits limits) {
  if (target_schema == nullptr) {
    return common::make_unexpected(invalid("target schema must not be null"));
  }
  common::Status status = from_arrow_status("validate imported Arrow table", table->ValidateFull());
  if (!status.is_ok()) {
    return common::make_unexpected(status);
  }
  status = validate_external_schema(*table->schema(), *target_schema);
  if (!status.is_ok()) {
    return common::make_unexpected(status);
  }
  if (table->num_rows() <= 0) {
    return common::make_unexpected(invalid("external table must contain at least one row"));
  }
  if (table->num_rows() > limits.max_rows ||
      table->num_rows() > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(invalid("external row count exceeds the configured limit"));
  }
  if (static_cast<std::size_t>(table->num_columns()) > limits.max_columns) {
    return common::make_unexpected(invalid("external column count exceeds the configured limit"));
  }
  common::Result<std::int64_t> decoded_bytes =
      from_arrow("measure decoded Arrow buffers", ::arrow::util::ReferencedBufferSize(*table));
  if (!decoded_bytes.has_value()) {
    return common::make_unexpected(decoded_bytes.error());
  }
  if (*decoded_bytes < 0 || static_cast<std::uint64_t>(*decoded_bytes) > limits.max_buffer_bytes ||
      static_cast<std::uint64_t>(*decoded_bytes) > limits.max_retained_buffer_bytes) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "decoded external buffers exceed the configured canonical batch limits"});
  }

  common::Result<std::shared_ptr<::arrow::Table>> combined = from_arrow(
      "combine imported Arrow chunks", table->CombineChunks(::arrow::default_memory_pool()),
      common::StatusCode::kResourceExhausted);
  if (!combined.has_value()) {
    return common::make_unexpected(combined.error());
  }
  std::vector<columnar::OwnedColumnVector> columns;
  columns.reserve(target_schema->columns().size());
  for (std::size_t ordinal = 0; ordinal < target_schema->columns().size(); ++ordinal) {
    const std::shared_ptr<::arrow::ChunkedArray>& chunks =
        (*combined)->column(static_cast<int>(ordinal));
    if (chunks->num_chunks() != 1) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal,
                         "Arrow chunk combination did not produce one nonempty chunk"});
    }
    common::Result<columnar::OwnedColumnVector> column =
        from_arrow_array(chunks->chunk(0), target_schema->columns()[ordinal]);
    if (!column.has_value()) {
      return common::make_unexpected(column.error());
    }
    columns.push_back(std::move(*column));
  }
  return columnar::OwnedColumnarBatch::create(std::move(target_schema), std::move(columns), limits);
}

[[nodiscard]] common::Status check_input_file(const std::filesystem::path& path,
                                              const ImportLimits limits) {
  std::error_code error;
  const std::uintmax_t size = std::filesystem::file_size(path, error);
  if (error) {
    return io_error("cannot stat interop input " + path.string() + ": " + error.message());
  }
  if (size > limits.max_file_bytes) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "interop input exceeds the configured file-byte limit"};
  }
  return common::Status::ok();
}

[[nodiscard]] std::filesystem::path temporary_path(const std::filesystem::path& final_path) {
  static std::atomic<std::uint64_t> sequence{0U};
  return final_path.string() + ".chronos.tmp." + std::to_string(::getpid()) + "." +
         std::to_string(sequence.fetch_add(1U, std::memory_order_relaxed));
}

class TemporaryExport {
public:
  explicit TemporaryExport(std::filesystem::path final_path)
      : final_(std::move(final_path)), temporary_(temporary_path(final_)) {}
  TemporaryExport(const TemporaryExport&) = delete;
  TemporaryExport& operator=(const TemporaryExport&) = delete;
  ~TemporaryExport() {
    if (!installed_) {
      std::error_code ignored;
      std::filesystem::remove(temporary_, ignored);
    }
  }
  [[nodiscard]] const std::filesystem::path& temporary() const noexcept {
    return temporary_;
  }
  [[nodiscard]] common::Status install() {
    std::error_code error;
    std::filesystem::rename(temporary_, final_, error);
    if (error) {
      return io_error("cannot install exported file " + final_.string() + ": " + error.message());
    }
    installed_ = true;
    return common::Status::ok();
  }

private:
  std::filesystem::path final_;
  std::filesystem::path temporary_;
  bool installed_{false};
};

} // namespace

common::Status write_arrow_ipc_file(const columnar::OwnedColumnarBatch& batch,
                                    const std::filesystem::path& path) {
  common::Result<std::shared_ptr<::arrow::RecordBatch>> record = to_record_batch(batch);
  if (!record.has_value()) {
    return record.error();
  }
  TemporaryExport output{path};
  common::Result<std::shared_ptr<::arrow::io::FileOutputStream>> stream =
      from_arrow("open Arrow IPC output", ::arrow::io::FileOutputStream::Open(output.temporary()),
                 common::StatusCode::kIoError);
  if (!stream.has_value()) {
    return stream.error();
  }
  common::Result<std::shared_ptr<::arrow::ipc::RecordBatchWriter>> writer = from_arrow(
      "create Arrow IPC writer", ::arrow::ipc::MakeFileWriter(*stream, (*record)->schema()),
      common::StatusCode::kIoError);
  if (!writer.has_value()) {
    return writer.error();
  }
  common::Status status = from_arrow_status(
      "write Arrow IPC batch", (*writer)->WriteRecordBatch(**record), common::StatusCode::kIoError);
  if (status.is_ok()) {
    status = from_arrow_status("close Arrow IPC writer", (*writer)->Close(),
                               common::StatusCode::kIoError);
  }
  if (status.is_ok()) {
    status = from_arrow_status("close Arrow IPC output", (*stream)->Close(),
                               common::StatusCode::kIoError);
  }
  return status.is_ok() ? output.install() : status;
}

common::Result<columnar::OwnedColumnarBatch>
read_arrow_ipc_file(const std::filesystem::path& path,
                    std::shared_ptr<const schema::TableSchema> target_schema,
                    const ImportLimits limits) {
  const common::Status checked = check_input_file(path, limits);
  if (!checked.is_ok()) {
    return common::make_unexpected(checked);
  }
  common::Result<std::shared_ptr<::arrow::io::ReadableFile>> input = from_arrow(
      "open Arrow IPC input", ::arrow::io::ReadableFile::Open(path), common::StatusCode::kIoError);
  if (!input.has_value()) {
    return common::make_unexpected(input.error());
  }
  common::Result<std::shared_ptr<::arrow::ipc::RecordBatchFileReader>> reader =
      from_arrow("open Arrow IPC reader", ::arrow::ipc::RecordBatchFileReader::Open(*input));
  if (!reader.has_value()) {
    return common::make_unexpected(reader.error());
  }
  common::Result<std::int64_t> row_count =
      from_arrow("count Arrow IPC rows", (*reader)->CountRows());
  if (!row_count.has_value()) {
    return common::make_unexpected(row_count.error());
  }
  if (*row_count <= 0 || *row_count > limits.batch.max_rows ||
      *row_count > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(invalid("Arrow IPC row count is outside configured limits"));
  }
  std::vector<std::shared_ptr<::arrow::RecordBatch>> batches;
  batches.reserve(static_cast<std::size_t>((*reader)->num_record_batches()));
  for (int index = 0; index < (*reader)->num_record_batches(); ++index) {
    common::Result<std::shared_ptr<::arrow::RecordBatch>> batch =
        from_arrow("read Arrow IPC record batch", (*reader)->ReadRecordBatch(index));
    if (!batch.has_value()) {
      return common::make_unexpected(batch.error());
    }
    batches.push_back(std::move(*batch));
  }
  common::Result<std::shared_ptr<::arrow::Table>> table = from_arrow(
      "assemble Arrow IPC table", ::arrow::Table::FromRecordBatches((*reader)->schema(), batches));
  if (!table.has_value()) {
    return common::make_unexpected(table.error());
  }
  return from_table(std::move(*table), std::move(target_schema), limits.batch);
}

common::Status write_parquet_file(const columnar::OwnedColumnarBatch& batch,
                                  const std::filesystem::path& path) {
  common::Result<std::shared_ptr<::arrow::RecordBatch>> record = to_record_batch(batch);
  if (!record.has_value()) {
    return record.error();
  }
  common::Result<std::shared_ptr<::arrow::Table>> table =
      from_arrow("assemble Parquet export table", ::arrow::Table::FromRecordBatches({*record}));
  if (!table.has_value()) {
    return table.error();
  }
  TemporaryExport output{path};
  common::Result<std::shared_ptr<::arrow::io::FileOutputStream>> stream =
      from_arrow("open Parquet output", ::arrow::io::FileOutputStream::Open(output.temporary()),
                 common::StatusCode::kIoError);
  if (!stream.has_value()) {
    return stream.error();
  }
  common::Status status = from_arrow_status(
      "write Parquet table",
      ::parquet::arrow::WriteTable(**table, ::arrow::default_memory_pool(), *stream,
                                   static_cast<std::int64_t>(batch.row_count())),
      common::StatusCode::kIoError);
  if (status.is_ok()) {
    status =
        from_arrow_status("close Parquet output", (*stream)->Close(), common::StatusCode::kIoError);
  }
  return status.is_ok() ? output.install() : status;
}

common::Result<columnar::OwnedColumnarBatch>
read_parquet_file(const std::filesystem::path& path,
                  std::shared_ptr<const schema::TableSchema> target_schema,
                  const ImportLimits limits) {
  const common::Status checked = check_input_file(path, limits);
  if (!checked.is_ok()) {
    return common::make_unexpected(checked);
  }
  common::Result<std::shared_ptr<::arrow::io::ReadableFile>> input = from_arrow(
      "open Parquet input", ::arrow::io::ReadableFile::Open(path), common::StatusCode::kIoError);
  if (!input.has_value()) {
    return common::make_unexpected(input.error());
  }
  common::Result<std::unique_ptr<::parquet::arrow::FileReader>> reader = from_arrow(
      "open Parquet reader", ::parquet::arrow::OpenFile(*input, ::arrow::default_memory_pool()));
  if (!reader.has_value()) {
    return common::make_unexpected(reader.error());
  }
  common::Result<std::shared_ptr<::arrow::Table>> table =
      from_arrow("read Parquet table", (*reader)->ReadTable());
  if (!table.has_value()) {
    return common::make_unexpected(table.error());
  }
  return from_table(std::move(*table), std::move(target_schema), limits.batch);
}

} // namespace chronos::interop
