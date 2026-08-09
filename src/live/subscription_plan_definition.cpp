#include "chronos/live/subscription_plan_definition.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <new>
#include <optional>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'S'},
                                           std::byte{'U'}, std::byte{'B'}, std::byte{'P'},
                                           std::byte{'D'}, std::byte{'1'}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return {common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status unsupported(std::string message) {
  return {common::StatusCode::kNotSupported, std::move(message)};
}

[[nodiscard]] bool valid_limits(const SubscriptionPlanDefinitionLimits& limits) noexcept {
  return limits.maximum_definition_bytes >=
             kSubscriptionPlanDefinitionHeaderSize + kSubscriptionPlanDefinitionTrailerSize + 1U &&
         limits.maximum_definition_bytes <= kMaximumSubscriptionPlanDefinitionSize &&
         limits.maximum_sql_bytes != 0U &&
         limits.maximum_sql_bytes <= limits.maximum_definition_bytes -
                                         kSubscriptionPlanDefinitionHeaderSize -
                                         kSubscriptionPlanDefinitionTrailerSize;
}

[[nodiscard]] common::Result<std::size_t>
encoded_size(const SubscriptionPlanDefinition& definition,
             const SubscriptionPlanDefinitionLimits& limits) {
  if (definition.database_id.is_nil() || definition.table_id.uuid().is_nil() ||
      definition.schema_id.uuid().is_nil() || definition.schema_version.value() == 0U ||
      definition.sql.empty())
    return common::make_unexpected(invalid("subscription plan definition identity is invalid"));
  if (definition.sql.size() > limits.maximum_sql_bytes)
    return common::make_unexpected(exhausted("subscription plan SQL exceeds its byte limit"));
  auto with_sql = common::checked_add(kSubscriptionPlanDefinitionHeaderSize, definition.sql.size());
  auto total = with_sql.has_value()
                   ? common::checked_add(*with_sql, kSubscriptionPlanDefinitionTrailerSize)
                   : std::nullopt;
  if (!total.has_value() || *total > limits.maximum_definition_bytes)
    return common::make_unexpected(exhausted("subscription plan definition size overflows"));
  return *total;
}

[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes,
                                     const std::size_t offset) noexcept {
  std::uint32_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[offset + index]))
             << (index * 8U);
  return value;
}

} // namespace

common::Result<std::vector<std::byte>>
encode_subscription_plan_definition_v1(const SubscriptionPlanDefinition& definition,
                                       const SubscriptionPlanDefinitionLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("subscription plan definition limits are invalid"));
  auto total = encoded_size(definition, limits);
  if (!total.has_value())
    return common::make_unexpected(total.error());
  try {
    std::vector<std::byte> bytes(*total, std::byte{0});
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kMagic);
    if (status.is_ok())
      status = writer.write_u16_le(kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(kSubscriptionPlanDefinitionHeaderSize);
    if (status.is_ok())
      status = writer.write_u64_le(bytes.size());
    if (status.is_ok())
      status = writer.write_u64_le(definition.sql.size());
    if (status.is_ok())
      status = writer.write_exact(definition.database_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(definition.table_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(definition.schema_id.bytes());
    if (status.is_ok())
      status = writer.write_u64_le(definition.schema_version.value());
    if (status.is_ok())
      status = writer.write_exact(definition.plan_fingerprint);
    if (status.is_ok())
      status = writer.zero_fill(8U);
    if (status.is_ok())
      status = writer.write_exact(
          std::as_bytes(std::span{definition.sql.data(), definition.sql.size()}));
    if (!status.is_ok() || writer.remaining() != kSubscriptionPlanDefinitionTrailerSize)
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "subscription plan definition layout mismatch"});
    status = writer.write_u32_le(common::crc32c(
        common::ByteView{bytes}.first(bytes.size() - kSubscriptionPlanDefinitionTrailerSize)));
    if (!status.is_ok() || !writer.full())
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "subscription plan definition trailer mismatch"});
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription plan encoding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("subscription plan exceeds container limits"));
  }
}

common::Result<SubscriptionPlanDefinition>
decode_subscription_plan_definition_v1(const common::ByteView bytes,
                                       const SubscriptionPlanDefinitionLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("subscription plan definition limits are invalid"));
  if (bytes.size() <
          kSubscriptionPlanDefinitionHeaderSize + kSubscriptionPlanDefinitionTrailerSize + 1U ||
      bytes.size() > limits.maximum_definition_bytes)
    return common::make_unexpected(corruption("subscription plan definition size is invalid"));
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic))
    return common::make_unexpected(corruption("subscription plan definition magic is invalid"));
  common::ByteReader prefix{bytes};
  static_cast<void>(prefix.skip(kMagic.size()));
  auto major = prefix.read_u16_le();
  auto minor = prefix.read_u16_le();
  auto header_size = prefix.read_u32_le();
  auto total_size = prefix.read_u64_le();
  auto sql_size = prefix.read_u64_le();
  if (!major.has_value() || !minor.has_value() || !header_size.has_value() ||
      !total_size.has_value() || !sql_size.has_value())
    return common::make_unexpected(corruption("subscription plan definition header is truncated"));
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(
        unsupported("subscription plan definition version is unsupported"));
  if (*header_size != kSubscriptionPlanDefinitionHeaderSize || *total_size != bytes.size() ||
      *sql_size == 0U || *sql_size > limits.maximum_sql_bytes ||
      *sql_size != bytes.size() - kSubscriptionPlanDefinitionHeaderSize -
                       kSubscriptionPlanDefinitionTrailerSize)
    return common::make_unexpected(corruption("subscription plan definition header is invalid"));
  if (load_u32(bytes, bytes.size() - kSubscriptionPlanDefinitionTrailerSize) !=
      common::crc32c(bytes.first(bytes.size() - kSubscriptionPlanDefinitionTrailerSize)))
    return common::make_unexpected(corruption("subscription plan definition checksum is invalid"));

  try {
    common::ByteReader reader{bytes};
    static_cast<void>(reader.skip(32U));
    auto database = reader.read_exact(16U);
    auto table = reader.read_exact(16U);
    auto schema_bytes = reader.read_exact(16U);
    auto schema_version = reader.read_u64_le();
    auto fingerprint = reader.read_exact(PlanFingerprint{}.size());
    auto reserved = reader.read_exact(8U);
    auto sql = reader.read_exact(static_cast<std::size_t>(*sql_size));
    if (!database.has_value() || !table.has_value() || !schema_bytes.has_value() ||
        !schema_version.has_value() || !fingerprint.has_value() || !reserved.has_value() ||
        !sql.has_value() ||
        !std::ranges::all_of(*reserved,
                             [](const std::byte value) { return value == std::byte{0}; }) ||
        reader.remaining() != kSubscriptionPlanDefinitionTrailerSize)
      return common::make_unexpected(corruption("subscription plan definition body is invalid"));
    common::Uuid::Bytes database_array{};
    common::Uuid::Bytes table_array{};
    common::Uuid::Bytes schema_array{};
    PlanFingerprint fingerprint_array{};
    std::ranges::copy(*database, database_array.begin());
    std::ranges::copy(*table, table_array.begin());
    std::ranges::copy(*schema_bytes, schema_array.begin());
    std::ranges::copy(*fingerprint, fingerprint_array.begin());
    common::Uuid database_id{database_array};
    auto table_id = schema::TableId::from_bytes(table_array);
    auto schema_id = schema::SchemaId::from_bytes(schema_array);
    auto version = schema::SchemaVersion::from_value(*schema_version);
    if (database_id.is_nil() || !table_id.has_value() || !schema_id.has_value() ||
        !version.has_value())
      return common::make_unexpected(
          corruption("subscription plan definition identity is invalid"));
    // SQL is persisted as exact bytes; char and std::byte may alias object representations.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    std::string sql_text{reinterpret_cast<const char*>(sql->data()), sql->size()};
    SubscriptionPlanDefinition definition{database_id, *table_id,         *schema_id,
                                          *version,    fingerprint_array, std::move(sql_text)};
    auto size = encoded_size(definition, limits);
    if (!size.has_value() || *size != bytes.size())
      return common::make_unexpected(corruption("subscription plan definition is noncanonical"));
    return definition;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription plan decoding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("subscription plan exceeds container limits"));
  }
}

} // namespace chronos::live
