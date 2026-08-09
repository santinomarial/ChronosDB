#include "chronos/live/subscription_plan.hpp"

#include "chronos/common/bytes.hpp"
#include "chronos/common/status.hpp"
#include "chronos/ingest/sha256.hpp"
#include "chronos/query/ast.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

constexpr std::array<std::byte, 29U> kFingerprintDomain{
    std::byte{'c'}, std::byte{'h'}, std::byte{'r'}, std::byte{'o'}, std::byte{'n'}, std::byte{'o'},
    std::byte{'s'}, std::byte{'-'}, std::byte{'s'}, std::byte{'u'}, std::byte{'b'}, std::byte{'s'},
    std::byte{'c'}, std::byte{'r'}, std::byte{'i'}, std::byte{'p'}, std::byte{'t'}, std::byte{'i'},
    std::byte{'o'}, std::byte{'n'}, std::byte{'-'}, std::byte{'p'}, std::byte{'l'}, std::byte{'a'},
    std::byte{'n'}, std::byte{'-'}, std::byte{'v'}, std::byte{'1'}, std::byte{0}};

[[nodiscard]] query::SqlDiagnostic diagnostic(const query::SqlDiagnosticCode code,
                                              const query::SourceSpan span,
                                              common::Status status) noexcept {
  return {code, span, std::move(status)};
}

[[nodiscard]] std::array<std::byte, sizeof(std::uint64_t)>
encode_u64(const std::uint64_t value) noexcept {
  std::array<std::byte, sizeof(std::uint64_t)> bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index)
    bytes[index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
  return bytes;
}

[[nodiscard]] common::Result<PlanFingerprint> fingerprint(std::string_view sql,
                                                          const query::BoundSqlSelect& bound) {
  try {
    const auto sql_size = encode_u64(static_cast<std::uint64_t>(sql.size()));
    const auto source_count = encode_u64(static_cast<std::uint64_t>(bound.sources().size()));
    std::vector<std::array<std::byte, sizeof(std::uint64_t)>> versions;
    versions.reserve(bound.sources().size());
    std::vector<common::ByteView> fragments;
    fragments.reserve(4U + bound.sources().size() * 3U);
    fragments.push_back(kFingerprintDomain);
    fragments.push_back(sql_size);
    fragments.push_back(source_count);
    fragments.push_back(std::as_bytes(std::span{sql.data(), sql.size()}));
    for (const query::BoundSqlSource& source : bound.sources()) {
      fragments.push_back(source.schema_ptr()->table_id().bytes());
      fragments.push_back(source.schema_ptr()->schema_id().bytes());
      versions.push_back(encode_u64(source.schema_ptr()->version().value()));
      fragments.push_back(versions.back());
    }
    auto digest = ingest::sha256(fragments);
    if (!digest.has_value())
      return common::make_unexpected(digest.error());
    return digest->bytes();
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "subscription fingerprint allocation failed"});
  }
}

} // namespace

PreparedSubscriptionPlan::PreparedSubscriptionPlan(
    PlanFingerprint fingerprint, std::shared_ptr<const schema::TableSchema> schema,
    query::PhysicalPipelinePlan physical_plan,
    std::vector<SnapshotSubscriptionColumn> columns) noexcept
    : fingerprint_(fingerprint), schema_(std::move(schema)),
      physical_plan_(std::move(physical_plan)), columns_(std::move(columns)) {}

const PlanFingerprint& PreparedSubscriptionPlan::fingerprint() const noexcept {
  return fingerprint_;
}

const std::shared_ptr<const schema::TableSchema>&
PreparedSubscriptionPlan::schema_ptr() const noexcept {
  return schema_;
}

const query::PhysicalPipelinePlan& PreparedSubscriptionPlan::physical_plan() const noexcept {
  return physical_plan_;
}

const std::vector<SnapshotSubscriptionColumn>& PreparedSubscriptionPlan::columns() const noexcept {
  return columns_;
}

SubscriptionSource
PreparedSubscriptionPlan::source(const common::Uuid database_id, const schema::TabletId tablet_id,
                                 const wal::WalId wal_id,
                                 const ResumeTokenMacKey token_key) const noexcept {
  return {database_id,  schema_->table_id(),  tablet_id,          wal_id,
          fingerprint_, schema_->schema_id(), schema_->version(), token_key};
}

SubscriptionRequest
PreparedSubscriptionPlan::request(const common::Uuid subscription_id) const noexcept {
  return {subscription_id, fingerprint_, schema_->schema_id(), schema_->version()};
}

query::SqlResult<PreparedSubscriptionPlan>
prepare_subscription_plan(const std::string_view sql,
                          std::shared_ptr<const query::QueryCatalogSnapshot> catalog,
                          const SubscriptionPlanLimits& limits) {
  auto parsed = query::parse_sql_v1_select(sql, limits.parser);
  if (!parsed.has_value())
    return std::unexpected(parsed.error());
  const query::SourceSpan span = parsed->span();
  if (parsed->mode() != query::SqlSelectMode::kSubscribe) {
    return std::unexpected(
        diagnostic(query::SqlDiagnosticCode::kUnsupportedSyntax, span,
                   common::Status{common::StatusCode::kNotSupported,
                                  "subscription request SQL must use SUBSCRIBE SELECT"}));
  }
  if (parsed->system_time().has_value() || !parsed->asof_joins().empty()) {
    return std::unexpected(diagnostic(
        query::SqlDiagnosticCode::kUnsupportedSyntax, span,
        common::Status{common::StatusCode::kNotSupported,
                       "live subscription planning currently requires one current source"}));
  }
  auto bound = query::bind_sql_v1_select(std::move(*parsed), std::move(catalog), limits.binder);
  if (!bound.has_value())
    return std::unexpected(bound.error());
  auto physical = query::lower_bound_sql_select(*bound, limits.lowering);
  if (!physical.has_value())
    return std::unexpected(physical.error());
  auto identity = fingerprint(sql, *bound);
  if (!identity.has_value())
    return std::unexpected(
        diagnostic(query::SqlDiagnosticCode::kExecutionFailure, span, identity.error()));
  try {
    std::vector<SnapshotSubscriptionColumn> columns;
    columns.reserve(bound->outputs().size());
    for (const query::BoundOutputColumn& output : bound->outputs())
      columns.push_back({output.name, output.type, output.nullable});
    return PreparedSubscriptionPlan{*identity, bound->sources().front().schema_ptr(),
                                    std::move(*physical), std::move(columns)};
  } catch (const std::bad_alloc&) {
    return std::unexpected(diagnostic(query::SqlDiagnosticCode::kResourceLimit, span,
                                      common::Status{common::StatusCode::kResourceExhausted,
                                                     "subscription plan allocation failed"}));
  }
}

} // namespace chronos::live
