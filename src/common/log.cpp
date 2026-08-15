#include "chronos/common/log.hpp"

#include "chronos/common/checked_math.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace chronos::common {
namespace {

constexpr std::array<std::string_view, 5U> kReservedFieldNames{"timestamp", "severity", "component",
                                                               "event", "message"};

[[nodiscard]] Status invalid(std::string message) {
  return {StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] bool is_continuation(const unsigned char value) noexcept {
  return value >= 0x80U && value <= 0xbfU;
}

[[nodiscard]] bool valid_field_name(const std::string_view name) noexcept {
  const auto first = [](const char value) {
    return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') || value == '_';
  };
  const auto later = [&](const char value) {
    return first(value) || (value >= '0' && value <= '9') || value == '.' || value == '-';
  };
  return !name.empty() && first(name.front()) && std::ranges::all_of(name.substr(1U), later);
}

[[nodiscard]] std::size_t valid_utf8_sequence_size(const std::string_view text,
                                                   const std::size_t offset) noexcept {
  const auto byte = [&](const std::size_t index) {
    return static_cast<unsigned char>(text[offset + index]);
  };
  const unsigned char first = byte(0U);
  const std::size_t remaining = text.size() - offset;
  if (first <= 0x7fU)
    return 1U;
  if (first >= 0xc2U && first <= 0xdfU)
    return remaining >= 2U && is_continuation(byte(1U)) ? 2U : 0U;
  if (first == 0xe0U) {
    return remaining >= 3U && byte(1U) >= 0xa0U && byte(1U) <= 0xbfU && is_continuation(byte(2U))
               ? 3U
               : 0U;
  }
  if ((first >= 0xe1U && first <= 0xecU) || (first >= 0xeeU && first <= 0xefU)) {
    return remaining >= 3U && is_continuation(byte(1U)) && is_continuation(byte(2U)) ? 3U : 0U;
  }
  if (first == 0xedU) {
    return remaining >= 3U && byte(1U) >= 0x80U && byte(1U) <= 0x9fU && is_continuation(byte(2U))
               ? 3U
               : 0U;
  }
  if (first == 0xf0U) {
    return remaining >= 4U && byte(1U) >= 0x90U && byte(1U) <= 0xbfU && is_continuation(byte(2U)) &&
                   is_continuation(byte(3U))
               ? 4U
               : 0U;
  }
  if (first >= 0xf1U && first <= 0xf3U) {
    return remaining >= 4U && is_continuation(byte(1U)) && is_continuation(byte(2U)) &&
                   is_continuation(byte(3U))
               ? 4U
               : 0U;
  }
  if (first == 0xf4U) {
    return remaining >= 4U && byte(1U) >= 0x80U && byte(1U) <= 0x8fU && is_continuation(byte(2U)) &&
                   is_continuation(byte(3U))
               ? 4U
               : 0U;
  }
  return 0U;
}

void append_hex_escape(std::string& output, const unsigned char value) {
  constexpr std::string_view digits{"0123456789abcdef"};
  output.append("\\u00");
  output.push_back(digits[(value >> 4U) & 0x0fU]);
  output.push_back(digits[value & 0x0fU]);
}

void append_json_string(std::string& output, const std::string_view text) {
  output.push_back('"');
  std::size_t offset = 0U;
  while (offset < text.size()) {
    const unsigned char value = static_cast<unsigned char>(text[offset]);
    if (value >= 0x80U) {
      const std::size_t sequence_size = valid_utf8_sequence_size(text, offset);
      if (sequence_size == 0U) {
        output.append("\\ufffd");
        ++offset;
      } else {
        output.append(text.substr(offset, sequence_size));
        offset += sequence_size;
      }
      continue;
    }
    switch (value) {
    case '"':
      output.append("\\\"");
      break;
    case '\\':
      output.append("\\\\");
      break;
    case '\b':
      output.append("\\b");
      break;
    case '\f':
      output.append("\\f");
      break;
    case '\n':
      output.append("\\n");
      break;
    case '\r':
      output.append("\\r");
      break;
    case '\t':
      output.append("\\t");
      break;
    default:
      if (value < 0x20U)
        append_hex_escape(output, value);
      else
        output.push_back(static_cast<char>(value));
      break;
    }
    ++offset;
  }
  output.push_back('"');
}

[[nodiscard]] Result<std::string>
format_timestamp(const std::chrono::sys_time<std::chrono::milliseconds> timestamp) {
  using namespace std::chrono;
  const sys_days day = floor<days>(timestamp);
  const year_month_day date{day};
  if (!date.ok())
    return make_unexpected(invalid("log timestamp calendar date is invalid"));
  const int year = static_cast<int>(date.year());
  if (year < 1 || year > 9999)
    return make_unexpected(invalid("log timestamp year is outside RFC 3339 bounds"));
  const hh_mm_ss time{timestamp - day};
  std::array<char, 25U> formatted{};
  const int written = std::snprintf(
      formatted.data(), formatted.size(), "%04d-%02u-%02uT%02lld:%02lld:%02lld.%03lldZ", year,
      static_cast<unsigned>(date.month()), static_cast<unsigned>(date.day()),
      static_cast<long long>(time.hours().count()), static_cast<long long>(time.minutes().count()),
      static_cast<long long>(time.seconds().count()),
      static_cast<long long>(time.subseconds().count()));
  if (written != static_cast<int>(formatted.size() - 1U))
    return make_unexpected(Status{StatusCode::kInternal, "log timestamp formatting failed"});
  return std::string{formatted.data(), static_cast<std::size_t>(written)};
}

[[nodiscard]] Status validate_record(const LogRecord& record) {
  if (record.component.empty() || record.component.size() > kMaximumLogComponentBytes)
    return invalid("log component length is outside bounds");
  if (record.event.empty() || record.event.size() > kMaximumLogEventBytes)
    return invalid("log event length is outside bounds");
  if (record.message.size() > kMaximumLogMessageBytes)
    return invalid("log message length exceeds the bound");
  if (record.fields.size() > kMaximumLogFields)
    return invalid("log field count exceeds the bound");

  std::size_t total = record.component.size();
  const auto with_event = checked_add(total, record.event.size());
  const auto with_message =
      with_event.has_value() ? checked_add(*with_event, record.message.size()) : std::nullopt;
  if (!with_message.has_value())
    return invalid("log record text length overflows");
  total = *with_message;
  for (std::size_t index = 0U; index < record.fields.size(); ++index) {
    const LogField& field = record.fields[index];
    if (field.name.size() > kMaximumLogFieldNameBytes || !valid_field_name(field.name))
      return invalid("log field name is outside the supported syntax or length bound");
    if (field.value.size() > kMaximumLogFieldValueBytes)
      return invalid("log field value length exceeds the bound");
    for (const std::string_view reserved : kReservedFieldNames) {
      if (field.name == reserved)
        return invalid("log field shadows a built-in field");
    }
    for (std::size_t prior = 0U; prior < index; ++prior) {
      if (field.name == record.fields[prior].name)
        return invalid("log field names must be unique");
    }
    const auto with_name = checked_add(total, field.name.size());
    const auto with_value =
        with_name.has_value() ? checked_add(*with_name, field.value.size()) : std::nullopt;
    if (!with_value.has_value() || *with_value > kMaximumLogRecordTextBytes)
      return invalid("log record text exceeds the bound");
    total = *with_value;
  }
  return Status::ok();
}

void append_field(std::string& output, const LogField field) {
  output.push_back(',');
  append_json_string(output, field.name);
  output.push_back(':');
  append_json_string(output, field.value);
}

} // namespace

std::string_view log_severity_name(const LogSeverity severity) noexcept {
  switch (severity) {
  case LogSeverity::kDebug:
    return "DEBUG";
  case LogSeverity::kInfo:
    return "INFO";
  case LogSeverity::kWarning:
    return "WARNING";
  case LogSeverity::kError:
    return "ERROR";
  case LogSeverity::kCritical:
    return "CRITICAL";
  }
  return "UNKNOWN";
}

Result<std::string> encode_json_log(const LogRecord& record) {
  try {
    const Status valid = validate_record(record);
    if (!valid.is_ok())
      return make_unexpected(valid);
    auto timestamp = format_timestamp(record.timestamp);
    if (!timestamp.has_value())
      return make_unexpected(timestamp.error());
    std::string output;
    output.reserve(256U + record.component.size() + record.event.size() + record.message.size());
    output.append("{\"timestamp\":");
    append_json_string(output, *timestamp);
    append_field(output, {.name = "severity", .value = log_severity_name(record.severity)});
    append_field(output, {.name = "component", .value = record.component});
    append_field(output, {.name = "event", .value = record.event});
    append_field(output, {.name = "message", .value = record.message});
    for (const LogField& field : record.fields)
      append_field(output, field);
    output.push_back('}');
    return output;
  } catch (const std::bad_alloc&) {
    return make_unexpected(
        Status{StatusCode::kResourceExhausted, "structured log allocation failed"});
  } catch (const std::length_error&) {
    return make_unexpected(invalid("structured log encoded size exceeds process limits"));
  }
}

Status write_json_log(std::FILE* const output, const LogRecord& record) {
  if (output == nullptr)
    return invalid("structured log output is null");
  auto encoded = encode_json_log(record);
  if (!encoded.has_value())
    return encoded.error();
  static std::mutex write_mutex;
  try {
    const std::lock_guard lock{write_mutex};
    const std::size_t written = std::fwrite(encoded->data(), 1U, encoded->size(), output);
    const int newline = written == encoded->size() ? std::fputc('\n', output) : EOF;
    if (written != encoded->size() || newline == EOF || std::fflush(output) != 0)
      return {StatusCode::kIoError, "structured log write failed"};
    return Status::ok();
  } catch (const std::system_error&) {
    return {StatusCode::kInternal, "structured log synchronization failed"};
  }
}

} // namespace chronos::common
