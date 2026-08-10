#include "query/timestamp_filter_kernel.hpp"

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <span>

#if (defined(__x86_64__) || defined(__i386__)) &&                                                  \
    (defined(__GNUC__) || defined(__clang__) || defined(__AVX2__))
#define CHRONOS_QUERY_HAS_AVX2_KERNEL 1
#include <immintrin.h>
#endif

#if defined(__aarch64__)
#define CHRONOS_QUERY_HAS_NEON_KERNEL 1
#include <arm_neon.h>
#endif

namespace chronos::query::detail {
namespace {

[[nodiscard]] std::int64_t read_i64_le(const common::ByteView values,
                                       const std::size_t row) noexcept {
  std::uint64_t bits = 0U;
  const std::size_t begin = row * sizeof(std::int64_t);
  for (std::size_t byte = 0U; byte < sizeof(std::int64_t); ++byte) {
    bits |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(values[begin + byte]))
            << (byte * 8U);
  }
  return std::bit_cast<std::int64_t>(bits);
}

[[nodiscard]] std::size_t compact_scalar(const common::ByteView values,
                                         const std::span<std::uint32_t> indices,
                                         const TimestampRangePredicate& predicate) noexcept {
  std::size_t output = 0U;
  for (std::size_t row = 0U; row < indices.size(); ++row) {
    if (predicate.matches(read_i64_le(values, row)))
      indices[output++] = static_cast<std::uint32_t>(row);
  }
  return output;
}

#if defined(CHRONOS_QUERY_HAS_AVX2_KERNEL)
[[nodiscard]]
#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx2")))
#endif
std::size_t compact_avx2(const common::ByteView values, const std::span<std::uint32_t> indices,
                         const TimestampRangePredicate& predicate) noexcept {
  std::size_t output = 0U;
  std::size_t row = 0U;
  const __m256i all = _mm256_set1_epi64x(-1);
  for (; indices.size() - row >= 4U; row += 4U) {
    __m256i timestamps;
    std::memcpy(&timestamps, values.data() + row * sizeof(std::int64_t), sizeof(timestamps));
    __m256i matches = all;
    if (predicate.lower.has_value()) {
      const __m256i lower = _mm256_set1_epi64x(predicate.lower->value);
      const __m256i accepted = predicate.lower->inclusive
                                   ? _mm256_xor_si256(_mm256_cmpgt_epi64(lower, timestamps), all)
                                   : _mm256_cmpgt_epi64(timestamps, lower);
      matches = _mm256_and_si256(matches, accepted);
    }
    if (predicate.upper.has_value()) {
      const __m256i upper = _mm256_set1_epi64x(predicate.upper->value);
      const __m256i accepted = predicate.upper->inclusive
                                   ? _mm256_xor_si256(_mm256_cmpgt_epi64(timestamps, upper), all)
                                   : _mm256_cmpgt_epi64(upper, timestamps);
      matches = _mm256_and_si256(matches, accepted);
    }
    std::uint32_t mask =
        static_cast<std::uint32_t>(_mm256_movemask_pd(_mm256_castsi256_pd(matches)));
    while (mask != 0U) {
      const std::uint32_t offset = std::countr_zero(mask);
      indices[output++] = static_cast<std::uint32_t>(row) + offset;
      mask &= mask - 1U;
    }
  }
  for (; row < indices.size(); ++row) {
    if (predicate.matches(read_i64_le(values, row)))
      indices[output++] = static_cast<std::uint32_t>(row);
  }
  return output;
}
#endif

#if defined(CHRONOS_QUERY_HAS_NEON_KERNEL)
[[nodiscard]] std::size_t compact_neon(const common::ByteView values,
                                       const std::span<std::uint32_t> indices,
                                       const TimestampRangePredicate& predicate) noexcept {
  std::size_t output = 0U;
  std::size_t row = 0U;
  alignas(16) std::uint64_t lanes[2]{};
  for (; indices.size() - row >= 2U; row += 2U) {
    alignas(16) std::int64_t input[2]{};
    std::memcpy(input, values.data() + row * sizeof(std::int64_t), sizeof(input));
    const int64x2_t timestamps = vld1q_s64(input);
    uint64x2_t matches = vdupq_n_u64(std::numeric_limits<std::uint64_t>::max());
    if (predicate.lower.has_value()) {
      const int64x2_t lower = vdupq_n_s64(predicate.lower->value);
      const uint64x2_t accepted =
          predicate.lower->inclusive ? vcgeq_s64(timestamps, lower) : vcgtq_s64(timestamps, lower);
      matches = vandq_u64(matches, accepted);
    }
    if (predicate.upper.has_value()) {
      const int64x2_t upper = vdupq_n_s64(predicate.upper->value);
      const uint64x2_t accepted =
          predicate.upper->inclusive ? vcleq_s64(timestamps, upper) : vcltq_s64(timestamps, upper);
      matches = vandq_u64(matches, accepted);
    }
    vst1q_u64(lanes, matches);
    for (std::uint32_t offset = 0U; offset < 2U; ++offset) {
      if (lanes[offset] != 0U)
        indices[output++] = static_cast<std::uint32_t>(row) + offset;
    }
  }
  for (; row < indices.size(); ++row) {
    if (predicate.matches(read_i64_le(values, row)))
      indices[output++] = static_cast<std::uint32_t>(row);
  }
  return output;
}
#endif

} // namespace

bool timestamp_filter_kernel_available(const TimestampFilterKernel kernel) noexcept {
  switch (kernel) {
  case TimestampFilterKernel::kScalar:
    return true;
  case TimestampFilterKernel::kAvx2:
#if defined(CHRONOS_QUERY_HAS_AVX2_KERNEL) && (defined(__GNUC__) || defined(__clang__))
    return std::endian::native == std::endian::little && __builtin_cpu_supports("avx2");
#elif defined(CHRONOS_QUERY_HAS_AVX2_KERNEL)
    return std::endian::native == std::endian::little;
#else
    return false;
#endif
  case TimestampFilterKernel::kNeon:
#if defined(CHRONOS_QUERY_HAS_NEON_KERNEL)
    return std::endian::native == std::endian::little;
#else
    return false;
#endif
  }
  return false;
}

TimestampFilterKernel preferred_timestamp_filter_kernel() noexcept {
  if (timestamp_filter_kernel_available(TimestampFilterKernel::kAvx2))
    return TimestampFilterKernel::kAvx2;
  if (timestamp_filter_kernel_available(TimestampFilterKernel::kNeon))
    return TimestampFilterKernel::kNeon;
  return TimestampFilterKernel::kScalar;
}

std::size_t compact_identity_timestamps(const common::ByteView values,
                                        const std::span<std::uint32_t> indices,
                                        const TimestampRangePredicate& predicate,
                                        const TimestampFilterKernel kernel) noexcept {
  if (values.size() % sizeof(std::int64_t) != 0 ||
      values.size() / sizeof(std::int64_t) != indices.size())
    return 0U;
  switch (kernel) {
  case TimestampFilterKernel::kAvx2:
#if defined(CHRONOS_QUERY_HAS_AVX2_KERNEL)
    if (timestamp_filter_kernel_available(kernel))
      return compact_avx2(values, indices, predicate);
#endif
    break;
  case TimestampFilterKernel::kNeon:
#if defined(CHRONOS_QUERY_HAS_NEON_KERNEL)
    if (timestamp_filter_kernel_available(kernel))
      return compact_neon(values, indices, predicate);
#endif
    break;
  case TimestampFilterKernel::kScalar:
    break;
  }
  return compact_scalar(values, indices, predicate);
}

} // namespace chronos::query::detail
