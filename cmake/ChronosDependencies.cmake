include_guard(GLOBAL)

include(FetchContent)

set(CHRONOS_GOOGLETEST_COMMIT "f8d7d77c06936315286eb55f8de22cd23c188571")
set(CHRONOS_GOOGLE_BENCHMARK_COMMIT "344117638c8ff7e239044fd0fa7085839fc03021")

function(chronos_fetch_googletest)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG "${CHRONOS_GOOGLETEST_COMMIT}"
    GIT_SHALLOW FALSE
  )
  FetchContent_MakeAvailable(googletest)
endfunction()

function(chronos_fetch_google_benchmark)
  set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
  set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
  set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
  FetchContent_Declare(
    google_benchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG "${CHRONOS_GOOGLE_BENCHMARK_COMMIT}"
    GIT_SHALLOW FALSE
  )
  FetchContent_MakeAvailable(google_benchmark)
endfunction()
