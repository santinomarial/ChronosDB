# Project Foundation

Phase 1A establishes enough real code to prove how ChronosDB will be built without pretending that
the database engine exists. `chronos_common` exposes stable version/build information,
`chronosctl version` renders it for people or automation, and unit tests exercise the same public
interface an eventual consumer will use.

## Why target-scoped CMake

CMake properties propagate along target relationships. ChronosDB therefore assigns the C++23
requirement, include paths, warnings, sanitizers, and static analysis to each owned target. This
makes the dependency graph explicit and prevents strict local flags from leaking into GoogleTest or
Google Benchmark. The build-tree alias and installed export are both `chronos::common`, so consumers
use one name while the concrete target remains `chronos_common`.

The current graph is:

```text
chronosctl ───────────► chronos::common
chronos_common_tests ─► chronos::common + GTest::gtest_main
chronos_common_benchmarks (optional) ─► chronos::common + benchmark::benchmark_main
```

## Reproducibility choices

GoogleTest and optional Google Benchmark are pinned by full commit hashes. A moving branch could
silently change behavior or disappear; an immutable revision makes dependency review and failure
reproduction practical. Configure-time metadata deliberately omits timestamps. CMake rewrites the
generated header only when content changes, avoiding gratuitous recompilation.

There is no default `-march=native`. That flag tailors binaries to the build host and can create
instructions unavailable on the deployment host, undermining portable artifacts and reproducible
comparison. A future measured, deployment-specific optimization may use an explicit profile.

## Why sanitizer configurations are separate

ASan finds invalid memory access and lifetime errors; UBSan detects selected language undefined
behavior, and their runtimes can coexist. TSan instruments synchronization and must run alone. It
also has higher overhead and platform/runtime constraints. Separate build directories prevent
instrumentation from contaminating normal binaries and make every result attributable to a known
configuration.

## Portable core and Linux-specific code

Common value types, codecs, and algorithms should compile on Linux and macOS. Future production
server code may depend on Linux facilities such as epoll or direct-I/O behavior. Such code will live
in an explicitly named target with a clear Linux configure guard. Portable interfaces will not fake
Linux semantics merely to make every target build on macOS.

## Extending the graph

For a future library, add its real sources with `add_library`, expose only necessary include paths
and links, require `cxx_std_23`, and invoke the Chronos warning, sanitizer, and analysis helpers. Add
an alias in the `chronos::` namespace and install it only when its public contract is stable.

For an executable, link the narrowest namespaced libraries and attach the same owned-target helpers.
For a test, create a focused executable linked to the subject plus `GTest::gtest_main`, then register
it with `gtest_discover_tests`. For a meaningful microbenchmark, place the target behind
`CHRONOS_BUILD_BENCHMARKS`, link Google Benchmark, consume or preserve its result so the optimizer
cannot remove the operation, and document the measurement method. A benchmark that only returns a
constant or sleeps is not evidence and should not be added.

## Interview questions

- Why do usage requirements belong on targets instead of directory-global compiler flags?
- What reproducibility problems do full dependency pins solve, and what do they not solve?
- Why can ASan and UBSan run together while TSan is isolated?
- How does `configure_file` avoid unnecessary rebuilds, and why are timestamps omitted?
- How would you keep a Linux-only reactor from leaking into portable common code?
- When should a benchmark become a CI gate, and what environment evidence would be required?
- What is the difference between a build-tree alias and an exported installed target?
