# Building ChronosDB

ChronosDB's implemented code currently consists of the Phase 1A common version library, its
command-line probe, and build verification. Engine components described elsewhere remain planned.
The reference production platform is Linux x86-64; this portable common code also supports modern
macOS, including Apple silicon.

## Prerequisites

- CMake 3.25 or newer
- Ninja 1.10 or newer
- a C++23 compiler: GCC 13+, Clang 17+, or a current AppleClang
- Git, so CMake can fetch pinned test dependencies and optionally record revision metadata
- Python only as required by CMake/GoogleTest test discovery
- clang-format and clang-tidy from a reasonably current LLVM release (17+ recommended)

The first test or benchmark configuration needs network access to fetch its pinned dependency.
Subsequent configurations reuse CMake's build-tree dependency checkout. No dependency source is
vendored into this repository.

## Linux setup

On Ubuntu 24.04, install the distribution packages for `cmake`, `ninja-build`, `g++`, `clang`,
`clang-format`, and `clang-tidy`. GCC and Clang are both CI-supported. Select a compiler before the
first configure when needed:

```sh
CC=gcc CXX=g++ cmake --preset debug
CC=clang CXX=clang++ cmake --preset debug
```

Do not switch compilers inside an existing build directory; use a fresh preset build directory.

## macOS setup

Install Xcode Command Line Tools and CMake/Ninja. Homebrew users can run:

```sh
xcode-select --install
brew install cmake ninja llvm
```

Homebrew's LLVM tools may not be on `PATH`. If so, set `CLANG_FORMAT` and `CLANG_TIDY` to the
corresponding executables under `$(brew --prefix llvm)/bin`. AppleClang builds the portable common
targets. Future server, direct-I/O, and reactor components may require Linux and will be guarded by
explicit platform checks rather than weakened portable interfaces.

## Configure, build, and test

The presets are Ninja-based and create independent directories under `build/`:

```sh
cmake --preset dev
cmake --build --preset dev
ctest --preset dev
```

Equivalent wrappers can be run from any directory:

```sh
scripts/configure.sh dev
scripts/build.sh dev
scripts/test.sh dev
```

`debug` is the CI-oriented debug configuration. `release` enables the compiler's normal optimized
release mode; it does not add host-specific architecture flags.

```sh
cmake --preset release
cmake --build --preset release
ctest --preset release
```

The resulting proof executable reports configure-time build metadata:

```sh
build/dev/chronosctl version
build/dev/chronosctl version --json
```

Install to a staging prefix with `cmake --install build/release --prefix <directory>`. This installs
`chronosctl`, the common library and public header, and a CMake package exporting
`chronos::common`.

## Sanitizers

ASan and UBSan intentionally share one Debug preset; TSan is isolated because the runtimes are
incompatible and diagnose different failure classes:

```sh
cmake --preset asan-ubsan
cmake --build --preset asan-ubsan
ctest --preset asan-ubsan

cmake --preset tsan
cmake --build --preset tsan
ctest --preset tsan
```

The flags attach only to ChronosDB targets. A requested unsupported compiler fails configuration.
Sanitizer support also depends on the compiler runtime and host OS; Linux Clang is the CI reference.

## Troubleshooting

- `Could not find Ninja`: install Ninja and rerun configure.
- a FetchContent clone fails: confirm Git/network access, then rerun the same configure command.
- `clang-tidy was not found`: install it or set `CLANG_TIDY` to its executable before running the
  lint script.
- stale compiler or option results: remove only the affected `build/<preset>` directory and
  reconfigure. The scripts never delete build trees.
- no Git metadata: builds outside a checkout remain valid and report Git fields as unavailable.
