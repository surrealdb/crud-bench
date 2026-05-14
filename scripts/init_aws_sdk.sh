#!/usr/bin/env bash
set -euo pipefail

# Builds AWS C++ SDK from source for the `clouddb` benchmark.
# Install location: <repo>/.do-not-commit/aws-sdk-install
#
# Mirrors the build used by the SurrealDB enterprise project so the AWS C++
# dependency for `surrealdb-rocksdb` cloud builds is consistent across repos.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DO_NOT_COMMIT="$PROJECT_DIR/.do-not-commit"

AWS_SDK_DIR="$DO_NOT_COMMIT/aws-sdk-cpp"
AWS_SDK_INSTALL="$DO_NOT_COMMIT/aws-sdk-install"

# Idempotent: skip if already built.
if [ -f "$AWS_SDK_INSTALL/lib/libaws-cpp-sdk-s3.a" ] || \
   [ -f "$AWS_SDK_INSTALL/lib/libaws-cpp-sdk-s3.so" ] || \
   [ -f "$AWS_SDK_INSTALL/lib64/libaws-cpp-sdk-s3.a" ] || \
   [ -f "$AWS_SDK_INSTALL/lib64/libaws-cpp-sdk-s3.so" ]; then
  echo "=== AWS C++ SDK already built at $AWS_SDK_INSTALL ==="
else
  echo "=== Building AWS C++ SDK from source ==="

  if [ ! -d "$AWS_SDK_DIR" ]; then
    echo "  Cloning aws-sdk-cpp..."
    git clone --recurse-submodules --depth 1 \
      https://github.com/aws/aws-sdk-cpp.git "$AWS_SDK_DIR"
  fi

  AWS_SDK_BUILD="$DO_NOT_COMMIT/aws-sdk-build"
  mkdir -p "$AWS_SDK_BUILD"
  echo "  Configuring..."
  cmake -S "$AWS_SDK_DIR" -B "$AWS_SDK_BUILD" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$AWS_SDK_INSTALL" \
    -DBUILD_ONLY="s3;transfer;core" \
    -DBUILD_SHARED_LIBS=OFF \
    -DENABLE_TESTING=OFF \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DMINIMIZE_SIZE=ON

  echo "  Building (this may take several minutes)..."
  if command -v nproc &>/dev/null; then
    NJOBS=$(nproc)
  else
    NJOBS=$(sysctl -n hw.ncpu)
  fi
  cmake --build "$AWS_SDK_BUILD" --parallel "$NJOBS"

  echo "  Installing..."
  cmake --install "$AWS_SDK_BUILD"

  echo "=== AWS C++ SDK installed at $AWS_SDK_INSTALL ==="
fi

echo "=== init_aws_sdk.sh complete ==="
echo ""
echo "AWS SDK install dir: $AWS_SDK_INSTALL"
echo "Set AWS_SDK_INSTALL_DIR=$AWS_SDK_INSTALL for build.rs"
