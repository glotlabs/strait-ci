#!/bin/sh
set -eu

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf '%s\n' "Missing required command: $1" >&2
    exit 1
  fi
}

require_env() {
  if [ -z "${1:-}" ]; then
    printf '%s\n' "Missing required environment variable: $2" >&2
    exit 1
  fi
}

require_env "${INPUT_SOURCE:-}" INPUT_SOURCE
require_env "${JANUS_WORKDIR:-}" JANUS_WORKDIR
require_env "${JANUS_OUTPUT_DIR:-}" JANUS_OUTPUT_DIR

require_command cargo
require_command pkg
require_command tar

if [ ! -f "$INPUT_SOURCE" ]; then
  printf '%s\n' "Source artifact not found: $INPUT_SOURCE" >&2
  exit 1
fi

SOURCE_DIR="$JANUS_WORKDIR/source"
PACKAGE_BUILD_DIR="$JANUS_WORKDIR/freebsd-packages"
SERVER_PACKAGE_DIR="$PACKAGE_BUILD_DIR/server/packages"
RUNNER_PACKAGE_DIR="$PACKAGE_BUILD_DIR/runner/packages"

mkdir -p "$SOURCE_DIR"

printf '%s\n' "Extracting source archive..."
tar -xzf "$INPUT_SOURCE" -C "$SOURCE_DIR"

if [ ! -f "$SOURCE_DIR/Cargo.toml" ]; then
  printf '%s\n' "Source archive does not contain Cargo.toml at its root." >&2
  exit 1
fi

printf '%s\n' "Building janus-server and janus-runner..."
(
  cd "$SOURCE_DIR"
  cargo build --release --locked -p janus-server -p janus-runner
)

printf '%s\n' "Creating janus-server package..."
(
  cd "$SOURCE_DIR"
  SKIP_BUILD=1 \
    BUILD_DIR="$PACKAGE_BUILD_DIR/server" \
    janus-server/packaging/freebsd/build-pkg.sh
)

printf '%s\n' "Creating janus-runner package..."
(
  cd "$SOURCE_DIR"
  SKIP_BUILD=1 \
    BUILD_DIR="$PACKAGE_BUILD_DIR/runner" \
    janus-runner/packaging/freebsd/build-pkg.sh
)

SERVER_VERSION=$(awk -F\" '/^version = / { print $2; exit }' "$SOURCE_DIR/janus-server/Cargo.toml")
RUNNER_VERSION=$(awk -F\" '/^version = / { print $2; exit }' "$SOURCE_DIR/janus-runner/Cargo.toml")
SERVER_PACKAGE="$SERVER_PACKAGE_DIR/janus-server-$SERVER_VERSION.pkg"
RUNNER_PACKAGE="$RUNNER_PACKAGE_DIR/janus-runner-$RUNNER_VERSION.pkg"

if [ ! -f "$SERVER_PACKAGE" ]; then
  printf '%s\n' "Expected package not found: $SERVER_PACKAGE" >&2
  exit 1
fi

if [ ! -f "$RUNNER_PACKAGE" ]; then
  printf '%s\n' "Expected package not found: $RUNNER_PACKAGE" >&2
  exit 1
fi

install -m 0644 "$SERVER_PACKAGE" "$JANUS_OUTPUT_DIR/janus-server.pkg"
install -m 0644 "$RUNNER_PACKAGE" "$JANUS_OUTPUT_DIR/janus-runner.pkg"

printf '%s\n' "Created janus-server.pkg and janus-runner.pkg"
