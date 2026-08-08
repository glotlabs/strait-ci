#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
PKG_DIR="${ROOT_DIR}/janus-runner/packaging/freebsd"
PREFIX="/opt/janus-runner"
VERSION="${VERSION:-$(awk -F\" '/^version = / { print $2; exit }' "${ROOT_DIR}/janus-runner/Cargo.toml")}"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/target/freebsd-runner-pkg}"
FAKE_ROOT="${BUILD_DIR}/root"
METADATA_DIR="${BUILD_DIR}/metadata"
OUT_DIR="${OUT_DIR:-${BUILD_DIR}/packages}"
PLIST="${PKG_DIR}/pkg-plist"
BINARY="${JANUS_RUNNER_BIN:-${ROOT_DIR}/target/release/janus-runner}"

if [ "${SKIP_BUILD:-0}" != "1" ]; then
    cargo build --release --locked -p janus-runner
fi

if [ ! -x "${BINARY}" ]; then
    echo "janus-runner binary not found or not executable: ${BINARY}" >&2
    echo "Set JANUS_RUNNER_BIN=/path/to/janus-runner or run without SKIP_BUILD=1." >&2
    exit 1
fi

rm -rf "${FAKE_ROOT}" "${METADATA_DIR}" "${OUT_DIR}"
install -d -m 0755 "${FAKE_ROOT}${PREFIX}/bin"
install -d -m 0755 "${FAKE_ROOT}${PREFIX}/etc/rc.d"
install -d -m 0755 "${FAKE_ROOT}${PREFIX}/jobs"
install -d -m 0755 "${FAKE_ROOT}${PREFIX}/manifests"
install -d -m 0755 "${FAKE_ROOT}${PREFIX}/share/examples/janus-runner"
install -d -m 0750 "${FAKE_ROOT}${PREFIX}/var"
install -d -m 0750 "${FAKE_ROOT}${PREFIX}/run"
install -d -m 0750 "${FAKE_ROOT}${PREFIX}/log"

install -m 0755 "${BINARY}" "${FAKE_ROOT}${PREFIX}/bin/janus-runner"
install -m 0640 "${ROOT_DIR}/janus-runner/runner.example.toml" "${FAKE_ROOT}${PREFIX}/etc/runner.toml.sample"
install -m 0755 "${PKG_DIR}/files/janus_runner.rc" "${FAKE_ROOT}${PREFIX}/etc/rc.d/janus_runner"
install -m 0644 "${ROOT_DIR}/janus-runner/manifests/build-app.example.toml" "${FAKE_ROOT}${PREFIX}/share/examples/janus-runner/build-app.toml.sample"
install -m 0755 "${ROOT_DIR}/janus-runner/jobs/build-app.sh" "${FAKE_ROOT}${PREFIX}/share/examples/janus-runner/build-app.sh.sample"
install -m 0644 "${ROOT_DIR}/janus-runner/manifests/build-freebsd-packages.example.toml" "${FAKE_ROOT}${PREFIX}/share/examples/janus-runner/build-freebsd-packages.toml.sample"
install -m 0755 "${ROOT_DIR}/janus-runner/jobs/build-freebsd-packages.sh" "${FAKE_ROOT}${PREFIX}/share/examples/janus-runner/build-freebsd-packages.sh.sample"

install -d -m 0755 "${METADATA_DIR}" "${OUT_DIR}"
sed "s/^version:.*/version: \"${VERSION}\"/" "${PKG_DIR}/+MANIFEST" > "${METADATA_DIR}/+MANIFEST"
install -m 0755 "${PKG_DIR}/+PRE_INSTALL" "${METADATA_DIR}/+PRE_INSTALL"
install -m 0755 "${PKG_DIR}/+POST_INSTALL" "${METADATA_DIR}/+POST_INSTALL"
install -m 0644 "${PKG_DIR}/+DISPLAY" "${METADATA_DIR}/+DISPLAY"

pkg create -r "${FAKE_ROOT}" -m "${METADATA_DIR}" -p "${PLIST}" -o "${OUT_DIR}"

echo "Package written to ${OUT_DIR}"
echo "Install with: pkg install ${OUT_DIR}/janus-runner-${VERSION}.pkg"
