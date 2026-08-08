#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

usage() {
    echo "Usage: $0 [version]" >&2
    echo "With no version, increments the minor version and resets the patch version." >&2
    echo "Example: $0 0.3.0" >&2
    exit 2
}

[ "$#" -le 1 ] || usage

package_version() {
    awk '
        /^\[package\]$/ { in_package = 1; next }
        /^\[/ { in_package = 0 }
        in_package && /^version = "/ {
            value = $0
            sub(/^version = "/, "", value)
            sub(/".*$/, "", value)
            print value
            exit
        }
    ' "$1"
}

OLD_VERSION=$(package_version "${ROOT_DIR}/janus-lib/Cargo.toml")
[ -n "${OLD_VERSION}" ] || {
    echo "Could not read the current Janus version" >&2
    exit 1
}

if [ "$#" -eq 1 ]; then
    NEW_VERSION=$1
else
    NEW_VERSION=$(printf '%s\n' "${OLD_VERSION}" | awk -F. '{ print $1 "." ($2 + 1) ".0" }')
fi

if ! printf '%s\n' "${NEW_VERSION}" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$'; then
    echo "Invalid semantic version: ${NEW_VERSION}" >&2
    exit 2
fi

for manifest in \
    "${ROOT_DIR}/janus-lib/Cargo.toml" \
    "${ROOT_DIR}/janus-runner/Cargo.toml" \
    "${ROOT_DIR}/janus-server/Cargo.toml"
do
    version=$(package_version "${manifest}")
    if [ "${version}" != "${OLD_VERSION}" ]; then
        echo "Package versions are inconsistent: ${manifest} has ${version}, expected ${OLD_VERSION}" >&2
        exit 1
    fi
done

TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/janus-version.XXXXXX")
trap 'rm -rf "${TMP_DIR}"' EXIT HUP INT TERM

stage_package_manifest() {
    source_file=$1
    output_file="${TMP_DIR}/$(printf '%s' "${source_file}" | sed 's|/|_|g')"
    awk -v new_version="${NEW_VERSION}" '
        BEGIN { in_package = 0; replacements = 0 }
        /^\[package\]$/ { in_package = 1 }
        /^\[/ && $0 != "[package]" { in_package = 0 }
        in_package && /^version = "/ {
            print "version = \"" new_version "\""
            replacements++
            next
        }
        { print }
        END { if (replacements != 1) exit 1 }
    ' "${source_file}" > "${output_file}"
    cp "${output_file}" "${source_file}"
}

stage_lockfile() {
    source_file=$1
    expected=$2
    output_file="${TMP_DIR}/$(printf '%s' "${source_file}" | sed 's|/|_|g')"
    awk -v new_version="${NEW_VERSION}" -v expected="${expected}" '
        BEGIN { janus_package = 0; replacements = 0 }
        /^\[\[package\]\]$/ { janus_package = 0 }
        /^name = "janus-(lib|runner|server)"$/ { janus_package = 1 }
        janus_package && /^version = "/ {
            print "version = \"" new_version "\""
            replacements++
            janus_package = 0
            next
        }
        { print }
        END { if (replacements != expected) exit 1 }
    ' "${source_file}" > "${output_file}"
    cp "${output_file}" "${source_file}"
}

stage_version_line() {
    source_file=$1
    output_file="${TMP_DIR}/$(printf '%s' "${source_file}" | sed 's|/|_|g')"
    awk -v new_version="${NEW_VERSION}" '
        BEGIN { replacements = 0 }
        /^version: "/ {
            print "version: \"" new_version "\""
            replacements++
            next
        }
        { print }
        END { if (replacements != 1) exit 1 }
    ' "${source_file}" > "${output_file}"
    cp "${output_file}" "${source_file}"
}

stage_readme() {
    source_file=$1
    package_name=$2
    output_file="${TMP_DIR}/$(printf '%s' "${source_file}" | sed 's|/|_|g')"
    awk -v package_name="${package_name}" -v old_version="${OLD_VERSION}" -v new_version="${NEW_VERSION}" '
        BEGIN { replacements = 0 }
        {
            old_name = package_name "-" old_version ".pkg"
            new_name = package_name "-" new_version ".pkg"
            position = index($0, old_name)
            if (position) {
                $0 = substr($0, 1, position - 1) new_name substr($0, position + length(old_name))
                replacements++
            }
            print
        }
        END { if (replacements < 1) exit 1 }
    ' "${source_file}" > "${output_file}"
    cp "${output_file}" "${source_file}"
}

stage_package_manifest "${ROOT_DIR}/janus-lib/Cargo.toml"
stage_package_manifest "${ROOT_DIR}/janus-runner/Cargo.toml"
stage_package_manifest "${ROOT_DIR}/janus-server/Cargo.toml"
stage_lockfile "${ROOT_DIR}/Cargo.lock" 3
stage_lockfile "${ROOT_DIR}/janus-runner/Cargo.lock" 1
stage_version_line "${ROOT_DIR}/janus-runner/packaging/freebsd/+MANIFEST"
stage_version_line "${ROOT_DIR}/janus-server/packaging/freebsd/+MANIFEST"
stage_readme "${ROOT_DIR}/janus-runner/packaging/freebsd/README.md" "janus-runner"
stage_readme "${ROOT_DIR}/janus-server/packaging/freebsd/README.md" "janus-server"

cargo metadata --manifest-path "${ROOT_DIR}/Cargo.toml" --locked --no-deps --format-version 1 >/dev/null

echo "Bumped Janus from ${OLD_VERSION} to ${NEW_VERSION}"
