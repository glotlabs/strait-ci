# FreeBSD pkg helper for janus-runner

This directory contains helper files for creating a `janus-runner` FreeBSD
package that installs under `/opt/janus-runner`.

Build the package on FreeBSD from the repository root:

```sh
janus-runner/packaging/freebsd/build-pkg.sh
```

The script builds `janus-runner`, prepares a fake root, copies package metadata,
and runs:

```sh
pkg create -r target/freebsd-runner-pkg/root -m target/freebsd-runner-pkg/metadata -p janus-runner/packaging/freebsd/pkg-plist -o target/freebsd-runner-pkg/packages
```

To package an already-built binary:

```sh
SKIP_BUILD=1 JANUS_RUNNER_BIN=/path/to/janus-runner janus-runner/packaging/freebsd/build-pkg.sh
```

Install the generated package:

```sh
pkg install target/freebsd-runner-pkg/packages/janus-runner-0.5.0.pkg
```

After install, `/opt/janus-runner/etc/runner.toml` is created from
`runner.toml.sample` if it does not already exist. Edit it and paste the
`[[auth.servers]]` trust snippet from `janus-server admin runner-key show`.

Example manifest and script files are installed in:

```text
/opt/janus-runner/share/examples/janus-runner
```

Copy edited job manifests into `/opt/janus-runner/manifests` and executable job
scripts into `/opt/janus-runner/jobs`.
