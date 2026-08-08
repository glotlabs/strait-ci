# FreeBSD pkg helper

This directory contains helper files for creating a `janus-server` FreeBSD
package that installs under `/opt/janus-server`.

Build the package on FreeBSD from the repository root:

```sh
janus-server/packaging/freebsd/build-pkg.sh
```

The script builds `janus-server`, prepares a fake root, copies package metadata,
and runs:

```sh
pkg create -r target/freebsd-pkg/root -m target/freebsd-pkg/metadata -p janus-server/packaging/freebsd/pkg-plist -o target/freebsd-pkg/packages
```

To package an already-built binary:

```sh
SKIP_BUILD=1 JANUS_SERVER_BIN=/path/to/janus-server janus-server/packaging/freebsd/build-pkg.sh
```

Install the generated package:

```sh
pkg install target/freebsd-pkg/packages/janus-server-0.5.0.pkg
```

After install, edit the config and initialize runner signing keys. The package
creates `/opt/janus-server/etc/server.toml` from `server.toml.sample` if it does
not already exist.

```sh
vi /opt/janus-server/etc/server.toml
chown root:janus /opt/janus-server/etc/server.toml
chmod 0640 /opt/janus-server/etc/server.toml
/opt/janus-server/bin/janus-server admin runner-key init --config /opt/janus-server/etc/server.toml
chown -R janus:janus /opt/janus-server/var/db /opt/janus-server/var/keys
chown -R janus:git /opt/janus-server/var/repos
chmod 2770 /opt/janus-server/var/repos
```

The package installs the rc script at
`/opt/janus-server/etc/rc.d/janus_server`. To use it with normal
`service janus_server` discovery, add that directory to `local_startup` or
symlink it into `/usr/local/etc/rc.d`.

For Git SSH access, add this block to `sshd_config`:

```sshconfig
Match User git
    PubkeyAuthentication yes
    PasswordAuthentication no
    X11Forwarding no
    AllowTcpForwarding no
    PermitTTY no
    ForceCommand /opt/janus-server/libexec/janus-git-ssh
```

Install Git SSH keys into:

```text
/opt/janus-server/home/git/.ssh/authorized_keys
```
