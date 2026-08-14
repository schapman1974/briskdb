# Debian package and systemd service

BriskDB publishes preview Debian packages for Ubuntu 24.04 `amd64` and `arm64`.
They install a native systemd service with distribution-standard paths. This is
still an alpha deployment and does not change the security or compatibility
boundaries in the release notes.

## Direct release installation

Download the matching `.deb` and `SHA256SUMS` from the GitHub release. Verify
the checksum before installation, then install the local package:

```bash
sha256sum --check SHA256SUMS --ignore-missing
sudo apt install ./briskdb_0.1.0.alpha.5-1_amd64.deb
```

Use the `_arm64.deb` package on 64-bit ARM. `apt` resolves the package's runtime
dependencies and enables and starts `briskdb.service` during installation.
The filename uses a GitHub-safe dot, while package metadata uses the Debian
prerelease version `0.1.0~alpha.5-1` so it sorts before the final release.

## Filesystem contract

| Purpose | Path |
| --- | --- |
| Server | `/usr/bin/briskdb` |
| Import utility | `/usr/bin/briskdb-import` |
| Administrator configuration | `/etc/default/briskdb` |
| Vendor systemd unit | `/lib/systemd/system/briskdb.service` |
| Persistent database state | `/var/lib/briskdb` |
| Logs | systemd journal for `briskdb.service` |

The package creates a locked, unprivileged `briskdb` system account. systemd's
`StateDirectory=briskdb` keeps `/var/lib/briskdb` owned by that account with
mode `0750`. The service has no writable access to `/etc`, `/usr`, home
directories, devices, kernel controls, or other application state.

An embedded Rust or Python application may share the ready data root with the
service on this host, but it must run with the same effective `briskdb` UID.
The coordination sidecars are deliberately owner-only (`0600`), so adding an
application user to the `briskdb` group is not sufficient. Run the worker as
`briskdb`, configure the same path and shard count, and stop every peer before
schema changes, upgrades, imports, backups, or restores. See the
[multi-process contract](MULTIPROCESS.md).

`/etc/default/briskdb` is a dpkg conffile: administrator edits survive upgrades
and removals. Edit it with `sudoedit`, validate the loopback/security boundary,
then restart:

```bash
sudoedit /etc/default/briskdb
sudo systemctl restart briskdb
sudo systemctl status briskdb --no-pager
```

The default database location is `/var/lib/briskdb/data`. The HTTP listener is
`127.0.0.1:7654`, and PostgreSQL is disabled. The server rejects non-loopback
listeners while authentication and TLS are absent.

## Enable PostgreSQL queries

PostgreSQL queries require an initialized registered catalog. Stop the service
and import a standard SQLite database into a destination that does not already
exist. The source and plan must be readable by the `briskdb` system user:

```bash
sudo systemctl stop briskdb
sudo -u briskdb /usr/bin/briskdb-import \
  --source /path/readable-by-briskdb/source.sqlite \
  --data-dir /var/lib/briskdb/imported-data \
  --plan /path/readable-by-briskdb/import-plan.json \
  --shards 4
```

Set these values in `/etc/default/briskdb`, keeping the listener on loopback:

```text
BRISKDB_POSTGRES_LISTEN=127.0.0.1:5433
BRISKDB_DATA_DIR=/var/lib/briskdb/imported-data
BRISKDB_SHARDS=4
```

Restart the service, then use the `psql` commands in the
[PostgreSQL query quickstart](POSTGRES_QUICKSTART.md). The initial interface
supports one simple-query statement at a time; it has no authentication, TLS,
DDL, savepoints, cross-shard transactions, or full PostgreSQL compatibility yet.

## Logging

The service writes structured text to stdout/stderr; systemd sends both streams
to journald under the `briskdb` identifier. No separate `/var/log` file or
logrotate rule is necessary. Inspect or follow logs with:

```bash
sudo journalctl -u briskdb.service --since today
sudo journalctl -u briskdb.service -f
```

Set `RUST_LOG` in `/etc/default/briskdb` to adjust verbosity, then restart the
service. Journal retention, forwarding, and disk limits remain the host
administrator's system-wide journald policy.

## Upgrade, backup, and removal

Before upgrading, stop BriskDB and copy the complete data directory according
to [the offline backup procedure](OFFLINE_BACKUP.md). Package upgrades restart
an already active service after replacing the binary. They do not replace a
locally edited `/etc/default/briskdb`.

```bash
sudo systemctl stop briskdb
# Complete the documented stopped-server backup here.
sudo apt install ./briskdb_NEW_VERSION_ARCH.deb
```

Removing the package stops the service and removes the binaries and vendor unit,
but deliberately retains `/etc/default/briskdb`, the `briskdb` account, and
`/var/lib/briskdb`. Even purge does not delete database state. After a verified
backup, an administrator must explicitly remove unwanted state.

## Signed APT repository boundary

A `.deb` attached to a GitHub release is not by itself an APT repository. APT
also requires `Packages`, `Release`, and signed `InRelease` metadata plus a
published archive signing key. The intended next layer is:

1. retain release `.deb` files in `pool/`;
2. generate `dists/alpha/main/binary-amd64/Packages` and the ARM64 equivalent;
3. generate and sign the `alpha` suite metadata;
4. deploy that static tree through GitHub Pages; and
5. give users a `signed-by=` source entry scoped to the BriskDB keyring.

Publication must not begin until the offline private signing key, recovery copy,
expiry/rotation policy, and GitHub Actions secret access are explicitly chosen.
Do not publish an unsigned or `trusted=yes` repository as a shortcut.
