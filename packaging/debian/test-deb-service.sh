#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || ! -f "$1" ]]; then
    echo "usage: $0 PACKAGE.deb" >&2
    exit 2
fi

package=$1
configuration=/etc/default/briskdb
state_directory=/var/lib/briskdb

dpkg-deb --info "$package"
dpkg-deb --contents "$package" | grep -F ./lib/systemd/system/briskdb.service
dpkg-deb --contents "$package" | grep -F ./etc/default/briskdb

sudo env DEBIAN_FRONTEND=noninteractive dpkg -i "$package"
sudo systemd-analyze verify /lib/systemd/system/briskdb.service

for attempt in {1..30}; do
    if curl --fail --silent --show-error http://127.0.0.1:7654/admin >/dev/null; then
        break
    fi
    if [[ "$attempt" -eq 30 ]]; then
        sudo systemctl status briskdb.service --no-pager || true
        sudo journalctl -u briskdb.service --no-pager || true
        exit 1
    fi
    sleep 1
done

sudo systemctl is-enabled --quiet briskdb.service
sudo systemctl is-active --quiet briskdb.service
getent passwd briskdb | grep -F /usr/sbin/nologin
test "$(stat -c '%U:%G:%a' "$state_directory")" = briskdb:briskdb:750
test "$(stat -c '%U:%G:%a' "$configuration")" = root:root:644
dpkg-query -W -f='${Conffiles}\n' briskdb | grep -F ' /etc/default/briskdb '
sudo journalctl -u briskdb.service --no-pager | grep -F 'BriskDB is ready'

printf '\n# package smoke-test local configuration\n' | sudo tee -a "$configuration" >/dev/null
sudo touch "$state_directory/package-smoke-state"
sudo env DEBIAN_FRONTEND=noninteractive dpkg -i "$package"
grep -F '# package smoke-test local configuration' "$configuration"
test -f "$state_directory/package-smoke-state"
sudo systemctl is-active --quiet briskdb.service

sudo env DEBIAN_FRONTEND=noninteractive dpkg -r briskdb
test -f "$configuration"
test -f "$state_directory/package-smoke-state"
if systemctl is-active --quiet briskdb.service; then
    echo "briskdb.service remained active after package removal" >&2
    exit 1
fi
