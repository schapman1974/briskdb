#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || ! -f "$1" ]]; then
    echo "usage: $0 PACKAGE.deb" >&2
    exit 2
fi

package=$1
configuration=/etc/default/briskdb
state_directory=/var/lib/briskdb

wait_for_service() {
    smoke_directory=$(mktemp -d)
    trap 'rm -rf "$smoke_directory"' RETURN
    for attempt in {1..30}; do
        if sudo systemctl is-active --quiet briskdb.service \
            && curl --fail --silent --show-error \
                --dump-header "$smoke_directory/data.headers" \
                --output "$smoke_directory/data.json" \
                http://127.0.0.1:7654/v1 \
            && curl --fail --silent --show-error \
                --dump-header "$smoke_directory/ready.headers" \
                --output "$smoke_directory/ready.json" \
                http://127.0.0.1:7655/v1/ready \
            && curl --fail --silent --show-error \
                --dump-header "$smoke_directory/stream.headers" \
                --output "$smoke_directory/stream.ndjson" \
                --header 'content-type: application/json' \
                --data '{"sql":"SELECT 1 AS value","shard_key":"debian-smoke"}' \
                http://127.0.0.1:7654/v1/query/stream \
            && curl --fail --silent --show-error http://127.0.0.1:7655/admin >/dev/null \
            && python3 - "$smoke_directory" <<'PY'
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
for name in ("data", "ready", "stream"):
    headers = (root / f"{name}.headers").read_text(encoding="utf-8")
    values = [
        line.split(":", 1)[1].strip()
        for line in headers.splitlines()
        if line.split(":", 1)[0].lower() == "briskdb-request-id"
    ]
    assert len(values) == 1 and re.fullmatch(r"[0-9a-f]{32}", values[0])
    assert values[0] != "0" * 32
stream_headers = (root / "stream.headers").read_text(encoding="utf-8").lower()
assert "content-type: application/x-ndjson; charset=utf-8" in stream_headers
records = [json.loads(line) for line in (root / "stream.ndjson").read_text().splitlines()]
assert [record["kind"] for record in records] == ["meta", "row", "complete"]
assert records[1]["values"] == [1]
assert records[2] == {"kind": "complete", "rows": 1}
PY
        then
            trap - RETURN
            rm -rf "$smoke_directory"
            return 0
        fi
        sleep 1
    done

    sudo systemctl status briskdb.service --no-pager || true
    sudo journalctl -u briskdb.service --no-pager || true
    trap - RETURN
    rm -rf "$smoke_directory"
    return 1
}

dpkg-deb --info "$package"
dpkg-deb --contents "$package" | grep -F ./lib/systemd/system/briskdb.service
dpkg-deb --contents "$package" | grep -F ./etc/default/briskdb
dpkg-deb --contents "$package" | grep -F ./usr/share/doc/briskdb/docs/openapi-v1.json

sudo env DEBIAN_FRONTEND=noninteractive dpkg -i "$package"
sudo systemd-analyze verify /lib/systemd/system/briskdb.service
wait_for_service

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
sudo test -f "$state_directory/package-smoke-state"
wait_for_service

sudo env DEBIAN_FRONTEND=noninteractive dpkg -r briskdb
test -f "$configuration"
sudo test -f "$state_directory/package-smoke-state"
if systemctl is-active --quiet briskdb.service; then
    echo "briskdb.service remained active after package removal" >&2
    exit 1
fi
