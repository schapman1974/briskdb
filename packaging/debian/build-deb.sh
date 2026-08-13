#!/usr/bin/env bash
set -euo pipefail

to_debian_version() {
    local version=$1
    if [[ "$version" == *-* ]]; then
        printf '%s~%s-1\n' "${version%%-*}" "${version#*-}"
    else
        printf '%s-1\n' "$version"
    fi
}

if [[ $# -eq 2 && "$1" == "--print-debian-version" ]]; then
    to_debian_version "$2"
    exit 0
fi

if [[ $# -ne 4 ]]; then
    echo "usage: $0 BINARY_DIRECTORY DEBIAN_ARCHITECTURE CARGO_VERSION OUTPUT_DIRECTORY" >&2
    exit 2
fi

binary_directory=$1
architecture=$2
cargo_version=$3
output_directory=$4

case "$architecture" in
    amd64|arm64) ;;
    *)
        echo "unsupported Debian architecture: $architecture" >&2
        exit 2
        ;;
esac

for binary in briskdb briskdb-import; do
    if [[ ! -x "$binary_directory/$binary" ]]; then
        echo "missing executable release binary: $binary_directory/$binary" >&2
        exit 1
    fi
done

debian_version=$(to_debian_version "$cargo_version")
package_basename="briskdb_${debian_version}_${architecture}"
temporary_directory=$(mktemp -d)
package_root="$temporary_directory/$package_basename"
cleanup() {
    rm -rf "$temporary_directory"
}
trap cleanup EXIT

install -d \
    "$package_root/DEBIAN" \
    "$package_root/usr/bin" \
    "$package_root/usr/share/doc/briskdb" \
    "$package_root/lib/systemd/system" \
    "$package_root/etc/default"

install -m 0755 "$binary_directory/briskdb" "$package_root/usr/bin/briskdb"
install -m 0755 "$binary_directory/briskdb-import" "$package_root/usr/bin/briskdb-import"
install -m 0644 packaging/debian/briskdb.service "$package_root/lib/systemd/system/briskdb.service"
install -m 0644 packaging/debian/briskdb.default "$package_root/etc/default/briskdb"
install -m 0644 README.md RELEASE_NOTES.md LICENSE "$package_root/usr/share/doc/briskdb/"
cp -R docs "$package_root/usr/share/doc/briskdb/docs"

install -m 0755 packaging/debian/postinst "$package_root/DEBIAN/postinst"
install -m 0755 packaging/debian/prerm "$package_root/DEBIAN/prerm"
install -m 0755 packaging/debian/postrm "$package_root/DEBIAN/postrm"
printf '%s\n' /etc/default/briskdb > "$package_root/DEBIAN/conffiles"

installed_size=$(du -sk "$package_root" | awk '{print $1}')
sed \
    -e "s|@DEBIAN_VERSION@|$debian_version|g" \
    -e "s|@ARCHITECTURE@|$architecture|g" \
    -e "s|@INSTALLED_SIZE@|$installed_size|g" \
    packaging/debian/control.in > "$package_root/DEBIAN/control"

mkdir -p "$output_directory"
dpkg-deb --build --root-owner-group "$package_root" "$output_directory/$package_basename.deb"
