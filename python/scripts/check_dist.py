#!/usr/bin/env python3
"""Inspect wheel/sdist contents and tags without installing the artifact."""

import argparse
import json
import pathlib
import subprocess
import tarfile
import zipfile
from email.parser import Parser
from typing import Iterable, Set

from packaging.utils import parse_wheel_filename
from packaging.version import Version


ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE_FILES = {
    "briskdb/__init__.py",
    "briskdb/__init__.pyi",
    "briskdb/_briskdb.pyi",
    "briskdb/api.py",
    "briskdb/api.pyi",
    "briskdb/NATIVE_NOTICES.txt",
    "briskdb/py.typed",
}
SDIST_FILES = {
    "API.md",
    "ASYNC_API.md",
    "CHANGELOG.md",
    "COMPATIBILITY.md",
    "BRISKDB_LICENSE.txt",
    "PUBLISHING.md",
    "README.md",
    "SERVERLESS.md",
    "VALUE_CONVERSIONS.md",
    "examples/asyncio.py",
    "examples/serverless_handler.py",
    "examples/sync.py",
    "pyproject.toml",
}


def release_version() -> Version:
    completed = subprocess.run(
        [
            "cargo",
            "metadata",
            "--locked",
            "--no-deps",
            "--format-version",
            "1",
            "--manifest-path",
            str(ROOT / "Cargo.toml"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    packages = json.loads(completed.stdout)["packages"]
    version = next(package["version"] for package in packages if package["name"] == "briskdb")
    return Version(version)


def require_suffixes(names: Iterable[str], suffixes: Set[str]) -> None:
    available = tuple(names)
    missing = sorted(
        suffix for suffix in suffixes if not any(name.endswith(suffix) for name in available)
    )
    if missing:
        raise SystemExit("artifact is missing: {}".format(", ".join(missing)))


def check_wheel(path: pathlib.Path, platform: str) -> None:
    distribution, version, _build, tags = parse_wheel_filename(path.name)
    if distribution != "briskdb" or version != release_version():
        raise SystemExit("wheel name/version does not match the Rust package: {}".format(path.name))
    expected_tag = "cp39-abi3-{}".format(platform)
    if expected_tag not in {str(tag) for tag in tags}:
        raise SystemExit(
            "wheel must carry exact supported tag {}: {}".format(expected_tag, path.name)
        )

    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        require_suffixes(names, PACKAGE_FILES)
        if not any(
            name.startswith("briskdb/_briskdb") and name.endswith((".so", ".pyd"))
            for name in names
        ):
            raise SystemExit("wheel does not contain the native extension")
        metadata_name = next(name for name in names if name.endswith(".dist-info/METADATA"))
        metadata = Parser().parsestr(archive.read(metadata_name).decode("utf-8"))
        if metadata["Requires-Python"] != ">=3.9":
            raise SystemExit("wheel has the wrong Requires-Python metadata")
        if metadata["License-Expression"] != "MIT":
            raise SystemExit("wheel has the wrong license expression")
        if not any(
            name.endswith(".dist-info/licenses/BRISKDB_LICENSE.txt") for name in names
        ):
            raise SystemExit("wheel does not contain the MIT license text")

    print("wheel contract passed: {}".format(path.name))


def check_sdist(path: pathlib.Path) -> None:
    expected = release_version()
    if not path.name.startswith("briskdb-{}".format(expected)):
        raise SystemExit("sdist name/version does not match the Rust package: {}".format(path.name))
    with tarfile.open(path, "r:gz") as archive:
        require_suffixes(archive.getnames(), SDIST_FILES)
    print("sdist contract passed: {}".format(path.name))


def main() -> None:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--wheel", type=pathlib.Path)
    group.add_argument("--sdist", type=pathlib.Path)
    parser.add_argument("--platform")
    arguments = parser.parse_args()
    if arguments.wheel:
        if not arguments.platform:
            parser.error("--platform is required with --wheel")
        check_wheel(arguments.wheel, arguments.platform)
    else:
        check_sdist(arguments.sdist)


if __name__ == "__main__":
    main()
