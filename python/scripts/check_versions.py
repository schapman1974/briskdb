#!/usr/bin/env python3
"""Fail a release when the Python wrapper and Rust core versions diverge."""

import json
import pathlib
import subprocess


ROOT = pathlib.Path(__file__).resolve().parents[2]


def main() -> None:
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
    packages = {package["name"]: package for package in json.loads(completed.stdout)["packages"]}
    core = packages["briskdb"]
    extension = packages["briskdb-python"]
    if core["version"] != extension["version"]:
        raise SystemExit(
            "version mismatch: briskdb={} briskdb-python={}".format(
                core["version"], extension["version"]
            )
        )

    core_dependencies = [
        dependency
        for dependency in extension["dependencies"]
        if dependency["name"] == "briskdb"
    ]
    if len(core_dependencies) != 1:
        raise SystemExit("briskdb-python must have exactly one briskdb dependency")
    dependency = core_dependencies[0]
    if dependency["uses_default_features"] or dependency["features"] != ["embedded"]:
        raise SystemExit(
            "briskdb-python must depend only on the listener-free embedded feature"
        )
    if pathlib.Path(dependency["path"]).resolve() != ROOT:
        raise SystemExit("briskdb-python must bind the workspace's exact briskdb core")

    pyproject = (ROOT / "python" / "pyproject.toml").read_text(encoding="utf-8")
    if 'maturin==1.14.1' not in pyproject:
        raise SystemExit("the Python build frontend must remain pinned to maturin 1.14.1")
    python_manifest = (ROOT / "python" / "Cargo.toml").read_text(encoding="utf-8")
    if 'features = ["abi3-py39"]' not in python_manifest:
        raise SystemExit("the supported wheel ABI must remain explicitly abi3-py39")

    print("Rust/Python release parity: {}".format(core["version"]))


if __name__ == "__main__":
    main()
