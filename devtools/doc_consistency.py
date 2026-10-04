#!/usr/bin/env python3
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Offline documentation checks with portable, independently testable extractors.

Only run CLI help/version; never initialize storage. Required coverage is in
manuals/getting-started, while error text, public-item docs and ADR references
are informational. Setting names come from the declared registry, not every
quoted string in the implementation. Source examples are never executed.
"""
from __future__ import annotations
import argparse
import os
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def commands(help_text: str) -> set[str]:
    match = re.search(r"^Commands:\s*\n(.*?)(?=^\S|\Z)", help_text, re.M | re.S)
    if not match:
        raise ValueError("CLI help has no Commands section")
    return set(re.findall(r"^\s+([a-z][a-z0-9-]*)\s+", match[1], re.M)) - {"help"}


def flags(help_text: str) -> set[str]:
    return set(re.findall(r"(?<!\w)--[a-z][a-z0-9-]*", help_text)) - {"--help", "--version"}


def settings(source: str, constants: str) -> set[str]:
    constants = dict(re.findall(r'pub const (\w+):\s*&str\s*=\s*"([^"]+)"', constants))
    result = set()
    for registry in ("KNOWN_KEYS", "READONLY_KEYS"):
        match = re.search(rf"pub const {registry}:.*?=\s*&\[(.*?)\];", source, re.S)
        if not match:
            raise ValueError(f"Missing setting registry {registry}")
        body = re.sub(r"//[^\n]*", "", match[1])
        result.update(re.findall(r'"([a-z][a-z0-9_]*)"', body))
        for name in re.findall(r"settings_keys::(\w+)", body):
            if name not in constants:
                raise ValueError(f"Unresolved setting constant {name}")
            result.add(constants[name])
    return result


def sample_keys(source: str) -> set[str]:
    return set(re.findall(r"^\s*#?\s*([a-z][a-z0-9_]*)\s*=", source, re.M))


def documented_commands(source: str) -> set[str]:
    # Only literal CLI examples can establish stale commands. Prose such as
    # 'manage their resources' must never become a command named 'their'.
    return set(re.findall(r"\bextenddb\s+manage\s+(?:--[a-z][\w-]+(?:=\S+|\s+\S+)\s+)*([a-z][\w-]*)", source))


def word_present(word: str, source: str) -> bool:
    return re.search(rf"(?<![\w-]){re.escape(word)}(?![\w-])", source) is not None


def check(root: Path, binary: Path) -> int:
    counts = [0, 0]
    def assert_ok(ok: bool, label: str):
        counts[0 if ok else 1] += 1
        print(f"{'PASS' if ok else 'FAIL'}: {label}")
    def run(*args):
        return subprocess.run([str(binary), *args], check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout
    assert_ok(binary.is_file() and os.access(binary, os.X_OK), "extenddb binary exists")
    if counts[1]:
        return 1
    try:
        docs = list((root / "docs/manuals").glob("*.md")) + [root / "docs/getting-started.md"]
        doc_text = "\n".join(p.read_text() for p in docs)
        for command in sorted(commands(run("--help"))):
            for flag in sorted(flags(run(command, "--help"))):
                assert_ok(word_present(flag, doc_text), f"flag {flag} for '{command}' documented")
        manage = commands(run("manage", "--help"))
        for command in sorted(manage):
            assert_ok(word_present(command, doc_text), f"manage subcommand '{command}' documented")
        for command in sorted(documented_commands(doc_text) - manage - {"help"}):
            assert_ok(False, f"manage subcommand '{command}' documented but absent from binary")
        for key in sorted(sample_keys((root / "extenddb.sample.toml").read_text())):
            assert_ok(word_present(key, doc_text), f"config key '{key}' documented")
        registry = (root / "crates/server/src/management/ops_settings.rs").read_text()
        constants = (root / "crates/core/src/settings_keys.rs").read_text()
        for key in sorted(settings(registry, constants)):
            assert_ok(word_present(key, doc_text), f"runtime setting '{key}' documented")
        pipeline = (root / "docs/build-docs.py").read_text()
        for path in docs:
            if path.parent.name == "manuals":
                slug = re.sub(r"^\d+-", "", path.stem)
                assert_ok(slug in pipeline, f"manual '{slug}' in build-docs.py")
        sources = list((root / "crates").rglob("*.rs"))
        for path in (root / "crates").glob("*/src/lib.rs"):
            assert_ok(any(line.startswith("//!") for line in path.read_text().splitlines()[:20]), f"{path.parent.parent.name} has module docs")
        cargo = (root / "Cargo.toml").read_text().split("[workspace.package]", 1)[1].split("\n[", 1)[0]
        expected = re.search(r'^version\s*=\s*"([^"]+)"', cargo, re.M).group(1)
        actual = re.search(r"\d+\.\d+\.\d+", run("version")).group(0)
        assert_ok(expected == actual, f"binary version matches Cargo.toml ({expected} vs {actual})")
        # These old heuristic checks remain advisory; they do not prove API
        # coverage or that a particular ADR identifier is still current.
        source_text = "\n".join(p.read_text() for p in sources if "target" not in p.parts)
        troubleshooting = (root / "docs/troubleshooting.md").read_text()
        missing_errors = {s for s in re.findall(r'(?:error|critical)!\s*\(\s*"([^"]+)"', source_text) if s[:30] not in troubleshooting}
        print(f"INFO: {len(missing_errors)} logged error strings absent from troubleshooting; review manually")
        for adr in (root / "docs/adr").glob("*.md"):
            refs = set(re.findall(r'`([A-Za-z_][A-Za-z0-9_:]+)`', adr.read_text()))
            stale = sum(len(r) >= 4 and r not in source_text for r in sorted(refs)[:20])
            if stale > 3:
                print(f"INFO: ADR '{adr.stem}' has {stale} unresolved references; review manually")
        undocumented = 0
        for p in sources:
            if "target" in p.parts:
                continue
            documented = False
            for line in p.read_text().splitlines():
                if line.lstrip().startswith("///"):
                    documented = True
                elif line.lstrip().startswith("#["):
                    continue
                else:
                    if re.match(r"\s*pub (?:async )?(?:fn|struct|enum|trait) ", line) and not documented:
                        undocumented += 1
                    documented = False
        print(f"INFO: {undocumented} public declarations lack adjacent documentation (heuristic)")
    except (OSError, ValueError, AttributeError, subprocess.CalledProcessError) as error:
        assert_ok(False, f"checker prerequisite/extraction error: {error}")
    print(f"RESULTS: {counts[0]} passed / {counts[1]} failed / {sum(counts)} total")
    return int(counts[1] != 0)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--binary", type=Path, default=Path(os.environ.get("EXTENDDB_BINARY", ROOT / "target/release/extenddb")))
    args = parser.parse_args()
    return check(args.root.resolve(), args.binary.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
