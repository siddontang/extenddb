# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Offline isolation contract for the multi-SDK backend runner."""
import importlib.machinery
import importlib.util
from pathlib import Path


def test_runner_environment_is_isolated_without_mutating_parent():
    path = Path(__file__).resolve().parents[1] / "devtools/run-tikv-tests"
    loader = importlib.machinery.SourceFileLoader("tikv_runner", str(path))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    runner = importlib.util.module_from_spec(spec)
    loader.exec_module(runner)
    parent = {
        "EXTENDDB__STORAGE__TIKV__NAMESPACE": "production",
        "EXTENDDB__SERVER__PORT": "443",
        "AWS_SESSION_TOKEN": "stale-session",
        "AWS_SECURITY_TOKEN": "stale-token",
        "AWS_ENDPOINT_URL_DYNAMODB": "https://unrelated.example",
        "DYNAMODB_ENDPOINT": "https://unrelated.example",
        "EXTENDDB_TEST_ACCOUNT_ID": "999999999999",
        "PATH": "/bin",
    }
    snapshot = dict(parent)
    endpoint = "https://127.0.0.1:12345"
    env = runner.test_environment(parent, endpoint, "/tmp/test-binary", "test-password")
    assert parent == snapshot
    assert env["PATH"] == "/bin"
    assert env["EXTENDDB_BINARY"] == "/tmp/test-binary"
    assert env["EXTENDDB_TEST_ACCOUNT_ID"] == "123456789012"
    for name in ("DYNAMODB_ENDPOINT", "EXTENDDB_ENDPOINT", "EXTENDDB_TEST_ENDPOINT"):
        assert env[name] == endpoint
    assert not any(k.startswith("EXTENDDB__") or k.startswith("AWS_ENDPOINT_URL") for k in env)
    assert "AWS_SESSION_TOKEN" not in env
    assert "AWS_SECURITY_TOKEN" not in env
