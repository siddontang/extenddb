# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Offline transport-boundary checks; these never invoke external test code."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import os

import pytest


def test_upstream_transport_matches_endpoint_without_weakening_origin_fence():
    assert adapter.transport_options('https://localhost:8443') == ['--https']
    assert adapter.transport_options('http://localhost:8000') == []
    with pytest.raises(ValueError):
        adapter.check_destination('http://localhost:8443', 'https://localhost:8443')

SPEC = importlib.util.spec_from_file_location(
    'alternator_adapter', Path(__file__).resolve().parents[1] / 'devtools/alternator_adapter.py')
adapter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(adapter)


@pytest.mark.parametrize('url', [
    'https://dynamodb.us-east-1.amazonaws.com', 'https://127.0.0.1.evil.example',
    'file:///tmp/data', 'https://user:secret@localhost:8443',
    'https://localhost:8443/path', 'https://localhost:8443?x=y',
])
def test_rejects_nonlocal_or_ambiguous_endpoints(url):
    with pytest.raises(ValueError):
        adapter.local_endpoint({'EXTENDDB_TEST_ENDPOINT': url})


@pytest.mark.parametrize('url', [
    'https://localhost:9443/', 'http://localhost:8443/',
    'https://localhost.evil.example:8443/', 'https://example.com/',
])
def test_rejects_redirects_outside_origin(url):
    with pytest.raises(ValueError):
        adapter.check_destination(url, 'https://localhost:8443')


def test_accepts_same_origin_paths_and_explicit_default_port():
    adapter.check_destination('https://localhost:443/health?probe=1', 'https://localhost')


def test_credentials_required_without_exposing_values():
    env = {'EXTENDDB_TEST_ENDPOINT': 'https://127.0.0.1:8443', 'AWS_ACCESS_KEY_ID': 'sensitive'}
    with pytest.raises(ValueError, match='AWS_SECRET_ACCESS_KEY') as error:
        adapter.local_endpoint(env)
    assert 'sensitive' not in str(error.value)


def test_http_fence_blocks_both_transports_before_io_and_restores_them():
    import requests
    import botocore.httpsession
    request = SimpleNamespace(url='https://other.example/')
    with patch.object(requests.Session, 'send') as raw, \
            patch.object(botocore.httpsession.URLLib3Session, 'send') as sdk:
        with adapter.local_http_only('https://localhost:8443'):
            with pytest.raises(ValueError):
                requests.Session().send(request)
            with pytest.raises(ValueError):
                botocore.httpsession.URLLib3Session().send(request)
            raw.assert_not_called()
            sdk.assert_not_called()
        assert requests.Session.send is raw
        assert botocore.httpsession.URLLib3Session.send is sdk


def test_http_fence_forwards_local_requests_and_restores_after_failure():
    import requests
    import botocore.httpsession
    request = SimpleNamespace(url='https://localhost:8443/health')
    with patch.object(requests.Session, 'send', return_value='raw-response') as raw, \
            patch.object(botocore.httpsession.URLLib3Session, 'send', return_value='sdk-response') as sdk:
        with pytest.raises(RuntimeError, match='test failure'):
            with adapter.local_http_only('https://localhost:8443'):
                assert requests.Session().send(request, timeout=5) == 'raw-response'
                assert botocore.httpsession.URLLib3Session().send(request) == 'sdk-response'
                assert raw.call_args.kwargs == {'timeout': 5}
                raise RuntimeError('test failure')
        assert requests.Session.send is raw
        assert botocore.httpsession.URLLib3Session.send is sdk


def test_health_probe_checks_the_selected_server_and_propagates_failure():
    import requests
    fixture = SimpleNamespace(argname='dynamodb_test_connection')
    adapter.LocalFixtures('https://localhost:8443').pytest_fixture_setup(fixture, None)
    with patch.dict(os.environ, {'AWS_CA_BUNDLE': '/temporary/ca.pem'}), \
            patch.object(requests, 'get') as get:
        get.return_value.raise_for_status.side_effect = requests.HTTPError('unhealthy')
        probe = fixture.func(dynamodb=object())
        next(probe)
        get.assert_not_called()
        with pytest.raises(pytest.exit.Exception, match='Local server health check failed'):
            next(probe)
        get.assert_called_once_with('https://localhost:8443/health', verify='/temporary/ca.pem', timeout=5)


def test_rejects_workers_that_would_not_inherit_transport_guard():
    config = SimpleNamespace(getoption=lambda *args, **kwargs: 2)
    with pytest.raises(pytest.UsageError, match='single pytest process'):
        adapter.LocalFixtures('https://localhost:8443').pytest_configure(config)


def test_only_scylla_internal_fixtures_are_skipped():
    items = []
    for names in (['dynamodb'], ['rest_api', 'dynamodb'], ['scylla_only'],
                  ['has_tablets'], ['waits_for_expiration'],
                  ['check_pre_consistent_cluster_management']):
        marks = []
        items.append(SimpleNamespace(fixturenames=names, marks=marks, add_marker=marks.append))
    adapter.LocalFixtures('https://localhost:8443').pytest_collection_modifyitems(items)
    assert items[0].marks == []
    assert all(item.marks[0].name == 'skip' for item in items[1:])
