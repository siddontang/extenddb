#!/usr/bin/env python3
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Run a pinned, external Alternator API suite against an isolated local server.

Use through run-tikv-tests --command. Only client credentials, the health probe,
and Scylla-only fixture selection are adapted; upstream assertions and xfail
markers stay intact. No upstream test source is copied into this repository.
"""
from __future__ import annotations

import argparse
from contextlib import closing, contextmanager
import importlib
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit
from unittest.mock import patch

REVISION = 'b8a9fd4e49e8923b3edffa53ffec3b6e46c50906'  # scylla-6.2.0
SUITES = (
    'item', 'batch', 'number', 'nested', 'condition_expression', 'expected',
    'filter_expression', 'projection_expression', 'update_expression',
    'key_condition_expression', 'key_conditions', 'query', 'query_filter',
    'scan', 'returnvalues', 'gsi', 'lsi', 'table', 'describe_table',
    'describe_endpoints', 'backup', 'tag', 'limits', 'streams', 'ttl',
)
SCYLLA_FIXTURES = frozenset({
    'scylla_only', 'rest_api', 'has_tablets', 'cql',
    # The upstream TTL polling fixture reads Scylla's system configuration
    # table on non-AWS hosts; it is not a portable TTL completion check.
    'waits_for_expiration',
    'check_pre_consistent_cluster_management',
})


def origin(url: str) -> tuple[str, str, int]:
    """Compare actual scheme/host/port, never a string prefix of a URL."""
    parts = urlsplit(url)
    if parts.scheme not in ('http', 'https') or not parts.hostname or parts.username or parts.password:
        raise ValueError('Expected an HTTP(S) URL without embedded credentials')
    return parts.scheme, parts.hostname, parts.port or (443 if parts.scheme == 'https' else 80)


def local_endpoint(env: dict[str, str]) -> str:
    endpoint = env.get('EXTENDDB_TEST_ENDPOINT', '')
    if origin(endpoint)[1] not in ('127.0.0.1', 'localhost', '::1'):
        raise ValueError('The external suite adapter requires an explicit loopback endpoint')
    if urlsplit(endpoint).path not in ('', '/') or urlsplit(endpoint).query or urlsplit(endpoint).fragment:
        raise ValueError('The test endpoint must be an origin without a path/query/fragment')
    for name in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_CA_BUNDLE'):
        if not env.get(name):
            raise ValueError(f'Missing isolated test configuration: {name}')
    return endpoint.rstrip('/')


def check_destination(url: str, endpoint: str) -> None:
    if origin(url) != origin(endpoint):
        raise ValueError('External suite attempted a request outside its isolated endpoint')


@contextmanager
def local_http_only(endpoint: str):
    """Fence both boto3 and raw requests, including redirects and new sessions."""
    import botocore.httpsession
    import requests
    boto_send = botocore.httpsession.URLLib3Session.send
    requests_send = requests.Session.send

    def send_boto(session, request):
        check_destination(request.url, endpoint)
        return boto_send(session, request)

    def send_requests(session, request, **kwargs):
        check_destination(request.url, endpoint)
        return requests_send(session, request, **kwargs)

    with patch.object(botocore.httpsession.URLLib3Session, 'send', send_boto), \
            patch.object(requests.Session, 'send', send_requests):
        yield


class LocalFixtures:
    """Explicit transport adaptations; unsupported Scylla fixtures remain skips."""
    def __init__(self, endpoint: str):
        self.endpoint = endpoint

    def pytest_configure(self, config):
        # This plugin instance and its transport patches live in one process.
        # xdist workers do not inherit pytest.main(plugins=[...]) instances.
        if config.getoption('numprocesses', default=None):
            import pytest
            raise pytest.UsageError('Alternator adapter requires a single pytest process; omit -n')
        # Some test clients ask for credentials directly instead of using the
        # dynamodb fixture. Supply the runner's IAM key, never a CQL role/hash.
        upstream = importlib.import_module('test.alternator.conftest')

        def credentials(url):
            check_destination(url, self.endpoint)
            return os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY']

        upstream.get_valid_alternator_role = credentials

    def pytest_collection_modifyitems(self, items):
        import pytest
        for item in items:
            unsupported = SCYLLA_FIXTURES.intersection(item.fixturenames)
            if unsupported:
                item.add_marker(pytest.mark.skip(reason='Requires Scylla internals: ' + ', '.join(sorted(unsupported))))

    def pytest_fixture_setup(self, fixturedef, request):
        # pytest's ordinary fixture machinery still owns caching and teardown.
        # Replace only the setup function before pytest's default hook runs.
        if fixturedef.argname == 'optional_rest_api':
            fixturedef.func = lambda **kwargs: None
        elif fixturedef.argname == 'dynamodb_test_connection':
            endpoint = self.endpoint

            def health(**kwargs):
                import requests
                import pytest
                yield
                try:
                    requests.get(endpoint + '/health', verify=os.environ['AWS_CA_BUNDLE'], timeout=5).raise_for_status()
                except requests.RequestException:
                    pytest.exit('Local server health check failed; stopping to avoid cascading connection failures', returncode=2)

            fixturedef.func = health


def verify_checkout(checkout: Path) -> None:
    head = subprocess.check_output(['git', '-C', str(checkout), 'rev-parse', 'HEAD'], text=True).strip()
    if head != REVISION:
        raise ValueError(f'Expected Alternator revision {REVISION}, got {head}')
    for revision in ([], ['--cached']):
        subprocess.run(['git', '-C', str(checkout), 'diff', '--exit-code', *revision,
                        '--', 'test/alternator', 'test/pylib/report_plugin.py'], check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--checkout', type=Path, required=True)
    parser.add_argument('--suite', choices=SUITES, action='append',
                        help='Run only these modules (repeatable); default: all selected modules')
    parser.add_argument('pytest_args', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    checkout = args.checkout.resolve()
    verify_checkout(checkout)
    endpoint = local_endpoint(dict(os.environ))
    # --aws selects ordinary SDK/environment credentials, not a real AWS target.
    # The common endpoint override and transport fence cover every SDK session.
    os.environ['AWS_ENDPOINT_URL'] = endpoint
    os.environ['AWS_EC2_METADATA_DISABLED'] = 'true'
    sys.path.insert(0, str(checkout))
    os.chdir(checkout)
    import boto3
    import pytest
    for service in ('dynamodb', 'dynamodbstreams'):
        with closing(boto3.client(service)) as client:
            check_destination(client.meta.endpoint_url, endpoint)
    extra = args.pytest_args
    if extra[:1] == ['--']:
        extra = extra[1:]
    suite = checkout / 'test/alternator'
    targets = [str(suite / f'test_{name}.py') for name in (args.suite or SUITES)]
    # Register this setup hook before pytest's default fixture implementation.
    LocalFixtures.pytest_fixture_setup = pytest.hookimpl(tryfirst=True)(LocalFixtures.pytest_fixture_setup)
    with local_http_only(endpoint):
        return pytest.main(['-c', str(suite / 'pytest.ini'), '--confcutdir', str(suite),
                            '--aws', '--timeout=120', '--timeout-method=signal',
                            '-q', '--tb=short', *targets, *extra], plugins=[LocalFixtures(endpoint)])


if __name__ == '__main__':
    raise SystemExit(main())
