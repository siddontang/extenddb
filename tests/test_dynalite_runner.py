# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Offline checks: external failures and broken lifecycle runs stay visible."""
from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path

loader = SourceFileLoader('dynalite_runner', str(Path(__file__).resolve().parents[1] / 'devtools/run-dynalite-tests'))
spec = spec_from_loader(loader.name, loader)
runner = module_from_spec(spec)
loader.exec_module(runner)


def report(name='scan', passes=2, failures=0, pending=1):
    return dict(suite=name, valid=True,
                stats=dict(tests=passes + failures + pending, passes=passes, failures=failures, pending=pending),
                passes=[{}] * passes, failures=[{}] * failures, pending=[{}] * pending)


def test_all_pass_and_pending_remain_separate():
    result = runner.aggregate([report()], {'scan': 0})
    assert result['exit_code'] == 0
    assert result['totals'] == dict(tests=3, passes=2, failures=0, pending=1)


def test_external_assertions_fail_the_combined_run():
    result = runner.aggregate([report(failures=1), report('query')], {'scan': 1, 'query': 0})
    assert result['exit_code'] == 1
    assert result['totals'] == dict(tests=7, passes=4, failures=1, pending=2)


def test_missing_report_and_cleanup_failure_are_infrastructure_errors():
    assert runner.aggregate([], {'scan': 2})['exit_code'] == 2
    assert runner.aggregate([report()], {'scan': 1})['exit_code'] == 2
    assert runner.aggregate([], {})['exit_code'] == 2


def test_duplicate_or_inconsistent_events_are_not_reported_as_valid_tests():
    broken = report()
    broken['stats']['failures'] = 2
    assert runner.aggregate([broken], {'scan': 1})['exit_code'] == 2
    broken = report()
    broken['valid'] = False
    assert runner.aggregate([broken], {'scan': 0})['exit_code'] == 2
    assert runner.aggregate([report(), report()], {'scan': 0})['exit_code'] == 2


def test_detailed_events_must_match_aggregate_counts():
    broken = report()
    broken['passes'].pop()
    assert runner.aggregate([broken], {'scan': 0})['exit_code'] == 2
