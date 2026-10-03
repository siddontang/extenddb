# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Offline regressions for the documentation checker's false-positive sources."""
import importlib.util
from pathlib import Path
import pytest

spec = importlib.util.spec_from_file_location("doc_checker", Path(__file__).parents[1] / "devtools/doc_consistency.py")
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


def test_flags_are_literal_patterns_and_commands_stay_in_section():
    text = "Usage: extenddb\n\nCommands:\n  init  Initialize\n  manage  Manage accounts\n  help  Help\n\nOptions:\n  --config <PATH>\n  --help\n"
    assert checker.commands(text) == {"init", "manage"}
    assert checker.flags(text) == {"--config"}
    assert checker.word_present("--config", "Use `--config` for the configuration path.")
    assert not checker.word_present("--config", "--configuration")


def test_registry_resolves_constants_and_ignores_validation_strings():
    source = '''pub const KNOWN_KEYS: &[(&str, Validator)] = &[
        ("log_level", validate_log_level),
        (extenddb_core::settings_keys::DELAY, validate_delay),
    ];
    pub const READONLY_KEYS: &[&str] = &["catalog_version"];
    fn arbitrary() { "armed"; "debug"; "true"; }
    '''
    assert checker.settings(source, 'pub const DELAY: &str = "delay_seconds";') == {"log_level", "delay_seconds", "catalog_version"}
    with pytest.raises(ValueError, match="Unresolved"):
        checker.settings(source, "")


def test_stale_command_detection_ignores_prose_but_accepts_global_options():
    source = "Users manage their resources.\n`extenddb manage --user admin --password <pw> create-user`\n`extenddb manage obsolete-command`"
    assert checker.documented_commands(source) == {"create-user", "obsolete-command"}


def test_commented_sample_keys_and_help_shape_fail_loudly():
    assert checker.sample_keys('[server]\nport = 1\n# import_export_root = ""\n# commentary') == {"port", "import_export_root"}
    with pytest.raises(ValueError):
        checker.commands("unexpected help layout")
