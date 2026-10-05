"""Paket surumu tek yerde tutarli olmali (PyPI ayni surumu iki kez kabul etmez)."""

import re
from pathlib import Path

import selection_service

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


def _pyproject_version() -> str:
    match = re.search(r'^version\s*=\s*"([^"]+)"', PYPROJECT.read_text(encoding="utf-8"), re.M)
    assert match, "pyproject.toml icinde version bulunamadi"
    return match.group(1)


def test_package_version_matches_pyproject():
    assert selection_service.__version__ == _pyproject_version()


def test_version_is_semver():
    assert re.fullmatch(r"\d+\.\d+\.\d+", _pyproject_version())
