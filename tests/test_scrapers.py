"""Tests for scraper helper functions — price fallback and safe conversion."""

from src.scrapers.airbnb_scraper import _safe_float, _safe_int, _bool_to_int


# --- _safe_float ---

def test_safe_float_number():
    assert _safe_float(42.5) == 42.5


def test_safe_float_string():
    assert _safe_float("99.9") == 99.9


def test_safe_float_none():
    assert _safe_float(None) is None


def test_safe_float_invalid():
    assert _safe_float("not-a-number") is None


# --- _safe_int ---

def test_safe_int_number():
    assert _safe_int(42) == 42


def test_safe_int_string():
    assert _safe_int("99") == 99


def test_safe_int_none():
    assert _safe_int(None) is None


def test_safe_int_float():
    assert _safe_int(3.7) == 3


# --- _bool_to_int ---

def test_bool_to_int_true():
    assert _bool_to_int(True) == 1
    assert _bool_to_int("yes") == 1


def test_bool_to_int_false():
    assert _bool_to_int(False) == 0
    assert _bool_to_int(None) == 0
    assert _bool_to_int("") == 0
