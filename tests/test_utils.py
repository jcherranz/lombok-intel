"""Tests for src/utils.py — zone assignment, helpers, and validators."""

from src.utils import (
    assign_zone,
    now_iso,
    validate_coordinates,
    validate_price,
)


# --- assign_zone ---

def test_assign_zone_kuta():
    """A point in the Kuta/Mandalika bounding box returns KUT."""
    assert assign_zone(-8.85, 116.30) == "KUT"


def test_assign_zone_gili():
    """A point in the Gili Islands bounding box returns GLI."""
    assert assign_zone(-8.35, 116.05) == "GLI"


def test_assign_zone_none_coords():
    """None coordinates return None."""
    assert assign_zone(None, None) is None
    assert assign_zone(-8.5, None) is None


def test_assign_zone_outside_lombok():
    """A point outside all zone bounds returns None."""
    assert assign_zone(0.0, 0.0) is None


# --- validate_coordinates ---

def test_validate_coordinates_valid():
    """Coordinates within Lombok return True."""
    assert validate_coordinates(-8.55, 116.25) is True


def test_validate_coordinates_outside():
    """Coordinates far from Lombok return False."""
    assert validate_coordinates(51.5, -0.1) is False


def test_validate_coordinates_none():
    """None coordinates are treated as valid (not invalid, just unknown)."""
    assert validate_coordinates(None, None) is True


# --- validate_price ---

def test_validate_price_valid():
    """Price within sanity range returns True."""
    assert validate_price(100.0) is True
    assert validate_price(5.0) is True
    assert validate_price(5000.0) is True


def test_validate_price_too_low():
    """Price below $5 returns False."""
    assert validate_price(1.0) is False


def test_validate_price_too_high():
    """Price above $5000 returns False."""
    assert validate_price(10000.0) is False


def test_validate_price_none():
    """None price is treated as valid."""
    assert validate_price(None) is True


# --- now_iso ---

def test_now_iso_format():
    """now_iso returns a string matching YYYY-MM-DD HH:MM:SS format."""
    result = now_iso()
    assert len(result) == 19
    assert result[4] == "-"
    assert result[10] == " "
