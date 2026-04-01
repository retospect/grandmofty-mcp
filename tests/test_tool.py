"""Tests for grandmofty.tool — formatted output."""

from __future__ import annotations

from grandmofty.db.query import get_by_id, get_shape, search
from grandmofty.tool import _format_id_result, _format_search, _format_shape


class TestFormatDetail:
    def test_single_mof(self, session):
        data = get_by_id(session, "refcode:SAHYIK")
        text = _format_id_result(data)
        assert "## MOF-5 (refcode SAHYIK)" in text
        assert "**LCD:** 11.1 Å" in text
        assert "**SA:** 3800 m²/g" in text
        assert "📎" in text

    def test_disambiguation(self, session):
        data = get_by_id(session, "name:MOF-5")
        text = _format_id_result(data)
        assert "2 matches" in text
        assert "pick one" in text
        assert "SAHYIK" in text
        assert "SAHYIK01" in text

    def test_isotherms_shown(self, session):
        data = get_by_id(session, "refcode:SAHYIK")
        text = _format_id_result(data, isotherms="CO2")
        assert "CO2" in text
        assert "UFF" in text


class TestFormatShape:
    def test_shape_output(self, session):
        data = get_shape(session)
        text = _format_shape(data)
        assert "grandMOFty" in text
        assert "Collections" in text
        assert "CoRE" in text
        assert "Sortable fields" in text
        assert "→ get" in text


class TestFormatSearch:
    def test_search_output(self, session):
        data = search(session, elements="Zn")
        text = _format_search(data, elements="Zn")
        assert "MOFs matching" in text
        assert "Shape" in text
        assert "→ get" in text
