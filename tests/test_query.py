"""Tests for grandmofty.db.query."""

from __future__ import annotations

import pytest
from chemdb.errors import IdNotFoundError, NoResultsError

from grandmofty.db.query import get_by_id, get_shape, search


class TestGetById:
    def test_refcode_prefix(self, session):
        result = get_by_id(session, "refcode:SAHYIK")
        assert result["type"] == "detail"
        assert result["name"] == "MOF-5"
        assert result["refcode"] == "SAHYIK"

    def test_bare_refcode(self, session):
        result = get_by_id(session, "SAHYIK")
        assert result["type"] == "detail"

    def test_name_single(self, session):
        result = get_by_id(session, "name:ZIF-8")
        assert result["type"] == "detail"
        assert result["name"] == "ZIF-8"

    def test_name_disambiguation(self, session):
        result = get_by_id(session, "name:MOF-5")
        assert result["type"] == "disambiguation"
        assert result["count"] == 2

    def test_not_found(self, session):
        with pytest.raises(IdNotFoundError):
            get_by_id(session, "refcode:XYZABC")


class TestSearch:
    def test_element_filter(self, session):
        result = search(session, elements="Zn")
        assert result["total"] == 4  # MOF-5 x2, ZIF-8, hMOF

    def test_element_and(self, session):
        result = search(session, elements="Cu")
        assert result["total"] == 1  # HKUST-1 only

    def test_lcd_range(self, session):
        result = search(session, lcd="10..")
        assert result["total"] == 4  # MOF-5 x2, ZIF-8, HKUST-1

    def test_database_filter(self, session):
        result = search(session, database="CoRE")
        assert result["total"] == 3

    def test_no_results(self, session):
        with pytest.raises(NoResultsError):
            search(session, elements="Au")

    def test_shape_on_page_1(self, session):
        result = search(session, elements="Zn")
        assert result["shape"] is not None
        assert "properties" in result["shape"]

    def test_pagination(self, session):
        result = search(session, elements="Zn", page=1)
        assert result["page"] == 1
        assert len(result["results"]) <= 10


class TestGetShape:
    def test_full_shape(self, session):
        shape = get_shape(session)
        assert shape["total"] == 5
        assert "CoRE" in shape["databases"]
        assert shape["databases"]["CoRE"] == 3
