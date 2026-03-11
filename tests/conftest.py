"""Shared fixtures for grandmofty tests."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from grandmofty.db.schema import Base, Isotherm, Mof


@pytest.fixture
def engine():
    """In-memory SQLite engine with schema tables."""
    eng = create_engine(
        "sqlite:///:memory:",
        execution_options={"schema_translate_map": {"mofty": None}},
    )

    @event.listens_for(eng, "connect")
    def _set_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()

    Base.metadata.create_all(eng)
    return eng


@pytest.fixture
def session(engine):
    """Session with sample MOF data."""
    Session = sessionmaker(bind=engine)
    s = Session()

    mofs = [
        Mof(
            name="MOF-5",
            refcode="SAHYIK",
            formula="Zn4O(BDC)3",
            topology="pcu",
            space_group="Fm-3m",
            lcd=11.1,
            pld=7.7,
            sa=3800,
            vf=0.81,
            database="CoRE",
            doi="10.1126/science.283.5405.1148",
            elements="C,O,Zn",
        ),
        Mof(
            name="MOF-5",
            refcode="SAHYIK01",
            formula="Zn4O(BDC)3",
            topology="pcu",
            space_group="Fm-3m",
            lcd=11.0,
            pld=7.6,
            sa=3750,
            vf=0.80,
            database="CSD",
            elements="C,O,Zn",
        ),
        Mof(
            name="ZIF-8",
            refcode="VELVOY",
            formula="Zn(MeIM)2",
            topology="sod",
            lcd=11.6,
            pld=3.4,
            sa=1630,
            vf=0.48,
            database="CoRE",
            elements="C,N,Zn",
        ),
        Mof(
            name="HKUST-1",
            refcode="FIQCEN",
            formula="Cu3(BTC)2",
            topology="tbo",
            lcd=13.2,
            pld=6.9,
            sa=1850,
            vf=0.72,
            database="CoRE",
            doi="10.1126/science.283.5405.1149",
            elements="C,Cu,O",
        ),
        Mof(
            name="hMOF-10001",
            source_id="hMOF-10001",
            lcd=8.5,
            pld=5.2,
            sa=2200,
            vf=0.65,
            database="hMOF",
            elements="C,N,O,Zn",
        ),
    ]
    s.add_all(mofs)
    s.flush()

    # Add an isotherm to MOF-5
    s.add(
        Isotherm(
            mof_id=mofs[0].id,
            gas="CO2",
            temperature_k=298,
            pressure_min=0.15,
            pressure_max=50,
            uptake_min=2.1,
            uptake_max=28.3,
            uptake_unit="mmol/g",
            force_field="UFF",
        )
    )
    s.commit()
    yield s
    s.close()
