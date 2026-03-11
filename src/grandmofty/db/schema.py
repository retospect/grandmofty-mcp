"""SQLAlchemy models for local MOF data."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, relationship

SCHEMA = "mofty"


class Base(DeclarativeBase):
    pass


class Mof(Base):
    """A Metal-Organic Framework record."""

    __tablename__ = "mofs"
    __table_args__ = {"schema": SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(256), index=True)
    refcode = Column(String(16), nullable=True, index=True)
    mofid = Column(Text, nullable=True, index=True)
    mofkey = Column(String(64), nullable=True, index=True)
    formula = Column(Text, nullable=True)
    topology = Column(String(32), nullable=True)
    space_group = Column(String(32), nullable=True)
    lcd = Column(Float, nullable=True)
    pld = Column(Float, nullable=True)
    sa = Column(Float, nullable=True)  # = GSA m²/g
    vf = Column(Float, nullable=True)
    density = Column(Float, nullable=True)  # g/cm³
    gsa = Column(Float, nullable=True)  # gravimetric SA m²/g
    vsa = Column(Float, nullable=True)  # volumetric SA m²/cm³
    lfpd = Column(Float, nullable=True)  # largest free path diameter Å
    metal_type = Column(String(32), nullable=True)  # e.g. "Zn", "Cu"
    has_oms = Column(Boolean, nullable=True)  # open metal sites
    cr_status = Column(String(4), nullable=True)  # CR or NCR
    year = Column(Integer, nullable=True)  # publication year
    n_atoms = Column(Integer, nullable=True)  # atoms in unit cell
    catenation = Column(Integer, nullable=True)  # interpenetration
    thermal_stability = Column(Float, nullable=True)  # °C
    database = Column(String(32), nullable=False)  # CoRE-2025, CoRE-2019, hMOF, ...
    source_id = Column(String(128), nullable=True)
    doi = Column(Text, nullable=True)
    elements = Column(Text, nullable=True)  # comma-separated sorted elements

    isotherms = relationship(
        "Isotherm", back_populates="mof", cascade="all, delete-orphan"
    )


class Isotherm(Base):
    """Adsorption isotherm data point."""

    __tablename__ = "isotherms"
    __table_args__ = {"schema": SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)
    mof_id = Column(
        Integer, ForeignKey(f"{SCHEMA}.mofs.id"), nullable=False, index=True
    )
    gas = Column(String(16), nullable=False)
    temperature_k = Column(Float, nullable=True)
    pressure_min = Column(Float, nullable=True)
    pressure_max = Column(Float, nullable=True)
    uptake_min = Column(Float, nullable=True)
    uptake_max = Column(Float, nullable=True)
    uptake_unit = Column(String(32), nullable=True)
    force_field = Column(String(64), nullable=True)

    mof = relationship("Mof", back_populates="isotherms")


class SyncLog(Base):
    """Sync history."""

    __tablename__ = "sync_log"
    __table_args__ = {"schema": SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(64), nullable=False)
    synced_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    row_count = Column(Integer, nullable=False, default=0)
    duration_s = Column(Float, nullable=True)


# Indexes for common queries
Index("idx_mofs_elements", Mof.elements)
Index("idx_mofs_database", Mof.database)
Index("idx_mofs_lcd", Mof.lcd)
Index("idx_mofs_sa", Mof.sa)
