"""Sync upstream MOF databases to local storage.

Sources:
- CoRE MOF 2025 (Zhao et al.) via Zenodo bulk JSON — primary
- mofdb-client streaming fetch() for older MOFDB data (CoRE 2019/2014, IZA, PCOD, Tobacco, hMOF)
- Materials Project — stub (needs mp-api + API key)
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

from chemdb.config import ChemdbConfig
from chemdb.db import ensure_schema, make_engine, make_session
from grandmofty.db.schema import SCHEMA, Base, Isotherm, Mof, SyncLog

log = logging.getLogger(__name__)

BATCH_SIZE = 500

# CoRE MOF 2025 Zenodo record
CORE2025_ZENODO_RECORD = "15621349"
CORE2025_FILES = [
    ("CR_meta_data_SI.json", "CR"),
    ("NCR_meta_data_SI.json", "NCR"),
]


class MoftySyncer:
    """Downloads MOF data from upstream sources into local DB."""

    def __init__(self, config: ChemdbConfig):
        self.config = config
        self.engine = make_engine(config, SCHEMA)
        self.Session = make_session(self.engine)

    def run(self, *, include_hmof: bool = False, force: bool = False):
        """Run the full sync."""
        ensure_schema(self.engine, SCHEMA)

        if force:
            Base.metadata.drop_all(self.engine)

        Base.metadata.create_all(self.engine)

        t0 = time.time()

        # Primary: CoRE MOF 2025 from Zenodo
        count = self._sync_core2025()
        # Secondary: older databases from mofdb-client
        count += self._sync_mofdb(include_hmof=include_hmof)
        if self.config.mp_api_key:
            count += self._sync_mp()

        duration = time.time() - t0
        self._log_sync("all", count, duration)
        log.info("Sync complete: %d rows in %.1fs", count, duration)

    # mofdb-client database names → our schema labels
    DATABASES = [
        ("CoREMOF 2019", "CoRE-2019"),
        ("CoREMOF 2014", "CoRE-2014"),
        ("IZA", "IZA"),
        ("PCOD-syn", "PCOD"),
        ("Tobacco", "Tobacco"),
        # CSD: broken in mofdb-client (StopIteration bug)
    ]
    HMOF_DB = ("hMOF", "hMOF")

    def _sync_mofdb(self, *, include_hmof: bool = False) -> int:
        """Stream MOFs via mofdb-client fetch(), one database at a time."""
        from mofdb_client import fetch

        databases = list(self.DATABASES)
        if include_hmof:
            databases.append(self.HMOF_DB)

        total = 0
        for client_name, label in databases:
            log.info("Syncing %s (%s)...", client_name, label)
            count = 0
            try:
                with self.Session() as session:
                    batch = []
                    for mof_data in fetch(database=client_name):
                        mof = _client_to_model(mof_data, label)
                        if mof is None:
                            continue

                        batch.append((mof, mof_data))
                        count += 1

                        if len(batch) >= BATCH_SIZE:
                            _flush_batch(session, batch)
                            batch.clear()
                            log.info("  %s: %d MOFs...", label, count)

                    if batch:
                        _flush_batch(session, batch)

                    session.commit()
            except Exception as exc:
                log.error("  %s failed after %d MOFs: %s", label, count, exc)

            log.info("  %s: %d MOFs", label, count)
            total += count

        log.info("MOFDB sync: %d MOFs total", total)
        return total

    def _sync_core2025(self) -> int:
        """Load CoRE MOF 2025 (Zhao et al.) from Zenodo JSON files."""
        import httpx

        cache_dir = Path(self.config.cache_dir) / "core2025"
        cache_dir.mkdir(parents=True, exist_ok=True)

        total = 0
        for filename, cr_status in CORE2025_FILES:
            path = cache_dir / filename
            if not path.exists():
                url = f"https://zenodo.org/records/{CORE2025_ZENODO_RECORD}/files/{filename}?download=1"
                log.info("Downloading %s from Zenodo...", filename)
                try:
                    resp = httpx.get(url, timeout=120, follow_redirects=True)
                    resp.raise_for_status()
                    path.write_bytes(resp.content)
                except Exception as exc:
                    log.error("Failed to download %s: %s", filename, exc)
                    continue

            log.info("Loading %s (%s)...", filename, cr_status)
            with open(path) as f:
                data = json.load(f)

            count = 0
            with self.Session() as session:
                batch = []
                for name, record in data.items():
                    mof = _zenodo_to_model(name, record, cr_status)
                    if mof is None:
                        continue
                    batch.append(mof)
                    count += 1
                    if len(batch) >= BATCH_SIZE:
                        for m in batch:
                            session.add(m)
                        session.flush()
                        batch.clear()
                        if count % 1000 == 0:
                            log.info("  CoRE-2025 %s: %d...", cr_status, count)

                if batch:
                    for m in batch:
                        session.add(m)
                    session.flush()
                session.commit()

            log.info("  CoRE-2025 %s: %d MOFs", cr_status, count)
            total += count

        log.info("CoRE-2025 sync: %d MOFs total", total)
        return total

    def _sync_mp(self) -> int:
        """Fetch MOF structures from Materials Project."""
        log.info("MP sync: not yet implemented (needs mp-api client)")
        return 0

    def _log_sync(self, source: str, count: int, duration: float):
        with self.Session() as session:
            session.add(
                SyncLog(
                    source=source,
                    synced_at=datetime.utcnow(),
                    row_count=count,
                    duration_s=duration,
                )
            )
            session.commit()


def _detect_database(mof_data) -> str:
    """Detect source collection from mofdb-client MOF object.

    The client object has no .database attribute — infer from name.
    """
    name = getattr(mof_data, "name", "") or ""
    if name.startswith("hMOF-"):
        return "hMOF"
    if name.startswith("QMOF-") or name.startswith("qmof-"):
        return "QMOF"
    # Most remaining are CoRE MOF / CSD entries
    return "CoRE"


def _client_to_model(mof_data, database: str) -> Mof | None:
    """Convert a mofdb-client MOF object to our schema.

    Actual attrs: name, lcd, pld, void_fraction, surface_area_m2g,
    surface_area_m2cm3, mofid, mofkey, url, pore_size_distribution, pxrd.
    No: refcode, formula, topology, space_group, database, doi, elements.
    """
    name = getattr(mof_data, "name", None)
    if not name:
        return None

    # Extract topology from mofid if available (format: "SMILES MOFid-v1.{topology}.cat0")
    mofid = getattr(mof_data, "mofid", None)
    topology = None
    if mofid and "MOFid-v1." in mofid:
        parts = mofid.split("MOFid-v1.", 1)[1].split(".")
        if parts:
            topology = parts[0]

    return Mof(
        name=name,
        mofid=mofid,
        mofkey=getattr(mof_data, "mofkey", None),
        topology=topology,
        lcd=_float(getattr(mof_data, "lcd", None)),
        pld=_float(getattr(mof_data, "pld", None)),
        sa=_float(getattr(mof_data, "surface_area_m2g", None)),
        vf=_float(getattr(mof_data, "void_fraction", None)),
        database=database,
    )


def _flush_batch(session, batch: list):
    """Merge a batch of MOFs + their isotherms into the session."""
    for mof, mof_data in batch:
        session.add(mof)
    session.flush()

    # Isotherms (second pass — mof.id is now set)
    for mof, mof_data in batch:
        isotherms = getattr(mof_data, "isotherms", None) or []
        for iso_data in isotherms:
            adsorbates = getattr(iso_data, "adsorbates", [])
            gas = str(adsorbates[0].formula) if adsorbates else "?"
            points = getattr(iso_data, "isotherm_data", []) or []
            pressures = [_float(getattr(p, "pressure", None)) for p in points]
            loadings = [_float(getattr(p, "total_adsorption", None)) for p in points]
            pressures = [p for p in pressures if p is not None]
            loadings = [l for l in loadings if l is not None]

            iso = Isotherm(
                mof_id=mof.id,
                gas=gas,
                temperature_k=_float(getattr(iso_data, "temperature", None)),
                pressure_min=min(pressures) if pressures else None,
                pressure_max=max(pressures) if pressures else None,
                uptake_min=min(loadings) if loadings else None,
                uptake_max=max(loadings) if loadings else None,
                uptake_unit=getattr(iso_data, "adsorptionUnits", None),
                force_field=getattr(iso_data, "adsorbent_forcefield", None),
            )
            session.add(iso)

    session.flush()


def _zenodo_to_model(name: str, record: dict, cr_status: str) -> Mof | None:
    """Convert a CoRE MOF 2025 Zenodo JSON record to a Mof model."""
    if not name:
        return None

    zeopp = record.get("Zeopp") or {}
    ref = record.get("reference") or {}
    id_info = record.get("id") or {}
    metal = record.get("metal") or {}
    struct = record.get("structure_info") or {}
    nets = record.get("CrystalNets") or {}
    stab = record.get("stability") or {}

    # Extract mofid (v1 preferred, fall back to v2)
    mofid = id_info.get("mofid-v1") or id_info.get("mofid-v2")
    if mofid and mofid.strip() == "nan":
        mofid = None

    # Topology from mofid
    topology = None
    if mofid and "MOFid-v1." in mofid:
        parts = mofid.split("MOFid-v1.", 1)[1].split(".")
        if parts and parts[0] != "ERROR":
            topology = parts[0]

    # Common name
    common_name = id_info.get("common_name")
    if common_name and common_name.strip() == "nan":
        common_name = None

    # Space group
    sg = struct.get("space_group") or {}
    space_group = sg.get("hall") or None

    # Year
    year_str = ref.get("year")
    year = None
    if year_str and year_str != "unknown":
        try:
            year = int(year_str)
        except (ValueError, TypeError):
            pass

    # Thermal stability
    thermal = _float(stab.get("thermal"))

    # OMS
    has_oms_str = metal.get("has_OMS")
    has_oms = True if has_oms_str == "Yes" else (False if has_oms_str == "No" else None)

    return Mof(
        name=name,
        mofid=mofid,
        topology=topology,
        space_group=space_group,
        lcd=_float(zeopp.get("LCD")),
        pld=_float(zeopp.get("PLD")),
        sa=_float(zeopp.get("GSA")),
        vf=_float(zeopp.get("VF")),
        density=_float(zeopp.get("density")),
        gsa=_float(zeopp.get("GSA")),
        vsa=_float(zeopp.get("VSA")),
        lfpd=_float(zeopp.get("LFPD")),
        metal_type=metal.get("metal_type") or None,
        has_oms=has_oms,
        cr_status=cr_status,
        year=year,
        n_atoms=struct.get("n_atoms"),
        catenation=nets.get("catenation"),
        thermal_stability=thermal,
        database="CoRE-2025",
        doi=ref.get("DOI"),
    )


def _float(v) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return f
    except (ValueError, TypeError):
        return None
