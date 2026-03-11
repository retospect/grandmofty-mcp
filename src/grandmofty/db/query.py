"""Read-only queries for the get tool.

All queries are local SQL — no runtime API calls.
Shape is computed on-the-fly via SQL aggregation.
"""

from __future__ import annotations

import re
from typing import Any

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from chemdb.errors import IdNotFoundError, InvalidRangeError, NoResultsError
from chemdb.ranges import parse_range
from chemdb.sort import parse_sort

from grandmofty.db.schema import Mof

PAGE_SIZE = 10

SORTABLE_FIELDS = {"lcd", "pld", "sa", "vf", "name", "relevance"}

# CSD refcode: letter + 4–7 alphanumerics
_REFCODE_RE = re.compile(r"^[A-Z][A-Z0-9]{4,7}$")


def get_by_id(session: Session, id_str: str) -> dict[str, Any]:
    """Look up a single MOF by identifier."""
    scheme, _, ident = id_str.partition(":")
    if not ident:
        # Bare string — auto-detect
        ident = scheme
        scheme = _detect_scheme(ident)

    q = session.query(Mof)
    if scheme == "refcode":
        q = q.filter(Mof.refcode == ident)
    elif scheme == "mofid":
        q = q.filter(Mof.mofid == ident)
    elif scheme == "mofkey":
        q = q.filter(Mof.mofkey == ident)
    elif scheme == "name":
        q = q.filter(Mof.name.ilike(f"%{ident}%"))
    else:
        raise IdNotFoundError(id_str)

    results = q.all()
    if not results:
        raise IdNotFoundError(id_str)

    if len(results) == 1:
        return _mof_to_detail(results[0])

    # Name disambiguation — multiple matches
    return _disambiguation(ident, results)


def search(
    session: Session,
    *,
    query: str = "",
    elements: str = "",
    lcd: str = "",
    pld: str = "",
    sa: str = "",
    vf: str = "",
    isotherms: str = "",
    database: str = "",
    sort: str = "",
    page: int = 1,
) -> dict[str, Any]:
    """Filter search with shape + paginated results."""
    q = session.query(Mof)

    # Element filter (AND logic)
    if elements:
        for elem in elements.split(","):
            elem = elem.strip()
            if elem:
                q = q.filter(Mof.elements.contains(elem))

    # Range filters
    for field_name, raw in [("lcd", lcd), ("pld", pld), ("sa", sa), ("vf", vf)]:
        if raw:
            try:
                r = parse_range(raw)
            except ValueError:
                raise InvalidRangeError(field_name, raw)
            clause, params = r.to_sql_clause(field_name)
            q = q.filter(text(clause).bindparams(**params))

    # Database filter
    if database:
        q = q.filter(Mof.database.ilike(database))

    # Isotherms filter (MOFs that have isotherm data for this gas)
    if isotherms:
        from grandmofty.db.schema import Isotherm

        q = q.join(Mof.isotherms).filter(Isotherm.gas.ilike(isotherms))

    # Get total count
    total = q.count()
    if total == 0:
        raise NoResultsError()

    # Sort
    sort_fields = parse_sort(sort, SORTABLE_FIELDS) if sort else []
    if sort_fields:
        for sf in sort_fields:
            if sf.name == "relevance":
                continue  # FTS rank handled separately
            col = getattr(Mof, sf.name, None)
            if col is not None:
                q = q.order_by(col.desc() if sf.descending else col.asc())
    else:
        q = q.order_by(Mof.name.asc())

    # Paginate
    offset = (page - 1) * PAGE_SIZE
    results = q.offset(offset).limit(PAGE_SIZE).all()

    # Shape (page 1 only)
    shape = _compute_shape(session, q) if page == 1 else None

    return {
        "total": total,
        "page": page,
        "page_size": PAGE_SIZE,
        "shape": shape,
        "results": [_mof_to_row(m) for m in results],
    }


def get_shape(session: Session) -> dict[str, Any]:
    """Shape of entire database (no-arg call)."""
    total = session.query(Mof).count()
    if total == 0:
        return {"total": 0, "databases": {}}

    shape = _compute_shape(session, session.query(Mof))
    shape["total"] = total
    return shape


def _compute_shape(session: Session, q) -> dict[str, Any]:
    """Compute shape statistics from a query."""
    # Property distributions
    stats = session.query(
        func.count(Mof.id),
        func.min(Mof.lcd),
        func.max(Mof.lcd),
        func.avg(Mof.lcd),
        func.min(Mof.pld),
        func.max(Mof.pld),
        func.avg(Mof.pld),
        func.min(Mof.sa),
        func.max(Mof.sa),
        func.avg(Mof.sa),
        func.min(Mof.vf),
        func.max(Mof.vf),
        func.avg(Mof.vf),
    ).one()

    # Database counts
    db_counts = (
        session.query(Mof.database, func.count(Mof.id))
        .group_by(Mof.database)
        .order_by(func.count(Mof.id).desc())
        .all()
    )

    return {
        "properties": {
            "lcd": {"min": stats[1], "max": stats[2], "avg": stats[3]},
            "pld": {"min": stats[4], "max": stats[5], "avg": stats[6]},
            "sa": {"min": stats[7], "max": stats[8], "avg": stats[9]},
            "vf": {"min": stats[10], "max": stats[11], "avg": stats[12]},
        },
        "databases": {db: count for db, count in db_counts},
    }


def _detect_scheme(s: str) -> str:
    """Auto-detect identifier type from a bare string."""
    if "[" in s:
        return "mofid"
    if _REFCODE_RE.match(s):
        return "refcode"
    return "name"


def _mof_to_detail(m: Mof) -> dict[str, Any]:
    """Full detail dict for a single MOF."""
    return {
        "type": "detail",
        "name": m.name,
        "refcode": m.refcode,
        "mofid": m.mofid,
        "formula": m.formula,
        "topology": m.topology,
        "space_group": m.space_group,
        "lcd": m.lcd,
        "pld": m.pld,
        "sa": m.sa,
        "vf": m.vf,
        "database": m.database,
        "doi": m.doi,
        "elements": m.elements,
        "isotherms": [
            {
                "gas": iso.gas,
                "temperature_k": iso.temperature_k,
                "uptake_min": iso.uptake_min,
                "uptake_max": iso.uptake_max,
                "uptake_unit": iso.uptake_unit,
                "force_field": iso.force_field,
            }
            for iso in (m.isotherms or [])
        ],
    }


def _mof_to_row(m: Mof) -> dict[str, Any]:
    """Summary row for search results."""
    return {
        "name": m.name,
        "refcode": m.refcode,
        "lcd": m.lcd,
        "pld": m.pld,
        "sa": m.sa,
        "vf": m.vf,
        "database": m.database,
    }


def _disambiguation(name: str, results: list[Mof]) -> dict[str, Any]:
    """Return a disambiguation list when name: matches multiple MOFs."""
    return {
        "type": "disambiguation",
        "name": name,
        "count": len(results),
        "matches": [
            {
                "name": m.name,
                "refcode": m.refcode,
                "database": m.database,
                "formula": m.formula,
                "lcd": m.lcd,
                "source_id": m.source_id,
            }
            for m in results[:20]  # cap at 20
        ],
    }
