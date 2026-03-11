"""get implementation — formats query results as markdown strings."""

from __future__ import annotations

from typing import Any

from chemdb.cite import format_citation
from chemdb.config import ChemdbConfig, load_config
from chemdb.db import make_engine, make_session
from chemdb.errors import ChemdbError

from grandmofty.db.query import PAGE_SIZE, get_by_id, get_shape, search
from grandmofty.db.schema import SCHEMA

_config: ChemdbConfig | None = None
_SessionFactory = None


def _get_session():
    global _config, _SessionFactory
    if _SessionFactory is None:
        _config = load_config()
        engine = make_engine(_config, SCHEMA)
        _SessionFactory = make_session(engine)
    return _SessionFactory()


def get(
    id: str = "",
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
) -> str:
    """Query Metal-Organic Framework databases.

    Returns formatted markdown with results, shape, and hints.
    """
    try:
        session = _get_session()

        # ID lookup mode
        if id:
            data = get_by_id(session, id)
            return _format_id_result(data, isotherms=isotherms)

        # No args → shape of entire database
        has_filters = any(
            [query, elements, lcd, pld, sa, vf, isotherms, database, sort]
        )
        if not has_filters and page == 1:
            data = get_shape(session)
            return _format_shape(data)

        # Filter search
        data = search(
            session,
            query=query,
            elements=elements,
            lcd=lcd,
            pld=pld,
            sa=sa,
            vf=vf,
            isotherms=isotherms,
            database=database,
            sort=sort,
            page=page,
        )
        return _format_search(data, elements=elements, lcd=lcd, pld=pld, sa=sa, vf=vf)

    except ChemdbError as exc:
        return exc.to_markdown()
    except Exception as exc:
        return f"⚠ Internal error: {exc}"
    finally:
        if "_SessionFactory" in dir() and _SessionFactory:
            pass  # session auto-closes via context


# ── Formatters ──────────────────────────────────────────────────────


def _format_id_result(data: dict[str, Any], isotherms: str = "") -> str:
    """Format single MOF detail or disambiguation list."""
    if data.get("type") == "disambiguation":
        return _format_disambiguation(data)

    lines = []
    name = data.get("name", "?")
    refcode = data.get("refcode", "")
    header = f"## {name}"
    if refcode:
        header += f" (refcode {refcode})"
    lines.append(header)
    lines.append("")

    if data.get("formula"):
        lines.append(f"- **Formula:** {data['formula']}")
    topo_sg = []
    if data.get("topology"):
        topo_sg.append(f"**Topology:** {data['topology']}")
    if data.get("space_group"):
        topo_sg.append(f"**Space group:** {data['space_group']}")
    if topo_sg:
        lines.append(f"- {' | '.join(topo_sg)}")

    props = []
    if data.get("lcd") is not None:
        props.append(f"**LCD:** {data['lcd']:.1f} Å")
    if data.get("pld") is not None:
        props.append(f"**PLD:** {data['pld']:.1f} Å")
    if data.get("sa") is not None:
        props.append(f"**SA:** {data['sa']:.0f} m²/g")
    if data.get("vf") is not None:
        props.append(f"**VF:** {data['vf']:.2f}")
    if props:
        lines.append(f"- {' | '.join(props)}")

    if data.get("database"):
        lines.append(f"- **Database:** {data['database']}")

    cite = format_citation(data.get("doi"), data.get("database", ""))
    if cite:
        lines.append(cite)

    # Isotherms
    isos = data.get("isotherms", [])
    if isotherms:
        isos = [i for i in isos if i["gas"].lower() == isotherms.lower()]
    if isos:
        lines.append("")
        lines.append(f"### Isotherms ({len(isos)} available)")
        lines.append("| Gas | T (K) | Uptake | Force field |")
        lines.append("|-----|-------|--------|-------------|")
        for iso in isos:
            uptake = ""
            if iso.get("uptake_min") is not None and iso.get("uptake_max") is not None:
                uptake = f"{iso['uptake_min']:.1f}–{iso['uptake_max']:.1f}"
                if iso.get("uptake_unit"):
                    uptake += f" {iso['uptake_unit']}"
            lines.append(
                f"| {iso['gas']} | {iso.get('temperature_k', '?')} | {uptake} | {iso.get('force_field', '?')} |"
            )

    # Hints
    lines.append("")
    if refcode:
        if not isotherms and isos:
            lines.append(
                f'→ get(id="{refcode}", isotherms="CO2") for specific gas'
            )

    return "\n".join(lines)


def _format_disambiguation(data: dict[str, Any]) -> str:
    """Format name disambiguation list."""
    lines = [f'## {data["count"]} matches for name "{data["name"]}" — pick one:']
    lines.append("")

    for i, m in enumerate(data["matches"], 1):
        parts = [m.get("name", "?")]
        if m.get("refcode"):
            parts[0] += f" ({m['refcode']})"
        parts.append(f"— {m.get('database', '?')}")
        if m.get("formula"):
            parts.append(f"| {m['formula']}")
        if m.get("lcd") is not None:
            parts.append(f"| LCD {m['lcd']:.1f} Å")
        lines.append(f"{i}. {' '.join(parts)}")

    lines.append("")
    for i, m in enumerate(data["matches"][:3], 1):
        if m.get("refcode"):
            lines.append(f'→ get(id="refcode:{m["refcode"]}") for #{i}')

    return "\n".join(lines)


def _format_shape(data: dict[str, Any]) -> str:
    """Format full database shape (no-arg call)."""
    total = data.get("total", 0)
    dbs = data.get("databases", {})
    n_collections = len(dbs)

    lines = [f"## grandMOFty — {n_collections} collections, ~{total:,} MOFs"]
    lines.append("")

    if dbs:
        lines.append("### Collections")
        lines.append("| Collection | MOFs |")
        lines.append("|------------|------|")
        for db, count in dbs.items():
            lines.append(f"| {db} | {count:,} |")

    props = data.get("properties", {})
    if props:
        lines.append("")
        lines.append("### Property ranges")
        lines.append("| Property | Min | Avg | Max | Unit |")
        lines.append("|----------|-----|-----|-----|------|")
        units = {"lcd": "Å", "pld": "Å", "sa": "m²/g", "vf": "—"}
        for key in ("lcd", "pld", "sa", "vf"):
            p = props.get(key, {})
            if p.get("min") is not None:
                lines.append(
                    f"| {key.upper()} | {p['min']:.1f} | {p['avg']:.1f} | {p['max']:.1f} | {units.get(key, '')} |"
                )

    lines.append("")
    lines.append("### Sortable fields")
    lines.append("lcd, pld, sa, vf, name")
    lines.append("")
    lines.append('→ get(elements="Zn") to browse zinc MOFs')
    lines.append('→ get(lcd="10..", sa=">2000") for large-pore, high-SA MOFs')
    lines.append('→ get(query="CO2 capture") for free-text search')

    return "\n".join(lines)


def _format_search(data: dict[str, Any], **filters) -> str:
    """Format search results with shape + hints."""
    total = data["total"]
    page = data["page"]
    start = (page - 1) * PAGE_SIZE + 1
    end = min(start + PAGE_SIZE - 1, total)

    lines = [f"## {total} MOFs matching (showing {start}–{end})"]
    lines.append("")

    # Shape on page 1
    shape = data.get("shape")
    if shape:
        lines.append("### Shape")
        props = shape.get("properties", {})
        for key in ("lcd", "pld", "sa", "vf"):
            p = props.get(key, {})
            if p.get("min") is not None:
                units = {"lcd": "Å", "pld": "Å", "sa": "m²/g", "vf": ""}
                lines.append(
                    f"  {key.upper()}  {p['min']:.1f}–{p['max']:.1f} {units.get(key, '')}  (mean {p['avg']:.1f})"
                )
        dbs = shape.get("databases", {})
        if dbs:
            db_parts = [f"{db} {count}" for db, count in dbs.items()]
            lines.append(f"  Databases: {' | '.join(db_parts)}")
        lines.append("")

    # Results
    for i, r in enumerate(data["results"], start):
        name = r.get("name", "?")
        refcode = r.get("refcode", "")
        label = f"{name} ({refcode})" if refcode else name
        parts = [label]
        if r.get("lcd") is not None:
            parts.append(f"LCD {r['lcd']:.1f} Å")
        if r.get("sa") is not None:
            parts.append(f"SA {r['sa']:.0f} m²/g")
        if r.get("database"):
            parts.append(r["database"])
        lines.append(f"{i:>2}. {' — '.join(parts[:1])} | {' | '.join(parts[1:])}")

    # Hints
    lines.append("")
    if data["results"]:
        first = data["results"][0]
        if first.get("refcode"):
            lines.append(f'→ get(id="{first["refcode"]}") for full details')
    if end < total:
        lines.append(f"→ get(..., page={page + 1}) for next {PAGE_SIZE}")

    return "\n".join(lines)
