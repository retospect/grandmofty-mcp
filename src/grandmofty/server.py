"""MCP server for grandMOFty — one tool: mofty.

Queries Metal-Organic Framework databases (CoRE MOF, hMOF, QMOF, CSD, MP).
All data is local (Postgres or SQLite). Run ``chemdb sync mofty`` first.
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from grandmofty import tool

mcp = FastMCP("mofty")


@mcp.tool()
def mofty(
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

    id: identifier — "refcode:SAHYIK", "mofid:[Zn]...", "name:MOF-5", or bare string
    query: FTS5 full-text search ("CO2 uptake", always AND with filters)
    elements: element filter, AND logic ("Zn", "Zn,Cu" = contains both)
    lcd: largest cavity diameter, Å  ("3..9", ">5", "<12")
    pld: pore limiting diameter, Å
    sa: surface area, m²/g
    vf: void fraction, 0–1
    isotherms: gas filter ("CO2", "CH4") — search: MOFs with data; detail: scope display
    database: subset filter ("CoRE", "hMOF", "CSD", "QMOF", "MP")
    sort: sort order ("lcd", "!sa", "!lcd,sa", "relevance")
    page: page number (10 results/page)

    No args → shape of entire database (collections, property ranges, top elements).
    id provided → single MOF detail (id wins; other filters ignored except isotherms).
    Any filter → search with results + shape on page 1.
    """
    return tool.get(
        id=id,
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


def main():
    """Run the MCP server (stdio transport)."""
    mcp.run()


if __name__ == "__main__":
    main()
