# grandmofty-mcp

MCP server for querying Metal-Organic Framework (MOF) databases. Exposes MOF properties, isotherms, and structural data via the Model Context Protocol.

## Features

- **MOF search** — query by name, topology, metal type, pore properties
- **Multi-database** — CoRE-2014, CoRE-2019, CoRE-2025, IZA, PCOD
- **Isotherm data** — gas adsorption isotherms
- **SQLAlchemy backend** — SQLite (default) or PostgreSQL
- **MCP protocol** — compatible with any MCP-aware LLM client

## Installation

```bash
uv pip install -e .
# With PostgreSQL support:
uv pip install -e ".[postgres]"
```

## Usage

```bash
grandmofty-mcp   # starts the MCP server
```

## Dependencies

- **chemdb-common** — shared database models and CLI
- **mcp** — Model Context Protocol server framework
- **mofdb-client** — client for MOF database APIs

## License

GPL-3.0-or-later — see [LICENSE](LICENSE).
