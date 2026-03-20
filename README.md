# 📜 Sefer-Graph

**Torah Citation Knowledge Graph** — 1.9M+ citations across 60+ sefarim, accessible via MCP.

Ask your AI: *"What does the Rambam cite when discussing damages?"* and get structured, sourced answers from the entire graph of Torah literature.

## 🚀 Quick Start (Claude Desktop / any MCP client)

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "sefer-graph": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/mordechaipotash/sefer-graph", "python", "mcp_server.py"],
      "env": {
        "SEFER_USER_ID": "your_name_here"
      }
    }
  }
}
```

Or clone and run locally:

```bash
git clone https://github.com/mordechaipotash/sefer-graph.git
cd sefer-graph
python -m venv .venv && source .venv/bin/activate
pip install mcp duckdb httpx
python mcp_server.py
```

## 🔧 Tools

| Tool | Description |
|------|-------------|
| `search_citations` | Find citations to/from any Torah reference |
| `top_cited` | Most frequently cited references in the graph |
| `citation_path` | Find connection chains between two texts |
| `graph_stats` | Overview statistics of the entire graph |
| `citation_types` | Distribution of citation types |
| `query_sql` | Raw SQL for power users |

Every tool response includes **3 suggested next actions** (SHELET protocol) to guide exploration.

## 📊 Alpha Test

**This is an alpha.** Every query is logged (tool, params, results, latency) to help us improve the graph.

Your `SEFER_USER_ID` is used only for usage analytics — no personal data collected.

Live dashboard: [dashboard.html](dashboard.html)

## 📐 The Graph

- **1,896,325 citations** extracted via LLM analysis
- **60+ sefarim** from Tanakh through Acharonim
- **Citation types:** explicit_verse, explicit_talmud, allusion, legal_derivation, and more
- **Confidence scores** (0.0-1.0) on every citation
- **Hebrew evidence** snippets showing the actual citation text

## 🏗️ Architecture

```
Claude Desktop → MCP Server → Supabase (Postgres) → sefer.citations
                                    ↓
                            sefer.query_log (100% logged)
```

Data backend: Supabase (hosted Postgres with indexes). Queries are ~10-50ms.

## License

MIT
