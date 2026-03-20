# 📜 Sefer-Graph

**Torah Citation Knowledge Graph** — 1.9M+ citations across 60+ sefarim, accessible via MCP.

Ask your AI: *"What does the Rambam cite when discussing damages?"* — and get structured, sourced answers from the entire graph of Torah literature.

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
pip install mcp httpx
python mcp_server.py
```

## 🔧 Tools (8)

### Core Search
| Tool | Description |
|------|-------------|
| `search_citations` | Find citations to/from any Torah reference. Filter by direction (outgoing/incoming/both), confidence, and limit. |
| `top_cited` | Most frequently cited references in the graph. Filter by corpus or citation type. |
| `citation_path` | Find connection chains between two texts via graph traversal (up to 4 hops). |

### Analytics
| Tool | Description |
|------|-------------|
| `graph_stats` | Overview statistics — total citations, unique sources/targets, confidence distribution. |
| `citation_types` | Distribution of citation types (explicit_verse, explicit_talmud, allusion, etc.), optionally filtered. |

### Advanced
| Tool | Description |
|------|-------------|
| `co_cited` | Find references that are frequently cited *together* in the same source. Reveals hidden study partnerships and conceptual clusters. |
| `compare_sources` | Compare citation DNA between two texts/corpora. Three modes: targets (what they cite), types (how they cite), overlap (shared vs unique). |
| `rare_finds` | Surface rare citations (≤2 occurrences) — hidden gems, unusual allusions, unique perspectives. |

Every tool response includes **3 suggested next actions** (SHELET protocol) to guide exploration.

## 📊 Alpha Test

This is an alpha. Usage is logged (tool, params, result count, latency) to help improve the graph.

Your `SEFER_USER_ID` is used only for usage analytics — no personal data collected.

**Live dashboard:** [mordechaipotash.github.io/sefer-graph/dashboard.html](https://mordechaipotash.github.io/sefer-graph/dashboard.html)

## 📐 The Graph

- **1,896,325 citations** extracted via AI analysis
- **60+ sefarim** from Tanakh through Acharonim
- **Citation types:** explicit_verse, explicit_talmud, explicit_mishnah, named_position, back_reference, conceptual_dependency, allusion, legal_principle, and more
- **Confidence scores** (0.0–1.0) on every citation
- **Hebrew evidence** snippets showing the actual citation text

### Sample Insights

- **Mishnah Kiddushin 1:1** — cited 3,473× across all corpora. The most referenced Mishnah.
- **Shabbat 108b** — 4,637 engagements from 11 different texts. The #1 Rishonim battleground.
- **Leviticus 19:19** — 2,282 citations. One verse about mixtures that radiates across all of halacha.
- **Rashi** is 49% back-references. **Meiri** is 50% explicit Talmud. Their citation DNA is completely different.

## 🏗️ Architecture

```
Claude Desktop → MCP Server → Supabase (Postgres) → sefer.citations
                                    ↓
                            sefer.query_log (usage tracking)
```

Data backend: Supabase (hosted Postgres with indexes). Query execution: <1ms. Network round-trip depends on location.

## License

MIT
