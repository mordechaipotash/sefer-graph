#!/usr/bin/env python3
"""
sefer-graph MCP Server — Torah Citation Knowledge Graph
Dual mode: local DuckDB (fast) or Supabase (shareable + logged).

EVERY tool response ends with 3 SHELET options — guiding the user's next action.
EVERY query is 100% logged: who, what, when, how fast, what came back.

Install:  pip install mcp duckdb httpx
Run:      python mcp_server.py                    # Supabase mode (default)
          SEFER_MODE=local python mcp_server.py   # local DuckDB mode

Claude Desktop config:
  "sefer-graph": {
    "command": "uvx",
    "args": ["--from", "git+https://github.com/mordechaipotash/sefer-graph", "python", "mcp_server.py"],
    "env": {"SEFER_USER_ID": "your_name"}
  }
"""

import json, os, time, uuid
from pathlib import Path
from mcp.server.fastmcp import FastMCP

# ── Config ──────────────────────────────────────────────
MODE = os.environ.get("SEFER_MODE", "supabase")
DB_PATH = Path(__file__).parent / "data" / "analytics" / "sefer_graph.duckdb"

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://xsjyfneizfkbitmzbrta.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhzanlmbmVpemZrYml0bXpicnRhIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTE4MDcxMzUsImV4cCI6MjA2NzM4MzEzNX0.wgrPV43hdznmYGYQIRgH2gxl_mByAS52S3d7vQLgYB8"
)
USER_ID = os.environ.get("SEFER_USER_ID", "anonymous")
SESSION_ID = str(uuid.uuid4())[:8]  # Unique per MCP server session

mcp = FastMCP(
    "sefer-graph",
    description=(
        "Torah Citation Knowledge Graph — 1.9M+ citations across 60+ sefarim. "
        "Tools: search_citations, top_cited, citation_path, graph_stats, citation_types. "
        "Alpha test — usage is logged for improvement."
    )
)


# ── Database Backends ───────────────────────────────────

def _local_query(sql: str):
    """Query local DuckDB."""
    import duckdb
    db = duckdb.connect(str(DB_PATH), read_only=True)
    result = db.execute(sql).fetchall()
    cols = [d[0] for d in db.description]
    db.close()
    return [dict(zip(cols, row)) for row in result]


def _supabase_query(sql: str):
    """Query Supabase via management API (requires SUPABASE_PAT env var)."""
    import httpx
    mgmt_token = os.environ.get("SUPABASE_PAT", "")
    if not mgmt_token:
        raise Exception("SUPABASE_PAT env var required for SQL queries. Set it to your Supabase Personal Access Token.")
    resp = httpx.post(
        f"https://api.supabase.com/v1/projects/xsjyfneizfkbitmzbrta/database/query",
        headers={"Authorization": f"Bearer {mgmt_token}"},
        json={"query": sql},
        timeout=30.0
    )
    if resp.status_code in (200, 201):
        return resp.json()
    raise Exception(f"Supabase error {resp.status_code}: {resp.text[:200]}")


def query(sql: str):
    """Route to the configured backend."""
    return _local_query(sql) if MODE == "local" else _supabase_query(sql)


def _sq(s):
    """SQL-safe string escape."""
    if s is None:
        return ''
    return str(s).replace("'", "''").replace('\x00', '')


def log_query(tool_name: str, params: dict, result_summary: str,
              result_count: int, latency_ms: int, error: str = None):
    """Log every query to sefer.query_log via PostgREST (uses anon key, safe)."""
    try:
        import httpx
        httpx.post(
            f"{SUPABASE_URL}/rest/v1/query_log",
            headers={
                "apikey": SUPABASE_ANON_KEY,
                "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                "Content-Profile": "sefer",
                "Content-Type": "application/json",
                "Prefer": "return=minimal"
            },
            json={
                "user_id": USER_ID,
                "session_id": SESSION_ID,
                "tool_name": tool_name,
                "params": params,
                "result_summary": (result_summary or "")[:500],
                "result_count": result_count,
                "latency_ms": latency_ms,
                "error": (str(error)[:500]) if error else None
            },
            timeout=10.0
        )
    except Exception:
        pass  # Never let logging break a tool


# ── SHELET Protocol ─────────────────────────────────────
# Every response ends with 3 contextual next-action suggestions.
# Named after the Hebrew word שלט (sign/menu) — guiding the user's path.

def shelet(result: str, options: list[str]) -> str:
    """Append SHELET 3-option menu to every tool response."""
    menu = "\n\n---\n**Next steps:**"
    for i, opt in enumerate(options[:3], 1):
        menu += f"\n[{i}] {opt}"
    return result + menu


# ── Tool 1: Search citations ──────────────────────────

@mcp.tool()
def search_citations(
    ref: str,
    direction: str = "both",
    min_confidence: float = 0.7,
    limit: int = 30
) -> str:
    """Search the Torah citation graph for connections to/from a text reference.
    
    Args:
        ref: Text reference (e.g. "Berakhot 2a", "Mishnah Kiddushin 1:1", "Leviticus 19:19")
        direction: "outgoing" (what this text cites), "incoming" (what cites this text), or "both"
        min_confidence: Minimum confidence threshold (0.0-1.0, default 0.7)
        limit: Max results to return (default 30)
    
    Returns results with SHELET menu for next actions.
    """
    t0 = time.time()
    parts = []
    total = 0
    ref_esc = _sq(ref)

    try:
        if direction in ("outgoing", "both"):
            rows = query(f"""
                SELECT target_ref, citation_type, confidence, evidence_hebrew
                FROM sefer.citations_public
                WHERE source_ref ILIKE '%{ref_esc}%' AND confidence >= {min_confidence}
                ORDER BY confidence DESC LIMIT {limit}
            """)
            if rows:
                out = [f"**Outgoing FROM '{ref}'** ({len(rows)}):"]
                for r in rows:
                    ev = (r.get('evidence_hebrew') or '')[:60]
                    out.append(f"  → {r['target_ref']} [{r['citation_type']}, {r['confidence']:.2f}] {ev}")
                parts.append("\n".join(out))
                total += len(rows)

        if direction in ("incoming", "both"):
            rows = query(f"""
                SELECT source_ref, citation_type, confidence, evidence_hebrew
                FROM sefer.citations_public
                WHERE target_ref ILIKE '%{ref_esc}%' AND confidence >= {min_confidence}
                ORDER BY confidence DESC LIMIT {limit}
            """)
            if rows:
                inc = [f"**Incoming TO '{ref}'** ({len(rows)}):"]
                for r in rows:
                    ev = (r.get('evidence_hebrew') or '')[:60]
                    inc.append(f"  ← {r['source_ref']} [{r['citation_type']}, {r['confidence']:.2f}] {ev}")
                parts.append("\n".join(inc))
                total += len(rows)

        result = "\n\n".join(parts) if parts else f"No citations found for '{ref}'"
        ms = int((time.time() - t0) * 1000)
        log_query("search_citations", {"ref": ref, "direction": direction, "min_confidence": min_confidence}, result[:200], total, ms)

        # SHELET: contextual next actions based on what we found
        first_target = rows[0]['target_ref'] if rows else None
        opts = []
        if total > 0 and first_target:
            opts.append(f"Explore the top result: search_citations(ref='{first_target}')")
        if direction == "both":
            opts.append(f"Show only outgoing: search_citations(ref='{ref}', direction='outgoing')")
        opts.append(f"See citation type breakdown: citation_types(ref_filter='{ref}')")
        if len(opts) < 3:
            opts.append("See overall graph stats: graph_stats()")

        return shelet(result, opts)

    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        log_query("search_citations", {"ref": ref}, None, 0, ms, str(e))
        return f"Error: {e}"


# ── Tool 2: Top cited ─────────────────────────────────

@mcp.tool()
def top_cited(
    corpus_filter: str = "",
    citation_type_filter: str = "",
    limit: int = 20
) -> str:
    """Get the most frequently cited Torah references in the entire graph.
    
    Args:
        corpus_filter: Filter sources by substring (e.g. "tosafot", "rambam", "rashi")
        citation_type_filter: Filter by citation type (e.g. "explicit_verse", "allusion", "talmud")
        limit: Number of results (default 20)
    """
    t0 = time.time()
    try:
        where = ["confidence >= 0.7"]
        if corpus_filter:
            where.append(f"source_ref ILIKE '%{_sq(corpus_filter)}%'")
        if citation_type_filter:
            where.append(f"citation_type ILIKE '%{_sq(citation_type_filter)}%'")

        rows = query(f"""
            SELECT target_ref, COUNT(*) as times_cited,
                   COUNT(DISTINCT source_ref) as unique_sources,
                   ROUND(AVG(confidence)::numeric, 2) as avg_conf
            FROM sefer.citations_public
            WHERE {' AND '.join(where)}
            GROUP BY target_ref
            ORDER BY times_cited DESC LIMIT {limit}
        """)

        lines = [f"**Top {len(rows)} most cited{f' (filtered: {corpus_filter or citation_type_filter})' if corpus_filter or citation_type_filter else ''}:**\n"]
        for i, r in enumerate(rows, 1):
            lines.append(f"{i}. **{r['target_ref']}** — {r['times_cited']:,}× from {r['unique_sources']:,} sources (avg {r['avg_conf']})")

        result = "\n".join(lines)
        ms = int((time.time() - t0) * 1000)
        log_query("top_cited", {"corpus_filter": corpus_filter, "type_filter": citation_type_filter}, result[:200], len(rows), ms)

        top_ref = rows[0]['target_ref'] if rows else "Berakhot 2a"
        return shelet(result, [
            f"Deep dive into #{1}: search_citations(ref='{top_ref}')",
            "Filter by corpus: top_cited(corpus_filter='rambam')" if not corpus_filter else "Remove filter: top_cited()",
            "See citation type distribution: citation_types()"
        ])

    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        log_query("top_cited", {"corpus_filter": corpus_filter}, None, 0, ms, str(e))
        return f"Error: {e}"


# ── Tool 3: Citation path ─────────────────────────────

@mcp.tool()
def citation_path(from_ref: str, to_ref: str, max_hops: int = 3) -> str:
    """Find how two Torah texts are connected through citation chains.
    Uses graph traversal to discover indirect connections.
    
    Args:
        from_ref: Starting text reference
        to_ref: Target text reference  
        max_hops: Maximum chain length (1-4, default 3). Higher = slower.
    """
    t0 = time.time()
    try:
        rows = query(f"""
            WITH RECURSIVE path AS (
                SELECT source_ref, target_ref, citation_type, confidence,
                       1 as hop, ARRAY[source_ref, target_ref] as chain
                FROM sefer.citations_public
                WHERE source_ref ILIKE '%{_sq(from_ref)}%' AND confidence >= 0.7
                UNION ALL
                SELECT p.target_ref, c.target_ref, c.citation_type, c.confidence,
                       p.hop + 1, p.chain || c.target_ref
                FROM path p
                JOIN sefer.citations_public c ON c.source_ref = p.target_ref
                WHERE p.hop < {min(max_hops, 4)} AND c.confidence >= 0.7
                  AND NOT c.target_ref = ANY(p.chain)
            )
            SELECT chain, hop, citation_type, confidence
            FROM path WHERE target_ref ILIKE '%{_sq(to_ref)}%'
            ORDER BY hop, confidence DESC LIMIT 5
        """)

        if not rows:
            result = f"No path found from '{from_ref}' to '{to_ref}' within {max_hops} hops."
            opts = [
                f"Search citations for start: search_citations(ref='{from_ref}')",
                f"Search citations for end: search_citations(ref='{to_ref}')",
                f"Try more hops: citation_path(from_ref='{from_ref}', to_ref='{to_ref}', max_hops={min(max_hops+1,4)})"
            ]
        else:
            lines = [f"**Paths: {from_ref} → {to_ref}**\n"]
            for r in rows:
                chain = r.get('chain', [])
                if isinstance(chain, str):
                    chain = chain.strip('{}').split(',')
                lines.append(f"  {' → '.join(chain)} ({r['hop']} hops, {r['citation_type']}, conf {r['confidence']:.2f})")
            result = "\n".join(lines)
            mid = chain[len(chain)//2] if chain and len(chain) > 2 else None
            opts = [
                f"Explore the midpoint: search_citations(ref='{mid}')" if mid else "See graph stats: graph_stats()",
                f"Reverse path: citation_path(from_ref='{to_ref}', to_ref='{from_ref}')",
                f"What else cites the target? search_citations(ref='{to_ref}', direction='incoming')"
            ]

        ms = int((time.time() - t0) * 1000)
        log_query("citation_path", {"from": from_ref, "to": to_ref, "max_hops": max_hops}, result[:200], len(rows), ms)
        return shelet(result, opts)

    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        log_query("citation_path", {"from": from_ref, "to": to_ref}, None, 0, ms, str(e))
        return f"Error: {e}"


# ── Tool 4: Graph stats ───────────────────────────────

@mcp.tool()
def graph_stats() -> str:
    """Overview statistics of the sefer-graph Torah citation database.
    Shows total citations, unique sources/targets, confidence distribution, and alpha status."""
    t0 = time.time()
    try:
        stats = query("""
            SELECT COUNT(*) as total,
                   COUNT(DISTINCT source_ref) as sources,
                   COUNT(DISTINCT target_ref) as targets,
                   ROUND(AVG(confidence)::numeric, 3) as avg_conf
            FROM sefer.citations_public
        """)[0]

        conf = query("""
            SELECT 
                COUNT(*) FILTER (WHERE confidence >= 0.9) as high,
                COUNT(*) FILTER (WHERE confidence >= 0.7 AND confidence < 0.9) as med,
                COUNT(*) FILTER (WHERE confidence < 0.7) as low
            FROM sefer.citations_public
        """)[0]

        types = query("""
            SELECT citation_type, COUNT(*) as n
            FROM sefer.citations_public GROUP BY citation_type ORDER BY n DESC LIMIT 5
        """)
        type_str = ", ".join(f"{t['citation_type']}={t['n']:,}" for t in types)

        result = f"""**Sefer-Graph: Torah Citation Knowledge Graph**

📊 **{stats['total']:,} citations** connecting {stats['sources']:,} sources → {stats['targets']:,} targets
📈 **Confidence:** {conf['high']:,} high (≥0.9) | {conf['med']:,} medium (0.7-0.9) | {conf['low']:,} low (<0.7)
🔬 **Average confidence:** {stats['avg_conf']}
📋 **Top types:** {type_str}
🏗️ **Status:** Alpha — every query you make is logged for analysis & improvement"""

        ms = int((time.time() - t0) * 1000)
        log_query("graph_stats", {}, result[:200], 1, ms)

        return shelet(result, [
            "See most cited references: top_cited()",
            "See citation type breakdown: citation_types()",
            "Search a specific text: search_citations(ref='...')"
        ])

    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        log_query("graph_stats", {}, None, 0, ms, str(e))
        return f"Error: {e}"


# ── Tool 5: Citation types ────────────────────────────

@mcp.tool()
def citation_types(ref_filter: str = "") -> str:
    """Distribution of citation types in the graph, optionally filtered.
    
    Args:
        ref_filter: Optional — filter by reference substring (e.g. "rambam", "genesis")
    """
    t0 = time.time()
    try:
        where = ""
        if ref_filter:
            where = f"WHERE source_ref ILIKE '%{_sq(ref_filter)}%' OR target_ref ILIKE '%{_sq(ref_filter)}%'"

        rows = query(f"""
            SELECT citation_type, COUNT(*) as n,
                   ROUND(100.0*COUNT(*) / SUM(COUNT(*)) OVER(), 1) as pct,
                   ROUND(AVG(confidence)::numeric, 3) as avg_conf
            FROM sefer.citations_public {where}
            GROUP BY citation_type ORDER BY n DESC LIMIT 15
        """)

        lines = [f"**Citation types{f' for «{ref_filter}»' if ref_filter else ''}:**\n"]
        for r in rows:
            bar = "█" * max(1, int(float(r['pct']) / 5))
            lines.append(f"  {r['citation_type']}: {r['n']:,} ({r['pct']}%) {bar} avg conf {r['avg_conf']}")

        result = "\n".join(lines)
        ms = int((time.time() - t0) * 1000)
        log_query("citation_types", {"ref_filter": ref_filter}, result[:200], len(rows), ms)

        top_type = rows[0]['citation_type'] if rows else ""
        return shelet(result, [
            f"See top cited for this type: top_cited(citation_type_filter='{top_type}')",
            "Search a specific reference: search_citations(ref='...')",
            "See overall stats: graph_stats()"
        ])

    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        log_query("citation_types", {"ref_filter": ref_filter}, None, 0, ms, str(e))
        return f"Error: {e}"


# Raw SQL tool removed — users interact only through the 5 structured tools above.
# This prevents them from discovering internal schema details (model column, etc.)


if __name__ == "__main__":
    mcp.run()
