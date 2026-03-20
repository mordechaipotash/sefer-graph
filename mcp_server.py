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
    instructions=(
        "Torah Citation Knowledge Graph — 1.9M+ citations across 60+ sefarim. "
        "Tools: search_citations, top_cited, citation_path, graph_stats, citation_types, co_cited, compare_sources, rare_finds. "
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
    """SQL-safe string escape — prevents injection."""
    if s is None:
        return ''
    s = str(s).replace("'", "''").replace('\x00', '')
    # Strip SQL injection patterns
    s = s.replace(';', '').replace('--', '').replace('/*', '').replace('*/', '')
    return s


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
    all_rows = []  # collect rows from both directions for SHELET

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
                all_rows.extend(rows)

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
                all_rows.extend(rows)

        result = "\n\n".join(parts) if parts else f"No citations found for '{ref}'"
        ms = int((time.time() - t0) * 1000)
        log_query("search_citations", {"ref": ref, "direction": direction, "min_confidence": min_confidence}, result[:200], total, ms)

        # SHELET: contextual next actions based on what we found
        first_target = (all_rows[0].get('target_ref') or all_rows[0].get('source_ref')) if all_rows else None
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


# ── Tool 6: Co-citation analysis ──────────────────────

@mcp.tool()
def co_cited(
    ref: str = "",
    top_n: int = 20,
    min_co_occurrences: int = 3
) -> str:
    """Find which Torah references or authorities are most frequently cited TOGETHER.
    When two targets appear in the same source segment, they are "co-cited" — meaning
    the author is engaging with both simultaneously (comparing, contrasting, synthesizing).
    
    Args:
        ref: Optional — focus on co-citations involving this reference
        top_n: Number of pairs to return (default 20)
        min_co_occurrences: Minimum times they must appear together (default 3)
    """
    t0 = time.time()
    try:
        if not ref:
            return shelet(
                "⚠️ `co_cited` requires a `ref` parameter — the global co-citation matrix is too large (1.9M×1.9M self-join).\n\n"
                "**Usage:** `co_cited(ref='Berakhot 2a')` — finds what's most often cited alongside Berakhot 2a.",
                [
                    "Try: co_cited(ref='Berakhot 2a')",
                    "Try: co_cited(ref='Leviticus 19:18')",
                    "See graph overview: graph_stats()"
                ]
            )

        ref_esc = _sq(ref)
        rows = query(f"""
            WITH my_sources AS (
                SELECT DISTINCT source_ref
                FROM sefer.citations_public
                WHERE target_ref ILIKE '%{ref_esc}%' AND confidence >= 0.7
            ),
            co AS (
                SELECT c.target_ref as paired_with, COUNT(*) as times_together,
                       ROUND(AVG(c.confidence)::numeric, 2) as avg_conf
                FROM sefer.citations_public c
                JOIN my_sources ms ON c.source_ref = ms.source_ref
                WHERE c.target_ref NOT ILIKE '%{ref_esc}%'
                  AND c.confidence >= 0.7
                GROUP BY c.target_ref
                HAVING COUNT(*) >= {min_co_occurrences}
                ORDER BY times_together DESC
                LIMIT {top_n}
            )
            SELECT * FROM co
        """)
        
        lines = [f"**Co-cited with '{ref}'** (appears together in the same source):\n"]
        for i, r in enumerate(rows, 1):
            lines.append(f"{i}. **{r['paired_with']}** — {r['times_together']}× together (avg conf {r['avg_conf']})")
        result = "\n".join(lines) if rows else f"No co-citation data found for '{ref}'"
        
        ms = int((time.time() - t0) * 1000)
        log_query("co_cited", {"ref": ref, "top_n": top_n}, result[:200], len(rows), ms)
        
        top_pair = rows[0] if rows else None
        opts = []
        if top_pair:
            opts.append(f"Explore the top pair: search_citations(ref='{top_pair['paired_with']}')")
            opts.append(f"Find path between them: citation_path(from_ref='{ref}', to_ref='{top_pair['paired_with']}')")
        opts.append("See overall stats: graph_stats()")
        return shelet(result, opts[:3])
        
    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        log_query("co_cited", {"ref": ref}, None, 0, ms, str(e))
        return f"Error: {e}"


# ── Tool 7: Compare sources ──────────────────────────

@mcp.tool()
def compare_sources(
    source_a: str,
    source_b: str,
    aspect: str = "targets"
) -> str:
    """Compare citation patterns between two Torah texts or corpora.
    Reveals how different authors approach the same material differently.
    
    Args:
        source_a: First text/corpus (e.g. "Rambam", "Tosafot", "Shulchan Arukh OC")
        source_b: Second text/corpus (e.g. "Rashba", "Ran", "Mishnah Berurah")
        aspect: What to compare — "targets" (what they cite), "types" (how they cite), or "overlap" (shared vs unique citations)
    """
    t0 = time.time()
    a_esc = _sq(source_a)
    b_esc = _sq(source_b)
    
    try:
        if aspect == "types":
            rows_a = query(f"""
                SELECT citation_type, COUNT(*) as n,
                       ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(), 1) as pct
                FROM sefer.citations_public
                WHERE source_ref ILIKE '%{a_esc}%' AND confidence >= 0.7
                GROUP BY citation_type ORDER BY n DESC LIMIT 10
            """)
            rows_b = query(f"""
                SELECT citation_type, COUNT(*) as n,
                       ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(), 1) as pct
                FROM sefer.citations_public
                WHERE source_ref ILIKE '%{b_esc}%' AND confidence >= 0.7
                GROUP BY citation_type ORDER BY n DESC LIMIT 10
            """)
            
            lines = [f"**Citation DNA comparison:**\n"]
            lines.append(f"**{source_a}:**")
            for r in rows_a:
                lines.append(f"  {r['citation_type']}: {r['n']:,} ({r['pct']}%)")
            lines.append(f"\n**{source_b}:**")
            for r in rows_b:
                lines.append(f"  {r['citation_type']}: {r['n']:,} ({r['pct']}%)")
            result = "\n".join(lines)
            count = len(rows_a) + len(rows_b)
            
        elif aspect == "overlap":
            rows = query(f"""
                WITH a_targets AS (
                    SELECT DISTINCT target_ref FROM sefer.citations_public
                    WHERE source_ref ILIKE '%{a_esc}%' AND confidence >= 0.7
                ),
                b_targets AS (
                    SELECT DISTINCT target_ref FROM sefer.citations_public
                    WHERE source_ref ILIKE '%{b_esc}%' AND confidence >= 0.7
                )
                SELECT 
                    (SELECT COUNT(*) FROM a_targets) as a_total,
                    (SELECT COUNT(*) FROM b_targets) as b_total,
                    (SELECT COUNT(*) FROM a_targets WHERE target_ref IN (SELECT target_ref FROM b_targets)) as shared,
                    (SELECT COUNT(*) FROM a_targets WHERE target_ref NOT IN (SELECT target_ref FROM b_targets)) as a_only,
                    (SELECT COUNT(*) FROM b_targets WHERE target_ref NOT IN (SELECT target_ref FROM a_targets)) as b_only
            """)
            r = rows[0]
            shared_pct = round(100 * int(r['shared']) / max(min(int(r['a_total']), int(r['b_total'])), 1), 1)
            result = f"""**Citation overlap: {source_a} vs {source_b}**

{source_a}: {r['a_total']:,} unique targets
{source_b}: {r['b_total']:,} unique targets
Shared: {r['shared']:,} targets ({shared_pct}% overlap)
Only in {source_a}: {r['a_only']:,}
Only in {source_b}: {r['b_only']:,}"""
            count = 1
            
        else:  # targets
            rows = query(f"""
                WITH a_top AS (
                    SELECT target_ref, COUNT(*) as a_count
                    FROM sefer.citations_public
                    WHERE source_ref ILIKE '%{a_esc}%' AND confidence >= 0.7
                    GROUP BY target_ref ORDER BY a_count DESC LIMIT 15
                ),
                b_counts AS (
                    SELECT target_ref, COUNT(*) as b_count
                    FROM sefer.citations_public
                    WHERE source_ref ILIKE '%{b_esc}%' AND confidence >= 0.7
                    GROUP BY target_ref
                )
                SELECT a.target_ref, a.a_count, COALESCE(b.b_count, 0) as b_count,
                       a.a_count - COALESCE(b.b_count, 0) as diff
                FROM a_top a LEFT JOIN b_counts b ON a.target_ref = b.target_ref
                ORDER BY a.a_count DESC
            """)
            
            lines = [f"**Top targets: {source_a} vs {source_b}**\n"]
            lines.append(f"{'Reference':<40} {source_a:>8} {source_b:>8}  Diff")
            for r in rows:
                diff = r['diff']
                indicator = "→" if diff > 0 else "←" if diff < 0 else "="
                lines.append(f"  {r['target_ref']:<38} {r['a_count']:>8} {r['b_count']:>8}  {indicator}{abs(diff)}")
            result = "\n".join(lines)
            count = len(rows)
        
        ms = int((time.time() - t0) * 1000)
        log_query("compare_sources", {"a": source_a, "b": source_b, "aspect": aspect}, result[:200], count, ms)
        
        return shelet(result, [
            f"Compare citation types: compare_sources(source_a='{source_a}', source_b='{source_b}', aspect='types')" if aspect != "types" else f"Compare overlap: compare_sources(source_a='{source_a}', source_b='{source_b}', aspect='overlap')",
            f"Co-citations in {source_a}: co_cited(ref='{source_a}')",
            f"Search {source_b}: search_citations(ref='{source_b}')"
        ])
        
    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        log_query("compare_sources", {"a": source_a, "b": source_b}, None, 0, ms, str(e))
        return f"Error: {e}"


# ── Tool 8: Rare finds / anomalies ──────────────────

@mcp.tool()
def rare_finds(
    corpus_filter: str = "",
    max_occurrences: int = 2,
    min_confidence: float = 0.8,
    limit: int = 15
) -> str:
    """Surface rare and unique citations — texts cited only once or twice in the entire graph.
    These are the hidden gems: unusual allusions, unexpected connections, singular references
    that reveal an author's unique perspective.
    
    Args:
        corpus_filter: Optional — filter by source corpus (e.g. "Meiri", "Rambam")
        max_occurrences: Maximum times a target can be cited to count as "rare" (default 2)
        min_confidence: Minimum confidence to avoid noise (default 0.8)
        limit: Number of results (default 15)
    """
    t0 = time.time()
    try:
        where = [f"confidence >= {min_confidence}"]
        if corpus_filter:
            where.append(f"source_ref ILIKE '%{_sq(corpus_filter)}%'")
        
        rows = query(f"""
            WITH rare AS (
                SELECT target_ref, COUNT(*) as times_cited,
                       array_agg(DISTINCT source_ref) as cited_by,
                       array_agg(DISTINCT citation_type) as types,
                       MAX(evidence_hebrew) as sample_evidence,
                       MAX(confidence) as best_conf
                FROM sefer.citations_public
                WHERE {' AND '.join(where)}
                GROUP BY target_ref
                HAVING COUNT(*) <= {max_occurrences}
                ORDER BY best_conf DESC, times_cited ASC
                LIMIT {limit}
            )
            SELECT * FROM rare
        """)
        
        lines = [f"**Rare citations{f' in {corpus_filter}' if corpus_filter else ''}** (≤{max_occurrences} occurrences, ≥{min_confidence} confidence):\n"]
        for i, r in enumerate(rows, 1):
            cited_by = r.get('cited_by', '')
            if isinstance(cited_by, list):
                cited_by = cited_by[0] if cited_by else ''
            elif isinstance(cited_by, str):
                cited_by = cited_by.strip('{}').split(',')[0] if cited_by else ''
            types = r.get('types', '')
            if isinstance(types, list):
                types = types[0] if types else ''
            elif isinstance(types, str):
                types = types.strip('{}').split(',')[0] if types else ''
            ev = (r.get('sample_evidence') or '')[:80]
            lines.append(f"{i}. **{r['target_ref']}** — {r['times_cited']}× [{types}] conf {r['best_conf']:.2f}")
            lines.append(f"   Found in: {cited_by}")
            if ev:
                lines.append(f"   Evidence: {ev}")
        
        result = "\n".join(lines) if rows else "No rare citations found with these filters."
        ms = int((time.time() - t0) * 1000)
        log_query("rare_finds", {"corpus_filter": corpus_filter, "max_occ": max_occurrences}, result[:200], len(rows), ms)
        
        top = rows[0] if rows else None
        return shelet(result, [
            f"Explore this rare find: search_citations(ref='{top['target_ref']}')" if top else "Try broader search: rare_finds(max_occurrences=5)",
            f"Compare: rare_finds(corpus_filter='Meiri')" if corpus_filter != "Meiri" else "Try Rambam: rare_finds(corpus_filter='Rambam')",
            "See overall stats: graph_stats()"
        ])
        
    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        log_query("rare_finds", {"corpus_filter": corpus_filter}, None, 0, ms, str(e))
        return f"Error: {e}"


if __name__ == "__main__":
    mcp.run()
