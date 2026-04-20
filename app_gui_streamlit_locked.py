"""
LeadCollector – Streamlit GUI v3
Run with:  streamlit run app_gui_streamlit.py
"""

from __future__ import annotations

import os
import sys
import signal
import subprocess
import time
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg
import streamlit as st
from psycopg.rows import dict_row

# ─────────────────────────────────────────────
#  Paths
# ─────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
SRC_ROOT     = PROJECT_ROOT / "app" / "src"
SCRIPTS_DIR  = SRC_ROOT / "lc"
DEFAULT_REG  = PROJECT_ROOT / "app" / "registry.yaml"
LOG_DIR      = PROJECT_ROOT / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────
#  Page config
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="RCG LeadCollector",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
#  Custom CSS
# ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');
html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
.stApp { background-color: #0d0f12; color: #c8cdd6; }
[data-testid="stSidebar"] { background-color: #111419; border-right: 1px solid #1e2330; }

[data-testid="stMetric"] { background: #151820; border: 1px solid #1e2330; border-radius: 8px; padding: 16px 20px; }
[data-testid="stMetricLabel"] { color: #6b7385; font-size: 11px; letter-spacing: 0.08em; text-transform: uppercase; }
[data-testid="stMetricValue"] { color: #e2e6ef; font-family: 'IBM Plex Mono', monospace; font-size: 2rem; }
[data-testid="stMetricDelta"] { font-family: 'IBM Plex Mono', monospace; }

.stButton > button {
    background: #1a1e2a; color: #c8cdd6; border: 1px solid #2a3045; border-radius: 6px;
    font-family: 'IBM Plex Mono', monospace; font-size: 13px; padding: 8px 18px; transition: all 0.15s;
}
.stButton > button:hover { background: #222840; border-color: #4a6fa5; color: #ffffff; }
.run-btn > button { background: #0f2a1a !important; border-color: #1a5c32 !important; color: #4ade80 !important; }
.run-btn > button:hover { background: #1a3d28 !important; border-color: #22c55e !important; }
.stop-btn > button { background: #2a0f0f !important; border-color: #5c1a1a !important; color: #f87171 !important; }
.stop-btn > button:hover { background: #3d1a1a !important; border-color: #ef4444 !important; }

.log-box {
    background: #090b0e; border: 1px solid #1e2330; border-radius: 6px; padding: 14px 16px;
    font-family: 'IBM Plex Mono', monospace; font-size: 12px; color: #7fba7a;
    height: 340px; overflow-y: auto; white-space: pre-wrap; word-break: break-all; line-height: 1.6;
}

.status-running { color: #4ade80; font-family: 'IBM Plex Mono', monospace; font-size: 13px; }
.status-idle    { color: #6b7385; font-family: 'IBM Plex Mono', monospace; font-size: 13px; }

.section-header {
    font-family: 'IBM Plex Mono', monospace; font-size: 11px; letter-spacing: 0.12em;
    text-transform: uppercase; color: #4a6fa5; border-bottom: 1px solid #1e2330;
    padding-bottom: 6px; margin-bottom: 16px;
}

.db-ok  { color: #4ade80; font-size: 12px; font-family: 'IBM Plex Mono', monospace; }
.db-err { color: #f87171; font-size: 12px; font-family: 'IBM Plex Mono', monospace; }
[data-testid="stDataFrame"] { border: 1px solid #1e2330; border-radius: 6px; }
.stTabs [data-baseweb="tab-list"] { background: #111419; border-bottom: 1px solid #1e2330; gap: 0; }
.stTabs [data-baseweb="tab"] { font-family: 'IBM Plex Mono', monospace; font-size: 12px; color: #6b7385; padding: 10px 20px; }
.stTabs [aria-selected="true"] { color: #c8cdd6 !important; border-bottom: 2px solid #4a6fa5 !important; }
hr { border-color: #1e2330; }
.js-plotly-plot { border-radius: 8px; }

/* Lead card */
.lead-card {
    background: #151820; border: 1px solid #1e2330; border-radius: 8px;
    padding: 16px 20px; margin-bottom: 8px;
}
.lead-card-accent { border-left: 4px solid #4ade80; }
.lead-card-warn   { border-left: 4px solid #facc15; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  DB helpers
# ─────────────────────────────────────────────
def get_dsn() -> str:
    dsn = os.getenv("DATABASE_URL", "postgresql://lc:lc@127.0.0.1:5432/lc")
    if "connect_timeout=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "connect_timeout=3"
    return dsn

def check_db() -> bool:
    try:
        with psycopg.connect(get_dsn()):
            return True
    except Exception:
        return False

def db_fetch(sql: str, params=()) -> list[dict]:
    try:
        with psycopg.connect(get_dsn(), row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
    except Exception as e:
        st.error(f"DB error: {e}")
        return []

def db_fetch_df(sql: str, params=()) -> pd.DataFrame:
    rows = db_fetch(sql, params)
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def get_db_conn():
    return psycopg.connect(get_dsn(), row_factory=dict_row)

# ─────────────────────────────────────────────
#  Process helpers
# ─────────────────────────────────────────────
def get_env(overrides: dict) -> dict:
    env = dict(os.environ)
    env.update(overrides)
    env["PYTHONUNBUFFERED"] = "1"
    if SRC_ROOT.exists():
        sep = ";" if os.name == "nt" else ":"
        existing = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = str(SRC_ROOT) + (sep + existing if existing else "")
    return env

def is_running() -> bool:
    p = st.session_state.get("proc")
    return p is not None and p.poll() is None

def start_job(job: str, cmd: list[str], env_overrides: dict):
    if is_running():
        st.warning("A job is already running — stop it first.")
        return
    log_path = LOG_DIR / f"{job}_{int(time.time())}.log"
    log_f = open(log_path, "w", encoding="utf-8", errors="replace", buffering=1)
    log_f.write(f"== {job} started {time.ctime()} ==\nCMD: {' '.join(cmd)}\n\n")
    log_f.flush()
    kwargs: dict = dict(stdout=log_f, stderr=subprocess.STDOUT, env=get_env(env_overrides))
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["preexec_fn"] = os.setsid
    proc = subprocess.Popen(cmd, **kwargs)
    st.session_state.proc = proc
    st.session_state.log_file = log_f
    st.session_state.log_path = log_path
    st.session_state.log_pos = 0
    st.session_state.job_name = job

def stop_job():
    proc = st.session_state.get("proc")
    if not proc:
        return
    try:
        if os.name == "nt":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            import os as _os
            _os.killpg(_os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
    finally:
        lf = st.session_state.get("log_file")
        if lf:
            try:
                lf.close()
            except Exception:
                pass
        st.session_state.proc = None
        st.session_state.log_file = None

def read_new_log() -> str:
    lp = st.session_state.get("log_path")
    if not lp or not Path(lp).exists():
        return ""
    try:
        with open(lp, "r", encoding="utf-8", errors="replace") as f:
            f.seek(st.session_state.get("log_pos", 0))
            chunk = f.read(32_000)
            st.session_state.log_pos = f.tell()
        return chunk
    except Exception:
        return ""

# ─────────────────────────────────────────────
#  Plotly theme
# ─────────────────────────────────────────────
PLOT_LAYOUT = dict(
    paper_bgcolor="#0d0f12",
    plot_bgcolor="#111419",
    font=dict(family="IBM Plex Mono", color="#6b7385", size=11),
    margin=dict(l=10, r=10, t=30, b=10),
    xaxis=dict(gridcolor="#1e2330", zerolinecolor="#1e2330"),
    yaxis=dict(gridcolor="#1e2330", zerolinecolor="#1e2330"),
)

# ─────────────────────────────────────────────
#  Session state init
# ─────────────────────────────────────────────
for key, default in [
    ("proc", None), ("log_file", None), ("log_path", None),
    ("log_pos", 0), ("job_name", None), ("log_buffer", ""),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ─────────────────────────────────────────────
#  SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🔍 RCG LeadCollector")
    st.caption("Automated freight lead discovery")
    st.markdown("---")

    db_ok = check_db()
    if db_ok:
        st.markdown('<span class="db-ok">● Connected</span>', unsafe_allow_html=True)
    else:
        st.markdown('<span class="db-err">● Database offline</span>', unsafe_allow_html=True)

    st.markdown("---")
    page = st.radio("Navigate", ["📊 Overview", "🎯 Leads", "📈 Analytics", "⚙️ System"], label_visibility="collapsed")
    st.markdown("---")

    if is_running():
        job = st.session_state.get("job_name", "job")
        st.markdown(f'<span class="status-running">▶ {job} running…</span>', unsafe_allow_html=True)
        st.markdown('<div class="stop-btn">', unsafe_allow_html=True)
        if st.button("⏹  Stop", use_container_width=True):
            stop_job()
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.markdown('<span class="status-idle">◼ Ready</span>', unsafe_allow_html=True)

    st.markdown("---")

    # Prompt version selector — reads from item_scores
    if db_ok:
        pv_rows = db_fetch("select distinct prompt_version from item_scores order by prompt_version")
        pv_list = [r["prompt_version"] for r in pv_rows] if pv_rows else ["E"]
        default_idx = pv_list.index("A") if "A" in pv_list else 0
        PV = st.selectbox("AI Prompt Version", pv_list, index=default_idx,
                          help="Select which AI scoring version to display")
    else:
        PV = "E"

    st.caption(f"v2.0 · gemma3:12b · Prompt {PV}")


# ═══════════════════════════════════════════════
#  PAGE: OVERVIEW
# ═══════════════════════════════════════════════
if page == "📊 Overview":
    if not db_ok:
        st.error("⚠️  Database is not reachable.")
        st.stop()

    # ── Hero metrics ──────────────────────────
    stats = db_fetch("""
        select
            (select count(distinct source_id) from items) as sources,
            (select count(*) from items) as articles,
            (select count(*) from item_scores where lead_score >= 5 and prompt_version = %s) as leads,
            (select count(*) from item_scores where lead_score >= 7 and prompt_version = %s) as strong_leads,
            (select count(*) from items where clean_text is not null
                and item_id not in (select item_id from item_scores where prompt_version = %s)) as pending
    """, (PV, PV, PV))
    if stats:
        s = stats[0]
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Sources Monitored", s["sources"])
        m2.metric("Articles Processed", f"{s['articles']:,}")
        m3.metric("🎯 Leads Found", s["leads"])
        m4.metric("⭐ Strong Leads", s["strong_leads"])
        m5.metric("Pending Analysis", s["pending"])

    st.markdown("---")

    # ── Two-column layout ─────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<p class="section-header">Articles by Source</p>', unsafe_allow_html=True)
        df_src = db_fetch_df("""
            select i.source_id, count(distinct i.item_id) as articles,
                   count(distinct s.item_id) filter (where s.lead_score >= 5) as leads
            from items i
            left join item_scores s on s.item_id = i.item_id and s.prompt_version = %s
            group by i.source_id order by articles desc limit 15
        """, (PV,))
        if not df_src.empty:
            fig = px.bar(df_src, x="articles", y="source_id", orientation="h",
                         color="leads", color_continuous_scale=["#1a3a5c", "#4ade80"],
                         hover_data=["leads"])
            fig.update_layout(**PLOT_LAYOUT, showlegend=False, coloraxis_showscale=False, height=380)
            fig.update_traces(marker_line_width=0)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<p class="section-header">Articles Over Time</p>', unsafe_allow_html=True)
        df_time = db_fetch_df("""
            select date_trunc('day', created_at)::date as day, count(*) as articles
            from items where created_at >= now() - interval '30 days'
            group by 1 order by 1
        """)
        if not df_time.empty:
            fig2 = px.area(df_time, x="day", y="articles", color_discrete_sequence=["#4a6fa5"])
            fig2.update_layout(**PLOT_LAYOUT, height=380)
            fig2.update_traces(line=dict(color="#7fb3e8", width=2), fillcolor="rgba(74,111,165,0.18)")
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No recent articles.")

    st.markdown("---")

    # ── Quick lead preview ────────────────────
    st.markdown('<p class="section-header">Latest Leads</p>', unsafe_allow_html=True)
    df_preview = db_fetch_df("""
        select s.lead_score as score, coalesce(s.lead_company,'—') as company,
               coalesce(s.lead_country,'—') as country,
               coalesce(s.lead_what,'—') as project,
               coalesce(i.title,'—') as article
        from item_scores s join items i using(item_id)
        where s.lead_score >= 5 and s.prompt_version = %s
        order by s.lead_score desc, s.classified_at desc limit 8
    """, (PV,))
    if not df_preview.empty:
        st.dataframe(df_preview, use_container_width=True, hide_index=True, height=300,
                     column_config={"score": st.column_config.NumberColumn("Score", format="%d ⭐")})
    else:
        st.info("No leads found yet. Run the pipeline to start discovering opportunities.")


# ═══════════════════════════════════════════════
#  PAGE: LEADS
# ═══════════════════════════════════════════════
elif page == "🎯 Leads":
    if not db_ok:
        st.error("⚠️  Database not reachable.")
        st.stop()

    # ── Metrics row ───────────────────────────
    s1, s2, s3, s4 = st.columns(4)
    total_leads  = db_fetch("select count(*) as n from item_scores where lead_score >= 5 and prompt_version = %s", (PV,))
    strong_leads = db_fetch("select count(*) as n from item_scores where lead_score >= 7 and prompt_version = %s", (PV,))
    total_items  = db_fetch("select count(*) as n from items")
    pending      = db_fetch("""select count(*) as n from items where clean_text is not null
                               and item_id not in (select item_id from item_scores where prompt_version = %s)""", (PV,))

    s1.metric("📦 Articles", f"{(total_items[0]['n'] if total_items else 0):,}")
    s2.metric("🎯 Leads (≥5)", f"{(total_leads[0]['n'] if total_leads else 0):,}")
    s3.metric("⭐ Strong (≥7)", f"{(strong_leads[0]['n'] if strong_leads else 0):,}")
    s4.metric("⏳ Pending", f"{(pending[0]['n'] if pending else 0):,}")

    st.markdown("---")

    # ── Pipeline funnel + score distribution ──
    fc1, fc2 = st.columns(2)

    with fc1:
        st.markdown('<p class="section-header">Discovery Funnel</p>', unsafe_allow_html=True)
        funnel = db_fetch("""
            select
                (select count(*) from url_state) as urls_discovered,
                (select count(*) from items) as articles_extracted,
                (select count(*) from items where prefilter_pass = true) as relevance_filter,
                (select count(*) from item_scores where prompt_version = %s) as ai_scored,
                (select count(*) from item_scores where lead_score >= 5 and prompt_version = %s) as leads_found,
                (select count(*) from item_scores where lead_score >= 7 and prompt_version = %s) as strong_leads
        """, (PV, PV, PV))
        if funnel:
            f = funnel[0]
            fig_fun = go.Figure(go.Funnel(
                y=["URLs Discovered", "Articles Extracted", "Relevance Filter", "AI Scored", "Leads Found", "Strong Leads"],
                x=[f["urls_discovered"], f["articles_extracted"], f["relevance_filter"],
                   f["ai_scored"], f["leads_found"], f["strong_leads"]],
                textinfo="value+percent initial",
                marker=dict(color=["#4a6fa5", "#5a8abf", "#6b9fd4", "#7fb3e8", "#4ade80", "#22c55e"]),
                connector=dict(line=dict(color="#1e2330")),
            ))
            fig_fun.update_layout(**PLOT_LAYOUT, height=340)
            st.plotly_chart(fig_fun, use_container_width=True)

    with fc2:
        st.markdown('<p class="section-header">Score Distribution</p>', unsafe_allow_html=True)
        df_scores = db_fetch_df("""
            select lead_score as score, count(*) as count
            from item_scores where lead_score is not null and prompt_version = %s
            group by lead_score order by lead_score
        """, (PV,))
        if not df_scores.empty:
            colors = ["#4ade80" if s >= 7 else "#facc15" if s >= 5 else "#6b7385"
                      for s in df_scores["score"]]
            fig_sc = px.bar(df_scores, x="score", y="count", color_discrete_sequence=["#4a6fa5"])
            fig_sc.update_traces(marker_color=colors, marker_line_width=0)
            fig_sc.update_layout(**PLOT_LAYOUT, showlegend=False, height=340)
            st.plotly_chart(fig_sc, use_container_width=True)

    st.markdown("---")

    # ── Tabs ──────────────────────────────────
    lead_tab1, lead_tab2 = st.tabs(["🔍  Browse Leads", "✅  Review & Label"])

    with lead_tab1:
        lf1, lf2 = st.columns([1, 3])
        with lf1:
            min_score = st.slider("Minimum score", 1, 10, 5)
        with lf2:
            lead_search = st.text_input("Search company / title", placeholder="keyword…")

        sql_leads = """
            select
                i.item_id,
                s.lead_score as score,
                coalesce(s.lead_company, '—') as company,
                coalesce(s.lead_city, '—') as city,
                coalesce(s.lead_country, '—') as country,
                coalesce(s.lead_who, '—') as who,
                coalesce(s.lead_what, '—') as what,
                coalesce(s.lead_when, '—') as "when",
                coalesce(s.lead_reason, '—') as reason,
                coalesce(s.lead_description, '—') as description,
                coalesce(i.title, '(no title)') as title,
                i.source_id, i.url,
                i.created_at::date as date,
                coalesce(ll.label, 'new') as status
            from item_scores s
            join items i using(item_id)
            left join (
                select distinct on (item_id) item_id, label
                from lead_labels order by item_id, labeled_at desc
            ) ll on ll.item_id = i.item_id
            where s.prompt_version = %s and s.lead_score >= %s
        """
        params_leads: list = [PV, min_score]
        if lead_search:
            sql_leads += " and (s.lead_company ilike %s or i.title ilike %s)"
            params_leads += [f"%{lead_search}%", f"%{lead_search}%"]
        sql_leads += " order by s.lead_score desc, i.created_at desc limit 200"

        df_l = db_fetch_df(sql_leads, tuple(params_leads))
        if not df_l.empty:
            # ── Expandable lead cards ─────────
            for _, lead in df_l.head(15).iterrows():
                score = lead["score"]
                company = lead["company"] if lead["company"] != "—" else "Unknown"
                color = "#4ade80" if score >= 7 else "#facc15" if score >= 5 else "#6b7385"
                accent = "lead-card-accent" if score >= 7 else "lead-card-warn" if score >= 5 else ""
                stars = "⭐" * min(score, 5)

                with st.expander(
                    f"{stars}  **{company}** · {lead['city']}, {lead['country']} · Score {score}/10",
                    expanded=(score >= 8)
                ):
                    # Top: company + score
                    tc1, tc2 = st.columns([3, 1])
                    with tc1:
                        st.markdown(f"<div style='font-size:20px; font-weight:600; color:#e2e6ef;'>{company}</div>",
                                    unsafe_allow_html=True)
                        score_bar = "★" * score + "☆" * (10 - score)
                        st.markdown(f"<div style='color:{color}; font-family:IBM Plex Mono; font-size:14px;'>{score_bar}</div>",
                                    unsafe_allow_html=True)
                    with tc2:
                        st.markdown(f"<div style='text-align:right; color:#6b7385; font-size:12px;'>{lead['source_id']}<br>{lead['date']}</div>",
                                    unsafe_allow_html=True)

                    # Description highlight
                    if lead["description"] != "—":
                        st.markdown(f"""<div style="background:#0d1117; border-left:3px solid {color}; padding:12px 16px;
                            border-radius:4px; color:#c8cdd6; font-size:14px; margin:12px 0;">
                            {lead['description']}</div>""", unsafe_allow_html=True)

                    # Who / What / When
                    wc1, wc2, wc3 = st.columns(3)
                    for col, label, val in [
                        (wc1, "Who", lead["who"]),
                        (wc2, "What", lead["what"]),
                        (wc3, "When", lead["when"]),
                    ]:
                        with col:
                            st.markdown(f"""<div style="color:#6b7385; font-size:11px; text-transform:uppercase;
                                letter-spacing:0.08em;">{label}</div>
                                <div style="color:#c8cdd6; font-size:14px; margin-top:2px;">
                                {val if val != '—' else 'Not specified'}</div>""", unsafe_allow_html=True)

                    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

                    # Location + AI reasoning
                    rc1, rc2 = st.columns(2)
                    with rc1:
                        st.markdown(f"""<div style="color:#6b7385; font-size:11px; text-transform:uppercase;
                            letter-spacing:0.08em;">Location</div>
                            <div style="color:#c8cdd6; font-size:14px; margin-top:2px;">
                            📍 {lead['city']}, {lead['country']}</div>""", unsafe_allow_html=True)
                    with rc2:
                        st.markdown(f"""<div style="color:#6b7385; font-size:11px; text-transform:uppercase;
                            letter-spacing:0.08em;">AI Assessment</div>
                            <div style="color:#9ba3b5; font-size:13px; margin-top:2px;">
                            {lead['reason'] if lead['reason'] != '—' else 'N/A'}</div>""", unsafe_allow_html=True)

                    # Source article
                    st.markdown(f"""<div style="background:#111419; border:1px solid #1e2330; border-radius:6px;
                        padding:10px 14px; margin-top:12px;">
                        <div style="color:#6b7385; font-size:11px; text-transform:uppercase;">Source Article</div>
                        <div style="color:#c8cdd6; font-size:13px; margin-top:4px;">{lead['title']}</div>
                        <a href="{lead['url']}" target="_blank" style="color:#4a6fa5; font-size:12px;">Open article →</a>
                    </div>""", unsafe_allow_html=True)

            # Full table
            st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)
            st.markdown('<p class="section-header">All Results</p>', unsafe_allow_html=True)
            st.dataframe(
                df_l.drop(columns=["item_id"]),
                use_container_width=True, height=400, hide_index=True,
                column_config={
                    "score": st.column_config.NumberColumn("Score", format="%d ⭐"),
                    "url":   st.column_config.LinkColumn("URL"),
                },
            )
            st.caption(f"{len(df_l)} results shown")
        else:
            st.info("No leads found. Try lowering the score threshold or run the pipeline first.")

    # ── Tab 2: Review & Label ─────────────────
    with lead_tab2:
        st.markdown("Review leads and update their status. Changes are saved instantly.")

        rc1, rc2 = st.columns([1, 2])
        with rc1:
            review_min = st.slider("Min score", 1, 10, 5, key="review_score")
        with rc2:
            status_filter = st.selectbox("Show status", ["new", "confirmed", "follow_up", "contacted", "rejected", "all"])

        sql_review = """
            select i.item_id, s.lead_score as score,
                coalesce(s.lead_company, '—') as company,
                coalesce(s.lead_country, '—') as country,
                coalesce(s.lead_what, '—') as project,
                coalesce(s.lead_reason, '—') as reason,
                coalesce(i.title, '—') as title,
                i.url,
                coalesce(ll.label, 'new') as status
            from item_scores s
            join items i using(item_id)
            left join (select distinct on (item_id) item_id, label from lead_labels order by item_id, labeled_at desc) ll on ll.item_id = i.item_id
            where s.prompt_version = %s and s.lead_score >= %s
        """
        rparams: list = [PV, review_min]
        if status_filter != "all":
            sql_review += " and coalesce(ll.label, 'new') = %s"
            rparams.append(status_filter)
        sql_review += " order by s.lead_score desc limit 100"

        df_review = db_fetch_df(sql_review, tuple(rparams))
        if df_review.empty:
            st.info("No leads to review.")
        else:
            STATUS_OPTIONS = ["new", "confirmed", "follow_up", "contacted", "rejected"]
            edited = st.data_editor(
                df_review.drop(columns=["item_id"]),
                use_container_width=True, height=450,
                column_config={
                    "score":  st.column_config.NumberColumn("Score", format="%d ⭐"),
                    "url":    st.column_config.LinkColumn("URL"),
                    "status": st.column_config.SelectboxColumn("Status", options=STATUS_OPTIONS, required=True),
                },
                disabled=["score", "company", "country", "project", "reason", "title", "url"],
            )

            if st.button("💾  Save Changes", type="primary"):
                saved = 0
                with get_db_conn() as conn:
                    with conn.cursor() as cur:
                        for i, row in edited.iterrows():
                            item_id = int(df_review.iloc[i]["item_id"])
                            if row["status"] != df_review.iloc[i]["status"]:
                                cur.execute("insert into lead_labels (item_id, label, labeled_by) values (%s, %s, 'gui')",
                                            (item_id, row["status"]))
                                saved += 1
                if saved:
                    st.success(f"Saved {saved} change(s).")
                    st.rerun()
                else:
                    st.info("No changes detected.")

            sc1, sc2, sc3, sc4, sc5 = st.columns(5)
            counts = df_review["status"].value_counts()
            sc1.metric("New",       counts.get("new", 0))
            sc2.metric("Confirmed", counts.get("confirmed", 0))
            sc3.metric("Follow Up", counts.get("follow_up", 0))
            sc4.metric("Contacted", counts.get("contacted", 0))
            sc5.metric("Rejected",  counts.get("rejected", 0))


# ═══════════════════════════════════════════════
#  PAGE: ANALYTICS
# ═══════════════════════════════════════════════
elif page == "📈 Analytics":
    if not db_ok:
        st.error("⚠️  Database not reachable.")
        st.stop()

    # ── Top metrics ───────────────────────────
    stats = db_fetch("""
        select
            (select count(distinct source_id) from items) as sources,
            (select count(*) from items) as articles,
            (select count(*) from item_scores where prompt_version = %s) as classified,
            (select count(*) from item_scores where lead_score >= 5 and prompt_version = %s) as leads
    """, (PV, PV))
    if stats:
        s = stats[0]
        am1, am2, am3, am4 = st.columns(4)
        am1.metric("Sources", s["sources"])
        am2.metric("Articles", f"{s['articles']:,}")
        am3.metric("AI Scored", f"{s['classified']:,}")
        am4.metric("Leads Found", s["leads"])

    st.markdown("---")

    tab1, tab2, tab3 = st.tabs(["📊  Scoring Overview", "🗂️  Source Performance", "🕐  System Runs"])

    # ── Tab 1: Scoring Overview ───────────────
    with tab1:
        ov1, ov2 = st.columns(2)

        with ov1:
            st.markdown('<p class="section-header">How Articles Were Classified</p>', unsafe_allow_html=True)
            df_class = db_fetch_df("""
                select category, sum(n) as n from (
                    -- Pre-filter rejects from items table
                    select
                        case classifier_model
                            when 'pre-filter' then 'Filtered (not relevant)'
                            when 'extract_v2' then 'Filtered (bad content)'
                            else 'Pending'
                        end as category, count(*) as n
                    from items
                    where item_id not in (select item_id from item_scores where prompt_version = %s)
                    group by 1
                    union all
                    -- AI scored from item_scores
                    select
                        case
                            when lead_score >= 7 then 'Strong lead (7-10)'
                            when lead_score >= 5 then 'Possible lead (5-6)'
                            when lead_score >= 3 then 'Weak signal (3-4)'
                            else 'Not a lead (0-2)'
                        end as category, count(*) as n
                    from item_scores
                    where prompt_version = %s
                    group by 1
                ) sub group by 1 order by 2 desc
            """, (PV, PV))
            if not df_class.empty:
                CAT_COLORS = {
                    "Strong lead (7-10)": "#4ade80", "Possible lead (5-6)": "#facc15",
                    "Weak signal (3-4)": "#4a6fa5", "Not a lead (0-2)": "#6b7385",
                    "Filtered (not relevant)": "#3d2020", "Filtered (bad content)": "#2d1515",
                    "Pending": "#1a1e2a",
                }
                fig_cl = px.pie(df_class, names="category", values="n",
                                color="category", color_discrete_map=CAT_COLORS, hole=0.5)
                fig_cl.update_layout(**PLOT_LAYOUT, height=320,
                                     legend=dict(font=dict(color="#6b7385", size=10)))
                fig_cl.update_traces(textfont_color="#c8cdd6")
                st.plotly_chart(fig_cl, use_container_width=True)

        with ov2:
            st.markdown('<p class="section-header">Leads by Country</p>', unsafe_allow_html=True)
            df_geo = db_fetch_df("""
                select lead_country as country, count(*) as leads
                from item_scores
                where lead_score >= 3 and lead_country is not null and lead_country != ''
                    and prompt_version = %s
                group by lead_country order by leads desc limit 12
            """, (PV,))
            if not df_geo.empty:
                fig_geo = px.bar(df_geo, x="leads", y="country", orientation="h",
                                 color="leads", color_continuous_scale=["#1a3a5c", "#4ade80"])
                fig_geo.update_layout(**PLOT_LAYOUT, height=320, showlegend=False,
                                      coloraxis_showscale=False)
                fig_geo.update_traces(marker_line_width=0)
                st.plotly_chart(fig_geo, use_container_width=True)
            else:
                st.info("No geographic data yet.")

        # Score distribution full width
        st.markdown('<p class="section-header">Score Distribution (all AI-scored articles)</p>', unsafe_allow_html=True)
        df_scores = db_fetch_df("""
            select lead_score as score, count(*) as articles
            from item_scores where lead_score is not null and prompt_version = %s
            group by lead_score order by lead_score
        """, (PV,))
        if not df_scores.empty:
            colors = ["#4ade80" if s >= 7 else "#facc15" if s >= 5 else "#6b7385" for s in df_scores["score"]]
            fig_sd = px.bar(df_scores, x="score", y="articles", text="articles")
            fig_sd.update_traces(marker_color=colors, marker_line_width=0,
                                 textposition="outside", textfont_color="#6b7385")
            fig_sd.update_layout(**PLOT_LAYOUT, showlegend=False, height=280)
            st.plotly_chart(fig_sd, use_container_width=True)

    # ── Tab 2: Source Performance ─────────────
    with tab2:
        st.markdown('<p class="section-header">Which Sources Produce the Best Leads?</p>', unsafe_allow_html=True)

        df_sp = db_fetch_df("""
            select i.source_id as source,
                count(distinct i.item_id) as articles,
                count(distinct s.item_id) as scored,
                round(avg(s.lead_score)::numeric, 1) as avg_score,
                max(s.lead_score) as best,
                count(distinct s.item_id) filter (where s.lead_score >= 5) as leads
            from items i
            left join item_scores s on s.item_id = i.item_id and s.prompt_version = %s
            group by i.source_id
            order by avg(s.lead_score) desc nulls last
        """, (PV,))
        if not df_sp.empty:
            st.dataframe(df_sp, use_container_width=True, hide_index=True, height=400)

            # Yield chart
            df_sp["yield"] = (df_sp["leads"] / df_sp["articles"].replace(0, 1) * 100).round(1)
            df_yield = df_sp[df_sp["articles"] >= 5].sort_values("yield", ascending=True).tail(12)
            if not df_yield.empty:
                st.markdown('<p class="section-header">Lead Yield (leads per 100 articles)</p>', unsafe_allow_html=True)
                fig_y = px.bar(df_yield, x="yield", y="source", orientation="h",
                               color="yield", color_continuous_scale=["#1a3a5c", "#4ade80"], text="yield")
                fig_y.update_layout(**PLOT_LAYOUT, height=320, showlegend=False, coloraxis_showscale=False)
                fig_y.update_traces(marker_line_width=0, texttemplate="%{text:.1f}%", textposition="outside")
                st.plotly_chart(fig_y, use_container_width=True)

            # Zero-yield warning
            zero = df_sp[(df_sp["leads"] == 0) & (df_sp["articles"] >= 5)]
            if not zero.empty:
                with st.expander(f"⚠️ {len(zero)} sources with zero leads", expanded=False):
                    st.caption("These sources have 5+ articles but produced no leads (score ≥5). Consider deactivating or replacing them.")
                    st.dataframe(zero[["source", "articles", "avg_score", "best"]], use_container_width=True, hide_index=True)

    # ── Tab 3: System Runs ────────────────────
    with tab3:
        # Check if pipeline_runs table exists
        has_runs = False
        try:
            with get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("select exists (select from information_schema.tables where table_name = 'pipeline_runs')")
                    has_runs = cur.fetchone()["exists"]
        except Exception:
            pass

        if not has_runs:
            st.info("No run data yet. Run the pipeline at least once.")
        else:
            runs_data = db_fetch("""
                select script, started_at, finished_at, duration_s,
                       articles_processed, articles_ok, articles_err,
                       articles_skipped, avg_s_per_article, notes
                from pipeline_runs where finished_at is not null
                order by started_at desc limit 100
            """)
            if not runs_data:
                st.info("No completed runs yet.")
            else:
                df_runs = pd.DataFrame(runs_data)
                df_runs["started_at"] = pd.to_datetime(df_runs["started_at"])

                rm1, rm2, rm3, rm4 = st.columns(4)
                rm1.metric("Total Runs", len(df_runs))
                rm2.metric("Articles Processed", int(df_runs["articles_processed"].sum()))
                rm3.metric("Errors", int(df_runs["articles_err"].sum()))
                rm4.metric("Last Run", df_runs.iloc[0]["started_at"].strftime("%d.%m %H:%M"))

                st.markdown("---")

                for script in ["classify", "extract", "fetch", "discover"]:
                    df_s = df_runs[df_runs["script"] == script]
                    if df_s.empty:
                        continue
                    with st.expander(f"**{script.capitalize()}** — {len(df_s)} runs", expanded=(script == "classify")):
                        c1, c2, c3 = st.columns(3)
                        c1.metric("Avg Duration", f"{df_s['duration_s'].mean():.0f}s")
                        c2.metric("Avg Articles", f"{df_s['articles_processed'].mean():.0f}")
                        avg_tpa = df_s["avg_s_per_article"].dropna().mean()
                        c3.metric("Avg Time/Article", f"{avg_tpa:.2f}s" if avg_tpa and avg_tpa > 0 else "—")

                # Timeline
                st.markdown('<p class="section-header">Run Timeline</p>', unsafe_allow_html=True)
                fig_tl = px.bar(df_runs.head(30), x="started_at", y="duration_s", color="script",
                                color_discrete_map={"discover": "#4C72B0", "fetch": "#DD8452",
                                                    "extract": "#55A868", "classify": "#C44E52"})
                fig_tl.update_layout(**PLOT_LAYOUT, height=280)
                st.plotly_chart(fig_tl, use_container_width=True)

    # ── Developer tools (hidden) ──────────────
    with st.expander("🔧 Developer: Prompt Comparison", expanded=False):
        df_pc = db_fetch_df("""
            select prompt_version, count(*) as articles,
                round(avg(lead_score)::numeric, 2) as avg_score,
                count(*) filter (where lead_score >= 5) as "leads_5+",
                count(*) filter (where lead_score >= 7) as "leads_7+"
            from item_scores group by prompt_version order by prompt_version
        """)
        if not df_pc.empty:
            st.dataframe(df_pc, use_container_width=True, hide_index=True)
        else:
            st.info("No multi-prompt data available.")


# ═══════════════════════════════════════════════
#  PAGE: SYSTEM
# ═══════════════════════════════════════════════
elif page == "⚙️ System":
    st.markdown('<p class="section-header">Pipeline Controls</p>', unsafe_allow_html=True)

    if not db_ok:
        st.error("⚠️  Database is not reachable.")

    # ── Settings ──────────────────────────────
    with st.expander("⚙️  Settings", expanded=False):
        c1, c2 = st.columns(2)
        with c1:
            registry     = st.text_input("Registry YAML path", value=str(DEFAULT_REG))
            disc_workers = st.number_input("DISCOVER_WORKERS", 1, 20, int(os.getenv("LC_DISCOVER_WORKERS", 5)))
            fetch_workers= st.number_input("FETCH_WORKERS",    1, 20, int(os.getenv("LC_FETCH_WORKERS", 5)))
        with c2:
            fetch_per_src  = st.number_input("FETCH_PER_SOURCE",   1, 500, int(os.getenv("LC_FETCH_PER_SOURCE", 50)))
            extract_workers= st.number_input("EXTRACT_WORKERS",    1, 20,  int(os.getenv("LC_EXTRACT_WORKERS", 4)))
            extract_batch  = st.number_input("EXTRACT_BATCH",      1, 200, int(os.getenv("LC_EXTRACT_BATCH", 25)))

    env_overrides = {
        "LC_DISCOVER_WORKERS": str(disc_workers),
        "LC_FETCH_WORKERS":    str(fetch_workers),
        "LC_FETCH_PER_SOURCE": str(fetch_per_src),
        "LC_EXTRACT_WORKERS":  str(extract_workers),
        "LC_EXTRACT_BATCH":    str(extract_batch),
    }

    py = str(Path(sys.executable))
    py_flags = [py, "-u"]
    disabled = is_running() or not db_ok

    # ── Classify settings ─────────────────────
    with st.expander("🎯  AI Classification Settings", expanded=False):
        cc1, cc2 = st.columns(2)
        with cc1:
            st.session_state["classify_limit"] = st.number_input("Articles per run", 1, 500, 50)
        with cc2:
            st.session_state["classify_prompt"] = st.selectbox("AI Prompt Version",
                ["E", "D", "A", "B", "C"], index=0,
                help="E = recommended (compact v2), D = full v2, A/B/C = legacy")
        st.caption("ℹ️ AI Engine: gemma3:12b · Context: 4096 tokens · Post-validation: enabled")

    # ── Run buttons ───────────────────────────
    st.markdown('<p class="section-header">Run Pipeline</p>', unsafe_allow_html=True)

    b1, b2, b3, b4, b5 = st.columns(5)

    with b1:
        st.markdown('<div class="run-btn">', unsafe_allow_html=True)
        if st.button("▶  Discover", use_container_width=True, disabled=disabled):
            reg = Path(registry)
            if not reg.exists():
                st.error(f"Registry not found: {reg}")
            else:
                start_job("discover", py_flags + [str(SCRIPTS_DIR / "discover.py"), str(reg)], env_overrides)
                st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    with b2:
        st.markdown('<div class="run-btn">', unsafe_allow_html=True)
        if st.button("▶  Fetch", use_container_width=True, disabled=disabled):
            start_job("fetch", py_flags + [str(SCRIPTS_DIR / "fetch.py")], env_overrides)
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    with b3:
        st.markdown('<div class="run-btn">', unsafe_allow_html=True)
        if st.button("▶  Extract", use_container_width=True, disabled=disabled):
            start_job("extract", py_flags + [str(SCRIPTS_DIR / "extract.py")], env_overrides)
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    with b4:
        st.markdown('<div class="run-btn">', unsafe_allow_html=True)
        if st.button("🎯  Classify", use_container_width=True, disabled=disabled):
            classify_limit = st.session_state.get("classify_limit", 50)
            classify_prompt = st.session_state.get("classify_prompt", "E")
            start_job(f"classify (prompt {classify_prompt})",
                      py_flags + [str(SCRIPTS_DIR / "classify.py"),
                      "--prompt", classify_prompt, "--limit", str(classify_limit)], env_overrides)
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    with b5:
        st.markdown('<div class="run-btn">', unsafe_allow_html=True)
        if st.button("▶  Run All", use_container_width=True, disabled=disabled,
                     help="Runs the full pipeline: discover → fetch → extract → classify"):
            disc     = str(SCRIPTS_DIR / "discover.py")
            fetch    = str(SCRIPTS_DIR / "fetch.py")
            ext      = str(SCRIPTS_DIR / "extract.py")
            classify = str(SCRIPTS_DIR / "classify.py")
            classify_prompt = st.session_state.get("classify_prompt", "E")
            start_job("run_all",
                py_flags + ["-c",
                 f"import subprocess,sys;"
                 f"subprocess.run([sys.executable,{disc!r},{registry!r}],check=True);"
                 f"subprocess.run([sys.executable,{fetch!r}],check=True);"
                 f"subprocess.run([sys.executable,{ext!r}],check=True);"
                 f"subprocess.run([sys.executable,{classify!r},'--prompt','{classify_prompt}'],check=True)"],
                env_overrides)
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Live log ──────────────────────────────
    st.markdown('<p class="section-header" style="margin-top:24px">System Log</p>', unsafe_allow_html=True)

    @st.fragment(run_every=2)
    def live_log_fragment():
        new_chunk = read_new_log()
        if new_chunk:
            st.session_state.log_buffer += new_chunk
            st.session_state.log_buffer = st.session_state.log_buffer[-10_000:]

        proc = st.session_state.get("proc")
        if proc and proc.poll() is not None:
            final = read_new_log()
            if final:
                st.session_state.log_buffer += final
            code = proc.returncode
            st.session_state.log_buffer += f"\n\n[✓] Job finished (exit code {code}).\n"
            lf = st.session_state.get("log_file")
            if lf:
                try:
                    lf.close()
                except Exception:
                    pass
            st.session_state.proc = None
            st.session_state.log_file = None
            st.rerun()

        log_text = st.session_state.log_buffer or "Ready. Press a run button to start.\n"
        safe = log_text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        st.markdown(f'<div class="log-box">{safe}</div>', unsafe_allow_html=True)

        lc1, lc2 = st.columns([1, 6])
        with lc1:
            if st.button("🗑  Clear"):
                st.session_state.log_buffer = ""
                st.rerun()
        with lc2:
            lp = st.session_state.get("log_path")
            if lp:
                st.caption(f"Log: `{lp}`")

    live_log_fragment()
