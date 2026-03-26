"""
LeadCollector – Streamlit GUI
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
import streamlit.components.v1 as components
from psycopg.rows import dict_row

# ─────────────────────────────────────────────
#  Paths
#  GUI lives at:  leadcollector/app_gui_streamlit.py
#  Scripts at:    leadcollector/app/src/lc/
# ─────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent          # leadcollector/
SRC_ROOT     = PROJECT_ROOT / "app" / "src"             # leadcollector/app/src/
SCRIPTS_DIR  = SRC_ROOT / "lc"                          # leadcollector/app/src/lc/
DEFAULT_REG  = PROJECT_ROOT / "app" / "registry.yaml"
LOG_DIR      = PROJECT_ROOT / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
PREVIEW_MODE = os.getenv("LC_PREVIEW_MODE", "1") == "1"

# ─────────────────────────────────────────────
#  Page config  (must be first Streamlit call)
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="LeadCollector",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
#  Custom CSS  – dark industrial theme
# ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
}

/* Background */
.stApp {
    background-color: #0d0f12;
    color: #c8cdd6;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background-color: #111419;
    border-right: 1px solid #1e2330;
}

/* Metric cards */
[data-testid="stMetric"] {
    background: #151820;
    border: 1px solid #1e2330;
    border-radius: 8px;
    padding: 16px 20px;
}
[data-testid="stMetricLabel"] { color: #6b7385; font-size: 11px; letter-spacing: 0.08em; text-transform: uppercase; }
[data-testid="stMetricValue"] { color: #e2e6ef; font-family: 'IBM Plex Mono', monospace; font-size: 2rem; }
[data-testid="stMetricDelta"] { font-family: 'IBM Plex Mono', monospace; }

/* Buttons */
.stButton > button {
    background: #1a1e2a;
    color: #c8cdd6;
    border: 1px solid #2a3045;
    border-radius: 6px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 13px;
    letter-spacing: 0.04em;
    padding: 8px 18px;
    transition: all 0.15s;
}
.stButton > button:hover {
    background: #222840;
    border-color: #4a6fa5;
    color: #ffffff;
}

/* Run buttons */
.run-btn > button {
    background: #0f2a1a !important;
    border-color: #1a5c32 !important;
    color: #4ade80 !important;
}
.run-btn > button:hover {
    background: #1a3d28 !important;
    border-color: #22c55e !important;
}

/* Locked preview buttons */
.locked-btn > button {
    background: #161920 !important;
    border-color: #3a3f4b !important;
    color: #7f8796 !important;
    cursor: not-allowed !important;
    opacity: 1 !important;
}
.locked-btn > button:hover {
    background: #161920 !important;
    border-color: #4a4f5c !important;
    color: #8a93a3 !important;
}
.locked-note {
    margin-top: 6px;
    text-align: center;
    color: #8a93a3;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 10px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}

/* Stop button */
.stop-btn > button {
    background: #2a0f0f !important;
    border-color: #5c1a1a !important;
    color: #f87171 !important;
}
.stop-btn > button:hover {
    background: #3d1a1a !important;
    border-color: #ef4444 !important;
}

/* Log box */
.log-box {
    background: #090b0e;
    border: 1px solid #1e2330;
    border-radius: 6px;
    padding: 14px 16px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 12px;
    color: #7fba7a;
    height: 340px;
    overflow-y: auto;
    white-space: pre-wrap;
    word-break: break-all;
    line-height: 1.6;
}

/* Status badge */
.status-running { color: #4ade80; font-family: 'IBM Plex Mono', monospace; font-size: 13px; }
.status-idle    { color: #6b7385; font-family: 'IBM Plex Mono', monospace; font-size: 13px; }
.status-error   { color: #f87171; font-family: 'IBM Plex Mono', monospace; font-size: 13px; }

/* Section headers */
.section-header {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #4a6fa5;
    border-bottom: 1px solid #1e2330;
    padding-bottom: 6px;
    margin-bottom: 16px;
}

/* DB status */
.db-ok  { color: #4ade80; font-size: 12px; font-family: 'IBM Plex Mono', monospace; }
.db-err { color: #f87171; font-size: 12px; font-family: 'IBM Plex Mono', monospace; }

/* Dataframe */
[data-testid="stDataFrame"] { border: 1px solid #1e2330; border-radius: 6px; }

/* Tabs */
.stTabs [data-baseweb="tab-list"] { background: #111419; border-bottom: 1px solid #1e2330; gap: 0; }
.stTabs [data-baseweb="tab"] {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 12px;
    color: #6b7385;
    padding: 10px 20px;
    letter-spacing: 0.05em;
}
.stTabs [aria-selected="true"] { color: #c8cdd6 !important; border-bottom: 2px solid #4a6fa5 !important; }

/* Selectbox / text input */
.stSelectbox > div, .stTextInput > div > div {
    background: #151820 !important;
    border-color: #1e2330 !important;
    color: #c8cdd6 !important;
}

/* Divider */
hr { border-color: #1e2330; }

/* Plotly chart bg */
.js-plotly-plot { border-radius: 8px; }
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
    env["PYTHONUNBUFFERED"] = "1"  # force subprocess to flush print() immediately
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
    log_f = open(log_path, "w", encoding="utf-8", errors="replace", buffering=1)  # line-buffered
    log_f.write(f"== {job} started {time.ctime()} ==\nCMD: {' '.join(cmd)}\n\n")
    log_f.flush()

    kwargs: dict = dict(
        stdout=log_f,
        stderr=subprocess.STDOUT,
        env=get_env(env_overrides),
    )
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["preexec_fn"] = os.setsid

    proc = subprocess.Popen(cmd, **kwargs)
    st.session_state.proc      = proc
    st.session_state.log_file  = log_f
    st.session_state.log_path  = log_path
    st.session_state.log_pos   = 0
    st.session_state.job_name  = job

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
        st.session_state.proc     = None
        st.session_state.log_file = None

def read_new_log() -> str:
    lp = st.session_state.get("log_path")
    if not lp or not Path(lp).exists():
        return ""
    try:
        with open(lp, "r", encoding="utf-8", errors="replace") as f:
            f.seek(st.session_state.get("log_pos", 0))
            chunk = f.read(32_000)  # cap so UI doesn't freeze
            st.session_state.log_pos = f.tell()
        return chunk
    except Exception:
        return ""

# ─────────────────────────────────────────────
#  Plotly theme helper
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

LEGACY_SOURCE_IDS = ("monitoring_import", "crm_import", "more_leads")
LEAD_SCOPE_OPTIONS = ["Recent found leads", "Legacy leads", "All leads"]

def get_lead_scope_condition(column: str = "source_id", scope: str | None = None) -> str:
    scope = scope or st.session_state.get("lead_scope", LEAD_SCOPE_OPTIONS[0])
    legacy_list = ", ".join([f"'{s}'" for s in LEGACY_SOURCE_IDS])
    if scope == "Legacy leads":
        return f"{column} in ({legacy_list})"
    if scope == "All leads":
        return "1=1"
    return f"{column} not in ({legacy_list})"

def render_lead_scope_selector(key: str = "lead_scope_selector", default: str = LEAD_SCOPE_OPTIONS[0]) -> str:
    if key not in st.session_state or st.session_state.get(key) not in LEAD_SCOPE_OPTIONS:
        st.session_state[key] = default
    choice = st.selectbox(
        "Lead scope",
        LEAD_SCOPE_OPTIONS,
        index=LEAD_SCOPE_OPTIONS.index(st.session_state.get(key, default)),
        key=key,
    )
    return choice


DATE_RANGE_OPTIONS = ["Since 2024", "Last 12 months", "All dates"]

def get_date_condition(column_published: str = "published_at", column_fallback: str = "created_at", date_range: str | None = None) -> str:
    date_range = date_range or st.session_state.get("date_range", DATE_RANGE_OPTIONS[0])
    if date_range == "All dates":
        return "1=1"
    date_expr = f"coalesce({column_published}, {column_fallback})"
    if date_range == "Last 12 months":
        return f"{date_expr} >= now() - interval '12 months'"
    return f"{date_expr} >= date '2024-01-01'"

def render_date_range_selector(key: str = "date_range_selector", default: str = DATE_RANGE_OPTIONS[0]) -> str:
    if key not in st.session_state or st.session_state.get(key) not in DATE_RANGE_OPTIONS:
        st.session_state[key] = default
    choice = st.selectbox(
        "Date range",
        DATE_RANGE_OPTIONS,
        index=DATE_RANGE_OPTIONS.index(st.session_state.get(key, default)),
        key=key,
    )
    return choice


def get_lead_when_condition(column: str = "lead_when", date_range: str | None = None) -> str:
    date_range = date_range or st.session_state.get("date_range", DATE_RANGE_OPTIONS[0])
    if date_range == "All dates":
        return "1=1"
    year_expr = f"nullif(substring(coalesce({column}, '') from '((?:19|20)\d{{2}})'), '')::int"
    if date_range == "Last 12 months":
        return f"({year_expr} is null or {year_expr} >= 2025)"
    return f"({year_expr} is null or {year_expr} >= 2024)"

# ─────────────────────────────────────────────
#  SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚡ LeadCollector")
    st.markdown("---")

    # DB status
    db_ok = check_db()
    if db_ok:
        st.markdown('<span class="db-ok">● DB connected</span>', unsafe_allow_html=True)
    else:
        st.markdown('<span class="db-err">● DB offline — run: docker-compose up -d db</span>', unsafe_allow_html=True)

    st.markdown("---")
    page = st.radio("Navigate", ["🚀 Pipeline", "📊 Dashboard", "🎯 Leads", "📈 Statistics", "📖 About"], label_visibility="collapsed")
    st.markdown("---")

    # Job status in sidebar
    if is_running():
        job = st.session_state.get("job_name", "job")
        st.markdown(f'<span class="status-running">▶ {job} running…</span>', unsafe_allow_html=True)
        with st.container():
            st.markdown('<div class="stop-btn">', unsafe_allow_html=True)
            if st.button("⏹  Stop job", width='stretch'):
                stop_job()
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.markdown('<span class="status-idle">◼ Idle</span>', unsafe_allow_html=True)

    # No full-page rerun here — the log fragment below handles its own refresh

# ─────────────────────────────────────────────
#  PAGE: PIPELINE
# ─────────────────────────────────────────────
if page == "🚀 Pipeline":
    st.markdown('<p class="section-header">Pipeline Controls</p>', unsafe_allow_html=True)

    if PREVIEW_MODE:
        st.warning("Preview mode is active. Pipeline actions are visible but locked.")

    if not db_ok:
        st.error("⚠️  Database is not reachable. Start it with:  `docker-compose up -d db`")

    # ── Settings ──────────────────────────────
    with st.expander("⚙️  Settings", expanded=False):
        if PREVIEW_MODE:
            st.caption("Settings are locked in preview mode.")
        c1, c2 = st.columns(2)
        with c1:
            registry     = st.text_input("Registry YAML path", value=str(DEFAULT_REG), disabled=PREVIEW_MODE)
            disc_workers = st.number_input("DISCOVER_WORKERS", 1, 20, int(os.getenv("LC_DISCOVER_WORKERS", 5)), disabled=PREVIEW_MODE)
            fetch_workers= st.number_input("FETCH_WORKERS",    1, 20, int(os.getenv("LC_FETCH_WORKERS", 5)), disabled=PREVIEW_MODE)
        with c2:
            fetch_per_src  = st.number_input("FETCH_PER_SOURCE",   1, 500, int(os.getenv("LC_FETCH_PER_SOURCE", 30)), disabled=PREVIEW_MODE)
            extract_workers= st.number_input("EXTRACT_WORKERS",    1, 20,  int(os.getenv("LC_EXTRACT_WORKERS", 4)), disabled=PREVIEW_MODE)
            extract_batch  = st.number_input("EXTRACT_BATCH",      1, 200, int(os.getenv("LC_EXTRACT_BATCH", 25)), disabled=PREVIEW_MODE)
            extract_exec   = st.selectbox("EXTRACT_EXECUTOR", ["process", "thread"],
                                          index=0 if os.getenv("LC_EXTRACT_EXECUTOR","process")=="process" else 1,
                                          disabled=PREVIEW_MODE)

    env_overrides = {
        "LC_DISCOVER_WORKERS": str(disc_workers),
        "LC_FETCH_WORKERS":    str(fetch_workers),
        "LC_FETCH_PER_SOURCE": str(fetch_per_src),
        "LC_EXTRACT_WORKERS":  str(extract_workers),
        "LC_EXTRACT_BATCH":    str(extract_batch),
        "LC_EXTRACT_EXECUTOR": extract_exec,
    }

    py = str(Path(sys.executable))
    py_flags = [py, "-u"]  # -u = unbuffered stdout/stderr

    # ── Run buttons ───────────────────────────
    st.markdown('<p class="section-header">Run</p>', unsafe_allow_html=True)

    b1, b2, b3, b4, b5 = st.columns([1, 1, 1, 1, 1])

    disabled = is_running() or not db_ok or PREVIEW_MODE

    with b1:
        if PREVIEW_MODE:
            st.markdown('<div class="locked-btn">', unsafe_allow_html=True)
            st.button("▶  Discover", width='stretch', disabled=True, key="preview_discover")
            st.markdown('</div><div class="locked-note">LOCKED · PREVIEW</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="run-btn">', unsafe_allow_html=True)
            if st.button("▶  Discover", width='stretch', disabled=disabled):
                reg = Path(registry)
                if not reg.exists():
                    st.error(f"Registry not found: {reg}")
                else:
                    start_job("discover", py_flags + [str(SCRIPTS_DIR / "discover.py"), str(reg)], env_overrides)
                    st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

    with b2:
        if PREVIEW_MODE:
            st.markdown('<div class="locked-btn">', unsafe_allow_html=True)
            st.button("▶  Fetch", width='stretch', disabled=True, key="preview_fetch")
            st.markdown('</div><div class="locked-note">LOCKED · PREVIEW</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="run-btn">', unsafe_allow_html=True)
            if st.button("▶  Fetch", width='stretch', disabled=disabled):
                start_job("fetch", py_flags + [str(SCRIPTS_DIR / "fetch.py")], env_overrides)
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

    with b3:
        if PREVIEW_MODE:
            st.markdown('<div class="locked-btn">', unsafe_allow_html=True)
            st.button("▶  Extract", width='stretch', disabled=True, key="preview_extract")
            st.markdown('</div><div class="locked-note">LOCKED · PREVIEW</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="run-btn">', unsafe_allow_html=True)
            if st.button("▶  Extract", width='stretch', disabled=disabled):
                start_job("extract", py_flags + [str(SCRIPTS_DIR / "extract.py")], env_overrides)
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

    with b4:
        if PREVIEW_MODE:
            st.markdown('<div class="locked-btn">', unsafe_allow_html=True)
            st.button("🎯  Classify", width='stretch', disabled=True, key="preview_classify", help="Locked because this is a public preview")
            st.markdown('</div><div class="locked-note">LOCKED · PREVIEW</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="run-btn">', unsafe_allow_html=True)
            if st.button("🎯  Classify", width='stretch', disabled=disabled,
                         help="Run LLM lead classifier on unclassified articles"):
                classify_limit = st.session_state.get("classify_limit", 50)
                start_job("classify", py_flags + [str(SCRIPTS_DIR / "classify.py"),
                          "--limit", str(classify_limit)], env_overrides)
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

    with b5:
        if PREVIEW_MODE:
            st.markdown('<div class="locked-btn">', unsafe_allow_html=True)
            st.button("▶  Run All", width='stretch', disabled=True, key="preview_run_all", help="Locked because this is a public preview")
            st.markdown('</div><div class="locked-note">LOCKED · PREVIEW</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="run-btn">', unsafe_allow_html=True)
            if st.button("▶  Run All", width='stretch', disabled=disabled,
                         help="Runs discover → fetch → extract → classify sequentially"):
                disc     = str(SCRIPTS_DIR / "discover.py")
                fetch    = str(SCRIPTS_DIR / "fetch.py")
                ext      = str(SCRIPTS_DIR / "extract.py")
                classify = str(SCRIPTS_DIR / "classify.py")
                start_job(
                    "run_all",
                    py_flags + ["-c",
                     f"import subprocess,sys;"
                     f"subprocess.run([sys.executable,{disc!r},{registry!r}],check=True);"
                     f"subprocess.run([sys.executable,{fetch!r}],check=True);"
                     f"subprocess.run([sys.executable,{ext!r}],check=True);"
                     f"subprocess.run([sys.executable,{classify!r}],check=True)"],
                    env_overrides,
                )
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

    # ── Classify settings ─────────────────────
    with st.expander("🎯  Classify Settings", expanded=False):
        st.session_state["classify_limit"] = st.number_input(
            "Articles to classify per run", 1, 500, 50,
            help="How many unclassified articles to process each time you click Classify",
            disabled=PREVIEW_MODE,
        )
        if PREVIEW_MODE:
            st.caption("Locked in preview mode.")

    # ── Live log ──────────────────────────────
    st.markdown('<p class="section-header" style="margin-top:24px">Live Logs</p>', unsafe_allow_html=True)

    # @st.fragment means ONLY this section reruns every 2s — not the whole page
    # This stops the buttons/settings from flickering while logs update
    @st.fragment(run_every=2)
    def live_log_fragment():
        # Read new output from log file
        new_chunk = read_new_log()
        if new_chunk:
            st.session_state.log_buffer += new_chunk
            st.session_state.log_buffer = st.session_state.log_buffer[-10_000:]

        # Detect job finishing and do a final drain
        proc = st.session_state.get("proc")
        if proc and proc.poll() is not None:
            final = read_new_log()
            if final:
                st.session_state.log_buffer += final
            code = proc.returncode
            st.session_state.log_buffer += f"\n\n[GUI] Job finished with exit code {code}.\n"
            lf = st.session_state.get("log_file")
            if lf:
                try:
                    lf.close()
                except Exception:
                    pass
            st.session_state.proc     = None
            st.session_state.log_file = None
            # full rerun only when job finishes so sidebar status updates
            st.rerun()

        log_text = st.session_state.log_buffer or "Ready. Press a run button to start.\n"
        safe_text = (
            log_text.replace("&", "&amp;")
                    .replace("<", "&lt;")
                    .replace(">", "&gt;")
        )
        components.html(
            f"""
            <div id="live-log-box" class="log-box">{safe_text}</div>
            <script>
                const box = document.getElementById("live-log-box");
                if (box) {{
                    box.scrollTop = box.scrollHeight;
                }}
            </script>
            """,
            height=360,
        )

        lc1, lc2 = st.columns([1, 6])
        with lc1:
            if st.button("🗑  Clear log"):
                st.session_state.log_buffer = ""
                st.rerun()
        with lc2:
            lp = st.session_state.get("log_path")
            if lp:
                st.caption(f"Log file: `{lp}`")

    live_log_fragment()

# ─────────────────────────────────────────────
#  PAGE: DASHBOARD
# ─────────────────────────────────────────────
elif page == "📊 Dashboard":
    if not db_ok:
        st.error("⚠️  Database not reachable.")
        st.stop()

    st.markdown('<p class="section-header">Overview</p>', unsafe_allow_html=True)

    # ── Filters ───────────────────────────────
    fc1, fc2, fc3, fc4, fc5 = st.columns([2, 2, 2, 3, 1])
    with fc1:
        lead_scope = render_lead_scope_selector("dashboard_lead_scope", default="Recent found leads")
    with fc2:
        date_range = render_date_range_selector("dashboard_date_range", default="Since 2024")
    with fc3:
        source_scope_sql = get_lead_scope_condition("source_id", lead_scope)
        item_date_scope_sql = get_date_condition("published_at", "created_at", date_range)
        sources_rows = db_fetch(f"select distinct source_id from items where {source_scope_sql} and {item_date_scope_sql} order by source_id")
        source_list  = ["(all)"] + [r["source_id"] for r in sources_rows]
        source_filter = st.selectbox("Source", source_list)
    with fc4:
        search = st.text_input("Search title / URL", placeholder="keyword…")
    with fc5:
        st.markdown("<br>", unsafe_allow_html=True)
        refresh = st.button("↻  Refresh", width='stretch')

    sid = source_filter if source_filter != "(all)" else None
    item_scope_sql = get_lead_scope_condition("source_id", lead_scope)
    item_date_sql = get_date_condition("published_at", "created_at", date_range)
    lead_when_sql = get_lead_when_condition("lead_when", date_range)
    item_scope_date_sql = f"({item_scope_sql}) and ({item_date_sql})"
    lead_item_scope_date_sql = f"({item_scope_date_sql}) and ({lead_when_sql})"
    url_scope_sql = get_lead_scope_condition("u.source_id", lead_scope)

    # ── Stat cards ────────────────────────────
    total_items = db_fetch(
        f"select count(*) as n from items where {item_scope_date_sql}" + (" and source_id=%s" if sid else ""),
        (sid,) if sid else ()
    )
    total_queued = db_fetch(
        f"select count(*) as n from url_state u where last_fetched_at is null and {url_scope_sql}"
        + (" and u.source_id=%s" if sid else ""),
        (sid,) if sid else ()
    )
    total_errors = db_fetch(
        f"select count(*) as n from url_state u where fetch_status >= 400 and {url_scope_sql}"
        + (" and u.source_id=%s" if sid else ""),
        (sid,) if sid else ()
    )
    total_sources = db_fetch(f"select count(distinct source_id) as n from items where {item_scope_date_sql}")
    today_items = db_fetch(
        f"select count(*) as n from items where created_at >= current_date and {item_scope_date_sql}"
        + (" and source_id=%s" if sid else ""),
        (sid,) if sid else ()
    )

    total_leads = db_fetch(f"select count(*) as n from items where lead_score >= 7 and {lead_item_scope_date_sql}")
    unclassified = db_fetch(f"select count(*) as n from items where lead_score is null and clean_text is not null and {item_scope_date_sql}")

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Total Items",       f"{(total_items[0]['n'] if total_items else 0):,}")
    m2.metric("Added Today",       f"{(today_items[0]['n'] if today_items else 0):,}")
    m3.metric("🎯 Leads (≥7)",     f"{(total_leads[0]['n'] if total_leads else 0):,}")
    m4.metric("Unclassified",      f"{(unclassified[0]['n'] if unclassified else 0):,}")
    m5.metric("Fetch Errors",      f"{(total_errors[0]['n'] if total_errors else 0):,}")
    m6.metric("Active Sources",    f"{(total_sources[0]['n'] if total_sources else 0):,}")

    st.markdown("---")

    # ── Charts row ────────────────────────────
    ch1, ch2 = st.columns(2)

    # Items per source
    with ch1:
        st.markdown('<p class="section-header">Items per Source</p>', unsafe_allow_html=True)
        df_src = db_fetch_df(
            f"select source_id, count(*) as items from items where {item_scope_date_sql} group by source_id order by items desc limit 20"
        )
        if not df_src.empty:
            fig = px.bar(
                df_src, x="items", y="source_id", orientation="h",
                color="items", color_continuous_scale=["#1a3a5c", "#4a6fa5", "#7fb3e8"],
            )
            fig.update_layout(**PLOT_LAYOUT, showlegend=False,
                              coloraxis_showscale=False, height=320)
            fig.update_traces(marker_line_width=0)
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("No items yet.")

    # Items over time (last 30 days)
    with ch2:
        st.markdown('<p class="section-header">Items Over Time (30 days)</p>', unsafe_allow_html=True)
        df_time = db_fetch_df(f"""
            select date_trunc('day', created_at)::date as day, count(*) as items
            from items
            where created_at >= now() - interval '30 days' and {item_scope_date_sql}
            group by 1 order by 1
        """)
        if not df_time.empty:
            fig2 = px.area(df_time, x="day", y="items",
                           color_discrete_sequence=["#4a6fa5"])
            fig2.update_layout(**PLOT_LAYOUT, height=320)
            fig2.update_traces(
                line=dict(color="#7fb3e8", width=2),
                fillcolor="rgba(74,111,165,0.18)",
            )
            st.plotly_chart(fig2, width='stretch')
        else:
            st.info("No time data yet.")

    # Fetch status breakdown
    ch3, ch4 = st.columns(2)

    with ch3:
        st.markdown('<p class="section-header">Fetch Status Breakdown</p>', unsafe_allow_html=True)
        df_status = db_fetch_df(f"""
            select
                case
                    when fetch_status between 200 and 299 then '2xx OK'
                    when fetch_status between 300 and 399 then '3xx Redirect'
                    when fetch_status between 400 and 499 then '4xx Client Error'
                    when fetch_status >= 500              then '5xx Server Error'
                    when fetch_status is null             then 'Pending'
                    else 'Other'
                end as status_group,
                count(*) as n
            from url_state u
            where {url_scope_sql}
            group by 1 order by 2 desc
        """)
        if not df_status.empty:
            COLOR_MAP = {
                "2xx OK":           "#4ade80",
                "Pending":          "#4a6fa5",
                "3xx Redirect":     "#facc15",
                "4xx Client Error": "#f87171",
                "5xx Server Error": "#ef4444",
                "Other":            "#6b7385",
            }
            fig3 = px.pie(
                df_status, names="status_group", values="n",
                color="status_group", color_discrete_map=COLOR_MAP,
                hole=0.55,
            )
            fig3.update_layout(**PLOT_LAYOUT, height=300,
                               legend=dict(font=dict(color="#6b7385", size=11)))
            fig3.update_traces(textfont_color="#c8cdd6")
            st.plotly_chart(fig3, width='stretch')
        else:
            st.info("No URL state data yet.")

    with ch4:
        st.markdown('<p class="section-header">Extraction Methods Used</p>', unsafe_allow_html=True)
        df_methods = db_fetch_df(f"""
            select
                case extract_method
                    when 'jsonld'            then 'JSON-LD'
                    when 'cheap'             then 'Fast (cheap)'
                    when 'readability'       then 'Readability'
                    when 'cheap+readability' then 'Fast + Readability'
                    when 'none'              then 'No text found'
                    else 'Legacy (pre-migration)'
                end as method,
                count(*) as n
            from items
            where {item_scope_date_sql}
            group by 1
            order by 2 desc
        """)
        if not df_methods.empty:
            METHOD_COLORS = {
                "JSON-LD":               "#4ade80",
                "Fast (cheap)":          "#7fb3e8",
                "Readability":           "#4a6fa5",
                "Fast + Readability":    "#facc15",
                "No text found":         "#f87171",
                "Legacy (pre-migration)":"#6b7385",
            }
            fig4 = px.bar(
                df_methods, x="method", y="n",
                color="method", color_discrete_map=METHOD_COLORS,
            )
            fig4.update_layout(**PLOT_LAYOUT, showlegend=False, height=300)
            fig4.update_traces(marker_line_width=0)
            st.plotly_chart(fig4, width='stretch')
            legacy = df_methods[df_methods["method"] == "Legacy (pre-migration)"]["n"].sum()
            if legacy > 0:
                st.caption(f"ℹ️ {int(legacy)} legacy items were extracted before diagnostic columns were added.")
        else:
            st.info("No items yet.")

    # ── Pre-filter stats ───────────────────────
    ch5, ch6 = st.columns(2)

    with ch5:
        st.markdown('<p class="section-header">Pre-filter Results</p>', unsafe_allow_html=True)
        df_pf = db_fetch_df(f"""
            select
                case prefilter_pass
                    when true  then 'Passed'
                    when false then 'Filtered out'
                    else 'Not processed'
                end as result,
                count(*) as n
            from items
            where {item_scope_date_sql}
            group by 1
        """)
        if not df_pf.empty:
            PF_COLORS = {"Passed": "#4ade80", "Filtered out": "#f87171", "Not processed": "#6b7385"}
            fig5 = px.pie(df_pf, names="result", values="n",
                          color="result", color_discrete_map=PF_COLORS, hole=0.55)
            fig5.update_layout(**PLOT_LAYOUT, height=280,
                               legend=dict(font=dict(color="#6b7385", size=11)))
            fig5.update_traces(textfont_color="#c8cdd6")
            st.plotly_chart(fig5, width='stretch')
        else:
            st.info("No data yet.")

    with ch6:
        st.markdown('<p class="section-header">Classifier Model Usage</p>', unsafe_allow_html=True)
        df_models = db_fetch_df(f"""
            select
                case
                    when classifier_model is not null then classifier_model
                    when lead_score = 0              then 'Pre-filter (no LLM)'
                    when lead_score is null          then 'Not classified yet'
                    else 'Legacy (pre-migration)'
                end as model,
                count(*) as n
            from items
            where {item_scope_date_sql}
            group by 1
            order by 2 desc
        """)
        if not df_models.empty:
            fig6 = px.bar(df_models, x="model", y="n",
                          color="n", color_continuous_scale=["#1a3a5c","#4a6fa5","#7fb3e8"])
            fig6.update_layout(**PLOT_LAYOUT, showlegend=False,
                               coloraxis_showscale=False, height=280)
            fig6.update_traces(marker_line_width=0)
            st.plotly_chart(fig6, width='stretch')
        else:
            st.info("No classified items yet.")

    st.markdown("---")

    # ── Data tables ───────────────────────────
    tab1, tab2, tab3, tab4 = st.tabs(["📄  Latest Items", "⏳  Queue", "❌  Errors", "🎯  Top Leads"])

    # Build search filter
    def search_clause(alias: str = "") -> str:
        pre = alias + "." if alias else ""
        return f" and ({pre}title ilike %s or {pre}url ilike %s)" if search else ""

    def search_params() -> list:
        return [f"%{search}%", f"%{search}%"] if search else []

    with tab1:
        sql = f"""
            select
                coalesce(published_at::text,'—') as published,
                source_id,
                coalesce(title,'(no title)') as title,
                url,
                length(clean_text) as text_len
            from items
            where 1=1
              and {item_scope_date_sql}
        """
        params: list = []
        if sid:
            sql += " and source_id=%s"; params.append(sid)
        sql += search_clause()
        params += search_params()
        sql += " order by published_at desc nulls last, created_at desc limit 300"

        df_items = db_fetch_df(sql, tuple(params))
        if not df_items.empty:
            st.dataframe(
                df_items,
                width='stretch',
                height=400,
                column_config={
                    "url":      st.column_config.LinkColumn("URL"),
                    "text_len": st.column_config.NumberColumn("Text len", format="%d chars"),
                },
            )
            st.caption(f"{len(df_items)} rows shown (max 300)")
        else:
            st.info("No items yet — run the pipeline first.")

    with tab2:
        sql2 = f"""
            select
                source_id,
                coalesce(fetch_status::text,'pending') as status,
                coalesce(last_fetched_at::text,'—') as last_fetched,
                url
            from url_state u
            where {url_scope_sql}
              and (last_fetched_at is null
               or fetch_status is null
               or fetch_status >= 500
               or (raw_path is not null and content_hash is null and fetch_status between 200 and 299))
        """
        params2: list = []
        if sid:
            sql2 += " and source_id=%s"; params2.append(sid)
        sql2 += search_clause()
        params2 += search_params()
        sql2 += " order by last_fetched asc nulls first limit 300"

        df_q = db_fetch_df(sql2, tuple(params2))
        if not df_q.empty:
            st.dataframe(df_q, width='stretch', height=400,
                         column_config={"url": st.column_config.LinkColumn("URL")})
            st.caption(f"{len(df_q)} rows")
        else:
            st.success("Queue is empty — everything has been fetched.")

    with tab3:
        sql3 = f"""
            select
                source_id,
                fetch_status::text as status,
                coalesce(last_fetched_at::text,'—') as last_fetched,
                url
            from url_state u
            where {url_scope_sql} and fetch_status >= 400
        """
        params3: list = []
        if sid:
            sql3 += " and source_id=%s"; params3.append(sid)
        sql3 += search_clause()
        params3 += search_params()
        sql3 += " order by last_fetched desc nulls last limit 300"

        df_err = db_fetch_df(sql3, tuple(params3))
        if not df_err.empty:
            st.dataframe(df_err, width='stretch', height=400,
                         column_config={"url": st.column_config.LinkColumn("URL")})
            st.caption(f"{len(df_err)} rows")
        else:
            st.success("No fetch errors.")

    with tab4:
        df_leads = db_fetch_df(f"""
            select
                lead_score                              as score,
                coalesce(lead_company, '—')             as company,
                coalesce(lead_city, '—')                as city,
                coalesce(lead_country, '—')             as country,
                coalesce(lead_who, '—')                 as who,
                coalesce(lead_what, '—')                as what,
                coalesce(lead_when, '—')                as when,
                coalesce(lead_reason, '—')              as reason,
                coalesce(title, '(no title)')           as title,
                url
            from items
            where lead_score >= 7 and {lead_item_scope_date_sql}
            order by lead_score desc, created_at desc
            limit 300
        """)
        if not df_leads.empty:
            st.dataframe(
                df_leads,
                width='stretch',
                height=400,
                column_config={
                    "score": st.column_config.NumberColumn("Score", format="%d ⭐"),
                    "url":   st.column_config.LinkColumn("URL"),
                },
            )
            st.caption(f"{len(df_leads)} leads shown")
        else:
            st.info("No leads yet — run Classify first.")

# ─────────────────────────────────────────────
#  PAGE: LEADS
# ─────────────────────────────────────────────
elif page == "🎯 Leads":
    if not db_ok:
        st.error("⚠️  Database not reachable.")
        st.stop()

    st.markdown('<p class="section-header">Lead Overview</p>', unsafe_allow_html=True)

    lf_scope1, lf_scope2 = st.columns(2)
    with lf_scope1:
        lead_scope = render_lead_scope_selector("leads_page_scope", default="Recent found leads")
    with lf_scope2:
        date_range = render_date_range_selector("leads_page_date_range", default="Since 2024")

    item_scope_sql = get_lead_scope_condition("source_id", lead_scope)
    item_scope_sql_i = get_lead_scope_condition("i.source_id", lead_scope)
    item_date_sql = get_date_condition("published_at", "created_at", date_range)
    item_date_sql_i = get_date_condition("i.published_at", "i.created_at", date_range)
    lead_when_sql = get_lead_when_condition("lead_when", date_range)
    lead_when_sql_i = get_lead_when_condition("i.lead_when", date_range)
    item_scope_date_sql = f"({item_scope_sql}) and ({item_date_sql})"
    lead_item_scope_date_sql = f"({item_scope_date_sql}) and ({lead_when_sql})"
    item_scope_date_sql_i = f"({item_scope_sql_i}) and ({item_date_sql_i}) and ({lead_when_sql_i})"

    # ── Stats ─────────────────────────────────
    s1, s2, s3, s4 = st.columns(4)
    total_leads  = db_fetch(f"select count(*) as n from items where lead_score >= 7 and {lead_item_scope_date_sql}")
    strong_leads = db_fetch(f"select count(*) as n from items where lead_score >= 9 and {lead_item_scope_date_sql}")
    unclassified = db_fetch(f"select count(*) as n from items where lead_score is null and clean_text is not null and {item_scope_date_sql}")
    avg_score    = db_fetch(f"select round(avg(lead_score),1) as n from items where lead_score is not null and {lead_item_scope_date_sql}")

    s1.metric("🎯 Leads (score ≥ 7)", f"{(total_leads[0]['n'] if total_leads else 0):,}")
    s2.metric("🔥 Strong (score ≥ 9)", f"{(strong_leads[0]['n'] if strong_leads else 0):,}")
    s3.metric("Avg Score",             f"{(avg_score[0]['n'] if avg_score and avg_score[0]['n'] else '—')}")
    s4.metric("Unclassified",          f"{(unclassified[0]['n'] if unclassified else 0):,}")

    st.markdown("---")

    # ── Score distribution chart ───────────────
    col_chart, col_export = st.columns([2, 1])

    with col_chart:
        st.markdown('<p class="section-header">Score Distribution</p>', unsafe_allow_html=True)
        df_scores = db_fetch_df(f"""
            select lead_score as score, count(*) as count
            from items
            where lead_score is not null and {lead_item_scope_date_sql}
            group by lead_score
            order by lead_score desc
        """)
        if not df_scores.empty:
            colors = ["#4ade80" if s >= 7 else "#facc15" if s >= 4 else "#6b7385"
                      for s in df_scores["score"]]
            fig = px.bar(df_scores, x="score", y="count", color="score",
                         color_continuous_scale=["#6b7385","#facc15","#4ade80"])
            fig.update_layout(**PLOT_LAYOUT, showlegend=False,
                              coloraxis_showscale=False, height=280)
            fig.update_traces(marker_line_width=0)
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("No classified articles yet — run Classify first.")

    with col_export:
        st.markdown('<p class="section-header">Export to CRM</p>', unsafe_allow_html=True)
        export_threshold = st.number_input("Min score", 1, 10, 7)
        export_days      = st.number_input("Last N days (0 = all)", 0, 365, 0)
        if st.button("📥  Export to Excel", width='stretch', disabled=not db_ok):
            export_script = SCRIPTS_DIR / "export_leads.py"
            if not export_script.exists():
                st.error("export_leads.py not found in scripts dir.")
            else:
                args = [str(Path(sys.executable)), "-u", str(export_script),
                        "--threshold", str(export_threshold)]
                if export_days > 0:
                    args += ["--since-days", str(export_days)]
                run_env = {**os.environ, "PYTHONUNBUFFERED": "1"}
                # ensure app/src is on PYTHONPATH so lc package is found
                src_path = str(PROJECT_ROOT / "app" / "src")
                existing_pp = run_env.get("PYTHONPATH", "")
                sep = ";" if os.name == "nt" else ":"
                run_env["PYTHONPATH"] = src_path + (sep + existing_pp if existing_pp else "")
                result = subprocess.run(args, capture_output=True, text=True,
                                        timeout=120,
                                        env=run_env)
                if result.returncode == 0:
                    st.success(result.stdout or "Export complete.")
                else:
                    st.error(result.stderr or result.stdout or "Export failed.")

    st.markdown("---")

    # ── Tabs ──────────────────────────────────
    lead_tab1, lead_tab2 = st.tabs(["🔍  Browse Leads", "✅  Review & Label"])

    # ── Tab 1: Browse ─────────────────────────
    with lead_tab1:
        lf1, lf2, lf3 = st.columns([1, 2, 1])
        with lf1:
            min_score = st.slider("Minimum score", 1, 10, 7)
        with lf2:
            lead_search = st.text_input("Search company / title", placeholder="keyword…")
        with lf3:
            st.markdown("<br>", unsafe_allow_html=True)
            st.button("↻  Refresh", width='stretch', key="leads_refresh")

        sql_leads = f"""
            select
                i.item_id,
                i.lead_score                             as score,
                coalesce(i.lead_company, '—')            as company,
                coalesce(i.lead_city, '—')               as city,
                coalesce(i.lead_country, '—')            as country,
                coalesce(i.lead_who, '—')                as who,
                coalesce(i.lead_what, '—')               as what,
                coalesce(i.lead_when, '—')               as when,
                coalesce(i.lead_reason, '—')             as reason,
                coalesce(i.lead_description, '—')        as description,
                coalesce(i.title, '(no title)')          as title,
                i.url,
                i.created_at::date                       as date,
                coalesce(ll.label, 'new')                as status
            from items i
            left join (
                select distinct on (item_id) item_id, label
                from lead_labels order by item_id, labeled_at desc
            ) ll on ll.item_id = i.item_id
            where i.lead_score >= %s
              and {item_scope_date_sql_i}
        """
        params_leads: list = [min_score]
        if lead_search:
            sql_leads += " and (i.lead_company ilike %s or i.title ilike %s)"
            params_leads += [f"%{lead_search}%", f"%{lead_search}%"]
        sql_leads += " order by i.lead_score desc, i.created_at desc limit 500"

        df_l = db_fetch_df(sql_leads, tuple(params_leads))
        if not df_l.empty:
            st.dataframe(
                df_l.drop(columns=["item_id"]),
                width='stretch',
                height=500,
                column_config={
                    "score":       st.column_config.NumberColumn("Score", format="%d ⭐"),
                    "url":         st.column_config.LinkColumn("URL"),
                    "description": st.column_config.TextColumn("Description", width="large"),
                    "status":      st.column_config.TextColumn("Status"),
                },
            )
            st.caption(f"{len(df_l)} leads shown")
        else:
            st.info("No leads found. Try lowering the minimum score or run Classify first.")

    # ── Tab 2: Review & Label ─────────────────
    with lead_tab2:
        st.markdown("Review leads and set their status. Click **Save Labels** when done.")

        # filter by status
        rc1, rc2, rc3 = st.columns([1, 2, 1])
        with rc1:
            review_min_score = st.slider("Min score", 1, 10, 7, key="review_score")
        with rc2:
            status_filter = st.selectbox(
                "Show status",
                ["new", "confirmed", "follow_up", "contacted", "rejected", "all"],
                index=0
            )
        with rc3:
            st.markdown("<br>", unsafe_allow_html=True)
            st.button("↻  Refresh", width='stretch', key="review_refresh")

        # fetch leads with current label
        sql_review = f"""
            select
                i.item_id,
                i.lead_score                        as score,
                coalesce(i.lead_company, '—')       as company,
                coalesce(i.lead_city, '—')          as city,
                coalesce(i.lead_country, '—')       as country,
                coalesce(i.lead_who, '—')           as who,
                coalesce(i.lead_what, '—')          as what,
                coalesce(i.lead_when, '—')          as when,
                coalesce(i.lead_reason, '—')        as reason,
                coalesce(i.title, '(no title)')     as title,
                i.url,
                coalesce(ll.label, 'new')           as status
            from items i
            left join (
                select distinct on (item_id) item_id, label
                from lead_labels order by item_id, labeled_at desc
            ) ll on ll.item_id = i.item_id
            where i.lead_score >= %s
              and {item_scope_date_sql_i}
        """
        rparams: list = [review_min_score]
        if status_filter != "all":
            sql_review += " and coalesce(ll.label, 'new') = %s"
            rparams.append(status_filter)
        sql_review += " order by i.lead_score desc limit 200"

        df_review = db_fetch_df(sql_review, tuple(rparams))

        if df_review.empty:
            st.info("No leads to review.")
        else:
            STATUS_OPTIONS = ["new", "confirmed", "follow_up", "contacted", "rejected"]

            edited = st.data_editor(
                df_review.drop(columns=["item_id"]),
                width='stretch',
                height=500,
                column_config={
                    "score":  st.column_config.NumberColumn("Score", format="%d ⭐"),
                    "url":    st.column_config.LinkColumn("URL"),
                    "status": st.column_config.SelectboxColumn(
                        "Status",
                        options=STATUS_OPTIONS,
                        required=True,
                    ),
                },
                disabled=["score", "who", "what", "when", "company", "city", "country", "reason", "title", "url"],
            )

            if st.button("💾  Save Labels", type="primary"):
                saved = 0
                with get_db_conn() as conn:
                    with conn.cursor() as cur:
                        for i, row in edited.iterrows():
                            item_id = int(df_review.iloc[i]["item_id"])
                            new_status = row["status"]
                            old_status = df_review.iloc[i]["status"]
                            if new_status != old_status:
                                cur.execute("""
                                    insert into lead_labels (item_id, label, labeled_by)
                                    values (%s, %s, 'gui')
                                """, (item_id, new_status))
                                saved += 1
                if saved:
                    st.success(f"Saved {saved} label(s).")
                    st.rerun()
                else:
                    st.info("No changes detected.")

            # quick stats
            status_counts = df_review["status"].value_counts()
            sc1, sc2, sc3, sc4, sc5 = st.columns(5)
            sc1.metric("New",       status_counts.get("new", 0))
            sc2.metric("Confirmed", status_counts.get("confirmed", 0))
            sc3.metric("Follow Up", status_counts.get("follow_up", 0))
            sc4.metric("Contacted", status_counts.get("contacted", 0))
            sc5.metric("Rejected",  status_counts.get("rejected", 0))

# ── PAGE: STATISTICS ──────────────────────────────────────────────────────────
elif page == "📈 Statistics":
    st.title("📈 Pipeline Statistics")

    statf1, statf2 = st.columns(2)
    with statf1:
        stats_scope = render_lead_scope_selector("statistics_lead_scope", default="Recent found leads")
    with statf2:
        stats_date_range = render_date_range_selector("statistics_date_range", default="Since 2024")

    stats_item_scope = get_lead_scope_condition("i.source_id", stats_scope)
    stats_item_date = get_date_condition("i.published_at", "i.created_at", stats_date_range)
    stats_item_scope_date = f"({stats_item_scope}) and ({stats_item_date})"
    st.caption("Pipeline run metrics stay global. The lead-scope and date selector affect the benchmark section below.")

    with get_db_conn() as conn:
        # check if pipeline_runs table exists yet
        with conn.cursor() as cur:
            cur.execute("""
                select exists (
                    select from information_schema.tables
                    where table_name = 'pipeline_runs'
                )
            """)
            table_exists = cur.fetchone()["exists"]

    if not table_exists:
        st.info("No run data yet. Run the pipeline at least once to see statistics.")
        st.code('Get-Content app/models.sql | docker exec -i leadcollector-db-1 psql -U lc -d lc',
                language="bash")
        st.stop()

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                select script, started_at, finished_at, duration_s,
                       articles_processed, articles_ok, articles_err,
                       articles_skipped, avg_s_per_article, notes
                from pipeline_runs
                where finished_at is not null
                order by started_at desc
                limit 200
            """)
            runs = cur.fetchall()

    if not runs:
        st.info("No completed runs yet. Run the pipeline to see statistics.")
        st.stop()

    df_runs = pd.DataFrame(runs)
    df_runs["started_at"] = pd.to_datetime(df_runs["started_at"])
    df_runs["date"]       = df_runs["started_at"].dt.date
    df_runs["label"]      = df_runs["script"] + (
        df_runs["notes"].apply(lambda n: f" ({n})" if n else "")
    )

    # ── Top metrics ───────────────────────────────────────────────────────────
    st.subheader("Overall")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Runs",       len(df_runs))
    m2.metric("Total Articles",   int(df_runs["articles_processed"].sum()))
    m3.metric("Total Errors",     int(df_runs["articles_err"].sum()))
    last_run = df_runs.iloc[0]
    m4.metric("Last Run",         str(last_run["started_at"].strftime("%d.%m %H:%M")))

    st.divider()

    # ── Per-script breakdown ──────────────────────────────────────────────────
    st.subheader("Per Script")
    for script in ["discover", "fetch", "extract", "classify"]:
        df_s = df_runs[df_runs["script"] == script]
        if df_s.empty:
            continue
        with st.expander(f"**{script.capitalize()}** — {len(df_s)} runs", expanded=True):
            c1, c2, c3, c4 = st.columns(4)
            avg_dur = df_s["duration_s"].mean()
            avg_art = df_s["articles_processed"].mean()
            avg_tpa = df_s["avg_s_per_article"].dropna().mean()
            err_rate = (df_s["articles_err"].sum() /
                        df_s["articles_processed"].sum() * 100
                        if df_s["articles_processed"].sum() > 0 else 0)

            c1.metric("Avg Duration",        f"{avg_dur:.0f}s" if avg_dur else "—")
            c2.metric("Avg Articles/Run",    f"{avg_art:.0f}" if avg_art else "—")
            c3.metric("Avg Time/Article",    f"{avg_tpa:.2f}s" if avg_tpa and avg_tpa > 0 else "—")
            c4.metric("Error Rate",          f"{err_rate:.1f}%")

    st.divider()

    # ── Timeline chart ────────────────────────────────────────────────────────
    st.subheader("Run Timeline")
    fig_timeline = px.bar(
        df_runs.head(50),
        x="started_at", y="duration_s",
        color="script",
        hover_data=["articles_processed", "articles_ok", "articles_err", "notes"],
        labels={"duration_s": "Duration (s)", "started_at": "Time", "script": "Script"},
        title="Last 50 runs — duration per script",
        color_discrete_map={
            "discover": "#4C72B0",
            "fetch":    "#DD8452",
            "extract":  "#55A868",
            "classify": "#C44E52",
        }
    )
    st.plotly_chart(fig_timeline, use_container_width=True)

    # ── Articles per minute throughput ────────────────────────────────────────
    st.subheader("Throughput")
    df_runs["articles_per_min"] = (
        df_runs["articles_processed"] / (df_runs["duration_s"] / 60)
    ).replace([float("inf"), float("nan")], 0)

    fig_tput = px.line(
        df_runs[df_runs["script"].isin(["fetch", "extract", "classify"])].sort_values("started_at"),
        x="started_at", y="articles_per_min",
        color="script",
        markers=True,
        labels={"articles_per_min": "Articles/min", "started_at": "Time"},
        title="Articles per minute over time",
    )
    st.plotly_chart(fig_tput, use_container_width=True)

    # ── Classify: time per article per model ──────────────────────────────────
    df_classify = df_runs[(df_runs["script"] == "classify") & (df_runs["avg_s_per_article"].notna())]
    if not df_classify.empty:
        st.subheader("Classify — Time per Article by Model")
        fig_model = px.box(
            df_classify,
            x="notes", y="avg_s_per_article",
            labels={"notes": "Model", "avg_s_per_article": "Avg s/article"},
            title="Classification speed by model",
        )
        st.plotly_chart(fig_model, use_container_width=True)

    # ── Raw run log ───────────────────────────────────────────────────────────
    st.subheader("Run Log")
    st.dataframe(
        df_runs[[
            "started_at", "script", "notes", "duration_s",
            "articles_processed", "articles_ok", "articles_err", "articles_skipped",
            "avg_s_per_article"
        ]].rename(columns={
            "started_at":         "Started",
            "script":             "Script",
            "notes":              "Notes",
            "duration_s":         "Duration (s)",
            "articles_processed": "Processed",
            "articles_ok":        "OK",
            "articles_err":       "Errors",
            "articles_skipped":   "Skipped",
            "avg_s_per_article":  "Avg s/article",
        }),
        use_container_width=True,
        hide_index=True,
    )


    # ── Benchmark Analysis ───────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("🤖 Model Benchmark")

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            # Check if item_scores has data for the selected scope
            cur.execute(f"""
                select count(*) as n
                from item_scores s
                join items i on i.item_id = s.item_id
                where {stats_item_scope_date}
            """)
            n_scores = cur.fetchone()["n"]

    if n_scores == 0:
        st.info("No benchmark data yet. Run classify.py with --model and --prompt arguments.")
    else:
        RELEVANT_LABELS = {"confirmed", "contacted", "follow_up"}

        with get_db_conn() as conn:
            with conn.cursor() as cur:
                # Ground truth
                cur.execute(f"""
                    select ll.item_id, ll.label
                    from lead_labels ll
                    join items i on i.item_id = ll.item_id
                    where ll.label in ('confirmed','contacted','follow_up','rejected')
                      and {stats_item_scope_date}
                """)
                label_rows = cur.fetchall()

                # All scores
                cur.execute(f"""
                    select s.item_id, s.model, s.prompt_version, s.lead_score
                    from item_scores s
                    join items i on i.item_id = s.item_id
                    where s.lead_score is not null
                      and {stats_item_scope_date}
                """)
                score_rows = cur.fetchall()

        # Build ground truth dict
        gt = {}
        for row in label_rows:
            val = 1 if row["label"] in RELEVANT_LABELS else 0
            if row["item_id"] not in gt or val > gt[row["item_id"]]:
                gt[row["item_id"]] = val

        threshold = st.slider("Score threshold for 'lead'", min_value=5, max_value=9, value=7)

        # Group scores by model + prompt
        from collections import defaultdict
        runs = defaultdict(list)
        for row in score_rows:
            key = (row["model"], row["prompt_version"] or "?")
            runs[key].append(row)

        metrics_rows = []
        for (model, prompt), scores in sorted(runs.items()):
            labeled = [(s["lead_score"], gt[s["item_id"]])
                       for s in scores if s["item_id"] in gt]
            if not labeled:
                continue

            tp = sum(1 for sc, g in labeled if sc >= threshold and g == 1)
            fp = sum(1 for sc, g in labeled if sc >= threshold and g == 0)
            fn = sum(1 for sc, g in labeled if sc < threshold  and g == 1)
            tn = sum(1 for sc, g in labeled if sc < threshold  and g == 0)

            precision = tp / (tp + fp) if (tp + fp) > 0 else 0
            recall    = tp / (tp + fn) if (tp + fn) > 0 else 0
            f1        = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
            accuracy  = (tp + tn) / len(labeled)

            all_scores_vals = [s["lead_score"] for s in scores]
            avg_score = sum(all_scores_vals) / len(all_scores_vals)
            high_pct  = sum(1 for v in all_scores_vals if v >= threshold) / len(all_scores_vals) * 100

            metrics_rows.append({
                "Model": model,
                "Prompt": prompt,
                "Total": len(scores),
                "Labeled": len(labeled),
                "TP": tp, "FP": fp, "FN": fn, "TN": tn,
                "Precision": round(precision, 3),
                "Recall":    round(recall, 3),
                "F1":        round(f1, 3),
                "Accuracy":  round(accuracy, 3),
                "Avg Score": round(avg_score, 2),
                "High Score %": round(high_pct, 1),
            })

        if metrics_rows:
            df_metrics = pd.DataFrame(metrics_rows)

            # Highlight best F1
            best_f1_idx = df_metrics["F1"].idxmax()

            st.markdown(f"**{stats_scope}** · **{len(gt)} labeled items** — "
                        f"{sum(gt.values())} relevant, {len(gt)-sum(gt.values())} rejected")

            col1, col2, col3 = st.columns(3)
            best = df_metrics.loc[best_f1_idx]
            col1.metric("Best F1", f"{best['F1']:.3f}", f"{best['Model']} prompt={best['Prompt']}")
            best_p = df_metrics.loc[df_metrics["Precision"].idxmax()]
            col2.metric("Best Precision", f"{best_p['Precision']:.3f}", f"{best_p['Model']} prompt={best_p['Prompt']}")
            best_r = df_metrics.loc[df_metrics["Recall"].idxmax()]
            col3.metric("Best Recall", f"{best_r['Recall']:.3f}", f"{best_r['Model']} prompt={best_r['Prompt']}")

            st.dataframe(
                df_metrics.style.highlight_max(subset=["F1","Precision","Recall"], color="#1a4a1a"),
                use_container_width=True,
                hide_index=True,
            )

            # F1 bar chart by model+prompt
            df_metrics["Label"] = df_metrics["Model"] + " / " + df_metrics["Prompt"]
            fig_f1 = px.bar(
                df_metrics.sort_values("F1", ascending=False),
                x="Label", y="F1", color="Prompt",
                title="F1 Score by Model & Prompt",
                color_discrete_map={"A": "#4a90d9", "B": "#e07b39"},
                text="F1",
            )
            fig_f1.update_traces(texttemplate="%{text:.3f}", textposition="outside")
            fig_f1.update_layout(yaxis_range=[0, 1], showlegend=True)
            st.plotly_chart(fig_f1, use_container_width=True)

            # P/R comparison
            df_pr = df_metrics.melt(
                id_vars=["Label"], value_vars=["Precision", "Recall", "F1"],
                var_name="Metric", value_name="Value"
            )
            fig_pr = px.bar(
                df_pr, x="Label", y="Value", color="Metric", barmode="group",
                title="Precision / Recall / F1 by Model & Prompt",
            )
            fig_pr.update_layout(yaxis_range=[0, 1])
            st.plotly_chart(fig_pr, use_container_width=True)

            # Score distribution heatmap
            st.subheader("Score Distributions")
            dist_data = []
            for (model, prompt), scores in sorted(runs.items()):
                vals = [s["lead_score"] for s in scores if s["lead_score"] is not None]
                for score_val in range(0, 11):
                    dist_data.append({
                        "Label": f"{model}/{prompt}",
                        "Score": score_val,
                        "Count": sum(1 for v in vals if v == score_val),
                    })
            df_dist = pd.DataFrame(dist_data)
            fig_dist = px.bar(
                df_dist, x="Score", y="Count", color="Label", barmode="group",
                title="Score Distribution per Model & Prompt",
            )
            st.plotly_chart(fig_dist, use_container_width=True)


            # Confusion matrices
            st.subheader("Confusion Matrices")
            st.caption("Threshold: score >= " + str(threshold))

            cm_cols = st.columns(2)
            for i, row in enumerate(metrics_rows):
                with cm_cols[i % 2]:
                    st.markdown(f"**{row['Model']} / Prompt {row['Prompt']}**")
                    cm_data = pd.DataFrame(
                        [[row['TP'], row['FN']], [row['FP'], row['TN']]],
                        index=["Actual LEAD", "Actual NOT LEAD"],
                        columns=["Pred LEAD", "Pred NOT LEAD"]
                    )
                    st.dataframe(
                        cm_data.style.apply(lambda x: [
                            "background-color: #c0dd97; color: #3b6d11" if (x.name == "Actual LEAD" and c == "Pred LEAD") or
                                                                            (x.name == "Actual NOT LEAD" and c == "Pred NOT LEAD")
                            else "background-color: #f7c1c1; color: #a32d2d" if (x.name == "Actual NOT LEAD" and c == "Pred LEAD")
                            else "background-color: #fac775; color: #854f0b"
                            for c in x.index
                        ], axis=1),
                        use_container_width=True,
                    )
                    st.caption(f"P={row['Precision']} · R={row['Recall']} · F1={row['F1']}")

            # Avg score comparison
            fig_avg = px.bar(
                df_metrics.sort_values("Avg Score", ascending=False),
                x="Label", y="Avg Score", color="Prompt",
                title="Average Score by Model & Prompt",
                color_discrete_map={"A": "#4a90d9", "B": "#e07b39"},
                text="Avg Score",
            )
            fig_avg.update_traces(texttemplate="%{text:.2f}", textposition="outside")
            st.plotly_chart(fig_avg, use_container_width=True)

# ─────────────────────────────────────────────
#  PAGE: ABOUT
# ─────────────────────────────────────────────
elif page == "📖 About":
    st.title("📖 About LeadCollector")

    st.markdown("""
    **LeadCollector** is an automated B2B lead generation pipeline built for
    **Rail Cargo Group (RCG)**, a rail freight transport company operating across Central and Eastern Europe.

    The core idea is simple: potential rail freight customers announce themselves in the news.
    A company building a new factory, expanding a warehouse, or opening a logistics hub will
    almost always publish a press release or appear in trade media — before they ever contact
    a freight provider. LeadCollector monitors these sources continuously and surfaces those
    signals automatically, so the BD team can focus on selling instead of searching.
    """)

    st.markdown("---")

    # ── How a lead is made ───────────────────────────────────────
    st.subheader("How a Lead is Created")
    st.markdown("""
    A lead goes through the following funnel from raw internet text to a CRM-ready contact:
    """)

    st.markdown("""
    **1. Source monitoring (`discover.py`)**
    The pipeline reads a registry of 50+ news sources — Austrian and German trade papers,
    logistics magazines, construction industry news, steel and chemical sector outlets,
    CEE regional business media, and railway-specific publications. For each source it uses
    RSS feeds, sitemaps, or HTML scraping to collect new article URLs. Already-seen URLs are
    tracked in the database so nothing is processed twice.

    **2. Fetching (`fetch.py`)**
    Each new URL is downloaded and the raw HTML is stored to disk. HTTP headers like ETag
    and Last-Modified are saved so unchanged pages can be skipped on future runs.

    **3. Text extraction (`extract.py`)**
    The raw HTML is parsed to extract clean article text. Three strategies are tried in order:
    first JSON-LD structured data (highest quality, used by ~30% of professional news sites),
    then fast paragraph scraping, then Mozilla Readability as a fallback. Paywalled pages,
    PDFs, and login pages are detected and discarded automatically.

    **4. Pre-filter (`filters.py`)**
    Before any LLM is involved, a fast keyword filter checks the title and first 800 characters
    for freight and industry signals. Articles about sports, politics, celebrity news, or pure
    finance with no logistics angle are discarded immediately — typically 60–80% of articles.
    This keeps LLM costs low and processing fast.

    **5. LLM classification (`classify.py`)**
    Remaining articles are sent to a local LLM running via Ollama. The model receives the
    article text and a structured prompt asking it to:
    - Score the article **1–10** for rail freight lead potential
    - Extract the **company name**, **city**, **country**
    - Describe **who** is involved, **what** they are doing, and **when**
    - Classify the **lead type** (factory expansion, warehouse, logistics hub, etc.)
    - Assess **transport need** (raw material inbound, finished goods outbound, etc.)
    - Rate **rail fit** (high / medium / low)

    Results are stored in the database with the model name and prompt version so multiple
    model runs can be compared side by side.

    **6. Human review (this GUI)**
    The sales team reviews articles scored ≥ 7 in the Leads tab. Each lead can be labeled
    as **confirmed**, **follow-up**, **contacted**, or **rejected**. This feedback forms the
    ground truth for benchmarking future model improvements.

    **7. CRM export (`export_leads.py`)**
    Confirmed leads are written into a Microsoft Dynamics CRM Excel import template with
    all required fields mapped: company, contact details, city, country, description,
    source campaign, and lead score.
    """)

    st.markdown("---")

    # ── Scripts overview ─────────────────────────────────────────
    st.subheader("Scripts Overview")

    scripts = {
        "discover.py": "Reads registry.yaml and finds new article URLs from 50+ sources via RSS, sitemap, or HTML scraping.",
        "fetch.py": "Downloads raw HTML for each discovered URL and stores it to disk with HTTP cache headers.",
        "extract.py": "Parses HTML and extracts clean article text using JSON-LD, paragraph scraping, or Readability.",
        "filters.py": "Keyword-based pre-filter that instantly discards irrelevant articles without using the LLM.",
        "classify.py": "Sends articles to a local Ollama LLM which scores them and extracts structured lead data.",
        "benchmark.py": "Evaluates model/prompt combinations against ground truth labels. Outputs precision, recall, F1.",
        "export_leads.py": "Exports confirmed leads to the Dynamics CRM Excel import template.",
        "import_crm_leads.py": "One-time import of existing CRM leads to build the gold benchmark dataset.",
    }

    for script, desc in scripts.items():
        with st.expander(f"`{script}`"):
            st.markdown(desc)

    st.markdown("---")

    # ── Scoring ──────────────────────────────────────────────────
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Lead Scoring")
        st.markdown("""
        | Score | Meaning |
        |-------|---------|
        | 9–10 | Very strong — major industrial site, confirmed freight demand |
        | 7–8 | Strong — clear physical investment, likely freight demand |
        | 5–6 | Moderate — early stage, indirect signal |
        | 3–4 | Weak — some relevance but no clear freight need |
        | 1–2 | Not relevant — finance, HR, opinion |
        | 0 | Pre-filtered — no LLM used |
        """)

    with col2:
        st.subheader("What Makes a Good Lead")
        st.markdown("""
        RCG is looking for companies that will need to **ship goods by rail**. Strong signals:

        - 🏭 New factory, production line, or industrial facility
        - 🏗️ Warehouse or logistics hub construction
        - ⛏️ Mining, quarrying, or raw material extraction
        - 🧪 Chemical plant expansion or new process line
        - 🌲 Sawmill, paper mill, or biomass facility
        - 🔋 Battery or EV manufacturing plant
        - 🏢 Large-scale construction requiring bulk materials

        Weak or irrelevant signals:
        - Financial results, management changes, M&A without logistics angle
        - Retail, consumer goods, software
        - Opinion pieces, political commentary
        """)

    st.markdown("---")

    # ── Benchmark ────────────────────────────────────────────────
    st.subheader("Benchmark Results")
    st.markdown("""
    4 models × 2 prompt strategies = **8 combinations** evaluated against
    172 manually labeled leads (47 relevant, 125 rejected) from RCG's CRM.
    Threshold: score ≥ 7 = predicted lead.
    """)

    benchmark_data = {
        "Model": ["gemma3:4b", "gemma3:4b", "mistral:7b", "mistral:7b",
                  "llama3.2:3b", "llama3.2:3b", "qwen2.5:3b", "qwen2.5:3b"],
        "Prompt": ["B (generous)", "A (conservative)", "B (generous)", "A (conservative)",
                   "A (conservative)", "B (generous)", "B (generous)", "A (conservative)"],
        "Precision": [0.302, 0.291, 0.294, 0.292, 0.289, 0.286, 0.246, 0.200],
        "Recall":    [0.957, 0.787, 0.933, 0.745, 0.936, 0.936, 0.362, 0.043],
        "F1":        [0.459, 0.425, 0.447, 0.419, 0.442, 0.438, 0.293, 0.070],
    }
    import pandas as pd
    df = pd.DataFrame(benchmark_data)
    st.dataframe(
        df.style.highlight_max(subset=["F1", "Recall"], color="#1a4a1a"),
        use_container_width=True,
        hide_index=True,
    )
    st.markdown("""
    **Key findings:**
    - Prompt B (generous/freight-first) consistently outperforms Prompt A across all models
    - gemma3:4b achieves the best F1 (0.459) and recall (0.957) — finding 45 of 47 relevant leads
    - Precision is similar across capable models (~0.29–0.30) — recall is the main differentiator
    - High recall matters more than precision for lead generation: missing a lead is more costly than reviewing a false positive
    - qwen2.5:3b is unsuitable — scores almost nothing with Prompt A (4.2% high scores)
    """)

    st.markdown("---")

    # ── Tech stack ───────────────────────────────────────────────
    st.subheader("Technology Stack")
    st.markdown("""
    | Layer | Technology |
    |-------|-----------|
    | Web scraping | Python, httpx, BeautifulSoup, feedparser |
    | Text extraction | Mozilla Readability, JSON-LD, lxml |
    | LLM inference | Ollama (local, privacy-preserving) |
    | Best model | gemma3:4b with Prompt B |
    | Database | PostgreSQL |
    | GUI | Streamlit |
    | CRM integration | openpyxl → Microsoft Dynamics |
    | Sources | 50+ RSS feeds and news sites |
    """)

    st.markdown("---")
    st.caption("Bachelor's thesis project · Rail Cargo Group · 2026")
