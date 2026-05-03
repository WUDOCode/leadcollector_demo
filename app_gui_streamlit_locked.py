"""
LeadCollector – Streamlit GUI professional palette
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
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

:root {
    /* Lead Management System palette */
    --primary: #0f172a;
    --secondary: #243455;
    --tertiary: #fc4640;
    --neutral: #f2f5f6;

    --bg: #f2f5f6;
    --panel: #ffffff;
    --panel-2: #f7f9fa;
    --border: #dfe5e8;
    --border-2: #cfd8dd;
    --text: #0f172a;
    --muted: #526173;
    --muted-2: #8b98a8;
    --navy: #0f172a;
    --blue: #243455;
    --blue-soft: #eef2f7;
    --green: #0f172a;
    --green-soft: #eef2f7;
    --yellow: #243455;
    --yellow-soft: #eef2f7;
    --red: #fc4640;
    --red-soft: #fff0ef;
}

html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
.stApp { background: var(--bg); color: var(--text); }
.block-container { padding-top: 1.6rem; padding-bottom: 3rem; max-width: 1260px; }

/* Hide Streamlit chrome a little for an app-like look */
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
header[data-testid="stHeader"] { background: rgba(242, 245, 246, 0.78); backdrop-filter: blur(10px); }

/* Sidebar */
[data-testid="stSidebar"] {
    background: #f7f9fa;
    border-right: 1px solid var(--border);
    box-shadow: 8px 0 24px rgba(15, 23, 42, 0.03);
}
[data-testid="stSidebar"] > div:first-child { padding-top: 1.2rem; }
.sidebar-brand {
    display: flex; align-items: center; gap: 10px;
    padding: 10px 6px 18px 6px;
}
.logo-box {
    width: 34px; height: 34px; border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    background: var(--navy); color: white; font-weight: 800; font-size: 14px;
    box-shadow: 0 8px 18px rgba(15, 23, 42, 0.16);
}
.brand-title { font-size: 15px; font-weight: 800; color: var(--text); line-height: 1.1; }
.brand-subtitle { font-size: 11px; color: var(--muted); margin-top: 2px; }

[data-testid="stSidebar"] .stRadio label { color: var(--muted); font-size: 12px; }
[data-testid="stSidebar"] [role="radiogroup"] label {
    border-radius: 10px; padding: 8px 10px; margin: 2px 0;
    color: var(--text); transition: all .15s ease;
}
[data-testid="stSidebar"] [role="radiogroup"] label:hover { background: #eef2f7; }
[data-testid="stSidebar"] hr { border-color: var(--border); }

/* Typography */
h1, h2, h3 { color: var(--text); letter-spacing: -0.03em; }
h1 { font-size: 34px !important; line-height: 1.05 !important; font-weight: 800 !important; margin-bottom: .25rem !important; }
.page-subtitle { color: var(--muted); font-size: 14px; margin-bottom: 18px; }
.page-kicker {
    color: var(--muted); font-size: 11px; letter-spacing: .12em; text-transform: uppercase;
    font-weight: 700; margin-bottom: 6px;
}
.section-header {
    display: flex; align-items: center; justify-content: space-between;
    font-size: 13px; font-weight: 800; color: var(--text);
    padding: 0; margin: 2px 0 14px 0; letter-spacing: -0.01em;
}
.section-caption { color: var(--muted); font-size: 12px; margin-top: -8px; margin-bottom: 12px; }

/* Cards and metrics */
[data-testid="stMetric"] {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 16px 18px;
    box-shadow: 0 8px 24px rgba(15, 23, 42, 0.04);
}
[data-testid="stMetricLabel"] {
    color: var(--muted); font-size: 11px; letter-spacing: 0.06em; text-transform: uppercase; font-weight: 700;
}
[data-testid="stMetricValue"] { color: var(--text); font-size: 1.75rem; font-weight: 800; letter-spacing: -0.04em; }
[data-testid="stMetricDelta"] { font-size: 12px; font-weight: 700; }

/* Containers */
[data-testid="stVerticalBlock"] > [style*="flex-direction: column;"] > [data-testid="stVerticalBlockBorderWrapper"],
[data-testid="stExpander"] {
    border-color: var(--border) !important;
    border-radius: 14px !important;
    background: var(--panel) !important;
    box-shadow: 0 8px 24px rgba(15, 23, 42, 0.035);
}

/* Inputs */
.stTextInput input, .stSelectbox [data-baseweb="select"], .stNumberInput input {
    background: var(--panel) !important;
    border-color: var(--border) !important;
    border-radius: 10px !important;
    color: var(--text) !important;
}
.stSlider [data-testid="stTickBar"] { display: none; }

/* Buttons */
.stButton > button {
    background: var(--panel); color: var(--text); border: 1px solid var(--border-2); border-radius: 10px;
    font-size: 13px; font-weight: 700; padding: 8px 16px; transition: all 0.15s ease;
    box-shadow: 0 3px 8px rgba(15, 23, 42, 0.04);
}
.stButton > button:hover { background: var(--navy); border-color: var(--navy); color: white; transform: translateY(-1px); }
.run-btn > button { background: var(--navy) !important; border-color: var(--navy) !important; color: #ffffff !important; }
.run-btn > button:hover { background: #243455 !important; border-color: #243455 !important; }
.stop-btn > button { background: var(--red-soft) !important; border-color: #ffc7c5 !important; color: var(--red) !important; }
.stop-btn > button:hover { background: #ffe4e3 !important; border-color: #ff9a96 !important; }

/* Status pills */
.db-ok, .status-running {
    display: inline-flex; align-items: center; gap: 6px; background: var(--green-soft); color: #0f172a;
    padding: 5px 9px; border-radius: 999px; font-size: 12px; font-weight: 700;
}
.db-err {
    display: inline-flex; align-items: center; gap: 6px; background: var(--red-soft); color: var(--red);
    padding: 5px 9px; border-radius: 999px; font-size: 12px; font-weight: 700;
}
.status-idle {
    display: inline-flex; align-items: center; gap: 6px; background: #eef2f7; color: #526173;
    padding: 5px 9px; border-radius: 999px; font-size: 12px; font-weight: 700;
}

/* Tables */
[data-testid="stDataFrame"] {
    border: 1px solid var(--border); border-radius: 14px; overflow: hidden;
    box-shadow: 0 8px 24px rgba(15, 23, 42, 0.035);
}
.stDataFrame, .stDataEditor { background: var(--panel); }

/* Tabs */
.stTabs [data-baseweb="tab-list"] { background: transparent; border-bottom: 1px solid var(--border); gap: 6px; }
.stTabs [data-baseweb="tab"] { font-size: 13px; color: var(--muted); padding: 10px 14px; font-weight: 700; }
.stTabs [aria-selected="true"] { color: var(--navy) !important; border-bottom: 2px solid var(--navy) !important; }
hr { border-color: var(--border); margin: 1.2rem 0; }

/* Plotly charts */
.js-plotly-plot {
    border-radius: 14px; border: 1px solid var(--border); background: var(--panel);
    box-shadow: 0 8px 24px rgba(15, 23, 42, 0.035);
}

/* Lead cards */
.lead-card {
    background: var(--panel); border: 1px solid var(--border); border-radius: 14px;
    padding: 16px 20px; margin-bottom: 10px; box-shadow: 0 8px 24px rgba(15, 23, 42, 0.035);
}
.lead-card-accent { border-left: 4px solid var(--green); }
.lead-card-warn { border-left: 4px solid var(--yellow); }
/* row-based leads */
.lead-row {background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:14px 18px;margin-bottom:8px;display:grid;grid-template-columns:110px 1.6fr 1fr 1.2fr 2.4fr 60px;gap:16px;align-items:center;box-shadow:0 4px 12px rgba(15,23,42,0.03);transition:all .15s ease;}
.lead-row:hover{border-color:var(--border-2);box-shadow:0 8px 24px rgba(15,23,42,0.07);transform:translateY(-1px);}
.lead-row-header{display:grid;grid-template-columns:110px 1.6fr 1fr 1.2fr 2.4fr 60px;gap:16px;padding:0 18px 10px;color:var(--muted);font-size:10px;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;}
.lead-avatar{width:34px;height:34px;border-radius:10px;display:inline-flex;align-items:center;justify-content:center;font-weight:700;color:white;font-size:13px;flex-shrink:0;}
.lead-company{display:flex;gap:12px;align-items:center;}
.lead-company-name{font-weight:700;color:var(--text);font-size:14px;line-height:1.2;}
.lead-company-sub{font-size:11px;color:var(--muted);margin-top:2px;}
.score-stars{color:#f59e0b;letter-spacing:1px;font-size:13px;}
.score-percent{color:var(--muted);font-size:11px;font-weight:600;margin-top:2px;}
.country-cell{display:flex;align-items:center;gap:8px;font-size:13px;color:var(--text);}
.country-flag{font-size:16px;line-height:1;}
.project-pill{background:var(--blue-soft);color:var(--navy);padding:5px 11px;border-radius:999px;font-size:11px;font-weight:600;display:inline-block;max-width:100%;text-overflow:ellipsis;overflow:hidden;white-space:nowrap;}
.article-title{font-size:13px;color:var(--text);font-weight:500;overflow:hidden;text-overflow:ellipsis;display:-webkit-box;-webkit-line-clamp:1;-webkit-box-orient:vertical;}
.article-meta{font-size:11px;color:var(--muted);margin-top:3px;}
.action-dots{font-size:18px;color:var(--muted-2);text-align:right;}
/* KPI cards */
.kpi-dark{background:var(--navy);color:white;border-radius:14px;padding:22px 24px;min-height:140px;box-shadow:0 8px 24px rgba(15,23,42,0.12);position:relative;overflow:hidden;}
.kpi-dark::before{content:"";position:absolute;top:12px;right:12px;width:44px;height:44px;border-radius:50%;background:rgba(255,255,255,0.06);}
.kpi-dark .kpi-label{color:rgba(255,255,255,0.55);font-size:10px;letter-spacing:0.12em;text-transform:uppercase;font-weight:700;}
.kpi-dark .kpi-value{font-size:38px;font-weight:800;letter-spacing:-0.04em;margin-top:8px;line-height:1;}
.kpi-dark .kpi-delta{color:#22c55e;font-size:12px;font-weight:700;margin-top:8px;}
.kpi-light{background:var(--panel);border:1px solid var(--border);border-radius:14px;padding:22px 24px;min-height:140px;box-shadow:0 8px 24px rgba(15,23,42,0.04);}
.kpi-light .kpi-label{color:var(--muted);font-size:10px;letter-spacing:0.12em;text-transform:uppercase;font-weight:700;}
.kpi-light .kpi-value{color:var(--text);font-size:38px;font-weight:800;letter-spacing:-0.04em;margin-top:8px;line-height:1;}
.kpi-light .kpi-caption{color:var(--muted);font-size:11px;margin-top:8px;line-height:1.4;}
.kpi-avatars{display:flex;align-items:center;margin-top:10px;}
.kpi-flag-bubble{width:28px;height:28px;border-radius:50%;background:var(--bg);display:inline-flex;align-items:center;justify-content:center;font-size:14px;border:2px solid white;margin-left:-6px;}
.kpi-flag-bubble:first-child{margin-left:0;}
.kpi-more-pill{background:var(--navy);color:white;border-radius:999px;padding:4px 10px;font-size:11px;font-weight:700;margin-left:6px;}
.detail-drawer{background:var(--panel-2);border:1px solid var(--border);border-left:4px solid var(--navy);border-radius:0 12px 12px 12px;padding:18px 22px;margin:-6px 0 14px 24px;box-shadow:0 8px 24px rgba(15,23,42,0.04);}
/* Score chip group — st.radio styled as pill toggle */
[data-testid="stRadio"][aria-label="score-chips"] > div, .score-chip-wrap [data-testid="stRadio"] > div { background:var(--panel); border:1px solid var(--border); border-radius:10px; padding:4px; }
.score-chip-wrap [data-testid="stRadio"] [role="radiogroup"] { gap:2px !important; flex-wrap:nowrap; }
.score-chip-wrap [data-testid="stRadio"] label { border-radius:8px !important; padding:6px 14px !important; margin:0 !important; font-size:12px; font-weight:600; color:var(--muted); cursor:pointer; transition:all .15s ease; background:transparent; }
.score-chip-wrap [data-testid="stRadio"] label:hover { background:#eef2f7; color:var(--text); }
.score-chip-wrap [data-testid="stRadio"] label:has(input:checked) { background:var(--navy) !important; color:white !important; }
.score-chip-wrap [data-testid="stRadio"] label > div:first-child { display:none; }
/* Action ⋮ link */
.action-link { color:var(--muted-2); text-decoration:none; font-size:18px; padding:6px 8px; border-radius:6px; transition:all .15s ease; }
.action-link:hover { color:var(--text); background:#eef2f7; }
.log-box {
    background: #0f172a; border: 1px solid #243455; border-radius: 14px; padding: 14px 16px;
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; color: #f2f5f6;
    height: 340px; overflow-y: auto; white-space: pre-wrap; word-break: break-all; line-height: 1.6;
    box-shadow: 0 8px 24px rgba(15, 23, 42, 0.08);
}
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
#  Lead-row rendering helpers (Phase 1)
# ─────────────────────────────────────────────
_AVATAR_PALETTE = ["#0f172a", "#243455", "#16a34a", "#fc4640", "#3b82f6", "#a855f7", "#0891b2", "#c2410c"]

def _avatar_color(name: str) -> str:
    if not name:
        return "#243455"
    return _AVATAR_PALETTE[sum(ord(c) for c in name) % len(_AVATAR_PALETTE)]

_COUNTRY_FLAGS = {
    "austria": "🇦🇹", "österreich": "🇦🇹",
    "germany": "🇩🇪", "deutschland": "🇩🇪",
    "italy": "🇮🇹", "italien": "🇮🇹",
    "switzerland": "🇨🇭", "schweiz": "🇨🇭",
    "netherlands": "🇳🇱", "niederlande": "🇳🇱",
    "belgium": "🇧🇪", "belgien": "🇧🇪",
    "france": "🇫🇷", "frankreich": "🇫🇷",
    "uk": "🇬🇧", "united kingdom": "🇬🇧", "great britain": "🇬🇧",
    "spain": "🇪🇸", "spanien": "🇪🇸",
    "portugal": "🇵🇹",
    "poland": "🇵🇱", "polen": "🇵🇱",
    "czech republic": "🇨🇿", "tschechien": "🇨🇿", "czechia": "🇨🇿",
    "slovakia": "🇸🇰", "slowakei": "🇸🇰",
    "hungary": "🇭🇺", "ungarn": "🇭🇺",
    "romania": "🇷🇴", "rumänien": "🇷🇴",
    "slovenia": "🇸🇮", "slowenien": "🇸🇮",
    "croatia": "🇭🇷", "kroatien": "🇭🇷",
    "serbia": "🇷🇸", "serbien": "🇷🇸",
    "bosnia": "🇧🇦", "bosnien": "🇧🇦",
    "uae": "🇦🇪", "united arab emirates": "🇦🇪",
    "usa": "🇺🇸", "united states": "🇺🇸", "us": "🇺🇸",
    "japan": "🇯🇵",
    "china": "🇨🇳",
    "sweden": "🇸🇪", "schweden": "🇸🇪",
    "norway": "🇳🇴", "norwegen": "🇳🇴",
    "denmark": "🇩🇰", "dänemark": "🇩🇰",
    "finland": "🇫🇮",
    "ireland": "🇮🇪", "irland": "🇮🇪",
    "turkey": "🇹🇷", "türkei": "🇹🇷",
    "greece": "🇬🇷", "griechenland": "🇬🇷",
    "bulgaria": "🇧🇬", "bulgarien": "🇧🇬",
}

def _country_flag(country: str) -> str:
    if not country:
        return "🌍"
    return _COUNTRY_FLAGS.get(country.lower().strip(), "🌍")

def _time_ago(dt) -> str:
    """Format a date or datetime as relative time string ('2h ago', '3d ago', ...)."""
    from datetime import datetime, date as date_cls
    if dt is None or (isinstance(dt, str) and not dt):
        return ""
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00").split("+")[0])
        except Exception:
            return str(dt)
    now = datetime.now()
    if isinstance(dt, datetime):
        delta = now - dt.replace(tzinfo=None)
        seconds = int(delta.total_seconds())
    elif isinstance(dt, date_cls):
        delta = now.date() - dt
        seconds = delta.days * 86400
    else:
        return str(dt)
    if seconds < 3600:
        m = max(1, seconds // 60)
        return f"{m}m ago"
    if seconds < 86400:
        h = seconds // 3600
        return f"{h}h ago"
    days = seconds // 86400
    if days < 7:
        return f"{days}d ago"
    if days < 30:
        return f"{days // 7}w ago"
    if days < 365:
        return f"{days // 30}mo ago"
    return f"{days // 365}y ago"

def _esc(s) -> str:
    return ("" if s is None else str(s)).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def render_lead_row(lead) -> str:
    """Render a single lead as a Lead-Management-style HTML row (compact view)."""
    company = lead["company"] if lead["company"] not in (None, "—", "") else "Unknown"
    score = int(lead["score"])
    score_pct = min(100, score * 10)
    sector = lead.get("sector") or ""
    if sector == "—":
        sector = ""
    country = lead.get("country") or ""
    if country == "—":
        country = ""
    project = lead.get("what") or ""
    if project == "—":
        project = ""
    title = lead.get("title") or ""
    avatar_initial = company[0].upper() if company else "?"
    avatar_color = _avatar_color(company)
    flag = _country_flag(country)
    ago = _time_ago(lead.get("date"))
    stars = "★" * min(score, 5) + "☆" * max(0, 5 - min(score, 5))
    return (
        f'<div class="lead-row">'
        f'  <div>'
        f'    <div class="score-stars">{stars}</div>'
        f'    <div class="score-percent">{score_pct}%</div>'
        f'  </div>'
        f'  <div class="lead-company">'
        f'    <div class="lead-avatar" style="background:{avatar_color};">{_esc(avatar_initial)}</div>'
        f'    <div>'
        f'      <div class="lead-company-name">{_esc(company)}</div>'
        f'      <div class="lead-company-sub">{_esc(sector or "—")}</div>'
        f'    </div>'
        f'  </div>'
        f'  <div class="country-cell">'
        f'    <span class="country-flag">{flag}</span>'
        f'    <span>{_esc(country or "—")}</span>'
        f'  </div>'
        f'  <div>'
        f'    <span class="project-pill">{_esc(project[:36] + "…" if len(project) > 36 else project) or "—"}</span>'
        f'  </div>'
        f'  <div>'
        f'    <div class="article-title">{_esc(title)}</div>'
        f'    <div class="article-meta">Published {ago}</div>'
        f'  </div>'
        f'  <div style="text-align:right;">'
        f'    <a class="action-link" href="{_esc(lead.get("url") or "#")}" target="_blank" '
        f'       title="Open article in new tab">⋮</a>'
        f'  </div>'
        f'</div>'
    )

def render_lead_header() -> str:
    return (
        '<div class="lead-row-header">'
        '  <div>Score</div>'
        '  <div>Company</div>'
        '  <div>Country</div>'
        '  <div>Project</div>'
        '  <div>Article</div>'
        '  <div>Action</div>'
        '</div>'
    )

# ─────────────────────────────────────────────
#  Plotly theme
# ─────────────────────────────────────────────
PLOT_LAYOUT = dict(
    paper_bgcolor="#ffffff",
    plot_bgcolor="#ffffff",
    font=dict(family="Inter", color="#526173", size=11),
    margin=dict(l=10, r=10, t=30, b=10),
    xaxis=dict(gridcolor="#eef2f7", zerolinecolor="#eef2f7"),
    yaxis=dict(gridcolor="#eef2f7", zerolinecolor="#eef2f7"),
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
    st.markdown("""
    <div class="sidebar-brand">
        <div class="logo-box">LC</div>
        <div>
            <div class="brand-title">LeadCollector</div>
            <div class="brand-subtitle">RCG freight intelligence</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
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
    st.markdown('<div class="page-kicker">Dashboard</div>', unsafe_allow_html=True)
    st.markdown('# Dashboard Overview')
    st.markdown('<div class="page-subtitle">Real-time performance analytics for the lead generation pipeline.</div>', unsafe_allow_html=True)
    if not db_ok:
        st.error("⚠️  Database is not reachable.")
        st.stop()

    # ── Hero metrics ──────────────────────────
    stats = db_fetch("""
        select
            (select count(distinct source_id) from items) as sources,
            (select count(*) from items) as articles,
            (select count(*) from item_scores where lead_score >= 5 and prompt_version = %s) as leads,
            (select count(*) from item_scores where lead_score >= 7 and prompt_version = %s) as strong_leads
    """, (PV, PV))
    if stats:
        s = stats[0]
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Sources Monitored", s["sources"])
        m2.metric("Articles Processed", f"{s['articles']:,}")
        m3.metric("🎯 Leads Found", s["leads"])
        m4.metric("⭐ Strong Leads", s["strong_leads"])
        

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
                         color="leads", color_continuous_scale=["#eef2f7", "#0f172a"],
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
            fig2 = px.area(df_time, x="day", y="articles", color_discrete_sequence=["#243455"])
            fig2.update_layout(**PLOT_LAYOUT, height=380)
            fig2.update_traces(line=dict(color="#243455", width=2), fillcolor="rgba(74,111,165,0.18)")
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No recent articles.")

    st.markdown("---")

    # ── Sector + Geography ────────────────────
    sg1, sg2 = st.columns(2)

    with sg1:
        st.markdown('<p class="section-header">Leads by Sector</p>', unsafe_allow_html=True)
        df_sec = db_fetch_df("""
            select coalesce(nullif(sector, ''), 'unspecified') as sector, count(*) as leads
            from items
            where lead_score >= 5
              and (cluster_id is null or is_cluster_canonical = true)
            group by 1
            order by leads desc
        """)
        if not df_sec.empty:
            fig_sec = px.bar(df_sec, x="leads", y="sector", orientation="h",
                             color_discrete_sequence=["#0f172a"])
            fig_sec.update_layout(**PLOT_LAYOUT, showlegend=False, height=380)
            fig_sec.update_traces(marker_line_width=0)
            fig_sec.update_yaxes(autorange="reversed")
            st.plotly_chart(fig_sec, use_container_width=True)
        else:
            st.info("No classified leads with sector data yet.")

    with sg2:
        st.markdown('<p class="section-header">Leads by Country</p>', unsafe_allow_html=True)
        df_geo = db_fetch_df("""
            select lead_country as country, count(*) as leads
            from items
            where lead_score >= 5
              and (cluster_id is null or is_cluster_canonical = true)
              and lead_country is not null and lead_country != ''
            group by 1
            order by leads desc
        """)
        if not df_geo.empty:
            fig_geo = px.choropleth(
                df_geo, locations="country", locationmode="country names",
                color="leads", color_continuous_scale=["#eef2f7", "#0f172a"],
                scope="europe",
            )
            fig_geo.update_geos(bgcolor="#ffffff", lakecolor="#ffffff",
                                landcolor="#f2f5f6", showframe=False,
                                showcountries=True, countrycolor="#dfe5e8")
            fig_geo.update_layout(
                paper_bgcolor="#ffffff",
                font=dict(family="IBM Plex Mono", color="#526173", size=11),
                height=380, coloraxis_showscale=False,
                margin=dict(l=0, r=0, t=10, b=10),
            )
            st.plotly_chart(fig_geo, use_container_width=True)
        else:
            st.info("No classified leads with country data yet.")

    st.markdown("---")

    # ── Lead activity timeline — last 30 days, stacked by score band
    st.markdown('<p class="section-header">📈 Lead Activity (last 30 days)</p>', unsafe_allow_html=True)
    st.caption("Daily classified leads, broken down by score band. Score ≥ 9 = elite, 7–8 = strong, 5–6 = candidate.")

    df_activity = db_fetch_df("""
        select date_trunc('day', s.classified_at)::date as day,
               case
                   when s.lead_score >= 9 then 'elite (≥9)'
                   when s.lead_score >= 7 then 'strong (7-8)'
                   when s.lead_score >= 5 then 'candidate (5-6)'
                   else 'weak (1-4)'
               end as band,
               count(*) as n
        from item_scores s
        where s.prompt_version = %s
          and s.classified_at >= now() - interval '30 days'
          and s.lead_score is not null
        group by 1, 2
        order by 1, 2
    """, (PV,))
    if not df_activity.empty:
        # Pivot to wide for stacked area
        pivot = df_activity.pivot(index="day", columns="band", values="n").fillna(0)
        # Maintain a stable order for the stack
        for col in ["weak (1-4)", "candidate (5-6)", "strong (7-8)", "elite (≥9)"]:
            if col not in pivot.columns:
                pivot[col] = 0
        pivot = pivot[["weak (1-4)", "candidate (5-6)", "strong (7-8)", "elite (≥9)"]]
        pivot_long = pivot.reset_index().melt(id_vars="day", var_name="band", value_name="n")

        color_map = {
            "weak (1-4)":      "#cfd8dd",
            "candidate (5-6)": "#8b98a8",
            "strong (7-8)":    "#243455",
            "elite (≥9)":      "#fc4640",
        }
        fig_act = px.area(
            pivot_long, x="day", y="n", color="band",
            category_orders={"band": ["weak (1-4)", "candidate (5-6)", "strong (7-8)", "elite (≥9)"]},
            color_discrete_map=color_map,
        )
        fig_act.update_layout(**PLOT_LAYOUT, height=320,
                              legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        fig_act.update_traces(line=dict(width=0))
        st.plotly_chart(fig_act, use_container_width=True)

        # Quick summary stats below the chart
        last7  = int(pivot.tail(7).sum().sum())
        prev7  = int(pivot.tail(14).head(7).sum().sum())
        elite7 = int(pivot["elite (≥9)"].tail(7).sum())
        strong7 = int(pivot["strong (7-8)"].tail(7).sum())

        st.markdown(
            f'<div style="display:flex; gap:16px; margin-top:8px; font-size:12px; color:var(--muted);">'
            f'<span><b style="color:var(--text);">{last7}</b> classified in last 7d</span>'
            f'<span>vs <b style="color:var(--text);">{prev7}</b> previous 7d</span>'
            f'<span>· <b style="color:#fc4640;">{elite7}</b> elite (≥9)</span>'
            f'<span>· <b style="color:#243455;">{strong7}</b> strong (7-8)</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        st.info("No classified items in the last 30 days. Run `python -m lc.classify` to populate.")


# ═══════════════════════════════════════════════
#  PAGE: LEADS
# ═══════════════════════════════════════════════
elif page == "🎯 Leads":
    st.markdown('<div class="page-kicker">Global leads · Active pipeline</div>', unsafe_allow_html=True)
    st.markdown('# Lead Management')
    st.markdown('<div class="page-subtitle">Review and qualify prospects from high-intent campaigns.</div>', unsafe_allow_html=True)
    if not db_ok:
        st.error("⚠️  Database not reachable.")
        st.stop()

    # ── Metrics row ───────────────────────────
    s1, s2, s3, s4 = st.columns(4)
    total_leads  = db_fetch("select count(*) as n from item_scores where lead_score >= 5 and prompt_version = %s", (PV,))
    strong_leads = db_fetch("select count(*) as n from item_scores where lead_score >= 7 and prompt_version = %s", (PV,))
    total_items  = db_fetch("select count(*) as n from items")
    pending      = db_fetch("""select count(*) as n from items where clean_text is not null
                               and length(clean_text) > 100 and lead_score is null""")

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
                marker=dict(color=["#243455", "#526173", "#8b98a8", "#243455", "#0f172a", "#243455"]),
                connector=dict(line=dict(color="#dfe5e8")),
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
            colors = ["#0f172a" if s >= 7 else "#243455" if s >= 5 else "#526173"
                      for s in df_scores["score"]]
            fig_sc = px.bar(df_scores, x="score", y="count", color_discrete_sequence=["#243455"])
            fig_sc.update_traces(marker_color=colors, marker_line_width=0)
            fig_sc.update_layout(**PLOT_LAYOUT, showlegend=False, height=340)
            st.plotly_chart(fig_sc, use_container_width=True)

    st.markdown("---")

    # ── Tabs ──────────────────────────────────
    lead_tab1, lead_tab2 = st.tabs(["🔍  Browse Leads", "✅  Review & Label"])

    with lead_tab1:
        # ── Filter row + pagination setup ───────────────────────────────
        lf1, lf2, lf3 = st.columns([2.4, 4.0, 1.2])
        with lf1:
            st.markdown('<div class="score-chip-wrap">', unsafe_allow_html=True)
            score_choice = st.radio(
                "Score filter",
                ["All", "≥4", "≥5", "≥7", "≥9"],
                index=3,
                horizontal=True,
                label_visibility="collapsed",
                key="score_chip_choice",
            )
            st.markdown('</div>', unsafe_allow_html=True)
            min_score = {"All": 1, "≥4": 4, "≥5": 5, "≥7": 7, "≥9": 9}[score_choice]
        with lf2:
            lead_search = st.text_input(
                "Search company / title",
                placeholder="Filter by company, sector, or keyword…",
                label_visibility="collapsed",
            )
        with lf3:
            per_page = st.selectbox(
                "Rows per page",
                [5, 10, 20, 50],
                index=1,
                label_visibility="collapsed",
            )

        # Reset to page 1 whenever the filter changes.
        filter_signature = f"{PV}|{min_score}|{lead_search}|{per_page}"
        if st.session_state.get("lead_filter_signature") != filter_signature:
            st.session_state.lead_page = 1
            st.session_state.lead_filter_signature = filter_signature
        if "lead_page" not in st.session_state:
            st.session_state.lead_page = 1

        # Shared FROM/WHERE block for count query and paginated result query.
        base_from_where = """
            from item_scores s
            join items i using(item_id)
            left join (
                select distinct on (item_id) item_id, label
                from lead_labels order by item_id, labeled_at desc
            ) ll on ll.item_id = i.item_id
            left join (
                select cluster_id,
                       count(distinct source_id) as source_count,
                       count(*)                  as item_count
                from items
                where cluster_id is not null
                group by cluster_id
            ) cs on cs.cluster_id = i.cluster_id
            where s.prompt_version = %s
              and s.lead_score >= %s
              and (i.cluster_id is null or i.is_cluster_canonical = true)
        """
        params_leads: list = [PV, min_score]
        if lead_search:
            base_from_where += " and (s.lead_company ilike %s or i.title ilike %s or i.sector ilike %s)"
            params_leads += [f"%{lead_search}%", f"%{lead_search}%", f"%{lead_search}%"]

        total_rows_result = db_fetch("select count(*) as n " + base_from_where, tuple(params_leads))
        total_rows = int(total_rows_result[0]["n"]) if total_rows_result else 0
        total_pages = max(1, (total_rows + per_page - 1) // per_page)
        st.session_state.lead_page = max(1, min(int(st.session_state.lead_page), total_pages))

        # Pagination controls, similar to the screenshot.
        # ── Pagination — info caption + numbered page buttons ──────────────
        cur_page = int(st.session_state.lead_page)

        # Compute which page numbers to display: always first, last,
        # current ± 1, with ellipses for the gaps. Up to 7 buttons total.
        def _page_window(cur: int, tot: int, w: int = 1):
            if tot <= 7:
                return list(range(1, tot + 1))
            keep = {1, tot, cur}
            for i in range(max(1, cur - w), min(tot, cur + w) + 1):
                keep.add(i)
            sorted_keep = sorted(keep)
            out: list[int | None] = []
            last = 0
            for p in sorted_keep:
                if last and p > last + 1:
                    out.append(None)  # ellipsis marker
                out.append(p)
                last = p
            return out

        page_window = _page_window(cur_page, total_pages)

        # Info caption above the buttons
        if total_rows:
            start_row = (cur_page - 1) * per_page + 1
            end_row = min(cur_page * per_page, total_rows)
            st.caption(
                f"Showing {start_row}–{end_row} of {total_rows} leads · "
                f"Page {cur_page} of {total_pages}"
            )
        else:
            st.caption("Showing 0 leads")

        # Numbered page-button row: ‹  1  …  4  5 [6] 7  …  50  ›
        button_cells = 1 + len(page_window) + 1  # prev + pages + next
        # Button columns share a fixed weight; trailing column absorbs the
        # remaining space so buttons stay compact instead of stretching.
        col_weights = [1] * button_cells + [max(2, 12 - button_cells)]
        cols = st.columns(col_weights)

        with cols[0]:
            if st.button("‹", disabled=cur_page <= 1, use_container_width=True, key="lead_prev_page"):
                st.session_state.lead_page = cur_page - 1
                st.rerun()

        for slot_idx, p in enumerate(page_window, start=1):
            with cols[slot_idx]:
                if p is None:
                    st.markdown(
                        "<div style='text-align:center; color:var(--muted-2); padding:6px 0;'>…</div>",
                        unsafe_allow_html=True,
                    )
                else:
                    is_current = (p == cur_page)
                    label = f"**{p}**" if is_current else str(p)
                    if st.button(label, disabled=is_current,
                                 use_container_width=True, key=f"lead_page_btn_{p}"):
                        st.session_state.lead_page = p
                        st.rerun()

        with cols[1 + len(page_window)]:
            if st.button("›", disabled=cur_page >= total_pages,
                         use_container_width=True, key="lead_next_page"):
                st.session_state.lead_page = cur_page + 1
                st.rerun()

        offset = (st.session_state.lead_page - 1) * per_page

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
                coalesce(ll.label, 'new') as status,
                coalesce(i.sector, '—')        as sector,
                coalesce(i.commodity_nst, '—') as commodity,
                coalesce(i.event_status, '—')  as event_status,
                coalesce(i.company_role, '—')  as company_role,
                i.investment_eur,
                coalesce(cs.source_count, 1)   as sources,
                i.cluster_id
        """ + base_from_where + " order by s.lead_score desc, i.created_at desc limit %s offset %s"

        df_l = db_fetch_df(sql_leads, tuple(params_leads + [per_page, offset]))
        if not df_l.empty:
            # ── Table-style header ─────────────────────────────────
            st.markdown(render_lead_header(), unsafe_allow_html=True)

            # ── Rows: HTML for the closed view + Streamlit expander for details
            for _, lead in df_l.iterrows():
                st.markdown(render_lead_row(lead), unsafe_allow_html=True)

                with st.expander("▾  View details", expanded=False):
                    score = int(lead["score"])
                    color = "#0f172a" if score >= 7 else "#243455" if score >= 5 else "#526173"

                    if lead["description"] != "—":
                        st.markdown(f"""<div style="background:#ffffff; border-left:3px solid var(--navy); padding:12px 16px;
                            border-radius:8px; color:var(--text); font-size:14px; margin-bottom:12px;">
                            {lead['description']}</div>""", unsafe_allow_html=True)

                    def _chip(label, value, fg="#0f172a", bg="#eef2f7"):
                        return (
                            f'<span style="background:{bg}; color:{fg}; padding:3px 9px; '
                            f'border-radius:10px; font-size:11px; font-weight:600;">'
                            f'<span style="color:#526173;">{label}:</span> {value}</span>'
                        )

                    chips = []
                    if pd.notna(lead.get("sources")) and lead["sources"] > 1:
                        chips.append(_chip("sources", int(lead["sources"]), fg="#fc4640", bg="#fff0ef"))
                    for lbl, col in [("commodity", "commodity"), ("event", "event_status"), ("role", "company_role")]:
                        if lead[col] != "—":
                            chips.append(_chip(lbl, lead[col]))
                    if pd.notna(lead.get("investment_eur")):
                        v = float(lead["investment_eur"])
                        if v >= 1_000_000_000:
                            amt = f"€{v / 1_000_000_000:.1f}B"
                        elif v >= 1_000_000:
                            amt = f"€{v / 1_000_000:.0f}M"
                        elif v >= 1_000:
                            amt = f"€{v / 1_000:.0f}K"
                        else:
                            amt = f"€{int(v):,}"
                        chips.append(_chip("investment", amt))
                    if chips:
                        st.markdown(f'<div style="margin-bottom:12px; display:flex; gap:6px; flex-wrap:wrap;">{"".join(chips)}</div>',
                                    unsafe_allow_html=True)

                    wc1, wc2, wc3 = st.columns(3)
                    for col_obj, label, val in [
                        (wc1, "Who", lead["who"]),
                        (wc2, "What", lead["what"]),
                        (wc3, "When", lead["when"]),
                    ]:
                        with col_obj:
                            st.markdown(f"""<div style="color:#526173; font-size:11px; text-transform:uppercase;
                                letter-spacing:0.08em;">{label}</div>
                                <div style="color:#344054; font-size:14px; margin-top:2px;">
                                {val if val != '—' else 'Not specified'}</div>""", unsafe_allow_html=True)

                    rc1, rc2 = st.columns(2)
                    with rc1:
                        st.markdown(f"""<div style="color:#526173; font-size:11px; text-transform:uppercase;
                            letter-spacing:0.08em; margin-top:12px;">Location</div>
                            <div style="color:#344054; font-size:14px; margin-top:2px;">
                            📍 {lead['city']}, {lead['country']}</div>""", unsafe_allow_html=True)
                    with rc2:
                        st.markdown(f"""<div style="color:#526173; font-size:11px; text-transform:uppercase;
                            letter-spacing:0.08em; margin-top:12px;">AI Assessment</div>
                            <div style="color:#9ba3b5; font-size:13px; margin-top:2px;">
                            {lead['reason'] if lead['reason'] != '—' else 'N/A'}</div>""", unsafe_allow_html=True)

                    st.markdown(f"""<div style="background:#ffffff; border:1px solid #dfe5e8; border-radius:8px;
                        padding:10px 14px; margin-top:12px;">
                        <div style="color:#526173; font-size:11px; text-transform:uppercase;">Source Article</div>
                        <div style="color:#344054; font-size:13px; margin-top:4px;">{lead['title']}</div>
                        <a href="{lead['url']}" target="_blank" style="color:#243455; font-size:12px;">Open article →</a>
                    </div>""", unsafe_allow_html=True)

                    if lead["sources"] and lead["sources"] > 1 and lead["cluster_id"]:
                        members = db_fetch(
                            """
                            select source_id, title, url, lead_score as score,
                                   coalesce(published_at, created_at)::date::text as date,
                                   item_id
                            from items
                            where cluster_id = %s
                            order by coalesce(published_at, created_at) asc nulls last
                            """,
                            (lead["cluster_id"],),
                        )
                        if members and len(members) > 1:
                            st.markdown(
                                '<div style="color:#526173; font-size:11px; text-transform:uppercase; '
                                'letter-spacing:0.08em; margin-top:14px; margin-bottom:6px;">'
                                f'📡 Coverage across {len(members)} sources</div>',
                                unsafe_allow_html=True,
                            )
                            for m in members:
                                is_canon = (m["item_id"] == lead["item_id"])
                                badge = (
                                    '<span style="color:#0f172a; font-size:10px; font-family:monospace; '
                                    'background:#eef2f7; padding:1px 5px; border-radius:6px; margin-left:6px;">canonical</span>'
                                    if is_canon else ''
                                )
                                title_clean = (m.get("title") or "")[:140].replace("<", "&lt;").replace(">", "&gt;")
                                st.markdown(
                                    f'<div style="background:#f7f9fa; border-left:2px solid #243455; '
                                    f'padding:8px 12px; border-radius:4px; margin:4px 0; font-size:13px;">'
                                    f'<span style="color:#243455; font-family:monospace;">{m["source_id"]}</span>{badge} '
                                    f'<span style="color:#526173; font-size:11px;">· {m["date"]} · score {m["score"]}</span><br>'
                                    f'<a href="{m["url"]}" target="_blank" '
                                    f'style="color:#344054; text-decoration:none;">{title_clean}</a>'
                                    f'</div>',
                                    unsafe_allow_html=True,
                                )
            # ── All-leads table + CSV download (entire filtered set, not just this page)
            st.markdown("<div style='height:18px'></div>", unsafe_allow_html=True)

            # Re-query without LIMIT/OFFSET to get the full filtered set for export.
            sql_full = """
                select
                    s.lead_score as score,
                    coalesce(s.lead_company, '') as company,
                    coalesce(i.sector, '') as sector,
                    coalesce(s.lead_city, '') as city,
                    coalesce(s.lead_country, '') as country,
                    coalesce(s.lead_what, '') as project,
                    coalesce(i.event_status, '') as event_status,
                    coalesce(i.company_role, '') as company_role,
                    coalesce(i.commodity_nst, '') as commodity,
                    i.investment_eur,
                    coalesce(cs.source_count, 1) as sources,
                    coalesce(s.lead_when, '') as "when",
                    coalesce(s.lead_reason, '') as reason,
                    coalesce(s.lead_description, '') as description,
                    coalesce(i.title, '') as title,
                    i.source_id as source,
                    i.url,
                    i.created_at::date as date,
                    coalesce(ll.label, 'new') as status,
                    i.cluster_id
            """ + base_from_where + " order by s.lead_score desc, i.created_at desc"
            df_full = db_fetch_df(sql_full, tuple(params_leads))

            head_l, head_r = st.columns([4, 1])
            with head_l:
                st.markdown(f'<p class="section-header">📊 All filtered leads ({len(df_full)} rows)</p>',
                            unsafe_allow_html=True)
            with head_r:
                if not df_full.empty:
                    csv_bytes = df_full.to_csv(index=False).encode("utf-8-sig")
                    st.download_button(
                        "📥 Download CSV",
                        data=csv_bytes,
                        file_name=f"leads_p{PV}_score{min_score}_{int(time.time())}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )

            if not df_full.empty:
                st.dataframe(
                    df_full[[
                        "score", "company", "sector", "country", "project",
                        "event_status", "company_role", "sources",
                        "title", "source", "url", "date",
                    ]],
                    use_container_width=True,
                    height=min(500, 80 + len(df_full) * 35),
                    hide_index=True,
                    column_config={
                        "score":        st.column_config.ProgressColumn("Score", min_value=0, max_value=10, format="%d/10"),
                        "company":      "Company",
                        "sector":       "Sector",
                        "country":      "Country",
                        "project":      "Project",
                        "event_status": "Event status",
                        "company_role": "Role",
                        "sources":      st.column_config.NumberColumn("Sources",
                                            help="How many distinct outlets cover this story"),
                        "title":        "Article",
                        "source":       "Source",
                        "url":          st.column_config.LinkColumn("Link"),
                        "date":         "Date",
                    },
                )
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
                coalesce(i.sector, '—') as sector,
                coalesce(i.title, '—') as title,
                i.url,
                coalesce(ll.label, 'new') as status,
                coalesce(cs.source_count, 1) as sources
            from item_scores s
            join items i using(item_id)
            left join (select distinct on (item_id) item_id, label from lead_labels order by item_id, labeled_at desc) ll on ll.item_id = i.item_id
            left join (
                select cluster_id, count(distinct source_id) as source_count
                from items where cluster_id is not null group by cluster_id
            ) cs on cs.cluster_id = i.cluster_id
            where s.prompt_version = %s
              and s.lead_score >= %s
              and (i.cluster_id is null or i.is_cluster_canonical = true)
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

    # ───────────────────────────────────────────────
    #  Bottom KPI strip (Phase 4)
    # ───────────────────────────────────────────────
    st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)
    kpi_data = db_fetch(
        """
        select
            (select count(*) from item_scores
                where lead_score >= 9 and prompt_version = %s
                  and classified_at >= now() - interval '7 days') as elite_7d,
            (select count(*) from item_scores
                where lead_score >= 9 and prompt_version = %s
                  and classified_at < now() - interval '7 days'
                  and classified_at >= now() - interval '14 days') as elite_prev7,
            (select round(avg(lead_score)::numeric, 1) from item_scores
                where lead_score is not null and prompt_version = %s) as avg_score,
            (select count(distinct i.lead_country) from items i
                join item_scores s using(item_id)
                where s.prompt_version = %s and s.lead_score >= 5
                  and i.lead_country is not null and i.lead_country <> '') as countries
        """,
        (PV, PV, PV, PV),
    )
    if kpi_data:
        k = kpi_data[0]
        elite_7d = int(k["elite_7d"] or 0)
        elite_prev7 = int(k["elite_prev7"] or 0)
        delta = elite_7d - elite_prev7
        if elite_prev7 > 0:
            delta_pct = round(100 * delta / elite_prev7, 0)
            delta_str = f"{'+' if delta >= 0 else ''}{int(delta_pct)}% vs prev 7d"
        else:
            delta_str = f"{'+' if delta >= 0 else ''}{delta} vs prev 7d"

        # Active countries with their flags
        df_countries = db_fetch_df(
            """
            select i.lead_country as country, count(*) as n
            from items i join item_scores s using(item_id)
            where s.prompt_version = %s and s.lead_score >= 5
              and i.lead_country is not null and i.lead_country <> ''
            group by 1 order by n desc limit 10
            """,
            (PV,),
        )

        kc1, kc2, kc3 = st.columns(3)
        with kc1:
            arrow = "▲" if delta >= 0 else "▼"
            delta_color = "#22c55e" if delta >= 0 else "#fc4640"
            st.markdown(
                f'<div class="kpi-dark">'
                f'<div class="kpi-label">Elite leads · last 7d</div>'
                f'<div class="kpi-value">{elite_7d}</div>'
                f'<div class="kpi-delta" style="color:{delta_color};">{arrow} {delta_str}</div>'
                f'<div style="color:rgba(255,255,255,0.45); font-size:11px; margin-top:4px;">'
                f'Score ≥ 9 — top-tier opportunities (was {elite_prev7} prev 7d)</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
        with kc2:
            avg = k["avg_score"] or 0
            st.markdown(
                f'<div class="kpi-light">'
                f'<div class="kpi-label">Average score</div>'
                f'<div class="kpi-value">{avg}<span style="color:#f59e0b;">★</span></div>'
                f'<div class="kpi-caption">Mean lead quality across all classified articles for prompt {PV}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
        with kc3:
            n_countries = int(k["countries"] or 0)
            flags_html = ""
            if not df_countries.empty:
                top_flags = [_country_flag(c) for c in df_countries["country"].head(4)]
                flags_html = "".join(f'<div class="kpi-flag-bubble">{f}</div>' for f in top_flags)
                if n_countries > 4:
                    flags_html += f'<span class="kpi-more-pill">+{n_countries - 4}</span>'
            st.markdown(
                f'<div class="kpi-light">'
                f'<div class="kpi-label">Active countries</div>'
                f'<div class="kpi-value">{n_countries}</div>'
                f'<div class="kpi-avatars">{flags_html}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    # ───────────────────────────────────────────────
    #  Page-header action buttons (Phase 2)
    # ───────────────────────────────────────────────
    # Note: rendered late so the page header reads naturally; the buttons
    # are functional Streamlit buttons that act on the current filter.
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    ac1, ac2, _, _ = st.columns([1.2, 1.2, 4, 1])
    with ac1:
        st.download_button(
            "📥 Export CSV",
            data=df_l.to_csv(index=False).encode() if 'df_l' in locals() and not df_l.empty else "".encode(),
            file_name=f"leads_p{PV}_score{min_score if 'min_score' in locals() else 5}.csv",
            mime="text/csv",
            use_container_width=True,
            disabled=('df_l' not in locals() or df_l.empty),
        )
    with ac2:
        st.button("⚡ Bulk Actions", use_container_width=True, disabled=True,
                  help="Multi-select bulk actions — coming soon")


# ═══════════════════════════════════════════════
#  PAGE: ANALYTICS
# ═══════════════════════════════════════════════
elif page == "📈 Analytics":
    st.markdown('<div class="page-kicker">Insights</div>', unsafe_allow_html=True)
    st.markdown('# Analytics')
    st.markdown('<div class="page-subtitle">Understand source quality, scoring behavior and pipeline performance.</div>', unsafe_allow_html=True)
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
                    "Strong lead (7-10)": "#0f172a", "Possible lead (5-6)": "#243455",
                    "Weak signal (3-4)": "#243455", "Not a lead (0-2)": "#526173",
                    "Filtered (not relevant)": "#e1e6ea", "Filtered (bad content)": "#d4dde3",
                    "Pending": "#f2f5f6",
                }
                fig_cl = px.pie(df_class, names="category", values="n",
                                color="category", color_discrete_map=CAT_COLORS, hole=0.5)
                fig_cl.update_layout(**PLOT_LAYOUT, height=320,
                                     legend=dict(font=dict(color="#526173", size=10)))
                fig_cl.update_traces(textfont_color="#344054")
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
                                 color="leads", color_continuous_scale=["#eef2f7", "#0f172a"])
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
            colors = ["#0f172a" if s >= 7 else "#243455" if s >= 5 else "#526173" for s in df_scores["score"]]
            fig_sd = px.bar(df_scores, x="score", y="articles", text="articles")
            fig_sd.update_traces(marker_color=colors, marker_line_width=0,
                                 textposition="outside", textfont_color="#526173")
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
                               color="yield", color_continuous_scale=["#eef2f7", "#0f172a"], text="yield")
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
                                color_discrete_map={"discover": "#243455", "fetch": "#8b98a8",
                                                    "extract": "#526173", "classify": "#fc4640"})
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
    st.markdown('<div class="page-kicker">Operations</div>', unsafe_allow_html=True)
    st.markdown('# System Control')
    st.markdown('<div class="page-subtitle">Run discovery, extraction and AI classification from one place.</div>', unsafe_allow_html=True)
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
