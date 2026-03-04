"""
⚡ FAMEL Electric Motorcycle Telemetry Dashboard v3
Cloud — Supabase + Streamlit Cloud
"""
import streamlit as st
import io, requests
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

st.set_page_config(
    page_title="FAMEL Telemetry",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Force dark theme (fixes light/dark flicker bug) ───────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Exo+2:wght@300;400;600;700&display=swap');

/* Override Streamlit theme variables */
:root, [data-theme="light"], [data-theme="dark"] {
    --background-color:           #0d1117 !important;
    --secondary-background-color: #161b22 !important;
    --text-color:                 #c9d1d9 !important;
    --font:                       'Exo 2', sans-serif !important;
}

html, body { background-color: #0d1117 !important; color: #c9d1d9 !important; }
.stApp, .main, .block-container { background-color: #0d1117 !important; color: #c9d1d9 !important; }

/* Sidebar */
section[data-testid="stSidebar"],
section[data-testid="stSidebar"] > div { background: #010409 !important; border-right: 1px solid #21262d; }

/* Tabs */
.stTabs [data-baseweb="tab-list"] { background: #0d1117; border-bottom: 1px solid #21262d; gap: 4px; }
.stTabs [data-baseweb="tab"] { background: #161b22; border: 1px solid #21262d; border-radius: 6px 6px 0 0; color: #8b949e; font-family: 'Exo 2', sans-serif; font-size: .82rem; padding: 6px 16px; }
.stTabs [aria-selected="true"] { background: #1f2937 !important; color: #f7931a !important; border-color: #f7931a #21262d #1f2937 #21262d; }
.stTabs [data-baseweb="tab-panel"] { background: #0d1117; padding-top: 12px; }

/* Cards / metrics */
[data-testid="metric-container"] { background: #161b22 !important; border: 1px solid #21262d !important; border-radius: 8px; padding: 12px 14px; }
[data-testid="stMetricValue"]     { font-family: 'Share Tech Mono', monospace !important; color: #c9d1d9 !important; }
[data-testid="stMetricLabel"]     { color: #8b949e !important; }

/* Inputs, selects */
.stSelectbox div[data-baseweb], .stMultiSelect div[data-baseweb],
.stTextInput input, .stNumberInput input, .stTextArea textarea {
    background: #161b22 !important; border-color: #30363d !important; color: #c9d1d9 !important;
}
.stSelectbox label, .stMultiSelect label, .stTextInput label,
.stNumberInput label, .stTextArea label, .stDateInput label,
.stSlider label, .stCheckbox label, .stRadio label,
p, li, span { color: #c9d1d9 !important; }

/* Expanders */
.streamlit-expanderHeader { background: #161b22 !important; color: #c9d1d9 !important; border: 1px solid #21262d !important; border-radius: 6px; }
.streamlit-expanderContent { background: #161b22 !important; border: 1px solid #21262d !important; }

/* Buttons */
.stButton > button { background: #21262d !important; color: #c9d1d9 !important; border: 1px solid #30363d !important; border-radius: 6px; }
.stButton > button:hover { border-color: #f7931a !important; color: #f7931a !important; }
button[kind="primary"] { background: #f7931a !important; color: #0d1117 !important; border: none !important; }

/* Tables / dataframes */
.stDataFrame, [data-testid="stDataFrameResizable"] { background: #161b22 !important; }
.stDataFrame table { background: #161b22 !important; color: #c9d1d9 !important; }

/* Divider */
hr { border-color: #21262d !important; margin: 8px 0; }

/* Custom classes */
.section-hdr {
    font-family: 'Share Tech Mono', monospace;
    font-size: .62rem; letter-spacing: 3px; text-transform: uppercase;
    color: #f7931a; border-bottom: 1px solid #21262d;
    padding-bottom: 5px; margin: 16px 0 10px 0;
}
.kpi-card {
    background: #161b22; border: 1px solid #21262d; border-radius: 8px;
    padding: 11px 10px; text-align: center;
}
.kpi-label { font-size: .58rem; color: #8b949e; letter-spacing: 1.5px; text-transform: uppercase; }
.kpi-value { font-family: 'Share Tech Mono', monospace; font-size: 1.15rem; margin-top: 3px; }
.fault-band { border-left: 3px solid; border-radius: 0 5px 5px 0; padding: 6px 12px; margin: 3px 0; font-size: .81rem; }
.logo-area { font-family: 'Share Tech Mono', monospace; font-size: 1.05rem; color: #f7931a; letter-spacing: 2px; }
.logo-sub  { font-size: .58rem; color: #8b949e; letter-spacing: 3px; text-transform: uppercase; }
.stat-badge { display: inline-block; background: #161b22; border: 1px solid #30363d; border-radius: 5px; padding: 3px 9px; font-family: 'Share Tech Mono', monospace; font-size: .71rem; color: #8b949e; margin: 2px; }
.ann-box { background: #1c2128; border-left: 3px solid #f7931a; border-radius: 0 6px 6px 0; padding: 8px 12px; margin: 4px 0; font-size: .82rem; }
.gps-ph  { background: #161b22; border: 1px dashed #30363d; border-radius: 8px; padding: 32px; text-align: center; color: #8b949e; font-family: 'Share Tech Mono', monospace; font-size: .78rem; }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────
CHANNELS = {
    "throttle":        "Throttle (%)",
    "speed_rpm":       "Speed (RPM)",
    "speed_kmh":       "Speed (km/h)",
    "brake":           "Brake",
    "torque_nm":       "Torque (Nm)",
    "soc_bms1":        "SOC BMS1 (%)",
    "soc_bms2":        "SOC BMS2 (%)",
    "volt_mcu":        "Voltage MCU (V)",
    "volt_bms1":       "Voltage BMS1 (V)",
    "volt_bms2":       "Voltage BMS2 (V)",
    "curr_mcu":        "Current MCU (A)",
    "curr_bms1":       "Current BMS1 (A)",
    "curr_bms2":       "Current BMS2 (A)",
    "motor_temp":      "Motor Temp (°C)",
    "mcu_temp":        "MCU Temp (°C)",
    "board_temp_bms1": "Board Temp BMS1 (°C)",
    "board_temp_bms2": "Board Temp BMS2 (°C)",
}
DEFAULT_THR = {
    "motor_temp":      {"warn": 90,  "crit": 120, "dir": "high"},
    "mcu_temp":        {"warn": 70,  "crit": 85,  "dir": "high"},
    "board_temp_bms1": {"warn": 55,  "crit": 70,  "dir": "high"},
    "board_temp_bms2": {"warn": 55,  "crit": 70,  "dir": "high"},
    "curr_mcu":        {"warn": 200, "crit": 300, "dir": "high"},
    "soc_bms1":        {"warn": 15,  "crit": 10,  "dir": "low"},
    "soc_bms2":        {"warn": 15,  "crit": 10,  "dir": "low"},
    "volt_bms1":       {"warn": 44,  "crit": 42,  "dir": "low"},
    "volt_bms2":       {"warn": 44,  "crit": 42,  "dir": "low"},
}
MCU_BIT_MAP = {
    0: ("warn","Throttle Signal Failure"), 2: ("warn","Brake Switch Failure"),
    3: ("crit","Position Sensor Failure"), 4: ("warn","Wrong Offset Angle"),
    5: ("crit","Motor Temp Sensor Failure"), 6: ("crit","Motor Temperature High"),
    7: ("crit","MOSFET Temperature High"), 8: ("crit","MOSFET Temp Sensor Failure"),
    9: ("crit","DC Bus Overcurrent"), 10: ("crit","Id Current Error"),
    11: ("crit","Iq Current Error"), 12: ("crit","Phase Overcurrent UVW"),
    13: ("warn","CAN Signal Failure"), 14: ("crit","KL15 Failure"),
    15: ("warn","KL15 Short to Ground"), 16: ("warn","KL15 Short to Battery+"),
    17: ("crit","Rotor Stall"), 18: ("crit","DC Current Sensor Failure"),
    19: ("crit","Phase Current Sensor Failure UV"), 20: ("crit","MOSFET Short to Battery+"),
    21: ("warn","I2T Thermal Overload"), 22: ("crit","Loss of BMS CAN Message"),
    23: ("crit","Motor Overspeed"), 24: ("warn","BEMF Too High"),
    25: ("crit","Short Circuit B+ to B-"), 26: ("warn","CAN Data Out of Range"),
    27: ("warn","Derating Mode Active"), 28: ("crit","12V Converter Failure"),
    29: ("warn","Limp Home Mode Active"), 30: ("warn","Loss of Cluster CAN Message"),
    31: ("warn","FNR Gear Fault"), 32: ("warn","IO Signal Profile Failure"),
    33: ("warn","IO Signals Failure Charger"), 34: ("crit","Phase Short to Ground"),
    35: ("warn","IO Signals Short to Ground"), 36: ("warn","IO Signals Short to Battery+"),
    37: ("crit","Encoder Failure"), 38: ("crit","DC Bus Undervoltage"),
    39: ("crit","DC Bus Overvoltage"), 40: ("warn","Throttle Short to Ground"),
    41: ("warn","Throttle Short to Battery+"), 42: ("warn","Ideal Voltage Error"),
    43: ("warn","Motor Direction Fault"), 44: ("crit","MOSFET Short to Ground"),
    45: ("warn","Voltage Unstable"), 46: ("warn","Battery Sensor Short to Ground"),
    47: ("warn","Battery Sensor Short to Battery+"), 48: ("warn","NVM Checksum Error"),
    49: ("warn","Y-Phase Current Medium"), 50: ("warn","R-Phase Current Medium"),
    51: ("warn","DC Current Sensor Short to Ground"), 52: ("warn","DC Current Sensor Short to Battery+"),
    53: ("crit","Motor Temp Short to Ground"), 54: ("crit","Motor Temp Short to Battery+"),
    55: ("warn","Motor Temp Sensor Short to Ground"), 56: ("warn","Motor Temp Sensor Short to Battery+"),
    57: ("crit","Phase Short to Battery+"), 58: ("crit","Gate Driver Short to Battery+"),
    59: ("crit","Gate Driver Short to Ground"),
}
BMS_BIT_MAP = {
    0: ("warn","Cell Voltage High L1"), 1: ("crit","Cell Voltage High L2"),
    2: ("warn","Cell Voltage Low L1"), 3: ("crit","Cell Voltage Low L2"),
    4: ("warn","Pack Voltage High L1"), 5: ("crit","Pack Voltage High L2"),
    6: ("warn","Pack Voltage Low L1"), 7: ("crit","Pack Voltage Low L2"),
    8: ("warn","Charge Temp High L1"), 9: ("crit","Charge Temp High L2"),
    10: ("warn","Charge Temp Low L1"), 11: ("crit","Charge Temp Low L2"),
    12: ("warn","Discharge Temp High L1"), 13: ("crit","Discharge Temp High L2"),
    14: ("warn","Discharge Temp Low L1"), 15: ("crit","Discharge Temp Low L2"),
    20: ("warn","SOC High L1"), 21: ("crit","SOC High L2"),
    22: ("warn","SOC Low L1"), 23: ("crit","SOC Low L2"),
    24: ("warn","Cell Voltage Spread Large L1"), 25: ("crit","Cell Voltage Spread Large L2"),
    26: ("warn","Temp Spread Large L1"), 27: ("crit","Temp Spread Large L2"),
    28: ("warn","MOS Temp High L1"), 29: ("crit","MOS Temp High L2"),
    30: ("warn","Ambient Temp High L1"), 31: ("crit","Ambient Temp High L2"),
    32: ("crit","Charge MOS Overtemp"), 33: ("crit","Discharge MOS Overtemp"),
    34: ("crit","Charge MOS Sensor Failure"), 35: ("crit","Discharge MOS Sensor Failure"),
    36: ("crit","Charge MOS Adhesion Failure"), 37: ("crit","Discharge MOS Adhesion Failure"),
    38: ("crit","Charge MOS Open Circuit"), 39: ("crit","Discharge MOS Open Circuit"),
    40: ("crit","AFE Chip Failure"), 41: ("crit","Cell Acquisition Disconnected"),
    42: ("crit","Temperature Sensor Failure"), 43: ("crit","EEPROM Failure"),
    44: ("warn","RTC Clock Failure"), 45: ("crit","Precharge Failed"),
    46: ("crit","Vehicle CAN Failure"), 47: ("crit","Internal Network Failure"),
    48: ("crit","Current Module Failure"), 49: ("crit","Pack Voltage Detection Failure"),
    50: ("crit","Short Circuit Protection Failure"), 51: ("warn","Low Voltage Charge Prohibited"),
    52: ("warn","GPS Soft Switch MOS Disconnected"), 53: ("warn","Charger Out of Cabinet"),
    54: ("crit","Thermal Runaway"), 55: ("crit","Heating Failure"),
    56: ("crit","Balance Module Communication Failure"), 57: ("warn","Equalisation Conditions Not Met"),
}

C = ["#f7931a","#58a6ff","#3fb950","#ff6b6b","#a78bfa","#34d399","#fbbf24","#60a5fa","#f472b6","#4ade80"]
SCROLLZOOM = {"scrollZoom": True, "displayModeBar": True, "modeBarButtonsToRemove": ["lasso2d","select2d"]}
PLOT_BASE = dict(
    paper_bgcolor="#161b22", plot_bgcolor="#0d1117",
    font=dict(family="Exo 2,sans-serif", color="#c9d1d9", size=11),
    xaxis=dict(gridcolor="#21262d", zerolinecolor="#21262d"),
    yaxis=dict(gridcolor="#21262d", zerolinecolor="#21262d"),
    legend=dict(bgcolor="rgba(22,27,34,0.8)", bordercolor="#21262d", borderwidth=1),
    margin=dict(t=36,b=36,l=55,r=20),
    hovermode="x unified",
)

def plt(overrides=None):
    d = {**PLOT_BASE}
    if overrides: d.update(overrides)
    return d

# ── DB ────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    return create_engine(st.secrets["DATABASE_URL"], pool_pre_ping=True)

def init_db():
    with get_engine().connect() as con:
        con.execute(text("""CREATE TABLE IF NOT EXISTS sessions (
            id SERIAL PRIMARY KEY, name TEXT NOT NULL, date TEXT, rider TEXT, track TEXT,
            weather TEXT, notes TEXT, firmware TEXT, config TEXT, ambient_temp REAL,
            upload_time TEXT, row_count INTEGER, duration_s REAL, start_hms TEXT)"""))
        try: con.execute(text("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS start_hms TEXT"))
        except: pass
        con.execute(text("""CREATE TABLE IF NOT EXISTS signals (
            session_id INTEGER, t REAL,
            throttle REAL, speed_rpm REAL, speed_kmh REAL, brake REAL, torque_nm REAL,
            soc_bms1 REAL, soc_bms2 REAL, volt_mcu REAL, volt_bms1 REAL, volt_bms2 REAL,
            curr_mcu REAL, curr_bms1 REAL, curr_bms2 REAL,
            motor_temp REAL, mcu_temp REAL, board_temp_bms1 REAL, board_temp_bms2 REAL,
            mcu_errors TEXT, bms1_errors TEXT, bms2_errors TEXT, lat REAL, lon REAL)"""))
        con.execute(text("CREATE INDEX IF NOT EXISTS idx_sig ON signals(session_id)"))
        con.execute(text("""CREATE TABLE IF NOT EXISTS annotations (
            id SERIAL PRIMARY KEY, session_id INTEGER, t REAL,
            lat REAL, lon REAL,
            label TEXT, severity TEXT, author TEXT, note TEXT, created_at TEXT)"""))
        try: con.execute(text("ALTER TABLE annotations ADD COLUMN IF NOT EXISTS lat REAL"))
        except: pass
        try: con.execute(text("ALTER TABLE annotations ADD COLUMN IF NOT EXISTS lon REAL"))
        except: pass
        con.commit()

try: init_db()
except Exception as e: st.error(f"DB: {e}"); st.stop()

@st.cache_data(ttl=30)
def load_sessions():
    return pd.read_sql("SELECT * FROM sessions ORDER BY date DESC, id DESC", get_engine())

@st.cache_data(ttl=60)
def load_signals(sid):
    return pd.read_sql(f"SELECT * FROM signals WHERE session_id={sid} ORDER BY t", get_engine())

@st.cache_data(ttl=30)
def load_anns(sid):
    try: return pd.read_sql(f"SELECT * FROM annotations WHERE session_id={sid} ORDER BY t", get_engine())
    except: return pd.DataFrame()

def bust():
    load_sessions.clear(); load_signals.clear(); load_anns.clear()

# ── Helpers ───────────────────────────────────────────────────────────────────
def num(df, col):
    if col not in df.columns: return pd.Series(np.nan, index=df.index)
    return pd.to_numeric(df[col], errors="coerce")

def ds(df, n=6000):
    return df.iloc[::max(1, len(df)//n)] if len(df) > n else df

def hms_to_s(hms):
    try:
        p = str(hms).split(":")
        return int(p[0])*3600 + int(p[1])*60 + float(p[2])
    except: return 0.0

def to_wc(t, start_hms):
    return datetime(2000,1,1) + timedelta(seconds=hms_to_s(start_hms)) + timedelta(seconds=float(t))

def wc_arr(df_t, start_hms):
    base = datetime(2000,1,1) + timedelta(seconds=hms_to_s(start_hms))
    return [base + timedelta(seconds=float(t)) for t in df_t]

# ── CSV parse ─────────────────────────────────────────────────────────────────
def parse_time_col(series):
    for fmt in ["%H:%M:%S.%f", "%H:%M:%S"]:
        try:
            dt = pd.to_datetime(series, format=fmt)
            s  = (dt - dt.iloc[0]).dt.total_seconds().values
            return np.where(s<0, s+86400, s), str(dt.iloc[0].time())
        except: pass
    try:
        dt = pd.to_datetime(series, infer_datetime_format=True)
        return (dt - dt.iloc[0]).dt.total_seconds().values, str(dt.iloc[0].time())
    except: pass
    nums = pd.to_numeric(series, errors="coerce")
    if nums.notna().sum() >= len(series)*0.5:
        r = nums.max()-nums.min()
        base = nums - nums.iloc[0]
        if r>1e9: return base.values/1e6, "00:00:00"
        elif r>1e6: return base.values/1e3, "00:00:00"
        return base.values, "00:00:00"
    return np.arange(len(series))/50.0, "00:00:00"

def parse_csv(raw):
    df = None
    for enc in ["utf-8","latin-1","cp1252"]:
        try: df=pd.read_csv(io.BytesIO(raw), encoding=enc); break
        except: continue
    if df is None: raise ValueError("Cannot parse CSV")
    ren = {}
    for col in df.columns:
        c = col.lower()
        if "timestamp" in c:                               ren[col]="timestamp_raw"
        elif "throttle" in c:                              ren[col]="throttle"
        elif "rpm" in c:                                   ren[col]="speed_rpm"
        elif "km" in c and ("speed" in c or "/h" in c):   ren[col]="speed_kmh"
        elif "brake" in c:                                 ren[col]="brake"
        elif "torque" in c:                                ren[col]="torque_nm"
        elif "soc" in c and "bms1" in c:                   ren[col]="soc_bms1"
        elif "soc" in c and "bms2" in c:                   ren[col]="soc_bms2"
        elif "volt" in c and "mcu" in c:                   ren[col]="volt_mcu"
        elif "volt" in c and "bms1" in c:                  ren[col]="volt_bms1"
        elif "volt" in c and "bms2" in c:                  ren[col]="volt_bms2"
        elif "curr" in c and "mcu" in c:                   ren[col]="curr_mcu"
        elif "curr" in c and "bms1" in c:                  ren[col]="curr_bms1"
        elif "curr" in c and "bms2" in c:                  ren[col]="curr_bms2"
        elif "motor" in c and "temp" in c:                 ren[col]="motor_temp"
        elif "mcu" in c and "temp" in c:                   ren[col]="mcu_temp"
        elif "board" in c and "bms1" in c:                 ren[col]="board_temp_bms1"
        elif "board" in c and "bms2" in c:                 ren[col]="board_temp_bms2"
        elif "mcu" in c and ("error" in c or "err" in c):  ren[col]="mcu_errors"
        elif "bms1" in c and ("error" in c or "hw" in c):  ren[col]="bms1_errors"
        elif "bms2" in c and ("error" in c or "hw" in c):  ren[col]="bms2_errors"
        elif col.lower()=="lat" or "latitude" in c:        ren[col]="lat"
        elif col.lower()=="lon" or "longitude" in c:       ren[col]="lon"
    df = df.rename(columns=ren)
    start_hms = "00:00:00"
    if "timestamp_raw" in df.columns:
        t_vals, start_hms = parse_time_col(df["timestamp_raw"])
        df["t"] = t_vals
    else:
        df["t"] = np.arange(len(df))/50.0
    if "brake" in df.columns:
        df["brake"] = df["brake"].map({"true":1.0,"false":0.0,True:1.0,False:0.0}).fillna(
            pd.to_numeric(df["brake"], errors="coerce"))
    return df, start_hms

def save_db(meta, df):
    cols = ["t","throttle","speed_rpm","speed_kmh","brake","torque_nm",
            "soc_bms1","soc_bms2","volt_mcu","volt_bms1","volt_bms2",
            "curr_mcu","curr_bms1","curr_bms2","motor_temp","mcu_temp",
            "board_temp_bms1","board_temp_bms2","mcu_errors","bms1_errors","bms2_errors","lat","lon"]
    for c in cols:
        if c not in df.columns: df[c] = np.nan
    eng = get_engine()
    with eng.connect() as con:
        r = con.execute(text(
            "INSERT INTO sessions(name,date,rider,track,weather,notes,firmware,config,"
            "ambient_temp,upload_time,row_count,duration_s,start_hms) "
            "VALUES(:name,:date,:rider,:track,:weather,:notes,:firmware,:config,"
            ":ambient_temp,:upload_time,:row_count,:duration_s,:start_hms) RETURNING id"), meta)
        sid = r.fetchone()[0]; con.commit()
    sig = df[cols].copy(); sig.insert(0,"session_id",sid)
    sig.to_sql("signals", eng, if_exists="append", index=False, method="multi", chunksize=2000)
    return sid

# ── Derived metrics ───────────────────────────────────────────────────────────
def derive(df):
    out = df.copy()
    v, i = num(df,"volt_mcu"), num(df,"curr_mcu")
    if v.notna().any() and i.notna().any():
        out["power_kw"] = (v * i / 1000).round(4)
        dt = pd.to_numeric(df["t"], errors="coerce").diff().fillna(0.02).clip(0, 1)
        wh = out["power_kw"] * dt * 1000 / 3600           # Wh increments
        out["wh_net"]  = wh.cumsum().round(3)              # positive=discharge, negative=regen
        out["wh_out"]  = wh.clip(lower=0).cumsum().round(3)
        out["wh_regen"]= (-wh.clip(upper=0)).cumsum().round(3)

    spd = num(df,"speed_kmh")
    if spd.notna().any():
        dt = pd.to_numeric(df["t"], errors="coerce").diff().fillna(0.02).clip(0, 1)
        out["dist_km"] = (spd.clip(lower=0) * dt / 3600).cumsum().round(5)

    rpm  = num(df,"speed_rpm")
    torq = num(df,"torque_nm")
    if rpm.notna().any() and torq.notna().any():
        omega = rpm * (2*np.pi/60)
        out["mech_kw"] = (torq * omega / 1000).round(4)

    return out

# ── Fault decode ──────────────────────────────────────────────────────────────
def decode_bits(hex_str, bit_map, source):
    s = str(hex_str).strip().lower()
    if s in ("","nan","none","0","0.0","0x0","0x0000000000000000"): return []
    try: val = int(s,16) if s.startswith("0x") else int(float(s))
    except: return []
    if val == 0: return []
    return [{"source":source,"bit":b,"sev":sev,"desc":desc,"raw":hex_str}
            for b,(sev,desc) in bit_map.items() if val & (1<<b)]

def decode_series(df, thr):
    """Build per-row list of active fault names; returns Series of lists."""
    result = [[] for _ in range(len(df))]
    for col, bm, lbl in [("mcu_errors",MCU_BIT_MAP,"MCU"),
                          ("bms1_errors",BMS_BIT_MAP,"BMS1"),
                          ("bms2_errors",BMS_BIT_MAP,"BMS2")]:
        if col not in df.columns: continue
        for idx, code in enumerate(df[col].astype(str)):
            for f in decode_bits(code, bm, lbl):
                result[idx].append(f"{lbl}:{f['desc']}")
    return result

def fault_summary(df):
    rows = []
    for col, bm, lbl in [("mcu_errors",MCU_BIT_MAP,"MCU"),
                          ("bms1_errors",BMS_BIT_MAP,"BMS1"),
                          ("bms2_errors",BMS_BIT_MAP,"BMS2")]:
        if col not in df.columns: continue
        vals = df[col].astype(str).str.strip()
        agg = {}
        for t_val, code in zip(df["t"], vals):
            for f in decode_bits(code, bm, lbl):
                key = (lbl, f["bit"], f["desc"], f["sev"])
                if key not in agg:
                    agg[key] = {"first":t_val,"last":t_val,"count":0}
                agg[key]["count"] += 1
                agg[key]["last"] = max(agg[key]["last"], t_val)
        for (src,bit,desc,sev),info in agg.items():
            if info["count"] < 2: continue
            # context: max speed + max temp during fault windows
            m = df[(df["t"] >= info["first"]) & (df["t"] <= info["last"])]
            rows.append({
                "Source": src, "Bit": bit,
                "Fault": desc,
                "Severity": "CRITICAL" if sev=="crit" else "WARNING",
                "Samples": info["count"],
                "Duration(s)": f'{info["count"]/50:.1f}',
                "First": f'T+{info["first"]:.1f}s',
                "Last": f'T+{info["last"]:.1f}s',
                "Max Speed": f'{num(m,"speed_kmh").max():.1f}' if "speed_kmh" in m else "-",
                "Max Motor°C": f'{num(m,"motor_temp").max():.1f}' if "motor_temp" in m else "-",
            })
    rows.sort(key=lambda x:(0 if x["Severity"]=="CRITICAL" else 1, -x["Samples"]))
    return rows

def threshold_events(df, thr):
    rows = []
    for ch, cfg in thr.items():
        if ch not in df.columns: continue
        s = num(df, ch); hi = cfg["dir"]=="high"
        for lvl, label in [("crit","CRITICAL"),("warn","WARNING")]:
            v = cfg[lvl]
            mask = (s>=v) if hi else (s<=v)
            if lvl=="warn":
                mask = ((s>=v)&(s<cfg["crit"])) if hi else ((s<=v)&(s>cfg["crit"]))
            cnt = mask.sum()
            if cnt < 10: continue
            peak = s[mask].max() if hi else s[mask].min()
            first_t = float(df.loc[mask,"t"].iloc[0])
            rows.append({"Source":"Threshold","Fault":CHANNELS.get(ch,ch),
                "Severity":label,"Samples":int(cnt),"Duration(s)":f'{cnt/50:.1f}',
                "First":f'T+{first_t:.1f}s',"Last":"—",
                "Peak/Min":f'{peak:.2f}',"Limit":f'{v}'})
    return rows

# ── Slider ────────────────────────────────────────────────────────────────────
def time_slider(df, start_hms, key):
    t_max = float(df["t"].max())
    origin = datetime(2000,1,1) + timedelta(seconds=hms_to_s(start_hms))
    sel = st.slider("Time window", min_value=origin,
        max_value=origin+timedelta(seconds=t_max),
        value=(origin, origin+timedelta(seconds=t_max)),
        step=timedelta(seconds=1), format="HH:mm:ss", key=key)
    return (sel[0]-origin).total_seconds(), (sel[1]-origin).total_seconds()

# ── Gauge ─────────────────────────────────────────────────────────────────────
def gauge(val, title, lo, hi, warn, crit, unit="", direction="high"):
    if pd.isna(val): color="#555"
    elif direction=="high": color="#ff4444" if val>=crit else ("#ffa500" if val>=warn else "#3fb950")
    else: color="#ff4444" if val<=crit else ("#ffa500" if val<=warn else "#3fb950")
    steps = ([{"range":[lo,warn],"color":"#1a3a1a"},{"range":[warn,crit],"color":"#3d2800"},{"range":[crit,hi],"color":"#4a1515"}]
             if direction=="high" else
             [{"range":[lo,crit],"color":"#4a1515"},{"range":[crit,warn],"color":"#3d2800"},{"range":[warn,hi],"color":"#1a3a1a"}])
    fig = go.Figure(go.Indicator(mode="gauge+number",
        value=val if not pd.isna(val) else lo,
        number={"suffix":unit,"font":{"family":"Share Tech Mono","color":color,"size":18}},
        title={"text":title,"font":{"family":"Exo 2","color":"#8b949e","size":10}},
        gauge={"axis":{"range":[lo,hi],"tickcolor":"#444","tickfont":{"size":8,"color":"#555"}},
               "bar":{"color":color,"thickness":0.22},"bgcolor":"#0d1117","bordercolor":"#21262d",
               "steps":steps,"threshold":{"line":{"color":color,"width":2},"thickness":0.75,"value":val if not pd.isna(val) else lo}}))
    fig.update_layout(paper_bgcolor="#161b22",font_color="#c9d1d9",height=155,margin=dict(t=28,b=8,l=18,r=18))
    return fig

# ── KPI card helper ───────────────────────────────────────────────────────────
def kpi(col, label, value, color="#c9d1d9"):
    col.markdown(f'<div class="kpi-card"><div class="kpi-label">{label}</div>'
                 f'<div class="kpi-value" style="color:{color}">{value}</div></div>',
                 unsafe_allow_html=True)

def kpi_color(val, ch, thr):
    if pd.isna(val) or ch not in thr: return "#c9d1d9"
    cfg=thr[ch]; hi=cfg["dir"]=="high"
    if (hi and val>=cfg["crit"]) or (not hi and val<=cfg["crit"]): return "#ff4444"
    if (hi and val>=cfg["warn"]) or (not hi and val<=cfg["warn"]): return "#ffa500"
    return "#3fb950"

def shdr(text):
    st.markdown(f'<div class="section-hdr">{text}</div>', unsafe_allow_html=True)

# ── AI ────────────────────────────────────────────────────────────────────────
def build_ctx(df, srow, thr):
    lines = [
        f"FAMEL ELECTRIC MOTORCYCLE — SESSION {srow.get('name','?')}",
        f"Date: {srow.get('date','?')} | Rider: {srow.get('rider','?')} | Road: {srow.get('track','?')}",
        f"FW: {srow.get('firmware','?')} | Config: {srow.get('config','?')}",
        f"Ambient: {srow.get('ambient_temp','?')}°C | Duration: {df['t'].max():.0f}s",
        f"Notes: {srow.get('notes','')}" ]
    lines.append("\nSTATISTICS min/mean/max")
    for ch,lbl in CHANNELS.items():
        s=num(df,ch).dropna()
        if not s.empty: lines.append(f"  {lbl}: {s.min():.2f} / {s.mean():.2f} / {s.max():.2f}")
    df2=derive(df)
    if "power_kw" in df2.columns: lines.append(f"  Power kW: {df2['power_kw'].min():.2f}/{df2['power_kw'].mean():.2f}/{df2['power_kw'].max():.2f}")
    if "wh_out" in df2.columns:   lines.append(f"  Energy out: {df2['wh_out'].max():.1f} Wh  Regen: {df2['wh_regen'].max():.1f} Wh")
    if "dist_km" in df2.columns:  lines.append(f"  Distance: {df2['dist_km'].max():.2f} km")
    faults = fault_summary(df) + threshold_events(df, thr)
    lines.append(f"\nFAULTS ({len(faults)})")
    for f in faults: lines.append(f"  [{f['Severity']}] {f['Fault']} | {f['Samples']} samples | {f['First']} | Dur {f['Duration(s)']}s")
    return "\n".join(lines)

def ask_ai(ctx, q, history):
    key = st.secrets.get("ANTHROPIC_API_KEY","")
    if not key: return "⚠️ Add ANTHROPIC_API_KEY to secrets."
    sys_p = ("You are a senior electric motorcycle engineer at FAMEL supporting homologation. "
             "Be precise, cite exact values and timestamps, flag safety concerns. "
             "Suggest root causes and corrective actions.\n\nSESSION DATA:\n" + ctx)
    msgs = [{"role":m["role"],"content":m["content"]} for m in history]
    msgs.append({"role":"user","content":q})
    r = requests.post("https://api.anthropic.com/v1/messages",
        headers={"x-api-key":key,"anthropic-version":"2023-06-01","content-type":"application/json"},
        json={"model":"claude-sonnet-4-20250514","max_tokens":1400,"system":sys_p,"messages":msgs},timeout=30)
    d = r.json()
    return d["content"][0]["text"] if "content" in d else f"Error: {d.get('error',{}).get('message',str(d))}"

# ═══════════════════════════════════════════════════════════════════════════════
# ANALYSE — TABS
# ═══════════════════════════════════════════════════════════════════════════════

def tab_overview(df, start_hms, thr):
    # ── KPI row ──────────────────────────────────────────────────────────────
    shdr("RIDE OVERVIEW")
    dur_s = float(df["t"].max())
    spd   = num(df,"speed_kmh")
    moving = spd > 2
    dist_km = df.get("dist_km", pd.Series([np.nan]))
    dist_val = dist_km.max() if "dist_km" in df.columns else np.nan
    avg_spd_moving = spd[moving].mean() if moving.sum()>0 else np.nan
    wh_out   = df["wh_out"].max()   if "wh_out"   in df.columns else np.nan
    wh_regen = df["wh_regen"].max() if "wh_regen" in df.columns else np.nan
    eff = (wh_out / dist_val) if (dist_val and dist_val>0) else np.nan
    regen_pct = (wh_regen/wh_out*100) if (wh_out and wh_out>0) else np.nan

    k = st.columns(9)
    kpi(k[0], "Duration",         f'{dur_s/60:.1f} min')
    kpi(k[1], "Distance",         f'{dist_val:.2f} km' if not np.isnan(dist_val) else "—")
    kpi(k[2], "Max Speed",        f'{spd.max():.1f} km/h', kpi_color(spd.max(),"speed_kmh",thr))
    kpi(k[3], "Avg Speed",        f'{spd.mean():.1f} km/h')
    kpi(k[4], "Avg Moving",       f'{avg_spd_moving:.1f} km/h' if not np.isnan(avg_spd_moving) else "—")
    kpi(k[5], "Energy Used",      f'{wh_out:.0f} Wh' if not np.isnan(wh_out) else "—")
    kpi(k[6], "Regen Recovered",  f'{wh_regen:.0f} Wh' if not np.isnan(wh_regen) else "—", "#34d399")
    kpi(k[7], "Efficiency",       f'{eff:.1f} Wh/km' if not np.isnan(eff) else "—")
    kpi(k[8], "Regen Share",      f'{regen_pct:.1f}%' if not np.isnan(regen_pct) else "—", "#34d399")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Speed / Throttle / Brake stacked ──────────────────────────────────────
    shdr("DRIVE TIMELINE — SPEED · THROTTLE · BRAKE")
    t0, t1 = time_slider(df, start_hms, key="ov_slider")
    sub = ds(df[(df["t"]>=t0)&(df["t"]<=t1)])
    xs  = wc_arr(sub["t"], start_hms)

    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.04,
                        row_heights=[0.5,0.3,0.2],
                        subplot_titles=["Speed (km/h)", "Throttle (%)", "Brake"])
    # Speed
    fig.add_trace(go.Scatter(x=xs, y=num(sub,"speed_kmh"), line=dict(color=C[0],width=1.5),
        name="Speed", hovertemplate="%{x|%H:%M:%S} → %{y:.1f} km/h<extra></extra>"), row=1, col=1)
    # Throttle
    fig.add_trace(go.Scatter(x=xs, y=num(sub,"throttle"), fill="tozeroy",
        line=dict(color=C[1],width=1), fillcolor="rgba(88,166,255,0.15)",
        name="Throttle", hovertemplate="%{y:.1f}%<extra></extra>"), row=2, col=1)
    # Brake
    brk = num(sub,"brake").fillna(0)
    fig.add_trace(go.Scatter(x=xs, y=brk, fill="tozeroy",
        line=dict(color="#ff6b6b",width=1), fillcolor="rgba(255,107,107,0.3)",
        name="Brake"), row=3, col=1)

    fig.update_layout(**plt({"height":480, "showlegend":True,
        "xaxis3":dict(gridcolor="#21262d",tickformat="%H:%M:%S"),
        "yaxis3":dict(gridcolor="#21262d",range=[-0.1,1.5],tickvals=[0,1]),
    }))
    for r in range(1,4):
        fig.update_xaxes(gridcolor="#21262d", row=r, col=1, tickformat="%H:%M:%S")
        fig.update_yaxes(gridcolor="#21262d", row=r, col=1)
    st.plotly_chart(fig, use_container_width=True, config=SCROLLZOOM)

    # Energy bars
    if "wh_out" in df.columns:
        shdr("ENERGY TIMELINE")
        ep = make_subplots(rows=1,cols=1)
        ep.add_trace(go.Scatter(x=wc_arr(sub["t"],start_hms), y=sub.get("wh_out",pd.Series()),
            line=dict(color=C[0],width=1.5), name="Energy used (Wh)", fill="tozeroy", fillcolor="rgba(247,147,26,0.15)"))
        ep.add_trace(go.Scatter(x=wc_arr(sub["t"],start_hms), y=sub.get("wh_regen",pd.Series()),
            line=dict(color=C[2],width=1.5), name="Regen (Wh)", fill="tozeroy", fillcolor="rgba(63,185,80,0.15)"))
        ep.update_layout(**plt({"height":220, "xaxis":dict(gridcolor="#21262d",tickformat="%H:%M:%S"), "yaxis_title":"Wh"}))
        st.plotly_chart(ep, use_container_width=True, config=SCROLLZOOM)


def tab_powertrain(df, start_hms, thr):
    shdr("POWERTRAIN PERFORMANCE")
    t0, t1 = time_slider(df, start_hms, key="pt_slider")
    sub = ds(df[(df["t"]>=t0)&(df["t"]<=t1)])
    xs  = wc_arr(sub["t"], start_hms)

    # ── Time-series 2x2 ───────────────────────────────────────────────────────
    r1c1, r1c2 = st.columns(2)

    # Torque
    with r1c1:
        fig = go.Figure()
        fig.add_hline(y=0, line_color="#555", line_width=1)
        fig.add_trace(go.Scatter(x=xs, y=num(sub,"torque_nm"), line=dict(color=C[0],width=1.5),
            name="Torque", hovertemplate="%{x|%H:%M:%S} %{y:.1f} Nm<extra></extra>"))
        fig.update_layout(**plt({"height":280,"title":dict(text="Torque (Nm) — drive vs regen",font=dict(size=12,color="#8b949e")),
            "xaxis":dict(gridcolor="#21262d",tickformat="%H:%M:%S"),"yaxis_title":"Nm"}))
        st.plotly_chart(fig, use_container_width=True, config=SCROLLZOOM)

    # Electrical power
    with r1c2:
        fig2 = go.Figure()
        fig2.add_hline(y=0, line_color="#555", line_width=1)
        if "power_kw" in sub.columns:
            pwr = num(sub,"power_kw")
            fig2.add_trace(go.Scatter(x=xs, y=pwr.clip(lower=0), fill="tozeroy",
                line=dict(color=C[0],width=1.2), fillcolor="rgba(247,147,26,0.15)", name="Drive kW"))
            fig2.add_trace(go.Scatter(x=xs, y=pwr.clip(upper=0), fill="tozeroy",
                line=dict(color=C[2],width=1.2), fillcolor="rgba(63,185,80,0.2)", name="Regen kW"))
        fig2.update_layout(**plt({"height":280,"title":dict(text="Electrical Power kW  (V·I)",font=dict(size=12,color="#8b949e")),
            "xaxis":dict(gridcolor="#21262d",tickformat="%H:%M:%S"),"yaxis_title":"kW"}))
        st.plotly_chart(fig2, use_container_width=True, config=SCROLLZOOM)

    r2c1, r2c2 = st.columns(2)
    # Motor RPM
    with r2c1:
        fig3 = make_subplots(specs=[[{"secondary_y":True}]])
        fig3.add_trace(go.Scatter(x=xs, y=num(sub,"speed_rpm"), line=dict(color=C[1],width=1.4), name="RPM"), secondary_y=False)
        fig3.add_trace(go.Scatter(x=xs, y=num(sub,"speed_kmh"), line=dict(color=C[0],width=1.2, dash="dot"), name="Speed km/h"), secondary_y=True)
        fig3.update_layout(**plt({"height":280,"title":dict(text="Motor RPM + Speed",font=dict(size=12,color="#8b949e")),
            "xaxis":dict(gridcolor="#21262d",tickformat="%H:%M:%S")}))
        fig3.update_yaxes(title_text="RPM", gridcolor="#21262d", secondary_y=False)
        fig3.update_yaxes(title_text="km/h", gridcolor="#21262d", secondary_y=True)
        st.plotly_chart(fig3, use_container_width=True, config=SCROLLZOOM)

    # Mech power estimate
    with r2c2:
        fig4 = go.Figure()
        if "mech_kw" in sub.columns:
            fig4.add_trace(go.Scatter(x=xs, y=num(sub,"mech_kw"), line=dict(color="#a78bfa",width=1.4), name="Mech kW (est.)"))
        if "power_kw" in sub.columns:
            fig4.add_trace(go.Scatter(x=xs, y=num(sub,"power_kw"), line=dict(color=C[0],width=1.2,dash="dot"), name="Electrical kW"))
        fig4.add_hline(y=0, line_color="#555", line_width=1)
        fig4.update_layout(**plt({"height":280,"title":dict(text="Mechanical vs Electrical Power (kW)",font=dict(size=12,color="#8b949e")),
            "xaxis":dict(gridcolor="#21262d",tickformat="%H:%M:%S"),"yaxis_title":"kW"}))
        st.plotly_chart(fig4, use_container_width=True, config=SCROLLZOOM)

    # ── Scatter plots ──────────────────────────────────────────────────────────
    shdr("SCATTER ANALYSIS")
    sc1, sc2, sc3 = st.columns(3)

    def mkscat(x_ch, y_ch, c_ch, c_title, title, col):
        sx = num(df, x_ch).dropna()
        sy = num(df, y_ch)
        mask = sx.index.intersection(sy.dropna().index)
        if len(mask) < 20: col.info(f"Needs {CHANNELS.get(x_ch,x_ch)} + {CHANNELS.get(y_ch,y_ch)}"); return
        cv  = num(df.loc[mask], c_ch) if c_ch and c_ch in df.columns else None
        sdf = ds(df.loc[mask], 3000)
        cv2 = num(sdf, c_ch) if c_ch and c_ch in df.columns else None
        fig = go.Figure(go.Scatter(
            x=num(sdf,x_ch), y=num(sdf,y_ch), mode="markers",
            marker=dict(size=3, opacity=0.65,
                color=cv2 if cv2 is not None else C[0],
                colorscale="RdYlGn_r" if cv2 is not None else None,
                showscale=cv2 is not None,
                colorbar=dict(title=c_title,thickness=10,titlefont=dict(size=9)) if cv2 is not None else None),
            hovertemplate=f'{CHANNELS.get(x_ch,x_ch)}: %{{x:.1f}}<br>{CHANNELS.get(y_ch,y_ch)}: %{{y:.1f}}<extra></extra>'))
        fig.update_layout(**plt({"height":270,"title":dict(text=title,font=dict(size=11,color="#8b949e")),
            "xaxis_title":CHANNELS.get(x_ch,x_ch),"yaxis_title":CHANNELS.get(y_ch,y_ch)}))
        col.plotly_chart(fig, use_container_width=True, config=SCROLLZOOM)

    mkscat("throttle","torque_nm",None,None,"Torque vs Throttle",sc1)
    mkscat("speed_kmh","torque_nm","brake",None,"Torque vs Speed  (brake overlay)",sc2)
    mkscat("speed_kmh","power_kw","motor_temp","Motor°C","Power vs Speed  (colour=Motor Temp)",sc3)

    # ── Limiting detection ─────────────────────────────────────────────────────
    if "throttle" in df.columns and "torque_nm" in df.columns:
        shdr("THROTTLE CLIPPING / LIMITING DETECTION")
        thr_100 = num(df,"throttle") >= 98
        if thr_100.sum() > 50:
            at_limit = df[thr_100]
            torq_limit = num(at_limit,"torque_nm")
            cols3 = st.columns(4)
            kpi(cols3[0], "Time at 100% throttle", f'{thr_100.sum()/50:.1f}s')
            kpi(cols3[1], "Torque at 100% (mean)", f'{torq_limit.mean():.1f} Nm' if not torq_limit.isna().all() else "—")
            kpi(cols3[2], "Torque at 100% (max)",  f'{torq_limit.max():.1f} Nm' if not torq_limit.isna().all() else "—")
            kpi(cols3[3], "Torque at 100% (min)",  f'{torq_limit.min():.1f} Nm' if not torq_limit.isna().all() else "—")
            if not torq_limit.isna().all() and torq_limit.std() > 5:
                st.warning("⚠️ Torque varies significantly at 100% throttle — possible thermal/current derating active.")


def tab_battery(df, start_hms, thr):
    shdr("BATTERY PACK HEALTH")
    t0, t1 = time_slider(df, start_hms, key="bat_slider")
    sub = ds(df[(df["t"]>=t0)&(df["t"]<=t1)])
    xs  = wc_arr(sub["t"], start_hms)

    # ── Imbalance KPIs ────────────────────────────────────────────────────────
    dsoc  = (num(df,"soc_bms1") - num(df,"soc_bms2")).abs()
    dv    = (num(df,"volt_bms1")- num(df,"volt_bms2")).abs()
    ci    = num(df,"curr_bms1") / (num(df,"curr_bms1")+num(df,"curr_bms2")).replace(0,np.nan)
    dis   = num(df,"curr_mcu") > 0
    regen = num(df,"curr_mcu") < 0

    k = st.columns(7)
    kpi(k[0],"Max ΔSOC BMS1/2", f'{dsoc.max():.2f}%' if not dsoc.isna().all() else "—",
        "#ff4444" if not dsoc.isna().all() and dsoc.max()>5 else "#c9d1d9")
    kpi(k[1],"Max ΔVoltage",    f'{dv.max():.3f} V' if not dv.isna().all() else "—",
        "#ff4444" if not dv.isna().all() and dv.max()>3 else "#c9d1d9")
    kpi(k[2],"SOC BMS1 min",    f'{num(df,"soc_bms1").min():.1f}%', kpi_color(num(df,"soc_bms1").min(),"soc_bms1",thr))
    kpi(k[3],"SOC BMS2 min",    f'{num(df,"soc_bms2").min():.1f}%', kpi_color(num(df,"soc_bms2").min(),"soc_bms2",thr))
    kpi(k[4],"Current share discharge", f'{ci[dis].mean():.2f}' if (dis.sum()>10 and not ci[dis].isna().all()) else "—")
    kpi(k[5],"Current share regen",     f'{ci[regen].mean():.2f}' if (regen.sum()>10 and not ci[regen].isna().all()) else "—")
    kpi(k[6],"Volt MCU min",    f'{num(df,"volt_mcu").min():.2f} V', kpi_color(num(df,"volt_mcu").min(),"volt_bms1",thr))

    st.markdown("<br>",unsafe_allow_html=True)

    bc1, bc2 = st.columns(2)
    # SOC comparison
    with bc1:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=xs, y=num(sub,"soc_bms1"), line=dict(color=C[0],width=1.5), name="SOC BMS1 (%)"))
        fig.add_trace(go.Scatter(x=xs, y=num(sub,"soc_bms2"), line=dict(color=C[1],width=1.5), name="SOC BMS2 (%)"))
        s1,s2 = num(sub,"soc_bms1"),num(sub,"soc_bms2")
        fig.add_trace(go.Scatter(x=xs+xs[::-1],
            y=pd.concat([s1,s2[::-1].reset_index(drop=True)]).tolist(),
            fill="toself", fillcolor="rgba(255,107,107,0.12)", line_color="rgba(0,0,0,0)", name="Imbalance zone"))
        if "soc_bms1" in thr:
            fig.add_hline(y=thr["soc_bms1"]["warn"],line_dash="dot",line_color="orange",opacity=0.5)
        fig.update_layout(**plt({"height":280,"title":dict(text="SOC Comparison BMS1 vs BMS2",font=dict(size=12,color="#8b949e")),
            "xaxis":dict(gridcolor="#21262d",tickformat="%H:%M:%S"),"yaxis_title":"%"}))
        st.plotly_chart(fig, use_container_width=True, config=SCROLLZOOM)

    # Voltage comparison
    with bc2:
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=xs, y=num(sub,"volt_bms1"), line=dict(color=C[0],width=1.5), name="Volt BMS1"))
        fig2.add_trace(go.Scatter(x=xs, y=num(sub,"volt_bms2"), line=dict(color=C[1],width=1.5), name="Volt BMS2"))
        fig2.add_trace(go.Scatter(x=xs, y=num(sub,"volt_mcu"),  line=dict(color=C[3],width=1.2, dash="dot"), name="Volt MCU"))
        if "volt_bms1" in thr:
            fig2.add_hline(y=thr["volt_bms1"]["warn"],line_dash="dot",line_color="orange",opacity=0.5)
            fig2.add_hline(y=thr["volt_bms1"]["crit"],line_dash="dash",line_color="#ff4444",opacity=0.5)
        fig2.update_layout(**plt({"height":280,"title":dict(text="Voltage BMS1 / BMS2 / MCU (V)",font=dict(size=12,color="#8b949e")),
            "xaxis":dict(gridcolor="#21262d",tickformat="%H:%M:%S"),"yaxis_title":"V"}))
        st.plotly_chart(fig2, use_container_width=True, config=SCROLLZOOM)

    bc3, bc4 = st.columns(2)
    # Current comparison
    with bc3:
        fig3 = go.Figure()
        fig3.add_trace(go.Scatter(x=xs, y=num(sub,"curr_bms1"), line=dict(color=C[0],width=1.4), name="Curr BMS1"))
        fig3.add_trace(go.Scatter(x=xs, y=num(sub,"curr_bms2"), line=dict(color=C[1],width=1.4), name="Curr BMS2"))
        fig3.add_trace(go.Scatter(x=xs, y=num(sub,"curr_mcu"),  line=dict(color=C[3],width=1.2, dash="dot"), name="Curr MCU"))
        fig3.add_hline(y=0, line_color="#555", line_width=1)
        if "curr_mcu" in thr:
            fig3.add_hline(y=thr["curr_mcu"]["warn"],line_dash="dot",line_color="orange",opacity=0.5)
        fig3.update_layout(**plt({"height":280,"title":dict(text="Current BMS1 / BMS2 / MCU (A)",font=dict(size=12,color="#8b949e")),
            "xaxis":dict(gridcolor="#21262d",tickformat="%H:%M:%S"),"yaxis_title":"A"}))
        st.plotly_chart(fig3, use_container_width=True, config=SCROLLZOOM)

    # Voltage sag scatter
    with bc4:
        sv = num(df,"volt_mcu").dropna(); si = num(df,"curr_mcu")
        mask = sv.index.intersection(si.dropna().index)
        if len(mask) > 50:
            sdf2 = ds(df.loc[mask], 3000)
            fig4 = go.Figure(go.Scatter(
                x=num(sdf2,"curr_mcu"), y=num(sdf2,"volt_mcu"), mode="markers",
                marker=dict(size=3, color=num(sdf2,"motor_temp") if "motor_temp" in sdf2.columns else C[0],
                    colorscale="RdYlGn_r", showscale=True, opacity=0.65,
                    colorbar=dict(title="Motor°C",thickness=10,titlefont=dict(size=9))),
                hovertemplate="I: %{x:.1f}A  V: %{y:.2f}V<extra></extra>"))
            fig4.update_layout(**plt({"height":280,"title":dict(text="Voltage Sag — V vs I  (colour = Motor Temp)",font=dict(size=12,color="#8b949e")),
                "xaxis_title":"Current MCU (A)","yaxis_title":"Voltage MCU (V)"}))
            st.plotly_chart(fig4, use_container_width=True, config=SCROLLZOOM)
        else:
            bc4.info("Needs volt_mcu + curr_mcu data")

    # Worst sag events table
    shdr("WORST VOLTAGE SAG EVENTS (top 10)")
    if "volt_mcu" in df.columns and "curr_mcu" in df.columns:
        vm = num(df,"volt_mcu"); im = num(df,"curr_mcu")
        sag_score = im.clip(lower=0) - vm   # high score = high current + low voltage
        top_idx = sag_score.nlargest(200).index
        top_df = df.loc[top_idx][["t","volt_mcu","curr_mcu","motor_temp","speed_kmh"]].dropna(subset=["volt_mcu","curr_mcu"])
        if not top_df.empty:
            top_df = top_df.head(10).copy()
            top_df["Time"] = [to_wc(t,start_hms).strftime("%H:%M:%S") for t in top_df["t"]]
            top_df = top_df[["Time","volt_mcu","curr_mcu","motor_temp","speed_kmh"]].rename(columns={
                "volt_mcu":"Volt (V)","curr_mcu":"Curr (A)","motor_temp":"Motor°C","speed_kmh":"Speed km/h"})
            st.dataframe(top_df, use_container_width=True, hide_index=True)


def tab_thermal(df, start_hms, thr):
    shdr("THERMAL DASHBOARD")
    t0, t1 = time_slider(df, start_hms, key="th_slider")
    sub = ds(df[(df["t"]>=t0)&(df["t"]<=t1)])
    xs  = wc_arr(sub["t"], start_hms)

    # Headroom KPIs
    k = st.columns(8)
    temp_chans = [("motor_temp","Motor Temp",120,160),("mcu_temp","MCU Temp",85,110),
                  ("board_temp_bms1","BMS1 Board",70,90),("board_temp_bms2","BMS2 Board",70,90)]
    for col,(ch,lbl,warn_val,crit_val) in zip(k[:4],temp_chans):
        v = num(df,ch).max()
        kpi(col, f"Peak {lbl}", f'{v:.1f}°C' if not np.isnan(v) else "—", kpi_color(v,ch,thr))
    for col,(ch,lbl,warn_val,crit_val) in zip(k[4:],temp_chans):
        above = (num(df,ch) > warn_val).sum()
        kpi(col, f"{lbl} >warn", f'{above/50:.0f}s', "#ffa500" if above>0 else "#3fb950")

    st.markdown("<br>",unsafe_allow_html=True)

    # Temp time-series with power overlay
    tc1, tc2 = st.columns(2)
    with tc1:
        fig = make_subplots(specs=[[{"secondary_y":True}]])
        for i,(ch,lbl) in enumerate([("motor_temp","Motor"),("mcu_temp","MCU"),("board_temp_bms1","BMS1"),("board_temp_bms2","BMS2")]):
            fig.add_trace(go.Scatter(x=xs, y=num(sub,ch), name=f'{lbl}°C',
                line=dict(color=C[i],width=1.5), hovertemplate=f'{lbl}: %{{y:.1f}}°C<extra></extra>'), secondary_y=False)
        for ch,v in [(thr.get("motor_temp",{}).get("crit",120),C[0]),(thr.get("mcu_temp",{}).get("crit",85),C[1])]:
            if ch: fig.add_hline(y=ch,line_dash="dash",line_color=v,opacity=0.4)
        if "power_kw" in sub.columns:
            fig.add_trace(go.Scatter(x=xs, y=num(sub,"power_kw"), name="Power kW",
                line=dict(color="#555",width=1,dash="dot"), hovertemplate="Power: %{y:.2f}kW<extra></extra>"), secondary_y=True)
        fig.update_layout(**plt({"height":320,"title":dict(text="Temperatures + Power Overlay",font=dict(size=12,color="#8b949e")),
            "xaxis":dict(gridcolor="#21262d",tickformat="%H:%M:%S")}))
        fig.update_yaxes(title_text="°C", gridcolor="#21262d", secondary_y=False)
        fig.update_yaxes(title_text="kW", gridcolor="#21262d", secondary_y=True, showgrid=False)
        st.plotly_chart(fig, use_container_width=True, config=SCROLLZOOM)

    # Temp rise rate vs power scatter
    with tc2:
        if "power_kw" in df.columns:
            dT_motor = num(df,"motor_temp").diff() * 50   # °C/s (50Hz)
            pwr = num(df,"power_kw")
            mask = dT_motor.dropna().index.intersection(pwr.dropna().index)
            sdf2 = ds(df.loc[mask], 3000)
            fig2 = go.Figure(go.Scatter(
                x=num(sdf2,"power_kw"), y=num(df.loc[sdf2.index],"motor_temp").diff()*50,
                mode="markers",
                marker=dict(size=3, color=num(sdf2,"speed_kmh"), colorscale="Viridis",
                    showscale=True, opacity=0.6,
                    colorbar=dict(title="Speed km/h",thickness=10,titlefont=dict(size=9))),
                hovertemplate="Power: %{x:.2f}kW<br>dT/dt: %{y:.3f}°C/s<extra></extra>"))
            fig2.update_layout(**plt({"height":320,"title":dict(text="Motor Temp Rise Rate vs Power  (colour=Speed)",font=dict(size=12,color="#8b949e")),
                "xaxis_title":"Power (kW)","yaxis_title":"Motor dT/dt (°C/s)"}))
            st.plotly_chart(fig2, use_container_width=True, config=SCROLLZOOM)
        else:
            tc2.info("Need volt_mcu + curr_mcu for power channel")

    # Heat soak detection
    shdr("HEAT SOAK DETECTION  (temp rising while speed drops)")
    if "motor_temp" in df.columns and "speed_kmh" in df.columns:
        window = 100  # 2s at 50Hz
        mt = num(df,"motor_temp")
        spd = num(df,"speed_kmh")
        rising = mt.diff(window) > 2    # +2°C in 2s
        slowing = spd.diff(window) < -5  # -5 km/h in 2s
        soak = rising & slowing
        if soak.sum() > 50:
            st.warning(f"⚠️ Heat soak detected: {soak.sum()/50:.1f}s where motor temp is rising while speed is dropping — check airflow / duty cycle")
            fig_hs = make_subplots(specs=[[{"secondary_y":True}]])
            xs_full = wc_arr(df["t"], start_hms)
            fig_hs.add_trace(go.Scatter(x=xs_full, y=mt, name="Motor°C",
                line=dict(color=C[0],width=1.4)), secondary_y=False)
            fig_hs.add_trace(go.Scatter(x=xs_full, y=spd, name="Speed km/h",
                line=dict(color=C[1],width=1.4)), secondary_y=True)
            soak_t = df.loc[soak,"t"]
            for t_val in soak_t.iloc[::50]:
                fig_hs.add_vline(x=to_wc(t_val,start_hms), line_color="#ff6b6b", opacity=0.15, line_width=8)
            fig_hs.update_layout(**plt({"height":260,"title":dict(text="Heat Soak Events (red bands)",font=dict(size=12,color="#8b949e")),
                "xaxis":dict(gridcolor="#21262d",tickformat="%H:%M:%S")}))
            fig_hs.update_yaxes(title_text="Motor°C", secondary_y=False, gridcolor="#21262d")
            fig_hs.update_yaxes(title_text="km/h", secondary_y=True, gridcolor="#21262d", showgrid=False)
            st.plotly_chart(fig_hs, use_container_width=True, config=SCROLLZOOM)
        else:
            st.success("✅ No heat soak pattern detected")


def tab_faults(df, start_hms, thr):
    shdr("FAULT DIAGNOSTICS")
    all_faults = fault_summary(df) + threshold_events(df, thr)

    if not all_faults:
        st.success("✅ No faults or threshold breaches detected in this session.")
        return

    crit_count = sum(1 for f in all_faults if f["Severity"]=="CRITICAL")
    warn_count = sum(1 for f in all_faults if f["Severity"]=="WARNING")
    k = st.columns(4)
    kpi(k[0],"Critical faults", str(crit_count), "#ff4444" if crit_count>0 else "#3fb950")
    kpi(k[1],"Warnings",        str(warn_count), "#ffa500" if warn_count>0 else "#3fb950")
    kpi(k[2],"Total events",    str(len(all_faults)))
    kpi(k[3],"Unique faults",   str(len(set(f["Fault"] for f in all_faults))))

    # Fault summary table
    shdr("FAULT SUMMARY TABLE")
    fdf = pd.DataFrame(all_faults)
    if not fdf.empty:
        st.dataframe(fdf, use_container_width=True, hide_index=True,
            column_config={"Samples":st.column_config.NumberColumn(format="%d")})

    # Fault timeline strip — one band per fault type over time
    shdr("FAULT TIMELINE")
    fault_names = [f["Fault"] for f in all_faults][:12]  # max 12 bands
    if fault_names:
        fig = go.Figure()
        for i, fname in enumerate(fault_names):
            active = np.zeros(len(df))
            for col_e, bm, lbl in [("mcu_errors",MCU_BIT_MAP,"MCU"),
                                    ("bms1_errors",BMS_BIT_MAP,"BMS1"),
                                    ("bms2_errors",BMS_BIT_MAP,"BMS2")]:
                if col_e not in df.columns: continue
                vals = df[col_e].astype(str)
                for j,code in enumerate(vals):
                    for f in decode_bits(code, bm, lbl):
                        if f["desc"]==fname: active[j]=1
            if active.sum() == 0: continue
            xs_f = wc_arr(df["t"], start_hms)
            fig.add_trace(go.Scatter(
                x=xs_f, y=[i+0.5 if a else np.nan for a in active],
                mode="markers", marker=dict(size=6, symbol="square",
                    color="#ff4444" if any(f.get("Severity")=="CRITICAL" for f in all_faults if f["Fault"]==fname) else "#ffa500"),
                name=fname, hovertemplate=f'{fname}<br>%{{x|%H:%M:%S}}<extra></extra>'))
        fig.update_layout(**plt({"height":max(200,40+len(fault_names)*28),
            "title":dict(text="Active Fault Bands Over Time",font=dict(size=12,color="#8b949e")),
            "xaxis":dict(gridcolor="#21262d",tickformat="%H:%M:%S"),
            "yaxis":dict(gridcolor="#21262d",tickvals=list(range(len(fault_names))),
                ticktext=fault_names[:len(fault_names)], tickfont=dict(size=9)),
            "showlegend":False}))
        st.plotly_chart(fig, use_container_width=True, config=SCROLLZOOM)

    # Context panel for selected fault
    shdr("FAULT CONTEXT PANEL  (30s window)")
    if all_faults:
        fault_opts = [f'{f["Severity"]} — {f["Fault"]} @ {f["First"]}' for f in all_faults]
        sel = st.selectbox("Select fault to inspect", fault_opts)
        sel_idx = fault_opts.index(sel)
        sel_fault = all_faults[sel_idx]
        # Parse T+ from "T+123.4s"
        try: t_center = float(sel_fault["First"].replace("T+","").replace("s",""))
        except: t_center = 0.0
        t_lo = max(0, t_center-15); t_hi = t_center+15
        ctx_df = ds(df[(df["t"]>=t_lo)&(df["t"]<=t_hi)], 1000)
        ctx_xs = wc_arr(ctx_df["t"], start_hms)

        fig_ctx = make_subplots(rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.05,
            subplot_titles=["Speed & Torque","Voltages","Currents","Temperatures"])
        fig_ctx.add_trace(go.Scatter(x=ctx_xs, y=num(ctx_df,"speed_kmh"), name="Speed km/h", line=dict(color=C[0])), row=1,col=1)
        fig_ctx.add_trace(go.Scatter(x=ctx_xs, y=num(ctx_df,"torque_nm"), name="Torque Nm", line=dict(color=C[3])), row=1,col=1)
        for ch,c in [("volt_mcu",C[0]),("volt_bms1",C[1]),("volt_bms2",C[2])]:
            fig_ctx.add_trace(go.Scatter(x=ctx_xs, y=num(ctx_df,ch), name=CHANNELS.get(ch,ch), line=dict(color=c,width=1.3)), row=2,col=1)
        for ch,c in [("curr_mcu",C[0]),("curr_bms1",C[1]),("curr_bms2",C[2])]:
            fig_ctx.add_trace(go.Scatter(x=ctx_xs, y=num(ctx_df,ch), name=CHANNELS.get(ch,ch), line=dict(color=c,width=1.3)), row=3,col=1)
        for ch,c in [("motor_temp",C[0]),("mcu_temp",C[1]),("board_temp_bms1",C[2]),("board_temp_bms2",C[3])]:
            fig_ctx.add_trace(go.Scatter(x=ctx_xs, y=num(ctx_df,ch), name=CHANNELS.get(ch,ch), line=dict(color=c,width=1.3)), row=4,col=1)
        # Mark fault center
        fault_wc = to_wc(t_center, start_hms)
        for r in range(1,5):
            fig_ctx.add_vline(x=fault_wc, line_color="#ff4444", line_dash="dash", opacity=0.7, row=r, col=1)
        fig_ctx.update_layout(**plt({"height":580,
            "title":dict(text=f'Context ±15s around: {sel_fault["Fault"]}',font=dict(size=12,color="#8b949e"))}))
        for r in range(1,5):
            fig_ctx.update_xaxes(gridcolor="#21262d", tickformat="%H:%M:%S", row=r, col=1)
            fig_ctx.update_yaxes(gridcolor="#21262d", row=r, col=1)
        st.plotly_chart(fig_ctx, use_container_width=True, config=SCROLLZOOM)


def tab_route(df, start_hms, sid):
    shdr("GPS ROUTE MAP")
    has_gps = "lat" in df.columns and num(df,"lat").notna().sum() > 10

    anns = load_anns(sid)

    if has_gps:
        gps = df[["t","lat","lon","speed_kmh"]].dropna(subset=["lat","lon"])
        fig_map = px.scatter_mapbox(gps, lat="lat", lon="lon", color="speed_kmh",
            color_continuous_scale="RdYlGn", range_color=[0, gps["speed_kmh"].quantile(0.95)],
            mapbox_style="carto-darkmatter",
            hover_data={"speed_kmh":":.1f","t":":.0f"},
            labels={"speed_kmh":"km/h","t":"T+s"}, zoom=13)

        # Add existing annotations as markers
        if not anns.empty and "lat" in anns.columns:
            ann_gps = anns.dropna(subset=["lat","lon"])
            if not ann_gps.empty:
                fig_map.add_trace(go.Scattermapbox(
                    lat=ann_gps["lat"], lon=ann_gps["lon"],
                    mode="markers+text",
                    marker=dict(size=12, color="#ff4444", symbol="circle"),
                    text=ann_gps["label"], textposition="top center",
                    name="Annotations"))

        fig_map.update_layout(paper_bgcolor="#161b22", font_color="#c9d1d9",
            height=480, margin=dict(t=0,b=0,l=0,r=0),
            legend=dict(bgcolor="rgba(22,27,34,0.8)"))

        # Click → annotation using plotly on_select
        st.info("🗺️ Click any point on the map to leave a comment at that location.")
        selected = st.plotly_chart(fig_map, use_container_width=True,
            on_select="rerun", key="map_chart", selection_mode=["points"])

        # Pre-fill form if map was clicked
        click_lat = click_lon = click_t = None
        if selected and hasattr(selected, "selection") and selected.selection.get("points"):
            pt = selected.selection["points"][0]
            click_lat = pt.get("lat")
            click_lon = pt.get("lon")
            click_t   = pt.get("customdata", [None])[0] if pt.get("customdata") else None
            st.success(f"📍 Selected: lat={click_lat:.5f}  lon={click_lon:.5f}  T+{click_t:.0f}s" if click_t else f"📍 lat={click_lat:.5f}  lon={click_lon:.5f}")

    else:
        st.markdown('<div class="gps-ph">📍 No GPS data in this log yet<br><span style="font-size:.7rem;color:#555">Add lat/lon columns to your CSV log to enable route map</span></div>', unsafe_allow_html=True)
        click_lat = click_lon = click_t = None

    # Annotation form
    shdr("ADD MAP ANNOTATION")
    with st.form("map_ann_form"):
        fc1,fc2,fc3 = st.columns(3)
        a_lat  = fc1.number_input("Lat",  value=float(click_lat)  if click_lat  else 0.0, format="%.6f", key="a_lat")
        a_lon  = fc2.number_input("Lon",  value=float(click_lon)  if click_lon  else 0.0, format="%.6f", key="a_lon")
        a_t    = fc3.number_input("T+ (s)", value=float(click_t) if click_t else 0.0, step=1.0, key="a_t")
        a_lbl  = st.text_input("Label", placeholder="e.g. Hard braking zone")
        a_sev  = st.selectbox("Severity", ["info","warning","critical"])
        a_aut  = st.text_input("Engineer")
        a_note = st.text_area("Note", height=65)
        if st.form_submit_button("📌 Save annotation", type="primary"):
            with get_engine().connect() as con:
                con.execute(text(
                    "INSERT INTO annotations(session_id,t,lat,lon,label,severity,author,note,created_at) "
                    "VALUES(:sid,:t,:lat,:lon,:label,:severity,:author,:note,:created_at)"),
                    dict(sid=sid,t=a_t,lat=a_lat if a_lat!=0 else None,
                         lon=a_lon if a_lon!=0 else None,
                         label=a_lbl,severity=a_sev,author=a_aut,note=a_note,
                         created_at=datetime.now().isoformat()))
                con.commit()
            bust(); st.rerun()

    # Annotation list
    shdr("ANNOTATIONS LOG")
    anns2 = load_anns(sid)
    if anns2.empty:
        st.markdown('<span style="color:#555;font-size:.82rem">No annotations yet.</span>', unsafe_allow_html=True)
    for _,ann in anns2.iterrows():
        bord="#ff4444" if ann["severity"]=="critical" else ("#ffa500" if ann["severity"]=="warning" else "#58a6ff")
        wc = to_wc(ann["t"],start_hms).strftime("%H:%M:%S")
        loc_str = f' · GPS: {ann["lat"]:.4f},{ann["lon"]:.4f}' if pd.notna(ann.get("lat")) and ann["lat"] else ""
        st.markdown(
            f'<div class="ann-box" style="border-left-color:{bord}">' +
            f'<b style="color:{bord}">{wc}</b>{loc_str} · <b>{ann["label"]}</b> · ' +
            f'<span style="color:#8b949e;font-size:.72rem">{ann.get("author","")} · {str(ann.get("created_at",""))[:10]}</span><br>' +
            f'<span style="color:#c9d1d9">{ann.get("note","")}</span></div>',
            unsafe_allow_html=True)
        if st.button("🗑️", key=f"del_ann_{ann['id']}"):
            with get_engine().connect() as con:
                con.execute(text("DELETE FROM annotations WHERE id=:id"),{"id":int(ann["id"])}); con.commit()
            bust(); st.rerun()


def tab_ai(df, start_hms, srow, thr):
    shdr("AI ENGINEERING ASSISTANT")
    st.caption("Full session context is injected automatically. Ask anything about faults, thermals, energy, derating, etc.")
    sid = int(srow["id"]) if "id" in srow else 0
    if "chat" not in st.session_state: st.session_state.chat = []
    if st.session_state.get("chat_sid") != sid:
        st.session_state.chat = []; st.session_state.chat_sid = sid
    ctx = build_ctx(df, srow, thr)
    for msg in st.session_state.chat:
        with st.chat_message(msg["role"]): st.markdown(msg["content"])
    if prompt := st.chat_input("e.g. Why did motor temp reach 146°C? Is BMS1/BMS2 imbalance within tolerance?"):
        st.session_state.chat.append({"role":"user","content":prompt})
        with st.chat_message("user"): st.markdown(prompt)
        with st.chat_message("assistant"):
            with st.spinner("Analysing…"): reply = ask_ai(ctx, prompt, st.session_state.chat[:-1])
            st.markdown(reply)
        st.session_state.chat.append({"role":"assistant","content":reply})
    if st.session_state.chat:
        if st.button("🗑️ Clear chat"): st.session_state.chat=[]; st.rerun()

# ═══════════════════════════════════════════════════════════════════════════════
# STATIC PAGES
# ═══════════════════════════════════════════════════════════════════════════════
def page_upload():
    shdr("NEW TEST SESSION")
    with st.form("upload_form"):
        c1,c2 = st.columns(2)
        name      = c1.text_input("Session name *", placeholder="Road Test 01 — EN125 Faro")
        date      = c1.date_input("Test date")
        rider     = c1.text_input("Rider")
        track     = c1.text_input("Road / Location", placeholder="EN125 Faro → Loulé")
        firmware  = c2.text_input("Firmware version", placeholder="v2.4.1")
        config    = c2.text_input("Setup / config changes", placeholder="Standard, tyre 2.5bar")
        ambient_t = c2.number_input("Ambient temp (°C)", value=20.0, step=0.5)
        weather   = c2.text_input("Weather", placeholder="Sunny 24°C, no wind")
        notes     = st.text_area("Engineering notes", placeholder="Test purpose, hypothesis, changes since last run…", height=80)
        f         = st.file_uploader("CSV log file *", type=["csv"])
        ok        = st.form_submit_button("💾 Save to database", type="primary", use_container_width=True)
    if ok:
        if not name: st.error("Session name required."); return
        if f is None: st.error("Select a CSV file."); return
        with st.spinner("Parsing…"):
            df, start_hms = parse_csv(f.read())
            df = derive(df)
            dur = float(df["t"].max())
            save_db(dict(name=name,date=str(date),rider=rider,track=track,weather=weather,
                notes=notes,firmware=firmware,config=config,ambient_temp=float(ambient_t),
                upload_time=datetime.now().isoformat(),row_count=len(df),
                duration_s=dur,start_hms=start_hms), df)
            bust()
        st.success(f"✅ '{name}' saved — {len(df):,} rows · {dur:.0f}s ({dur/60:.1f} min) · Start {start_hms}")

def page_sessions():
    shdr("SESSION LIBRARY")
    sess = load_sessions()
    if sess.empty: st.info("No sessions yet."); return
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Sessions", len(sess))
    c2.metric("Test time", f'{sess["duration_s"].sum()/3600:.1f} h')
    c3.metric("Data rows", f'{sess["row_count"].sum():,}')
    c4.metric("Riders", sess["rider"].nunique())
    st.divider()
    for _,r in sess.iterrows():
        dur = r["duration_s"]
        with st.expander(f'**{r["name"]}** · {r["date"]} · {r.get("rider","—") or "—"} · {r.get("track","—") or "—"}'):
            ca,cb,cc,cd,ce = st.columns(5)
            ca.markdown(f'**FW** {r.get("firmware","—") or "—"}')
            cb.markdown(f'**Config** {r.get("config","—") or "—"}')
            cc.markdown(f'**Duration** {dur:.0f}s / {dur/60:.1f}min')
            cd.markdown(f'**Ambient** {r.get("ambient_temp","—") or "—"}°C')
            ce.markdown(f'**Start** {r.get("start_hms","—") or "—"}')
            if r.get("weather"): st.markdown(f'**Weather:** {r["weather"]}')
            if r.get("notes"):   st.markdown(f'**Notes:** {r["notes"]}')
            if st.button("🗑️ Delete", key=f'del_{r["id"]}'):
                with get_engine().connect() as con:
                    for tbl in ["signals","annotations","sessions"]:
                        con.execute(text(f'DELETE FROM {tbl} WHERE {"session_id" if tbl!="sessions" else "id"}=:id'),{"id":r["id"]})
                    con.commit()
                bust(); st.rerun()

def page_analyse(thr):
    shdr("SESSION ANALYSIS")
    sess = load_sessions()
    if sess.empty: st.info("No sessions yet."); return
    opts = {f'{r["name"]}  [{r["date"]}]': (int(r["id"]), r) for _,r in sess.iterrows()}
    label = st.selectbox("Select session", list(opts.keys()))
    sid, srow = opts[label]
    df = derive(load_signals(sid))
    start_hms = str(srow.get("start_hms") or "00:00:00")

    tabs = st.tabs(["📊 Overview","⚙️ Powertrain","🔋 Battery","🌡️ Thermal","🔴 Faults","🗺️ Route","🤖 AI"])
    with tabs[0]: tab_overview(df, start_hms, thr)
    with tabs[1]: tab_powertrain(df, start_hms, thr)
    with tabs[2]: tab_battery(df, start_hms, thr)
    with tabs[3]: tab_thermal(df, start_hms, thr)
    with tabs[4]: tab_faults(df, start_hms, thr)
    with tabs[5]: tab_route(df, start_hms, sid)
    with tabs[6]: tab_ai(df, start_hms, srow.to_dict(), thr)

def page_compare(thr):
    shdr("SESSION COMPARISON")
    sess = load_sessions()
    if len(sess)<2: st.info("Upload at least 2 sessions."); return
    opts = {f'{r["name"]}  [{r["date"]}]': int(r["id"]) for _,r in sess.iterrows()}
    keys = list(opts.keys())
    c1,c2 = st.columns(2)
    la = c1.selectbox("Session A", keys, key="ca")
    lb = c2.selectbox("Session B", [k for k in keys if k!=la], key="cb")
    df_a = derive(load_signals(opts[la]))
    df_b = derive(load_signals(opts[lb]))
    avail = [k for k in list(CHANNELS.keys())+["power_kw"] if k in df_a.columns and k in df_b.columns]
    defs  = [c for c in ["motor_temp","soc_bms1","speed_kmh","curr_mcu","volt_mcu"] if c in avail]
    sel   = st.multiselect("Channels", avail, defs, format_func=lambda x:CHANNELS.get(x,x.replace("_"," ").title()))
    if not sel: return
    # Stats table
    shdr("STATISTICS TABLE")
    rows=[]
    for ch in sel:
        va,vb = num(df_a,ch).dropna(),num(df_b,ch).dropna()
        if va.empty or vb.empty: continue
        d = vb.mean()-va.mean()
        rows.append({"Channel":CHANNELS.get(ch,ch),"A Min":f'{va.min():.2f}',"A Mean":f'{va.mean():.2f}',"A Max":f'{va.max():.2f}',
                     "B Min":f'{vb.min():.2f}',"B Mean":f'{vb.mean():.2f}',"B Max":f'{vb.max():.2f}',
                     "Δ Mean":f'{"↑" if d>0 else "↓"} {abs(d):.2f}'})
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    # Overlay charts
    shdr("SIGNAL OVERLAY")
    for ch in sel:
        fig = go.Figure()
        sa,sb = ds(df_a),ds(df_b)
        fig.add_trace(go.Scatter(x=sa["t"],y=num(sa,ch),name=f'A: {la[:30]}',line=dict(color="#58a6ff",width=1.8)))
        fig.add_trace(go.Scatter(x=sb["t"],y=num(sb,ch),name=f'B: {lb[:30]}',line=dict(color="#f7931a",width=1.8)))
        if ch in thr:
            fig.add_hline(y=thr[ch]["warn"],line_dash="dot",line_color="orange",opacity=0.4)
            fig.add_hline(y=thr[ch]["crit"],line_dash="dash",line_color="#ff4444",opacity=0.4)
        fig.update_layout(**plt({"height":240,"title":dict(text=CHANNELS.get(ch,ch),font=dict(size=12,color="#8b949e")),
            "xaxis_title":"Time (s)","legend":dict(bgcolor="rgba(22,27,34,.8)",bordercolor="#21262d",borderwidth=1,orientation="h",y=-0.3)}))
        st.plotly_chart(fig, use_container_width=True, config=SCROLLZOOM)

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    with st.sidebar:
        st.markdown('<div class="logo-area">⚡ FAMEL</div><div class="logo-sub">Electric Moto Telemetry</div>', unsafe_allow_html=True)
        st.divider()
        sess = load_sessions()
        st.markdown(
            f'<div class="stat-badge">{len(sess)} sessions</div>' +
            f'<div class="stat-badge">{sess["duration_s"].sum()/3600:.1f} h logged</div>',
            unsafe_allow_html=True)
        st.divider()
        page = st.radio("Navigation",
            ["📤 Upload","📋 Sessions","📊 Analyse","🔄 Compare"],
            label_visibility="collapsed")
        st.divider()
        with st.expander("⚙️ Thresholds"):
            thr = {}
            for ch, cfg in DEFAULT_THR.items():
                lbl=CHANNELS.get(ch,ch); hi=cfg["dir"]=="high"
                thr[ch] = {
                    "warn": st.number_input(f'{lbl} {"↑W" if hi else "↓W"}',value=float(cfg["warn"]),key=f'w_{ch}',step=1.0),
                    "crit": st.number_input(f'{lbl} {"↑C" if hi else "↓C"}',value=float(cfg["crit"]),key=f'c_{ch}',step=1.0),
                    "dir":  cfg["dir"]
                }
    if   page=="📤 Upload":   page_upload()
    elif page=="📋 Sessions":  page_sessions()
    elif page=="📊 Analyse":   page_analyse(thr)
    elif page=="🔄 Compare":   page_compare(thr)

if __name__ == "__main__":
    main()
