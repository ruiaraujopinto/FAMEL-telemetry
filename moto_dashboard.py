"""
⚡ FAMEL Electric Motorcycle Telemetry Dashboard
Cloud Version — Supabase + Streamlit Cloud
"""
import streamlit as st
import io, requests
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta, time as dtime
from sqlalchemy import create_engine, text

st.set_page_config(page_title="FAMEL Telemetry", page_icon="⚡", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Exo+2:wght@300;400;600;700&display=swap');
  html, body, [class*="css"] { font-family: 'Exo 2', sans-serif; }
  .stApp { background-color: #0d1117; }
  section[data-testid="stSidebar"] { background: #0d1117; border-right: 1px solid #21262d; }
  [data-testid="metric-container"] { background: #161b22; border: 1px solid #21262d; border-radius: 8px; padding: 12px 16px; }
  [data-testid="stMetricValue"] { font-family: 'Share Tech Mono', monospace !important; font-size: 1.5rem !important; }
  .section-hdr { font-family:'Share Tech Mono',monospace; font-size:.65rem; letter-spacing:3px; text-transform:uppercase; color:#f7931a; border-bottom:1px solid #21262d; padding-bottom:5px; margin:18px 0 10px 0; }
  .stat-badge { display:inline-block; background:#161b22; border:1px solid #30363d; border-radius:5px; padding:3px 9px; font-family:'Share Tech Mono',monospace; font-size:.72rem; color:#8b949e; margin:2px; }
  .logo-area { font-family:'Share Tech Mono',monospace; font-size:1.05rem; color:#f7931a; letter-spacing:2px; }
  .logo-sub { font-size:.6rem; color:#8b949e; letter-spacing:3px; text-transform:uppercase; }
  .gps-ph { background:#161b22; border:1px dashed #30363d; border-radius:8px; padding:35px; text-align:center; color:#8b949e; font-family:'Share Tech Mono',monospace; font-size:.8rem; }
  .ann-box { background:#1c2128; border-left:3px solid #f7931a; border-radius:0 6px 6px 0; padding:9px 13px; margin:5px 0; font-size:.84rem; }
  hr { border-color:#21262d !important; }
</style>
""", unsafe_allow_html=True)

# ── Channel map ───────────────────────────────────────────────────────────────
CHANNELS = {
    "throttle":"Throttle (%)","speed_rpm":"Speed (RPM)","speed_kmh":"Speed (km/h)",
    "brake":"Brake","torque_nm":"Torque (Nm)","soc_bms1":"SOC BMS1 (%)","soc_bms2":"SOC BMS2 (%)",
    "volt_mcu":"Voltage MCU (V)","volt_bms1":"Voltage BMS1 (V)","volt_bms2":"Voltage BMS2 (V)",
    "curr_mcu":"Current MCU (A)","curr_bms1":"Current BMS1 (A)","curr_bms2":"Current BMS2 (A)",
    "motor_temp":"Motor Temp (°C)","mcu_temp":"MCU Temp (°C)",
    "board_temp_bms1":"Board Temp BMS1 (°C)","board_temp_bms2":"Board Temp BMS2 (°C)",
}
CHANNEL_GROUPS = {
    "🌡️ Temperatures":  ["motor_temp","mcu_temp","board_temp_bms1","board_temp_bms2"],
    "🔋 Battery":        ["soc_bms1","soc_bms2","volt_mcu","volt_bms1","volt_bms2"],
    "⚡ Current":        ["curr_mcu","curr_bms1","curr_bms2"],
    "🏍️ Performance":   ["speed_kmh","speed_rpm","throttle","torque_nm","brake"],
}
DEFAULT_THR = {
    "motor_temp":      {"warn":90,  "crit":120, "dir":"high"},
    "mcu_temp":        {"warn":70,  "crit":85,  "dir":"high"},
    "board_temp_bms1": {"warn":55,  "crit":70,  "dir":"high"},
    "board_temp_bms2": {"warn":55,  "crit":70,  "dir":"high"},
    "curr_mcu":        {"warn":200, "crit":300, "dir":"high"},
    "soc_bms1":        {"warn":15,  "crit":10,  "dir":"low"},
    "soc_bms2":        {"warn":15,  "crit":10,  "dir":"low"},
    "volt_bms1":       {"warn":44,  "crit":42,  "dir":"low"},
    "volt_bms2":       {"warn":44,  "crit":42,  "dir":"low"},
}

# ── Error code bit maps (parsed from DBC files) ───────────────────────────────
# MCU — each bit in the 64-bit error word maps to one fault
MCU_BIT_MAP = {
    0:  ("warn", "Throttle Signal Failure"),
    2:  ("warn", "Brake Switch Failure"),
    3:  ("crit", "Position Sensor Failure"),
    4:  ("warn", "Wrong Offset Angle"),
    5:  ("crit", "Motor Temp Sensor Failure"),
    6:  ("crit", "Motor Temperature High"),
    7:  ("crit", "MOSFET Temperature High"),
    8:  ("crit", "MOSFET Temp Sensor Failure"),
    9:  ("crit", "DC Bus Overcurrent"),
    10: ("crit", "Id Current Error"),
    11: ("crit", "Iq Current Error"),
    12: ("crit", "Phase Overcurrent (UVW)"),
    13: ("warn", "CAN Signal Failure"),
    14: ("crit", "KL15 Failure"),
    15: ("warn", "KL15 Short to Ground"),
    16: ("warn", "KL15 Short to Battery+"),
    17: ("crit", "Rotor Stall"),
    18: ("crit", "DC Current Sensor Failure"),
    19: ("crit", "Phase Current Sensor Failure (UV)"),
    20: ("crit", "MOSFET Short to Battery+"),
    21: ("warn", "I²T Thermal Overload"),
    22: ("crit", "Loss of BMS CAN Message"),
    23: ("crit", "Motor Overspeed"),
    24: ("warn", "BEMF Too High"),
    25: ("crit", "Short Circuit B+ to B−"),
    26: ("warn", "CAN Data Out of Range"),
    27: ("warn", "Derating Mode Active"),
    28: ("crit", "12V Converter Failure"),
    29: ("warn", "Limp Home Mode Active"),
    30: ("warn", "Loss of Cluster CAN Message"),
    31: ("warn", "FNR Gear Fault"),
    32: ("warn", "IO Signal Profile Failure"),
    33: ("warn", "IO Signals Failure — Charger"),
    34: ("crit", "Phase Short to Ground"),
    35: ("warn", "IO Signals Short to Ground"),
    36: ("warn", "IO Signals Short to Battery+"),
    37: ("crit", "Encoder Failure"),
    38: ("crit", "DC Bus Undervoltage"),
    39: ("crit", "DC Bus Overvoltage"),
    40: ("warn", "Throttle Short to Ground"),
    41: ("warn", "Throttle Short to Battery+"),
    42: ("warn", "Ideal Voltage Error"),
    43: ("warn", "Motor Direction Fault"),
    44: ("crit", "MOSFET Short to Ground"),
    45: ("warn", "Voltage Unstable"),
    46: ("warn", "Battery Sensor Short to Ground"),
    47: ("warn", "Battery Sensor Short to Battery+"),
    48: ("warn", "NVM Checksum Error"),
    49: ("warn", "Y-Phase Current Medium"),
    50: ("warn", "R-Phase Current Medium"),
    51: ("warn", "DC Current Sensor Short to Ground"),
    52: ("warn", "DC Current Sensor Short to Battery+"),
    53: ("crit", "Motor Temp Short to Ground"),
    54: ("crit", "Motor Temp Short to Battery+"),
    55: ("warn", "Motor Temp Sensor Short to Ground"),
    56: ("warn", "Motor Temp Sensor Short to Battery+"),
    57: ("crit", "Phase Short to Battery+"),
    58: ("crit", "Gate Driver Short to Battery+"),
    59: ("crit", "Gate Driver Short to Ground"),
}

# BMS — same 64-bit bitmask structure (applies to both BMS1 and BMS2)
BMS_BIT_MAP = {
    0:  ("warn", "Cell Voltage Too High — Level 1"),
    1:  ("crit", "Cell Voltage Too High — Level 2"),
    2:  ("warn", "Cell Voltage Too Low — Level 1"),
    3:  ("crit", "Cell Voltage Too Low — Level 2"),
    4:  ("warn", "Pack Voltage Too High — Level 1"),
    5:  ("crit", "Pack Voltage Too High — Level 2"),
    6:  ("warn", "Pack Voltage Too Low — Level 1"),
    7:  ("crit", "Pack Voltage Too Low — Level 2"),
    8:  ("warn", "Charging Temp Too High — Level 1"),
    9:  ("crit", "Charging Temp Too High — Level 2"),
    10: ("warn", "Charging Temp Too Low — Level 1"),
    11: ("crit", "Charging Temp Too Low — Level 2"),
    12: ("warn", "Discharging Temp Too High — Level 1"),
    13: ("crit", "Discharging Temp Too High — Level 2"),
    14: ("warn", "Discharging Temp Too Low — Level 1"),
    15: ("crit", "Discharging Temp Too Low — Level 2"),
    20: ("warn", "SOC Too High — Level 1"),
    21: ("crit", "SOC Too High — Level 2"),
    22: ("warn", "SOC Too Low — Level 1"),
    23: ("crit", "SOC Too Low — Level 2"),
    24: ("warn", "Cell Voltage Spread Too Large — Level 1"),
    25: ("crit", "Cell Voltage Spread Too Large — Level 2"),
    26: ("warn", "Temperature Spread Too Large — Level 1"),
    27: ("crit", "Temperature Spread Too Large — Level 2"),
    28: ("warn", "MOS Temperature Too High — Level 1"),
    29: ("crit", "MOS Temperature Too High — Level 2"),
    30: ("warn", "Ambient Temp Too High — Level 1"),
    31: ("crit", "Ambient Temp Too High — Level 2"),
    32: ("crit", "Charging MOS Overtemperature"),
    33: ("crit", "Discharging MOS Overtemperature"),
    34: ("crit", "Charging MOS Temp Sensor Failure"),
    35: ("crit", "Discharging MOS Temp Sensor Failure"),
    36: ("crit", "Charging MOS Adhesion Failure"),
    37: ("crit", "Discharging MOS Adhesion Failure"),
    38: ("crit", "Charging MOS Open Circuit"),
    39: ("crit", "Discharging MOS Open Circuit"),
    40: ("crit", "AFE Acquisition Chip Failure"),
    41: ("crit", "Cell Acquisition Disconnected"),
    42: ("crit", "Temperature Sensor Failure"),
    43: ("crit", "EEPROM Storage Failure"),
    44: ("warn", "RTC Clock Failure"),
    45: ("crit", "Precharge Failed"),
    46: ("crit", "Vehicle CAN Communication Failure"),
    47: ("crit", "Internal Network Communication Failure"),
    48: ("crit", "Current Module Failure"),
    49: ("crit", "Pack Voltage Detection Module Failure"),
    50: ("crit", "Short Circuit Protection Failure"),
    51: ("warn", "Low Voltage Charging Prohibited"),
    52: ("warn", "GPS/Soft Switch — MOS Disconnected"),
    53: ("warn", "Charger Out of Cabinet"),
    54: ("crit", "Thermal Runaway"),
    55: ("crit", "Heating Failure"),
    56: ("crit", "Balance Module Communication Failure"),
    57: ("warn", "Equalisation Conditions Not Met"),
}

def decode_error_code(hex_str, bit_map, source):
    """
    Decode a hex error code (bitmask) into a list of active fault dicts.
    Returns list of {source, bit, sev, desc}.
    """
    s = str(hex_str).strip().lower()
    if s in ("", "nan", "none", "0", "0.0", "0x0", "0x0000000000000000"):
        return []
    try:
        val = int(s, 16) if s.startswith("0x") else int(float(s))
    except (ValueError, OverflowError):
        return []
    if val == 0:
        return []
    faults = []
    for bit, (sev, desc) in bit_map.items():
        if val & (1 << bit):
            faults.append({"source": source, "bit": bit, "sev": sev, "desc": desc, "raw": hex_str})
    return faults

COLORS = ["#f7931a","#58a6ff","#3fb950","#ff6b6b","#a78bfa","#34d399","#fbbf24","#60a5fa","#f472b6","#4ade80"]
PLOT_BASE = dict(
    paper_bgcolor="#161b22", plot_bgcolor="#0d1117",
    font=dict(family="Exo 2,sans-serif", color="#c9d1d9", size=11),
    xaxis=dict(gridcolor="#21262d", zerolinecolor="#21262d"),
    yaxis=dict(gridcolor="#21262d", zerolinecolor="#21262d"),
    legend=dict(bgcolor="#161b22", bordercolor="#21262d", borderwidth=1),
    margin=dict(t=35,b=35,l=50,r=20),
)

# ── Database ──────────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    return create_engine(st.secrets["DATABASE_URL"], pool_pre_ping=True)

def init_db():
    with get_engine().connect() as con:
        con.execute(text("""CREATE TABLE IF NOT EXISTS sessions (
            id SERIAL PRIMARY KEY, name TEXT NOT NULL, date TEXT, rider TEXT, track TEXT,
            weather TEXT, notes TEXT, firmware TEXT, config TEXT, ambient_temp REAL,
            upload_time TEXT, row_count INTEGER, duration_s REAL,
            start_hms TEXT)"""))
        # Add start_hms column to existing tables if not present
        try:
            con.execute(text("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS start_hms TEXT"))
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
            label TEXT, severity TEXT, author TEXT, note TEXT, created_at TEXT)"""))
        con.commit()

try:
    init_db()
except Exception as e:
    st.error(f"DB error: {e}"); st.stop()

@st.cache_data(ttl=30)
def load_sessions():
    return pd.read_sql("SELECT * FROM sessions ORDER BY date DESC, id DESC", get_engine())

@st.cache_data(ttl=60)
def load_signals(sid):
    return pd.read_sql(f"SELECT * FROM signals WHERE session_id={sid} ORDER BY t", get_engine())

@st.cache_data(ttl=30)
def load_annotations(sid):
    try: return pd.read_sql(f"SELECT * FROM annotations WHERE session_id={sid} ORDER BY t", get_engine())
    except: return pd.DataFrame()

def bust_cache():
    load_sessions.clear(); load_signals.clear(); load_annotations.clear()

def num(df, col):
    if col not in df.columns: return pd.Series(dtype=float)
    return pd.to_numeric(df[col], errors="coerce")

def downsample(df, n=5000):
    return df.iloc[::max(1,len(df)//n)] if len(df)>n else df

# ── CSV Parsing ───────────────────────────────────────────────────────────────
def parse_time_col(series):
    for fmt in ["%H:%M:%S.%f","%H:%M:%S"]:
        try:
            dt=pd.to_datetime(series,format=fmt)
            s=(dt-dt.iloc[0]).dt.total_seconds().values
            return np.where(s<0,s+86400,s), str(dt.iloc[0].time())
        except: pass
    try:
        dt=pd.to_datetime(series,infer_datetime_format=True)
        return (dt-dt.iloc[0]).dt.total_seconds().values, str(dt.iloc[0].time())
    except: pass
    nums=pd.to_numeric(series,errors="coerce")
    if nums.notna().sum()>=len(series)*0.5:
        rng=nums.max()-nums.min()
        if rng>1e9: return (nums-nums.iloc[0]).values/1e6, "00:00:00"
        elif rng>1e6: return (nums-nums.iloc[0]).values/1e3, "00:00:00"
        return (nums-nums.iloc[0]).values, "00:00:00"
    return np.arange(len(series))/50.0, "00:00:00"

def parse_csv(raw_bytes):
    df=None
    for enc in ["utf-8","latin-1","cp1252"]:
        try: df=pd.read_csv(io.BytesIO(raw_bytes),encoding=enc); break
        except: continue
    if df is None: raise ValueError("Cannot read CSV.")
    rename={}
    for col in df.columns:
        c=col.lower()
        if "timestamp" in c:                              rename[col]="timestamp_raw"
        elif "driving" in c:                              rename[col]="driving_profile"
        elif "throttle" in c:                             rename[col]="throttle"
        elif "rpm" in c:                                  rename[col]="speed_rpm"
        elif "km" in c and ("speed" in c or "/h" in c):  rename[col]="speed_kmh"
        elif "brake" in c:                                rename[col]="brake"
        elif "torque" in c:                               rename[col]="torque_nm"
        elif "soc" in c and "bms1" in c:                  rename[col]="soc_bms1"
        elif "soc" in c and "bms2" in c:                  rename[col]="soc_bms2"
        elif "volt" in c and "mcu" in c:                  rename[col]="volt_mcu"
        elif "volt" in c and "bms1" in c:                 rename[col]="volt_bms1"
        elif "volt" in c and "bms2" in c:                 rename[col]="volt_bms2"
        elif "curr" in c and "mcu" in c:                  rename[col]="curr_mcu"
        elif "curr" in c and "bms1" in c:                 rename[col]="curr_bms1"
        elif "curr" in c and "bms2" in c:                 rename[col]="curr_bms2"
        elif "motor" in c and "temp" in c:                rename[col]="motor_temp"
        elif "mcu" in c and "temp" in c:                  rename[col]="mcu_temp"
        elif "board" in c and "bms1" in c:                rename[col]="board_temp_bms1"
        elif "board" in c and "bms2" in c:                rename[col]="board_temp_bms2"
        elif "mcu" in c and ("error" in c or "err" in c): rename[col]="mcu_errors"
        elif "bms1" in c and ("error" in c or "hw" in c): rename[col]="bms1_errors"
        elif "bms2" in c and ("error" in c or "hw" in c): rename[col]="bms2_errors"
        elif col.lower()=="lat" or "latitude" in c:       rename[col]="lat"
        elif col.lower()=="lon" or "longitude" in c:      rename[col]="lon"
    df=df.rename(columns=rename)
    start_hms = "00:00:00"
    if "timestamp_raw" in df.columns:
        t_vals, start_hms = parse_time_col(df["timestamp_raw"])
        df["t"] = t_vals
    else:
        df["t"] = np.arange(len(df))/50.0
    if "brake" in df.columns:
        df["brake"]=df["brake"].map({"true":1.0,"false":0.0,True:1.0,False:0.0}).fillna(
            pd.to_numeric(df["brake"],errors="coerce"))
    return df, start_hms

def save_to_db(meta, df):
    sig_cols=["t","throttle","speed_rpm","speed_kmh","brake","torque_nm",
              "soc_bms1","soc_bms2","volt_mcu","volt_bms1","volt_bms2",
              "curr_mcu","curr_bms1","curr_bms2","motor_temp","mcu_temp",
              "board_temp_bms1","board_temp_bms2","mcu_errors","bms1_errors","bms2_errors","lat","lon"]
    for c in sig_cols:
        if c not in df.columns: df[c]=np.nan
    engine=get_engine()
    with engine.connect() as con:
        r=con.execute(text(
            "INSERT INTO sessions (name,date,rider,track,weather,notes,firmware,config,"
            "ambient_temp,upload_time,row_count,duration_s,start_hms) "
            "VALUES (:name,:date,:rider,:track,:weather,:notes,:firmware,:config,"
            ":ambient_temp,:upload_time,:row_count,:duration_s,:start_hms) RETURNING id"),meta)
        sid=r.fetchone()[0]; con.commit()
    sig=df[sig_cols].copy(); sig.insert(0,"session_id",sid)
    sig.to_sql("signals",engine,if_exists="append",index=False,method="multi",chunksize=2000)
    return sid

def compute_derived(df):
    out=df.copy()
    v,i=num(df,"volt_mcu"),num(df,"curr_mcu")
    if v.notna().any() and i.notna().any():
        out["power_kw"]=(v*i/1000).round(3)
        dt=df["t"].diff().fillna(0.02)
        out["energy_kwh"]=(out["power_kw"]*dt/3600).cumsum().round(4)
    return out

# ── Time helpers ──────────────────────────────────────────────────────────────
def hms_to_seconds(hms_str):
    """Convert HH:MM:SS[.ffffff] to seconds since midnight."""
    try:
        parts = str(hms_str).split(":")
        h,m,s = int(parts[0]), int(parts[1]), float(parts[2])
        return h*3600 + m*60 + s
    except: return 0.0

def seconds_to_wallclock(elapsed_s, start_hms):
    """Given elapsed seconds and session start HH:MM:SS, return wall-clock datetime."""
    origin = datetime(2000,1,1) + timedelta(seconds=hms_to_seconds(start_hms))
    return origin + timedelta(seconds=float(elapsed_s))

def make_time_slider(df, start_hms, key):
    """
    Render a time-range slider showing real wall-clock times (HH:MM:SS).
    Returns (t_min, t_max) in elapsed seconds.
    """
    t_max = float(df["t"].max())
    origin = datetime(2000,1,1) + timedelta(seconds=hms_to_seconds(start_hms))
    dt_start = origin
    dt_end   = origin + timedelta(seconds=t_max)
    # Minimum step = 1 second
    selected = st.slider(
        "Time window",
        min_value=dt_start,
        max_value=dt_end,
        value=(dt_start, dt_end),
        step=timedelta(seconds=1),
        format="HH:mm:ss",
        key=key,
    )
    t_min_sel = (selected[0] - origin).total_seconds()
    t_max_sel = (selected[1] - origin).total_seconds()
    return t_min_sel, t_max_sel

# ── Anomaly Detection ─────────────────────────────────────────────────────────
def detect(df, thr):
    issues=[]

    # Bitmask error codes
    for col_e, bit_map, lbl in [
        ("mcu_errors",  MCU_BIT_MAP, "MCU"),
        ("bms1_errors", BMS_BIT_MAP, "BMS1"),
        ("bms2_errors", BMS_BIT_MAP, "BMS2"),
    ]:
        if col_e not in df.columns: continue
        vals = df[col_e].astype(str).str.strip()
        # Collect unique non-zero codes
        seen_bits = {}  # bit -> (first_t, count)
        for idx, (row_t, code) in enumerate(zip(df["t"], vals)):
            faults = decode_error_code(code, bit_map, lbl)
            for f in faults:
                key = (lbl, f["bit"])
                if key not in seen_bits:
                    seen_bits[key] = {"t": row_t, "count": 1, "sev": f["sev"], "desc": f["desc"], "raw": code}
                else:
                    seen_bits[key]["count"] += 1
        for (src, bit), info in seen_bits.items():
            if info["count"] < 2: continue
            issues.append({
                "sev":  "🔴 Critical" if info["sev"]=="crit" else "🟡 Warning",
                "type": f"{src} — {info['desc']}",
                "code": f"Bit {bit} | {info['raw']}",
                "t":    float(info["t"]),
                "dur":  float(info["count"]/50),
                "ch":   col_e,
                "count":int(info["count"]),
            })

    # Threshold breaches
    for ch,cfg in thr.items():
        if ch not in df.columns: continue
        s=num(df,ch)
        if s.isna().all(): continue
        hi=cfg["dir"]=="high"
        for lvl,sev in [("crit","🔴 Critical"),("warn","🟡 Warning")]:
            v=cfg[lvl]
            mask=(s>=v) if hi else (s<=v)
            if lvl=="warn":
                oth=cfg["crit"]
                mask=((s>=v)&(s<oth)) if hi else ((s<=v)&(s>oth))
            if mask.sum()<10: continue
            peak=s[mask].max() if hi else s[mask].min()
            issues.append({"sev":sev,"type":CHANNELS.get(ch,ch),
                "code":f"{'Peak' if hi else 'Min'}: {peak:.1f} (limit {v})",
                "t":float(df.loc[mask,"t"].iloc[0]),"dur":float(mask.sum()/50),
                "ch":ch,"count":int(mask.sum())})

    # BMS imbalance
    for a,b,lim,unit,label in [
        ("volt_bms1","volt_bms2",3,"V","Voltage Imbalance BMS1/BMS2"),
        ("soc_bms1","soc_bms2",5,"%","SOC Imbalance BMS1/BMS2"),
        ("curr_bms1","curr_bms2",20,"A","Current Imbalance BMS1/BMS2"),
    ]:
        if a not in df.columns or b not in df.columns: continue
        diff=(num(df,a)-num(df,b)).abs(); bad=diff>lim
        if bad.sum()>50:
            issues.append({"sev":"🟡 Warning","type":label,
                "code":f"Max diff: {diff.max():.2f}{unit}",
                "t":float(df.loc[bad,"t"].iloc[0]),"dur":float(bad.sum()/50),"ch":a,"count":int(bad.sum())})

    issues.sort(key=lambda x:(0 if "🔴" in x["sev"] else (1 if "🟡" in x["sev"] else 2),x["t"]))
    return issues

# ── Gauge ─────────────────────────────────────────────────────────────────────
def make_gauge(value, title, mn, mx, warn, crit, unit="", direction="high"):
    if pd.isna(value): color="#555"
    elif direction=="high": color="#ff4444" if value>=crit else ("#ffa500" if value>=warn else "#3fb950")
    else: color="#ff4444" if value<=crit else ("#ffa500" if value<=warn else "#3fb950")
    if direction=="high":
        steps=[{"range":[mn,warn],"color":"#1a3a1a"},{"range":[warn,crit],"color":"#3d2800"},{"range":[crit,mx],"color":"#4a1515"}]
    else:
        steps=[{"range":[mn,crit],"color":"#4a1515"},{"range":[crit,warn],"color":"#3d2800"},{"range":[warn,mx],"color":"#1a3a1a"}]
    fig=go.Figure(go.Indicator(
        mode="gauge+number",
        value=value if not pd.isna(value) else mn,
        number={"suffix":unit,"font":{"family":"Share Tech Mono","color":color,"size":18}},
        title={"text":title,"font":{"family":"Exo 2","color":"#8b949e","size":10}},
        gauge={"axis":{"range":[mn,mx],"tickcolor":"#444","tickfont":{"size":8,"color":"#555"}},
               "bar":{"color":color,"thickness":0.22},"bgcolor":"#0d1117","bordercolor":"#21262d",
               "steps":steps,"threshold":{"line":{"color":color,"width":2},"thickness":0.75,
               "value":value if not pd.isna(value) else mn}}
    ))
    fig.update_layout(paper_bgcolor="#161b22",font_color="#c9d1d9",height=155,margin=dict(t=28,b=8,l=18,r=18))
    return fig

# ── AI ────────────────────────────────────────────────────────────────────────
def build_context(df, issues, srow):
    lines=["FAMEL ELECTRIC MOTORCYCLE — TELEMETRY SESSION",
           f"Session: {srow.get('name','?')} | Date: {srow.get('date','?')} | Rider: {srow.get('rider','?')}",
           f"Road: {srow.get('track','?')} | FW: {srow.get('firmware','?')} | Config: {srow.get('config','?')}",
           f"Ambient: {srow.get('ambient_temp','?')}°C | Duration: {df['t'].max():.0f}s | Samples: {len(df):,}",
           f"Start time: {srow.get('start_hms','?')}"]
    if srow.get("notes"): lines.append(f"Notes: {srow['notes']}")
    lines.append("\nSTATISTICS (min/mean/max)")
    for ch,lbl in CHANNELS.items():
        s=num(df,ch).dropna()
        if not s.empty: lines.append(f"  {lbl}: {s.min():.2f} / {s.mean():.2f} / {s.max():.2f}")
    df2=compute_derived(df)
    if "power_kw" in df2.columns:
        p=df2["power_kw"].dropna()
        lines.append(f"  Power kW: {p.min():.2f}/{p.mean():.2f}/{p.max():.2f}")
    if "energy_kwh" in df2.columns:
        lines.append(f"  Energy consumed: {df2['energy_kwh'].max():.3f} kWh")
    lines.append(f"\nANOMALIES: {len(issues)}")
    for iss in issues:
        lines.append(f"  [{iss['sev']}] {iss['type']} | {iss['code']} | T+{iss['t']:.1f}s | {iss['dur']:.1f}s | {iss['count']} samples")
    return "\n".join(lines)

def ask_claude(context, question, history):
    api_key=st.secrets.get("ANTHROPIC_API_KEY","")
    if not api_key: return "⚠️ Add ANTHROPIC_API_KEY to Streamlit secrets."
    system=(
        "You are a senior electric motorcycle engineer for FAMEL supporting homologation validation. "
        "Analyse telemetry precisely, reference actual values and timestamps, flag safety concerns clearly, "
        "suggest root causes and corrective actions.\n\nSESSION DATA:\n"+context)
    msgs=[{"role":m["role"],"content":m["content"]} for m in history]
    msgs.append({"role":"user","content":question})
    resp=requests.post("https://api.anthropic.com/v1/messages",
        headers={"x-api-key":api_key,"anthropic-version":"2023-06-01","content-type":"application/json"},
        json={"model":"claude-sonnet-4-20250514","max_tokens":1200,"system":system,"messages":msgs},timeout=30)
    data=resp.json()
    return data["content"][0]["text"] if "content" in data else f"API error: {data.get('error',{}).get('message',str(data))}"

# ═══════════════════════════════════════════════════════════════════════════════
# PAGES
# ═══════════════════════════════════════════════════════════════════════════════
def page_upload():
    st.markdown('<div class="section-hdr">NEW TEST SESSION</div>', unsafe_allow_html=True)
    with st.form("upload_form"):
        c1,c2=st.columns(2)
        name     =c1.text_input("Session name *",placeholder="Road Test 01 — EN125 Faro")
        date     =c1.date_input("Test date")
        rider    =c1.text_input("Rider")
        track    =c1.text_input("Road / Location",placeholder="EN125 Faro → Loulé")
        firmware =c2.text_input("Firmware version",placeholder="v2.4.1")
        config   =c2.text_input("Motorcycle config / setup changes",placeholder="Standard, tyre 2.5bar")
        ambient_t=c2.number_input("Ambient temp (°C)",value=20.0,step=0.5)
        weather  =c2.text_input("Weather",placeholder="Sunny, 24°C, no wind")
        notes    =st.text_area("Engineering notes",placeholder="Test purpose, hypothesis, changes since last run…",height=80)
        f        =st.file_uploader("CSV log file *",type=["csv"])
        ok       =st.form_submit_button("💾 Save to database",type="primary",use_container_width=True)
    if ok:
        if not name: st.error("Session name required."); return
        if f is None: st.error("Select a CSV file."); return
        with st.spinner("Parsing and uploading…"):
            df, start_hms = parse_csv(f.read())
            dur=float(df["t"].max())
            save_to_db(dict(name=name,date=str(date),rider=rider,track=track,weather=weather,
                notes=notes,firmware=firmware,config=config,ambient_temp=float(ambient_t),
                upload_time=datetime.now().isoformat(),row_count=len(df),duration_s=dur,
                start_hms=start_hms),df)
            bust_cache()
        st.success(f"✅ '{name}' saved — {len(df):,} rows · {dur:.0f}s ({dur/60:.1f} min) · Start: {start_hms}")

def page_sessions():
    st.markdown('<div class="section-hdr">SESSION LIBRARY</div>', unsafe_allow_html=True)
    sess=load_sessions()
    if sess.empty: st.info("No sessions yet — upload your first CSV."); return
    c1,c2,c3,c4=st.columns(4)
    c1.metric("Sessions",len(sess)); c2.metric("Test time",f"{sess['duration_s'].sum()/3600:.1f} h")
    c3.metric("Data rows",f"{sess['row_count'].sum():,}"); c4.metric("Riders",sess['rider'].nunique())
    st.divider()
    for _,r in sess.iterrows():
        dur=r['duration_s']
        with st.expander(f"**{r['name']}** · {r['date']} · {r.get('rider','—') or '—'} · {r.get('track','—') or '—'}"):
            ca,cb,cc,cd,ce=st.columns(5)
            ca.markdown(f"**Firmware**<br>{r.get('firmware','—') or '—'}",unsafe_allow_html=True)
            cb.markdown(f"**Config**<br>{r.get('config','—') or '—'}",unsafe_allow_html=True)
            cc.markdown(f"**Duration**<br>{dur:.0f}s / {dur/60:.1f}min",unsafe_allow_html=True)
            cd.markdown(f"**Ambient**<br>{r.get('ambient_temp','—') or '—'}°C",unsafe_allow_html=True)
            ce.markdown(f"**Start time**<br>{r.get('start_hms','—') or '—'}",unsafe_allow_html=True)
            if r.get("weather"): st.markdown(f"**Weather:** {r['weather']}")
            if r.get("notes"):   st.markdown(f"**Notes:** {r['notes']}")
            if st.button("🗑️ Delete",key=f"del_{r['id']}"):
                with get_engine().connect() as con:
                    con.execute(text("DELETE FROM signals WHERE session_id=:id"),{"id":r["id"]})
                    con.execute(text("DELETE FROM annotations WHERE session_id=:id"),{"id":r["id"]})
                    con.execute(text("DELETE FROM sessions WHERE id=:id"),{"id":r["id"]}); con.commit()
                bust_cache(); st.rerun()

def page_analyse(thr):
    st.markdown('<div class="section-hdr">SESSION ANALYSIS</div>', unsafe_allow_html=True)
    sess=load_sessions()
    if sess.empty: st.info("No sessions yet."); return
    opts={f"{r['name']}  [{r['date']}]":(int(r["id"]),r) for _,r in sess.iterrows()}
    label=st.selectbox("Select session",list(opts.keys()))
    sid,srow=opts[label]
    df=compute_derived(load_signals(sid))
    anns=load_annotations(sid)
    issues=detect(df,thr)
    start_hms = str(srow.get("start_hms") or "00:00:00")

    def vx(c,fn): v=num(df,c).dropna(); return fn(v) if not v.empty else float("nan")
    def vmax(c): return vx(c,max)
    def vmin(c): return vx(c,min)

    # ── Stat cards ───────────────────────────────────────────────────────────
    st.markdown('<div class="section-hdr">KEY METRICS</div>', unsafe_allow_html=True)
    metrics=[
        ("Max Speed",       f"{vmax('speed_kmh'):.1f} km/h", "speed_kmh","max"),
        ("Max Motor Temp",  f"{vmax('motor_temp'):.1f} °C",  "motor_temp","max"),
        ("Max MCU Temp",    f"{vmax('mcu_temp'):.1f} °C",    "mcu_temp","max"),
        ("Min SOC BMS1",    f"{vmin('soc_bms1'):.1f} %",     "soc_bms1","min"),
        ("Max Current",     f"{vmax('curr_mcu'):.1f} A",     "curr_mcu","max"),
        ("Max Power",       (f"{vmax('power_kw'):.2f} kW" if "power_kw" in df else "— kW"), None, None),
        ("Energy Used",     (f"{vmax('energy_kwh'):.3f} kWh" if "energy_kwh" in df else "— kWh"), None, None),
        ("Duration",        f"{df['t'].max()/60:.1f} min", None, None),
    ]
    cols=st.columns(len(metrics))
    for col,(lbl,val,ch,_) in zip(cols,metrics):
        try: fval=float(val.split()[0])
        except: fval=float("nan")
        color="#c9d1d9"
        if ch and ch in thr and not pd.isna(fval):
            cfg=thr[ch]; hi=cfg["dir"]=="high"
            if (hi and fval>=cfg["crit"]) or (not hi and fval<=cfg["crit"]): color="#ff4444"
            elif (hi and fval>=cfg["warn"]) or (not hi and fval<=cfg["warn"]): color="#ffa500"
            else: color="#3fb950"
        col.markdown(
            f'<div style="background:#161b22;border:1px solid #21262d;border-radius:8px;'
            f'padding:11px 9px;text-align:center">'
            f'<div style="font-size:.6rem;color:#8b949e;letter-spacing:1px;text-transform:uppercase">{lbl}</div>'
            f'<div style="font-family:\'Share Tech Mono\',monospace;font-size:1.2rem;color:{color};margin-top:3px">{val}</div>'
            f'</div>',unsafe_allow_html=True)

    # ── Gauges + Fault table ─────────────────────────────────────────────────
    st.markdown('<div class="section-hdr">GAUGES &amp; FAULT CODES</div>', unsafe_allow_html=True)
    gc=st.columns([1,1,1,1,2])
    gauges_cfg=[("motor_temp","Motor Temp",0,160,"°C","high"),
                ("mcu_temp","MCU Temp",0,110,"°C","high"),
                ("soc_bms1","SOC BMS1",0,100,"%","low"),
                ("board_temp_bms1","BMS1 Board",0,90,"°C","high")]
    for col,(ch,title,mn,mx,unit,direction) in zip(gc[:4],gauges_cfg):
        val=vmax(ch) if direction=="high" else vmin(ch)
        w=thr.get(ch,DEFAULT_THR.get(ch,{"warn":80}))["warn"]
        c_=thr.get(ch,DEFAULT_THR.get(ch,{"crit":100}))["crit"]
        col.plotly_chart(make_gauge(val,title,mn,mx,w,c_,unit,direction),
                         use_container_width=True,config={"displayModeBar":False})

    with gc[4]:
        st.markdown("**🔴 Fault / Error Log**")
        fault_rows=[]
        for col_e, bit_map, lbl in [
            ("mcu_errors",  MCU_BIT_MAP, "MCU"),
            ("bms1_errors", BMS_BIT_MAP, "BMS1"),
            ("bms2_errors", BMS_BIT_MAP, "BMS2"),
        ]:
            if col_e not in df.columns: continue
            vals=df[col_e].astype(str).str.strip()
            # Aggregate by bit
            bit_counts = {}
            for code in vals:
                for f in decode_error_code(code, bit_map, lbl):
                    key=(lbl,f["bit"],f["desc"])
                    bit_counts[key] = bit_counts.get(key,0)+1
            for (src,bit,desc),cnt in sorted(bit_counts.items(),key=lambda x:-x[1]):
                sev_txt="CRITICAL" if bit_map.get(bit,("warn",""))[0]=="crit" else "WARNING"
                fault_rows.append({"Source":src,"Bit":bit,"Description":desc,
                    "Severity":sev_txt,"Samples":cnt,"Duration(s)":f"{cnt/50:.1f}"})
        if fault_rows:
            st.dataframe(pd.DataFrame(fault_rows).sort_values("Samples",ascending=False),
                         use_container_width=True,hide_index=True)
        else:
            st.markdown('<span style="color:#3fb950;font-family:\'Share Tech Mono\',monospace;'
                        'font-size:.82rem">✅ No fault codes detected</span>',unsafe_allow_html=True)

    # ── Signal Explorer ──────────────────────────────────────────────────────
    st.markdown('<div class="section-hdr">SIGNAL EXPLORER</div>', unsafe_allow_html=True)
    avail=[k for k in list(CHANNELS.keys())+["power_kw","energy_kwh"]
           if k in df.columns and num(df,k).notna().sum()>10]
    tc1,tc2=st.columns([3,1])
    with tc2:
        st.markdown("**Quick select**")
        sel_ch=[]
        for grp,chs in CHANNEL_GROUPS.items():
            if st.checkbox(grp,key=f"grp_{grp}"): sel_ch+=[c for c in chs if c in avail]
        if st.checkbox("⚡ Power/Energy"): sel_ch+=[c for c in ["power_kw","energy_kwh"] if c in avail]
    with tc1:
        all_sel=st.multiselect(
            "Channels",avail,
            sel_ch or [c for c in ["motor_temp","soc_bms1","speed_kmh","curr_mcu"] if c in avail],
            format_func=lambda x:CHANNELS.get(x,x.replace("_"," ").title()))
        if all_sel:
            t_min_sel, t_max_sel = make_time_slider(df, start_hms, key="ts_slider")
            sub=df[(df["t"]>=t_min_sel)&(df["t"]<=t_max_sel)]
            sub_ds=downsample(sub)
            fig=go.Figure()
            for i,ch in enumerate(all_sel):
                s=num(sub_ds,ch)
                if s.isna().all(): continue
                # Convert x-axis to wall-clock datetimes
                x_times=[seconds_to_wallclock(t, start_hms) for t in sub_ds["t"]]
                fig.add_trace(go.Scatter(
                    x=x_times, y=s, name=CHANNELS.get(ch,ch),
                    line=dict(color=COLORS[i%len(COLORS)],width=1.5),
                    hovertemplate=f"<b>{CHANNELS.get(ch,ch)}</b><br>%{{x|%H:%M:%S}} → %{{y:.2f}}<extra></extra>"))
                if ch in thr:
                    fig.add_hline(y=thr[ch]["warn"],line_dash="dot",line_color="orange",opacity=0.5,
                                  annotation_text="warn",annotation_font_color="orange")
                    fig.add_hline(y=thr[ch]["crit"],line_dash="dash",line_color="#ff4444",opacity=0.5,
                                  annotation_text="crit",annotation_font_color="#ff4444")
            if not anns.empty:
                for _,ann in anns.iterrows():
                    bc="#ff4444" if ann["severity"]=="critical" else ("#ffa500" if ann["severity"]=="warning" else "#58a6ff")
                    ann_dt=seconds_to_wallclock(ann["t"],start_hms)
                    fig.add_vline(x=ann_dt,line_color=bc,line_dash="dot",opacity=0.7,
                                  annotation_text=ann["label"],annotation_font_color=bc,annotation_font_size=9)
            fig.update_layout(**{**PLOT_BASE,"height":340,"hovermode":"x unified",
                "xaxis":{"gridcolor":"#21262d","tickformat":"%H:%M:%S","title":"Time"}})
            st.plotly_chart(fig,use_container_width=True,config={"displayModeBar":True})

    # ── Scatter + Histogram ──────────────────────────────────────────────────
    st.markdown('<div class="section-hdr">SCATTER &amp; DISTRIBUTIONS</div>', unsafe_allow_html=True)
    s1,s2,s3=st.columns(3)
    def scatter(x_ch,y_ch,color_ch,title):
        sx=num(df,x_ch).dropna(); sy=num(df,y_ch)
        mask=sx.index.intersection(sy.dropna().index)
        if len(mask)<10: return None
        c_vals=num(df.loc[mask],color_ch) if color_ch and color_ch in df.columns else None
        fig=go.Figure(go.Scatter(x=num(df.loc[mask],x_ch),y=num(df.loc[mask],y_ch),mode="markers",
            marker=dict(size=3,opacity=0.6,color=c_vals if c_vals is not None else "#f7931a",
                colorscale="RdYlGn_r" if c_vals is not None else None,showscale=c_vals is not None,
                colorbar=dict(thickness=8,title=CHANNELS.get(color_ch,""),titlefont=dict(size=9)) if c_vals is not None else None),
            hovertemplate=f"{CHANNELS.get(x_ch,x_ch)}: %{{x:.1f}}<br>{CHANNELS.get(y_ch,y_ch)}: %{{y:.1f}}<extra></extra>"))
        fig.update_layout(**{**PLOT_BASE,"height":270,
            "title":dict(text=title,font=dict(size=11,color="#8b949e")),
            "xaxis_title":CHANNELS.get(x_ch,x_ch),"yaxis_title":CHANNELS.get(y_ch,y_ch)})
        return fig
    with s1:
        f1=scatter("speed_kmh","torque_nm","motor_temp","Torque vs Speed  (colour = Motor Temp)")
        if f1: st.plotly_chart(f1,use_container_width=True,config={"displayModeBar":False})
        else:  st.info("Needs speed + torque data")
    with s2:
        f2=scatter("speed_kmh","curr_mcu","motor_temp","Current vs Speed  (colour = Motor Temp)")
        if f2: st.plotly_chart(f2,use_container_width=True,config={"displayModeBar":False})
        else:  st.info("Needs speed + current data")
    with s3:
        hist_opts=[c for c in avail if c in CHANNELS]
        if hist_opts:
            default_idx=hist_opts.index("speed_kmh") if "speed_kmh" in hist_opts else 0
            hist_ch=st.selectbox("Histogram channel",hist_opts,index=default_idx,key="hist_ch")
            s_hist=num(df,hist_ch).dropna()
            fig_h=go.Figure(go.Histogram(x=s_hist,nbinsx=40,marker_color="#f7931a",opacity=0.8,
                hovertemplate=f"{CHANNELS.get(hist_ch,'')}: %{{x:.1f}}<br>Count: %{{y}}<extra></extra>"))
            fig_h.update_layout(**{**PLOT_BASE,"height":270,
                "title":dict(text=f"Distribution — {CHANNELS.get(hist_ch,'')}",font=dict(size=11,color="#8b949e")),
                "xaxis_title":CHANNELS.get(hist_ch,""),"yaxis_title":"Count"})
            st.plotly_chart(fig_h,use_container_width=True,config={"displayModeBar":False})

    # ── GPS Map ──────────────────────────────────────────────────────────────
    st.markdown('<div class="section-hdr">GPS ROUTE MAP</div>', unsafe_allow_html=True)
    has_gps="lat" in df.columns and num(df,"lat").notna().sum()>10
    if has_gps:
        gps=df[["t","lat","lon","speed_kmh"]].dropna(subset=["lat","lon"])
        fig_map=px.scatter_mapbox(gps,lat="lat",lon="lon",color="speed_kmh",
            color_continuous_scale="RdYlGn",range_color=[0,gps["speed_kmh"].quantile(0.95)],
            mapbox_style="carto-darkmatter",hover_data={"speed_kmh":":.1f","t":":.1f"},zoom=13)
        fig_map.update_layout(**{**PLOT_BASE,"height":380,"margin":dict(t=0,b=0,l=0,r=0)})
        st.plotly_chart(fig_map,use_container_width=True)
    else:
        st.markdown('<div class="gps-ph">📍 GPS data not available in this log<br>'
            '<span style="font-size:.7rem;color:#444">lat/lon columns will be plotted automatically once your logger includes GPS</span>'
            '</div>',unsafe_allow_html=True)

    # ── Anomaly Report ───────────────────────────────────────────────────────
    st.markdown('<div class="section-hdr">ANOMALY REPORT</div>', unsafe_allow_html=True)
    if not issues:
        st.markdown('<span style="color:#3fb950;font-family:\'Share Tech Mono\',monospace">✅ No anomalies detected</span>',unsafe_allow_html=True)
    for iss in issues:
        bg="#2d0f0f" if "🔴" in iss["sev"] else ("#2d1a00" if "🟡" in iss["sev"] else "#0f1a2d")
        bord="#ff4444" if "🔴" in iss["sev"] else ("#ffa500" if "🟡" in iss["sev"] else "#58a6ff")
        # Convert t to wall-clock
        wc=seconds_to_wallclock(iss["t"],start_hms).strftime("%H:%M:%S")
        st.markdown(
            f'<div style="background:{bg};border-left:3px solid {bord};border-radius:0 6px 6px 0;'
            f'padding:8px 13px;margin:3px 0;font-size:.83rem">'
            f'<b style="color:{bord}">{iss["sev"]} — {iss["type"]}</b>'
            f'&nbsp;&nbsp;<span style="color:#8b949e;font-family:\'Share Tech Mono\',monospace;font-size:.73rem">{iss["code"]}</span><br>'
            f'<span style="color:#8b949e">@ <b>{wc}</b> (T+{iss["t"]:.0f}s) · {iss["dur"]:.1f}s · {iss["count"]} samples</span>'
            f'</div>',unsafe_allow_html=True)

    # ── Annotations ──────────────────────────────────────────────────────────
    st.markdown('<div class="section-hdr">ENGINEER ANNOTATIONS</div>', unsafe_allow_html=True)
    ac1,ac2=st.columns([2,1])
    with ac2:
        with st.expander("➕ Add annotation"):
            with st.form("ann_form"):
                a_t  =st.number_input("Time (s elapsed)",0.0,float(df["t"].max()),step=0.5)
                a_lbl=st.text_input("Label",placeholder="Motor overheat event")
                a_sev=st.selectbox("Severity",["info","warning","critical"])
                a_aut=st.text_input("Engineer")
                a_nte=st.text_area("Note",height=70)
                if st.form_submit_button("Save"):
                    with get_engine().connect() as con:
                        con.execute(text("INSERT INTO annotations (session_id,t,label,severity,author,note,created_at) "
                            "VALUES (:sid,:t,:label,:severity,:author,:note,:created_at)"),
                            dict(sid=sid,t=a_t,label=a_lbl,severity=a_sev,author=a_aut,note=a_nte,
                                 created_at=datetime.now().isoformat()))
                        con.commit()
                    bust_cache(); st.rerun()
    with ac1:
        anns2=load_annotations(sid)
        if anns2.empty:
            st.markdown('<span style="color:#555;font-size:.83rem">No annotations yet.</span>',unsafe_allow_html=True)
        for _,ann in anns2.iterrows():
            bord="#ff4444" if ann["severity"]=="critical" else ("#ffa500" if ann["severity"]=="warning" else "#58a6ff")
            wc=seconds_to_wallclock(ann["t"],start_hms).strftime("%H:%M:%S")
            st.markdown(
                f'<div class="ann-box" style="border-left-color:{bord}">'
                f'<b style="color:{bord}">{wc}</b> · <b>{ann["label"]}</b> · '
                f'<span style="color:#8b949e;font-size:.73rem">{ann.get("author","")}'
                f' · {str(ann.get("created_at",""))[:10]}</span><br>'
                f'<span style="color:#c9d1d9">{ann.get("note","")}</span></div>',unsafe_allow_html=True)

    # ── AI Assistant ─────────────────────────────────────────────────────────
    st.divider()
    st.markdown('<div class="section-hdr">AI ENGINEERING ASSISTANT</div>', unsafe_allow_html=True)
    if "chat" not in st.session_state: st.session_state.chat=[]
    if st.session_state.get("chat_sid")!=sid: st.session_state.chat=[]; st.session_state.chat_sid=sid
    context=build_context(df,issues,srow.to_dict())
    for msg in st.session_state.chat:
        with st.chat_message(msg["role"]): st.markdown(msg["content"])
    if prompt:=st.chat_input("e.g. Why did motor temp spike? Is BMS behaviour normal?"):
        st.session_state.chat.append({"role":"user","content":prompt})
        with st.chat_message("user"): st.markdown(prompt)
        with st.chat_message("assistant"):
            with st.spinner("Analysing…"): reply=ask_claude(context,prompt,st.session_state.chat[:-1])
            st.markdown(reply)
        st.session_state.chat.append({"role":"assistant","content":reply})
    if st.session_state.chat:
        if st.button("🗑️ Clear chat"): st.session_state.chat=[]; st.rerun()

def page_compare(thr):
    st.markdown('<div class="section-hdr">SESSION COMPARISON</div>', unsafe_allow_html=True)
    sess=load_sessions()
    if len(sess)<2: st.info("Upload at least 2 sessions to compare."); return
    opts={f"{r['name']}  [{r['date']}]":int(r["id"]) for _,r in sess.iterrows()}
    keys=list(opts.keys())
    c1,c2=st.columns(2)
    lbl_a=c1.selectbox("Session A",keys,key="ca")
    lbl_b=c2.selectbox("Session B",[k for k in keys if k!=lbl_a],key="cb")
    df_a=compute_derived(load_signals(opts[lbl_a])); df_b=compute_derived(load_signals(opts[lbl_b]))
    ia=detect(df_a,thr); ib=detect(df_b,thr)
    avail=[k for k in list(CHANNELS.keys())+["power_kw"] if k in df_a.columns and k in df_b.columns]
    defaults=[c for c in ["motor_temp","soc_bms1","speed_kmh","curr_mcu","volt_mcu"] if c in avail]
    sel=st.multiselect("Channels",avail,defaults,format_func=lambda x:CHANNELS.get(x,x.replace("_"," ").title()))
    if not sel: return

    st.markdown('<div class="section-hdr">STATISTICS TABLE</div>', unsafe_allow_html=True)
    rows=[]
    for ch in sel:
        va,vb=num(df_a,ch).dropna(),num(df_b,ch).dropna()
        if va.empty or vb.empty: continue
        d=vb.mean()-va.mean()
        rows.append({"Channel":CHANNELS.get(ch,ch),"A Min":f"{va.min():.2f}","A Mean":f"{va.mean():.2f}",
                     "A Max":f"{va.max():.2f}","B Min":f"{vb.min():.2f}","B Mean":f"{vb.mean():.2f}",
                     "B Max":f"{vb.max():.2f}","Δ Mean":f"{'↑' if d>0 else '↓'} {abs(d):.2f}"})
    st.dataframe(pd.DataFrame(rows),use_container_width=True,hide_index=True)

    cc1,cc2=st.columns(2)
    for col,lst,lbl in [(cc1,ia,lbl_a),(cc2,ib,lbl_b)]:
        with col:
            st.markdown(f"**{lbl[:40]} — {len(lst)} anomalies**")
            for iss in lst:
                bord="#ff4444" if "🔴" in iss["sev"] else "#ffa500"
                st.markdown(f'<div style="border-left:2px solid {bord};padding:2px 7px;margin:2px 0;font-size:.78rem">'
                            f'{iss["sev"]} {iss["type"]} — {iss["code"]}</div>',unsafe_allow_html=True)
            if not lst: st.markdown('<span style="color:#3fb950;font-size:.82rem">✅ No anomalies</span>',unsafe_allow_html=True)

    st.markdown('<div class="section-hdr">SIGNAL OVERLAY</div>', unsafe_allow_html=True)
    sa,sb=downsample(df_a),downsample(df_b)
    for ch in sel:
        fig=go.Figure()
        fig.add_trace(go.Scatter(x=sa["t"],y=num(sa,ch),name=f"A: {lbl_a[:30]}",
            line=dict(color="#58a6ff",width=1.8),hovertemplate="<b>A</b> %{y:.2f}<extra></extra>"))
        fig.add_trace(go.Scatter(x=sb["t"],y=num(sb,ch),name=f"B: {lbl_b[:30]}",
            line=dict(color="#f7931a",width=1.8),hovertemplate="<b>B</b> %{y:.2f}<extra></extra>"))
        if ch in thr:
            fig.add_hline(y=thr[ch]["warn"],line_dash="dot",line_color="orange",opacity=0.4)
            fig.add_hline(y=thr[ch]["crit"],line_dash="dash",line_color="#ff4444",opacity=0.4)
        fig.update_layout(**{**PLOT_BASE,"height":250,
            "title":dict(text=CHANNELS.get(ch,ch),font=dict(size=12,color="#8b949e")),
            "hovermode":"x unified","xaxis_title":"Time (s)",
            "legend":dict(**PLOT_BASE["legend"],orientation="h",y=-0.35)})
        st.plotly_chart(fig,use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    with st.sidebar:
        st.markdown('<div class="logo-area">⚡ FAMEL</div><div class="logo-sub">Electric Moto Telemetry</div>',unsafe_allow_html=True)
        st.divider()
        sess=load_sessions()
        st.markdown(f'<div class="stat-badge">{len(sess)} sessions</div>'
                    f'<div class="stat-badge">{sess["duration_s"].sum()/3600:.1f} h logged</div>',unsafe_allow_html=True)
        st.divider()
        page=st.radio("Navigation",
            ["📤 Upload Session","📋 All Sessions","📊 Analyse Session","🔄 Compare Sessions"],
            label_visibility="collapsed")
        st.divider()
        with st.expander("⚙️ Alert Thresholds"):
            thr={}
            for ch,cfg in DEFAULT_THR.items():
                lbl=CHANNELS.get(ch,ch); hi=cfg["dir"]=="high"
                thr[ch]={"warn":st.number_input(f"{lbl} {'↑W' if hi else '↓W'}",value=float(cfg["warn"]),key=f"w_{ch}",step=1.0),
                         "crit":st.number_input(f"{lbl} {'↑C' if hi else '↓C'}",value=float(cfg["crit"]),key=f"c_{ch}",step=1.0),
                         "dir":cfg["dir"]}

    if   page=="📤 Upload Session":  page_upload()
    elif page=="📋 All Sessions":    page_sessions()
    elif page=="📊 Analyse Session": page_analyse(thr)
    elif page=="🔄 Compare Sessions":page_compare(thr)

if __name__=="__main__":
    main()
