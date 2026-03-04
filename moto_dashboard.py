"""
⚡ Electric Motorcycle Telemetry Dashboard — Cloud Version
Database: Supabase (PostgreSQL) | Hosting: Streamlit Cloud
"""
import streamlit as st
import io, requests
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Moto Telemetry", page_icon="⚡", layout="wide")

CHANNELS = {
    "throttle":"Throttle (%)","speed_rpm":"Speed (RPM)","speed_kmh":"Speed (km/h)",
    "brake":"Brake","torque_nm":"Torque (Nm)","soc_bms1":"SOC BMS1 (%)","soc_bms2":"SOC BMS2 (%)",
    "volt_mcu":"Voltage MCU (V)","volt_bms1":"Voltage BMS1 (V)","volt_bms2":"Voltage BMS2 (V)",
    "curr_mcu":"Current MCU (A)","curr_bms1":"Current BMS1 (A)","curr_bms2":"Current BMS2 (A)",
    "motor_temp":"Motor Temp (°C)","mcu_temp":"MCU Temp (°C)",
    "board_temp_bms1":"Board Temp BMS1 (°C)","board_temp_bms2":"Board Temp BMS2 (°C)",
}
DEFAULT_THR = {
    "motor_temp":{"warn":80,"crit":100,"dir":"high"},
    "mcu_temp":{"warn":70,"crit":85,"dir":"high"},
    "board_temp_bms1":{"warn":60,"crit":75,"dir":"high"},
    "board_temp_bms2":{"warn":60,"crit":75,"dir":"high"},
    "curr_mcu":{"warn":250,"crit":350,"dir":"high"},
    "soc_bms1":{"warn":15,"crit":10,"dir":"low"},
    "soc_bms2":{"warn":15,"crit":10,"dir":"low"},
}
COLORS = px.colors.qualitative.Plotly

@st.cache_resource
def get_engine():
    return create_engine(st.secrets["DATABASE_URL"], pool_pre_ping=True)

def init_db():
    with get_engine().connect() as con:
        con.execute(text("""CREATE TABLE IF NOT EXISTS sessions (
            id SERIAL PRIMARY KEY, name TEXT NOT NULL, date TEXT, rider TEXT,
            track TEXT, weather TEXT, notes TEXT, upload_time TEXT,
            row_count INTEGER, duration_s REAL)"""))
        con.execute(text("""CREATE TABLE IF NOT EXISTS signals (
            session_id INTEGER, t REAL,
            throttle REAL, speed_rpm REAL, speed_kmh REAL, brake REAL, torque_nm REAL,
            soc_bms1 REAL, soc_bms2 REAL, volt_mcu REAL, volt_bms1 REAL, volt_bms2 REAL,
            curr_mcu REAL, curr_bms1 REAL, curr_bms2 REAL,
            motor_temp REAL, mcu_temp REAL, board_temp_bms1 REAL, board_temp_bms2 REAL,
            mcu_errors TEXT, bms1_errors TEXT, bms2_errors TEXT)"""))
        con.execute(text("CREATE INDEX IF NOT EXISTS idx_sig ON signals(session_id)"))
        con.commit()

try:
    init_db()
except Exception as e:
    st.error(f"Database connection failed: {e}")
    st.info("Set DATABASE_URL in Streamlit Cloud → Settings → Secrets")
    st.stop()

# ── CSV Parsing ───────────────────────────────────────────────────────────────
def parse_time_col(series):
    """Handle all timestamp formats including time-only HH:MM:SS.mmm"""

    # Format 1: time-only  e.g. "15:16:15.603"
    try:
        dt = pd.to_datetime(series, format='%H:%M:%S.%f')
        secs = (dt - dt.iloc[0]).dt.total_seconds().values
        # handle midnight roll-over
        secs = np.where(secs < 0, secs + 86400, secs)
        return secs
    except Exception:
        pass

    # Format 2: time-only without milliseconds e.g. "15:16:15"
    try:
        dt = pd.to_datetime(series, format='%H:%M:%S')
        return (dt - dt.iloc[0]).dt.total_seconds().values
    except Exception:
        pass

    # Format 3: full datetime string
    try:
        dt = pd.to_datetime(series, infer_datetime_format=True)
        return (dt - dt.iloc[0]).dt.total_seconds().values
    except Exception:
        pass

    # Format 4: numeric (ms or us since epoch)
    nums = pd.to_numeric(series, errors="coerce")
    if nums.notna().sum() >= len(series) * 0.5:
        rng = nums.max() - nums.min()
        if rng > 1e9:   return (nums - nums.iloc[0]).values / 1e6
        elif rng > 1e6: return (nums - nums.iloc[0]).values / 1e3
        return (nums - nums.iloc[0]).values

    # Fallback: assume 50 Hz
    return np.arange(len(series)) / 50.0

def parse_csv(raw_bytes):
    df = None
    for enc in ["utf-8", "latin-1", "cp1252"]:
        try:
            df = pd.read_csv(io.BytesIO(raw_bytes), encoding=enc)
            break
        except Exception:
            continue
    if df is None:
        raise ValueError("Could not read CSV file.")

    rename = {}
    for col in df.columns:
        c = col.lower()
        if "timestamp" in c:                               rename[col] = "timestamp_raw"
        elif "driving" in c:                               rename[col] = "driving_profile"
        elif "throttle" in c:                              rename[col] = "throttle"
        elif "rpm" in c:                                   rename[col] = "speed_rpm"
        elif "km" in c:                                    rename[col] = "speed_kmh"
        elif "brake" in c:                                 rename[col] = "brake"
        elif "torque" in c:                                rename[col] = "torque_nm"
        elif "soc" in c and "bms1" in c:                   rename[col] = "soc_bms1"
        elif "soc" in c and "bms2" in c:                   rename[col] = "soc_bms2"
        elif "volt" in c and "mcu" in c:                   rename[col] = "volt_mcu"
        elif "volt" in c and "bms1" in c:                  rename[col] = "volt_bms1"
        elif "volt" in c and "bms2" in c:                  rename[col] = "volt_bms2"
        elif "curr" in c and "mcu" in c:                   rename[col] = "curr_mcu"
        elif "curr" in c and "bms1" in c:                  rename[col] = "curr_bms1"
        elif "curr" in c and "bms2" in c:                  rename[col] = "curr_bms2"
        elif "motor" in c and "temp" in c:                 rename[col] = "motor_temp"
        elif "mcu" in c and "temp" in c:                   rename[col] = "mcu_temp"
        elif "board" in c and "bms1" in c:                 rename[col] = "board_temp_bms1"
        elif "board" in c and "bms2" in c:                 rename[col] = "board_temp_bms2"
        elif "mcu" in c and ("error" in c or "err" in c):  rename[col] = "mcu_errors"
        elif "bms1" in c and ("error" in c or "hw" in c):  rename[col] = "bms1_errors"
        elif "bms2" in c and ("error" in c or "hw" in c):  rename[col] = "bms2_errors"

    df = df.rename(columns=rename)

    if "timestamp_raw" in df.columns:
        df["t"] = parse_time_col(df["timestamp_raw"])
    else:
        df["t"] = np.arange(len(df)) / 50.0

    # Convert brake to numeric (it's "true"/"false" in this file)
    if "brake" in df.columns:
        df["brake"] = df["brake"].map({"true": 1.0, "false": 0.0, True: 1.0, False: 0.0}).fillna(
            pd.to_numeric(df["brake"], errors="coerce"))

    return df

def save_to_db(meta, df):
    sig_cols = ["t","throttle","speed_rpm","speed_kmh","brake","torque_nm",
                "soc_bms1","soc_bms2","volt_mcu","volt_bms1","volt_bms2",
                "curr_mcu","curr_bms1","curr_bms2","motor_temp","mcu_temp",
                "board_temp_bms1","board_temp_bms2","mcu_errors","bms1_errors","bms2_errors"]
    for c in sig_cols:
        if c not in df.columns:
            df[c] = np.nan
    engine = get_engine()
    with engine.connect() as con:
        r = con.execute(text(
            "INSERT INTO sessions (name,date,rider,track,weather,notes,upload_time,row_count,duration_s) "
            "VALUES (:name,:date,:rider,:track,:weather,:notes,:upload_time,:row_count,:duration_s) RETURNING id"),
            {"name":meta[0],"date":meta[1],"rider":meta[2],"track":meta[3],"weather":meta[4],
             "notes":meta[5],"upload_time":meta[6],"row_count":meta[7],"duration_s":meta[8]})
        sid = r.fetchone()[0]; con.commit()
    sig = df[sig_cols].copy(); sig.insert(0,"session_id",sid)
    sig.to_sql("signals", engine, if_exists="append", index=False, method="multi", chunksize=2000)
    return sid

@st.cache_data(ttl=30)
def load_sessions():
    return pd.read_sql("SELECT * FROM sessions ORDER BY date DESC, id DESC", get_engine())

@st.cache_data(ttl=60)
def load_signals(sid):
    return pd.read_sql(f"SELECT * FROM signals WHERE session_id={sid} ORDER BY t", get_engine())

def bust_cache():
    load_sessions.clear()
    load_signals.clear()

def num(df, col):
    return pd.to_numeric(df[col], errors="coerce") if col in df.columns else pd.Series(dtype=float)

def downsample(df, n=4000):
    return df.iloc[::max(1,len(df)//n)] if len(df)>n else df

# ── Anomaly Detection ─────────────────────────────────────────────────────────
def detect(df, thr):
    issues = []

    # Error codes
    for col,lbl in [("mcu_errors","MCU"),("bms1_errors","BMS1"),("bms2_errors","BMS2")]:
        if col not in df.columns: continue
        vals = df[col].astype(str).str.strip()
        bad = vals.notna() & ~vals.isin(["0","0.0","0x0","0x0000","0x0000000000000000","nan",""])
        if bad.sum() > 0:
            codes = vals[bad].unique()[:5]
            issues.append({"sev":"🔴 Critical","type":f"{lbl} Error Code",
                "desc":f'Codes detected: {", ".join(str(x) for x in codes)}',
                "t":float(df.loc[bad,"t"].iloc[0]),"dur":float(bad.sum()/50),"ch":col})

    # Threshold breaches
    for ch,cfg in thr.items():
        if ch not in df.columns: continue
        s = pd.to_numeric(df[ch], errors="coerce")
        if s.isna().all(): continue
        hi = cfg["dir"]=="high"
        c_mask = (s>=cfg["crit"]) if hi else (s<=cfg["crit"])
        w_mask = ((s>=cfg["warn"])&(s<cfg["crit"])) if hi else ((s<=cfg["warn"])&(s>cfg["crit"]))
        for mask,sev,tv in [(c_mask,"🔴 Critical",cfg["crit"]),(w_mask,"🟡 Warning",cfg["warn"])]:
            if mask.sum()<10: continue
            peak = s[mask].max() if hi else s[mask].min()
            issues.append({"sev":sev,
                "type":f'{CHANNELS.get(ch,ch)} {"Critical" if "🔴" in sev else "Warning"}',
                "desc":f'{"Peak" if hi else "Min"}: {peak:.1f}  (limit: {tv})',
                "t":float(df.loc[mask,"t"].iloc[0]),"dur":float(mask.sum()/50),"ch":ch})

    # BMS imbalances
    def imbal(a,b,lim,unit,label):
        if a not in df.columns or b not in df.columns: return
        diff=(pd.to_numeric(df[a],errors="coerce")-pd.to_numeric(df[b],errors="coerce")).abs()
        bad=diff>lim
        if bad.sum()>50:
            issues.append({"sev":"🟡 Warning","type":label,
                "desc":f'Max diff: {diff.max():.2f}{unit}  (limit: {lim}{unit})',
                "t":float(df.loc[bad,"t"].iloc[0]),"dur":float(bad.sum()/50),"ch":a})
    imbal("volt_bms1","volt_bms2",3.0,"V","Voltage Imbalance BMS1/BMS2")
    imbal("soc_bms1","soc_bms2",5.0,"%","SOC Imbalance BMS1/BMS2")
    imbal("curr_bms1","curr_bms2",20.0,"A","Current Imbalance BMS1/BMS2")

    # Rapid motor temp rise
    if "motor_temp" in df.columns:
        temp = pd.to_numeric(df["motor_temp"],errors="coerce")
        rate = (temp.diff()/df["t"].diff().replace(0,np.nan)).rolling(50,min_periods=10).mean()
        spikes = rate>2.0
        if spikes.sum()>25:
            issues.append({"sev":"🟡 Warning","type":"Rapid Motor Temp Rise",
                "desc":f'Rising >2°C/s for {spikes.sum()/50:.1f}s',
                "t":float(df.loc[spikes,"t"].iloc[0]) if spikes.any() else 0,
                "dur":float(spikes.sum()/50),"ch":"motor_temp"})

    issues.sort(key=lambda x:(0 if "🔴" in x["sev"] else 1, x["t"]))
    return issues

# ── AI Assistant ──────────────────────────────────────────────────────────────
def build_context(df, issues, srow):
    lines = [
        "ELECTRIC MOTORCYCLE TELEMETRY SESSION SUMMARY",
        f"Session: {srow.get('name','?')} | Date: {srow.get('date','?')} | Rider: {srow.get('rider','?')} | Track: {srow.get('track','?')}",
        f"Duration: {df['t'].max():.0f}s ({df['t'].max()/60:.1f} min) | Samples: {len(df):,} | Weather: {srow.get('weather','?')}",
    ]
    if srow.get("notes"): lines.append(f"Notes: {srow['notes']}")
    lines.append("\nCHANNEL STATISTICS (min / mean / max)")
    for ch,label in CHANNELS.items():
        if ch not in df.columns: continue
        s = pd.to_numeric(df[ch],errors="coerce").dropna()
        if not s.empty: lines.append(f"  {label}: {s.min():.2f} / {s.mean():.2f} / {s.max():.2f}")
    lines.append(f"\nANOMALIES DETECTED: {len(issues)}")
    for iss in issues:
        lines.append(f"  [{iss['sev']}] {iss['type']} — {iss['desc']} @ T+{iss['t']:.1f}s for {iss['dur']:.1f}s")
    return "\n".join(lines)

def ask_claude(context, question, history):
    api_key = st.secrets.get("ANTHROPIC_API_KEY","")
    if not api_key:
        return "⚠️ No ANTHROPIC_API_KEY in secrets. Add it in Streamlit Cloud → Settings → Secrets."
    system = (
        "You are an expert electric motorcycle engineer and data analyst for an R&D team. "
        "Analyse telemetry data and answer questions clearly, referencing actual values and timestamps. "
        "Be concise but thorough. Mention any additional issues you spot beyond the automatic detection."
        f"\n\nSESSION DATA:\n{context}"
    )
    msgs = [{"role":m["role"],"content":m["content"]} for m in history]
    msgs.append({"role":"user","content":question})
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"x-api-key":api_key,"anthropic-version":"2023-06-01","content-type":"application/json"},
        json={"model":"claude-sonnet-4-20250514","max_tokens":1024,"system":system,"messages":msgs},
        timeout=30)
    data = resp.json()
    return data["content"][0]["text"] if "content" in data else f"API error: {data.get('error',{}).get('message',str(data))}"

# ── Pages ─────────────────────────────────────────────────────────────────────
def page_upload():
    st.title("📤 Upload New Session")
    with st.form("upload"):
        c1,c2 = st.columns(2)
        name    = c1.text_input("Session name *", placeholder="Track Day 1 – Morning Run")
        date    = c1.date_input("Date")
        rider   = c1.text_input("Rider")
        track   = c2.text_input("Track / Location")
        weather = c2.text_input("Weather", placeholder="Sunny, 22°C, dry")
        notes   = c2.text_area("Notes")
        f       = st.file_uploader("CSV file *", type=["csv"])
        ok      = st.form_submit_button("💾 Save to database", type="primary")
    if ok:
        if not name: st.error("Session name required."); return
        if f is None: st.error("Select a CSV file."); return
        with st.spinner("Parsing and saving…"):
            df = parse_csv(f.read())
            dur = float(df["t"].max()) if "t" in df.columns else 0.0
            save_to_db((name,str(date),rider,track,weather,notes,
                        datetime.now().isoformat(),len(df),dur), df)
            bust_cache()
        st.success(f"✅ '{name}' saved — {len(df):,} rows, {dur:.0f}s ({dur/60:.1f} min)")

def page_sessions():
    st.title("📋 All Sessions")
    sess = load_sessions()
    if sess.empty: st.info("No sessions yet — upload your first CSV!"); return
    c1,c2,c3 = st.columns(3)
    c1.metric("Sessions",   len(sess))
    c2.metric("Total time", f"{sess['duration_s'].sum()/3600:.1f} hrs")
    c3.metric("Total rows", f"{sess['row_count'].sum():,}")
    st.divider()
    for _,r in sess.iterrows():
        with st.expander(f"**{r['name']}** — {r['date']}  |  {r.get('track','') or '—'}  |  {r.get('rider','') or '—'}"):
            ca,cb,cc,cd = st.columns(4)
            ca.write(f"**Rider:** {r.get('rider','—') or '—'}")
            cb.write(f"**Track:** {r.get('track','—') or '—'}")
            cc.write(f"**Duration:** {r['duration_s']:.0f}s ({r['duration_s']/60:.1f} min)")
            cd.write(f"**Rows:** {r['row_count']:,}")
            if r.get("weather"): st.write(f"**Weather:** {r['weather']}")
            if r.get("notes"):   st.write(f"**Notes:** {r['notes']}")
            if st.button("🗑️ Delete", key=f"del{r['id']}"):
                with get_engine().connect() as con:
                    con.execute(text("DELETE FROM signals WHERE session_id=:id"),{"id":r["id"]})
                    con.execute(text("DELETE FROM sessions WHERE id=:id"),{"id":r["id"]}); con.commit()
                bust_cache(); st.rerun()

def page_analyse(thr):
    st.title("📊 Analyse Session")
    sess = load_sessions()
    if sess.empty: st.info("No sessions yet."); return
    opts  = {f"{r['name']} ({r['date']})": (r["id"],r) for _,r in sess.iterrows()}
    label = st.selectbox("Session", list(opts.keys()))
    sid, srow = opts[label]
    df = load_signals(sid)

    # Key metrics
    st.subheader("Key Metrics")
    m1,m2,m3,m4,m5,m6 = st.columns(6)
    def vmax(c): v=num(df,c); return f"{v.max():.1f}" if v.notna().any() else "—"
    def vmin(c): v=num(df,c); return f"{v.min():.1f}" if v.notna().any() else "—"
    def dur_fmt():
        d = df['t'].max()
        return f"{d:.0f}s / {d/60:.1f}min" if pd.notna(d) else "—"
    m1.metric("Max Motor Temp",  f"{vmax('motor_temp')} °C")
    m2.metric("Max MCU Temp",    f"{vmax('mcu_temp')} °C")
    m3.metric("Min SOC BMS1",    f"{vmin('soc_bms1')} %")
    m4.metric("Max Current MCU", f"{vmax('curr_mcu')} A")
    m5.metric("Max Speed",       f"{vmax('speed_kmh')} km/h")
    m6.metric("Duration",        dur_fmt())

    # Anomalies
    st.subheader("⚠️ Anomaly Report")
    issues = detect(df, thr)
    if not issues:
        st.success("✅ No anomalies detected.")
    for iss in issues:
        color = "#c0392b" if "🔴" in iss["sev"] else "#e67e22"
        bg    = "#fdf0f0" if "🔴" in iss["sev"] else "#fef9e7"
        st.markdown(
            f'<div style="border-left:4px solid {color};background:{bg};padding:8px 14px;margin:4px 0;border-radius:4px">'
            f'<b>{iss["sev"]} — {iss["type"]}</b><br>'
            f'{iss["desc"]} &nbsp;|&nbsp; First at <b>T+{iss["t"]:.1f}s</b>'
            f' &nbsp;|&nbsp; Duration: <b>{iss["dur"]:.1f}s</b></div>',
            unsafe_allow_html=True)

    # Charts
    st.subheader("Channel Charts")
    avail    = [k for k in CHANNELS if k in df.columns and num(df,k).notna().sum()>10]
    defaults = [c for c in ["motor_temp","mcu_temp","soc_bms1","speed_kmh","curr_mcu"] if c in avail]
    sel      = st.multiselect("Channels to show", avail, defaults, format_func=lambda x: CHANNELS[x])
    if sel:
        t_max   = float(df["t"].max())
        t_range = st.slider("Time window (s)", 0.0, t_max, (0.0,t_max), step=1.0)
        sub     = downsample(df[(df["t"]>=t_range[0])&(df["t"]<=t_range[1])])
        fig = go.Figure()
        for i,ch in enumerate(sel):
            fig.add_trace(go.Scatter(x=sub["t"], y=num(sub,ch), name=CHANNELS[ch],
                line=dict(color=COLORS[i%len(COLORS)],width=1.5),
                hovertemplate=f"<b>{CHANNELS[ch]}</b><br>T: %{{x:.1f}}s<br>%{{y:.2f}}<extra></extra>"))
            if ch in thr:
                fig.add_hline(y=thr[ch]["warn"],line_dash="dot", line_color="orange",opacity=0.6,
                              annotation_text="warn")
                fig.add_hline(y=thr[ch]["crit"],line_dash="dash",line_color="red",   opacity=0.6,
                              annotation_text="crit")
        for iss in issues:
            if iss["ch"] in sel:
                fig.add_vrect(x0=iss["t"],x1=iss["t"]+max(iss["dur"],2),
                    fillcolor="red" if "🔴" in iss["sev"] else "orange",opacity=0.08,line_width=0)
        fig.update_layout(height=500,hovermode="x unified",xaxis_title="Time (s)",
                          legend=dict(orientation="h",y=-0.2))
        st.plotly_chart(fig, use_container_width=True)

    # AI Assistant
    st.divider()
    st.subheader("🤖 AI Assistant")
    st.caption("Ask anything about this session in plain English.")
    if "chat" not in st.session_state: st.session_state.chat = []
    if st.session_state.get("chat_sid") != sid:
        st.session_state.chat = []; st.session_state.chat_sid = sid
    context = build_context(df, issues, srow.to_dict())
    for msg in st.session_state.chat:
        with st.chat_message(msg["role"]): st.markdown(msg["content"])
    if prompt := st.chat_input("e.g. Why did motor temp spike? Is battery health normal?"):
        st.session_state.chat.append({"role":"user","content":prompt})
        with st.chat_message("user"): st.markdown(prompt)
        with st.chat_message("assistant"):
            with st.spinner("Analysing…"):
                reply = ask_claude(context, prompt, st.session_state.chat[:-1])
            st.markdown(reply)
        st.session_state.chat.append({"role":"assistant","content":reply})
    if st.session_state.chat:
        if st.button("🗑️ Clear chat"): st.session_state.chat=[]; st.rerun()

def page_compare(thr):
    st.title("🔄 Compare Sessions")
    sess = load_sessions()
    if len(sess)<2: st.info("Upload at least 2 sessions to compare."); return
    opts = {f"{r['name']} ({r['date']})":r["id"] for _,r in sess.iterrows()}
    keys = list(opts.keys())
    c1,c2 = st.columns(2)
    lbl_a = c1.selectbox("Session A", keys, key="ca")
    lbl_b = c2.selectbox("Session B", [k for k in keys if k!=lbl_a], key="cb")
    df_a,df_b = load_signals(opts[lbl_a]),load_signals(opts[lbl_b])
    avail    = [k for k in CHANNELS if k in df_a.columns and k in df_b.columns]
    defaults = [c for c in ["motor_temp","soc_bms1","speed_kmh","curr_mcu","volt_mcu"] if c in avail]
    sel      = st.multiselect("Channels", avail, defaults, format_func=lambda x: CHANNELS[x])
    if not sel: return

    # Stats table
    st.subheader("Statistics")
    rows=[]
    for ch in sel:
        va,vb = num(df_a,ch).dropna(), num(df_b,ch).dropna()
        if va.empty or vb.empty: continue
        rows.append({"Channel":CHANNELS[ch],
            "A Mean":f"{va.mean():.2f}","A Max":f"{va.max():.2f}","A Min":f"{va.min():.2f}",
            "B Mean":f"{vb.mean():.2f}","B Max":f"{vb.max():.2f}","B Min":f"{vb.min():.2f}"})
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # Anomalies
    ia,ib = detect(df_a,thr), detect(df_b,thr)
    with st.expander(f"⚠️ Anomalies — A ({len(ia)} issues)"):
        for iss in ia: st.markdown(f"**{iss['sev']}** {iss['type']} — {iss['desc']} @ T+{iss['t']:.1f}s")
        if not ia: st.success("No anomalies.")
    with st.expander(f"⚠️ Anomalies — B ({len(ib)} issues)"):
        for iss in ib: st.markdown(f"**{iss['sev']}** {iss['type']} — {iss['desc']} @ T+{iss['t']:.1f}s")
        if not ib: st.success("No anomalies.")

    # Overlay charts
    st.subheader("Overlay Charts")
    sa,sb = downsample(df_a), downsample(df_b)
    for ch in sel:
        fig=go.Figure()
        fig.add_trace(go.Scatter(x=sa["t"],y=num(sa,ch),name=f"A: {lbl_a[:30]}",
            line=dict(color="#1f77b4",width=1.8),
            hovertemplate=f"<b>A</b> {CHANNELS[ch]}: %{{y:.2f}}<extra></extra>"))
        fig.add_trace(go.Scatter(x=sb["t"],y=num(sb,ch),name=f"B: {lbl_b[:30]}",
            line=dict(color="#ff7f0e",width=1.8),
            hovertemplate=f"<b>B</b> {CHANNELS[ch]}: %{{y:.2f}}<extra></extra>"))
        if ch in thr:
            fig.add_hline(y=thr[ch]["warn"],line_dash="dot", line_color="orange",opacity=0.5)
            fig.add_hline(y=thr[ch]["crit"],line_dash="dash",line_color="red",   opacity=0.5)
        fig.update_layout(title=CHANNELS[ch],height=300,hovermode="x unified",
                          xaxis_title="Time (s)",margin=dict(t=35,b=30),
                          legend=dict(orientation="h",y=-0.35))
        st.plotly_chart(fig,use_container_width=True)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    st.sidebar.title("⚡ Moto Telemetry")
    st.sidebar.caption("Electric Motorcycle Dashboard")
    sess = load_sessions()
    st.sidebar.caption(f"{len(sess)} session{'s' if len(sess)!=1 else ''} in database")
    st.sidebar.divider()
    page = st.sidebar.radio("Go to",[
        "📤 Upload Session","📋 All Sessions","📊 Analyse Session","🔄 Compare Sessions"])
    with st.sidebar.expander("⚙️ Alert Thresholds"):
        thr={}
        for ch,cfg in DEFAULT_THR.items():
            lbl=CHANNELS.get(ch,ch)
            w=st.number_input(f"{lbl} {'warn' if cfg['dir']=='high' else 'low warn'}",
                              value=cfg["warn"],key=f"w{ch}")
            c=st.number_input(f"{lbl} {'crit' if cfg['dir']=='high' else 'low crit'}",
                              value=cfg["crit"],key=f"c{ch}")
            thr[ch]={"warn":w,"crit":c,"dir":cfg["dir"]}
    if   page=="📤 Upload Session":   page_upload()
    elif page=="📋 All Sessions":     page_sessions()
    elif page=="📊 Analyse Session":  page_analyse(thr)
    elif page=="🔄 Compare Sessions": page_compare(thr)

if __name__=="__main__": main()
