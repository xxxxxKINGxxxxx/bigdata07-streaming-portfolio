import json
import time
import os
from typing import Any
import redis
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Page config ──────────────────────────────────────────────────
st.set_page_config(
    page_title="AQI Live Stream Dashboard",
    page_icon="🌬️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── Config ───────────────────────────────────────────────────────
REDIS_HOST: str = os.getenv("REDIS_HOST", "aqi-redis")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
REFRESH_INTERVAL: int = int(os.getenv("REFRESH_INTERVAL", "5"))

# ── Band colours ─────────────────────────────────────────────────
BAND_COLORS: dict[str, str] = {
    "Good":                            "#00e400",
    "Moderate":                        "#ffff00",
    "Unhealthy for Sensitive Groups":  "#ff7e00",
    "Unhealthy":                       "#ff0000",
    "Very Unhealthy":                  "#8f3f97",
    "Hazardous":                       "#7e0023",
}

# ── Redis connection ─────────────────────────────────────────────
@st.cache_resource
def get_redis() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )

# ── Data fetchers ────────────────────────────────────────────────
def fetch_window_data(r: redis.Redis) -> list[dict[str, Any]]:
    keys: list[str] = r.keys("window:*")
    records: list[dict[str, Any]] = []
    for key in keys:
        data: dict = r.hgetall(key)
        if data:
            try:
                records.append({
                    "state":        data.get("state", ""),
                    "window_start": data.get("window_start", ""),
                    "avg_aqi":      float(data.get("avg_aqi", 0)),
                    "max_aqi":      float(data.get("max_aqi", 0)),
                    "min_aqi":      float(data.get("min_aqi", 0)),
                    "record_count": int(data.get("record_count", 0)),
                    "band":         data.get("band", "Unknown"),
                    "batch_id":     int(data.get("batch_id", 0)),
                })
            except (ValueError, TypeError):
                continue
    return sorted(records, key=lambda x: x["window_start"], reverse=True)

def fetch_band_counts(r: redis.Redis) -> dict[str, int]:
    raw: dict = r.hgetall("band_counts")
    return {k: int(v) for k, v in raw.items()} if raw else {}

def fetch_recent_records(r: redis.Redis, n: int = 10) -> list[dict[str, Any]]:
    raw: list[str] = r.lrange("recent_records", 0, n - 1)
    records: list[dict[str, Any]] = []
    for item in raw:
        try:
            records.append(json.loads(item))
        except json.JSONDecodeError:
            continue
    return records

def fetch_sliding_data(r: redis.Redis) -> list[dict[str, Any]]:
    keys: list[str] = r.keys("sliding:*")
    records: list[dict[str, Any]] = []
    for key in keys:
        data: dict = r.hgetall(key)
        if data:
            try:
                records.append({
                    "state":        data.get("state", ""),
                    "window_start": data.get("window_start", ""),
                    "avg_aqi":      float(data.get("avg_aqi", 0)),
                    "record_count": int(data.get("record_count", 0)),
                    "band":         data.get("band", "Unknown"),
                })
            except (ValueError, TypeError):
                continue
    return sorted(records, key=lambda x: x["window_start"], reverse=True)

# ── Main dashboard ───────────────────────────────────────────────
def main() -> None:
    r: redis.Redis = get_redis()

    st.title("🌬️ US Air Quality Index — Live Stream Dashboard")
    st.caption("Powered by Kafka → PySpark Structured Streaming → Redis")

    # ── Fetch all data ───────────────────────────────────────────
    window_data:   list[dict] = fetch_window_data(r)
    band_counts:   dict[str, int] = fetch_band_counts(r)
    recent:        list[dict] = fetch_recent_records(r)
    sliding_data:  list[dict] = fetch_sliding_data(r)

    if not window_data:
        st.warning("Waiting for data from PySpark streaming job...")
        time.sleep(REFRESH_INTERVAL)
        st.rerun()
        return

    # ── KPI row ──────────────────────────────────────────────────
    total_records:  int   = sum(d["record_count"] for d in window_data)
    avg_aqi_global: float = round(
        sum(d["avg_aqi"] for d in window_data) / len(window_data), 2
    )
    max_aqi_global: float = max(d["max_aqi"] for d in window_data)
    total_windows:  int   = len(window_data)
    dominant_band:  str   = (
        max(band_counts, key=band_counts.get) if band_counts else "N/A"
    )

    st.subheader("📊 Live KPIs")
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total Records Processed", f"{total_records:,}")
    k2.metric("Global Avg AQI",          f"{avg_aqi_global}")
    k3.metric("Peak AQI Observed",       f"{max_aqi_global}")
    k4.metric("Active Windows",          f"{total_windows}")
    k5.metric("Dominant Band",           dominant_band)

    st.divider()

    # ── Row 1: AQI trend + Band distribution ─────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📈 Avg AQI by State (Latest Window)")
        state_latest: dict[str, float] = {}
        state_band:   dict[str, str]   = {}
        for d in window_data:
            if d["state"] not in state_latest:
                state_latest[d["state"]] = d["avg_aqi"]
                state_band[d["state"]]   = d["band"]

        states:    list[str]   = list(state_latest.keys())
        aqi_vals:  list[float] = [state_latest[s] for s in states]
        colors:    list[str]   = [
            BAND_COLORS.get(state_band[s], "#888888") for s in states
        ]

        fig1 = go.Figure(go.Bar(
            x=states,
            y=aqi_vals,
            marker_color=colors,
            text=[f"{v:.1f}" for v in aqi_vals],
            textposition="outside"
        ))
        fig1.update_layout(
            xaxis_title="State",
            yaxis_title="Avg AQI",
            height=400,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="white"
        )
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.subheader("🎨 Classification Band Distribution")
        if band_counts:
            bands:  list[str] = list(band_counts.keys())
            counts: list[int] = list(band_counts.values())
            colors_pie: list[str] = [
                BAND_COLORS.get(b, "#888888") for b in bands
            ]
            fig2 = go.Figure(go.Pie(
                labels=bands,
                values=counts,
                marker_colors=colors_pie,
                hole=0.4
            ))
            fig2.update_layout(
                height=400,
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font_color="white"
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Band distribution loading...")

    st.divider()

    # ── Row 2: AQI timeline + Sliding vs Tumbling ─────────────────
    col3, col4 = st.columns(2)

    with col3:
        st.subheader("⏱️ AQI Timeline — Tumbling Windows")
        timeline_data: list[dict] = sorted(
            window_data, key=lambda x: x["window_start"]
        )[-50:]
        if timeline_data:
            times:    list[str]   = [d["window_start"] for d in timeline_data]
            aqi_time: list[float] = [d["avg_aqi"]      for d in timeline_data]
            fig3 = go.Figure(go.Scatter(
                x=times,
                y=aqi_time,
                mode="lines+markers",
                line=dict(color="#00bfff", width=2),
                marker=dict(size=6)
            ))
            fig3.update_layout(
                xaxis_title="Window Start",
                yaxis_title="Avg AQI",
                height=350,
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font_color="white"
            )
            st.plotly_chart(fig3, use_container_width=True)

    with col4:
        st.subheader("🔄 Tumbling vs Sliding Window Comparison")
        if sliding_data and window_data:
            tumbling_avgs: dict[str, float] = {}
            for d in window_data:
                if d["state"] not in tumbling_avgs:
                    tumbling_avgs[d["state"]] = d["avg_aqi"]

            sliding_avgs: dict[str, float] = {}
            for d in sliding_data:
                if d["state"] not in sliding_avgs:
                    sliding_avgs[d["state"]] = d["avg_aqi"]

            common_states: list[str] = sorted(
                set(tumbling_avgs.keys()) & set(sliding_avgs.keys())
            )[:15]

            if common_states:
                fig4 = go.Figure()
                fig4.add_trace(go.Bar(
                    name="Tumbling (30s)",
                    x=common_states,
                    y=[tumbling_avgs[s] for s in common_states],
                    marker_color="#00bfff"
                ))
                fig4.add_trace(go.Bar(
                    name="Sliding (60s/15s)",
                    x=common_states,
                    y=[sliding_avgs[s] for s in common_states],
                    marker_color="#ff7e00"
                ))
                fig4.update_layout(
                    barmode="group",
                    xaxis_title="State",
                    yaxis_title="Avg AQI",
                    height=350,
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    font_color="white",
                    legend=dict(orientation="h")
                )
                st.plotly_chart(fig4, use_container_width=True)
        else:
            st.info("Sliding window data loading...")

    st.divider()

    # ── Row 3: Classification table + Recent records feed ─────────
    col5, col6 = st.columns(2)

    with col5:
        st.subheader("📋 Latest Window Results")
        if window_data:
            display_data: list[dict] = []
            for d in window_data[:20]:
                display_data.append({
                    "State":        d["state"],
                    "Window Start": d["window_start"],
                    "Avg AQI":      d["avg_aqi"],
                    "Max AQI":      d["max_aqi"],
                    "Records":      d["record_count"],
                    "Band":         d["band"],
                })
            st.dataframe(
                display_data,
                use_container_width=True,
                height=300
            )

    with col6:
        st.subheader("📡 Live Record Feed")
        if recent:
            for rec in recent:
                band:    str   = rec.get("band", "Unknown")
                color:   str   = BAND_COLORS.get(band, "#888888")
                state:   str   = rec.get("state", "")
                avg_aqi: float = rec.get("avg_aqi", 0)
                count:   int   = rec.get("count", 0)
                window:  str   = rec.get("window", "")
                st.markdown(
                    f'<div style="background:{color}22; border-left: 4px solid {color}; '
                    f'padding: 6px 12px; margin: 4px 0; border-radius: 4px; color: white;">'
                    f'<b>{state}</b> — AQI: {avg_aqi} '
                    f'<span style="background:{color}; color:black; padding:2px 6px; '
                    f'border-radius:3px; font-size:0.8em;">{band}</span> '
                    f'<span style="opacity:0.6; font-size:0.8em;">{count} records | {window}</span>'
                    f'</div>',
                    unsafe_allow_html=True
                )
        else:
            st.info("Waiting for live records...")

    # ── Auto refresh ─────────────────────────────────────────────
    st.divider()
    st.caption(f"🔄 Auto-refreshing every {REFRESH_INTERVAL} seconds")
    time.sleep(REFRESH_INTERVAL)
    st.rerun()

if __name__ == "__main__":
    main()
