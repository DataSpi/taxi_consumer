"""NYC Taxi Analytics dashboard.

Week 4: reads the dbt marts straight from Snowflake (TAXI_CONSUMERS.ANALYTICS)
and reuses the same AIRFLOW_CONN_SNOWFLAKE_DEFAULT credential blob the
pipeline already uses -- one credential source, no separate copy for the
dashboard (see docs/best_practices.md #3).
"""

from __future__ import annotations

import json
import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DATABASE = "TAXI_CONSUMERS"
SCHEMA = "ANALYTICS"

# Fixed-order categorical palette + single-hue sequential ramp (dataviz
# skill: identity vs magnitude get different color treatments; order is
# the CVD-safety mechanism, never re-sorted per filter).
BLUE, GREEN, MAGENTA, YELLOW, AQUA, ORANGE, VIOLET, RED = (
    "#2a78d6", "#008300", "#e87ba4", "#eda100",
    "#1baf7a", "#eb6834", "#4a3aa7", "#e34948",
)
CATEGORICAL = [BLUE, GREEN, MAGENTA, YELLOW, AQUA, ORANGE, VIOLET, RED]
SEQUENTIAL_BLUE = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95"]

st.set_page_config(page_title="NYC Taxi Analytics", page_icon="🚕", layout="wide")


@st.cache_resource
def get_connection() -> snowflake.connector.SnowflakeConnection:
    conn_json = json.loads(os.environ["AIRFLOW_CONN_SNOWFLAKE_DEFAULT"])
    extra = conn_json.get("extra", {})
    return snowflake.connector.connect(
        account=extra["account"],
        user=conn_json["login"],
        password=conn_json["password"],
        warehouse=extra["warehouse"],
        database=DATABASE,
        schema=SCHEMA,
        role=extra.get("role"),
    )


@st.cache_data(ttl=600)
def load_daily_summary() -> pd.DataFrame:
    query = f"""
        select
            s.pickup_date,
            s.pickup_location_id,
            s.trip_count,
            s.total_fare_amount,
            s.total_tip_amount,
            s.total_revenue,
            s.avg_trip_distance,
            s.avg_trip_duration_minutes,
            z.borough,
            z.zone,
            d.is_weekend
        from {DATABASE}.{SCHEMA}.fct_trips_daily_summary s
        left join {DATABASE}.{SCHEMA}.dim_zone z on s.pickup_location_id = z.location_id
        left join {DATABASE}.{SCHEMA}.dim_date d on s.pickup_date = d.date_day
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(query)
        df = cur.fetch_pandas_all()
    df.columns = [c.lower() for c in df.columns]
    df["pickup_date"] = pd.to_datetime(df["pickup_date"])
    return df


st.title("🚕 NYC Taxi Analytics")
st.caption("Yellow Taxi trips, 2024 · dbt marts in `TAXI_CONSUMERS.ANALYTICS` · Airflow-orchestrated pipeline")

try:
    df = load_daily_summary()
except Exception as exc:  # noqa: BLE001 -- surface connection/config errors in the UI, not a stack trace
    st.error(f"Could not load data from Snowflake: {exc}")
    st.stop()

if df.empty:
    st.warning("No rows in `fct_trips_daily_summary` yet -- trigger/backfill the `taxi_pipeline` DAG first.")
    st.stop()

# --- Filters (sidebar, one row of controls -- dataviz skill: filters above/beside charts, not scattered) ---
min_date, max_date = df["pickup_date"].min().date(), df["pickup_date"].max().date()
boroughs = sorted(b for b in df["borough"].dropna().unique())

with st.sidebar:
    st.header("Filters")
    date_range = st.date_input(
        "Pickup date range", value=(min_date, max_date), min_value=min_date, max_value=max_date
    )
    selected_boroughs = st.multiselect("Borough", options=boroughs, default=boroughs)

if len(date_range) != 2:
    st.stop()
start_date, end_date = date_range

mask = (
    (df["pickup_date"].dt.date >= start_date)
    & (df["pickup_date"].dt.date <= end_date)
    & (df["borough"].isin(selected_boroughs))
)
filtered = df.loc[mask]

if filtered.empty:
    st.warning("No trips match the current filters.")
    st.stop()

# --- KPI row (stat tiles -- st.metric is Streamlit's native tile component) ---
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total trips", f"{filtered['trip_count'].sum():,.0f}")
col2.metric("Total revenue", f"${filtered['total_revenue'].sum():,.0f}")
avg_fare = filtered["total_fare_amount"].sum() / filtered["trip_count"].sum()
col3.metric("Avg fare / trip", f"${avg_fare:,.2f}")
avg_distance = (filtered["avg_trip_distance"] * filtered["trip_count"]).sum() / filtered["trip_count"].sum()
col4.metric("Avg trip distance", f"{avg_distance:,.1f} mi")

st.divider()

left, right = st.columns(2)

# --- Daily trip trend: single series -> one hue, no legend needed ---
daily = filtered.groupby("pickup_date", as_index=False)["trip_count"].sum().sort_values("pickup_date")
fig_trend = px.line(daily, x="pickup_date", y="trip_count", template="plotly_white")
fig_trend.update_traces(line_color=BLUE, line_width=2, hovertemplate="%{x|%b %d}<br>%{y:,} trips<extra></extra>")
fig_trend.update_layout(
    title="Daily trips",
    xaxis_title=None,
    yaxis_title="Trips",
    margin=dict(t=48, l=0, r=0, b=0),
)
left.plotly_chart(fig_trend, use_container_width=True)

# --- Revenue by borough: identity across boroughs -> fixed categorical order ---
by_borough = filtered.groupby("borough", as_index=False)["total_revenue"].sum().sort_values(
    "total_revenue", ascending=True
)
color_map = {b: CATEGORICAL[i % len(CATEGORICAL)] for i, b in enumerate(sorted(by_borough["borough"]))}
fig_borough = go.Figure(
    go.Bar(
        x=by_borough["total_revenue"],
        y=by_borough["borough"],
        orientation="h",
        marker_color=[color_map[b] for b in by_borough["borough"]],
        hovertemplate="%{y}<br>$%{x:,.0f}<extra></extra>",
    )
)
fig_borough.update_layout(
    title="Revenue by borough",
    template="plotly_white",
    xaxis_title="Revenue ($)",
    yaxis_title=None,
    margin=dict(t=48, l=0, r=0, b=0),
    showlegend=False,
)
right.plotly_chart(fig_borough, use_container_width=True)

left2, right2 = st.columns(2)

# --- Top pickup zones: ranked magnitude -> single-hue sequential ramp ---
by_zone = (
    filtered.groupby("zone", as_index=False)["trip_count"]
    .sum()
    .sort_values("trip_count", ascending=False)
    .head(10)
    .sort_values("trip_count", ascending=True)
)
fig_zone = px.bar(
    by_zone,
    x="trip_count",
    y="zone",
    orientation="h",
    template="plotly_white",
    color="trip_count",
    color_continuous_scale=SEQUENTIAL_BLUE,
)
fig_zone.update_traces(hovertemplate="%{y}<br>%{x:,} trips<extra></extra>")
fig_zone.update_layout(
    title="Top 10 pickup zones",
    xaxis_title="Trips",
    yaxis_title=None,
    margin=dict(t=48, l=0, r=0, b=0),
    coloraxis_showscale=False,
)
left2.plotly_chart(fig_zone, use_container_width=True)

# --- Weekday vs weekend: two-category comparison, direct labels ---
by_weekday = filtered.groupby("is_weekend", as_index=False)["trip_count"].sum()
by_weekday["label"] = by_weekday["is_weekend"].map({True: "Weekend", False: "Weekday"})
fig_weekday = go.Figure(
    go.Bar(
        x=by_weekday["label"],
        y=by_weekday["trip_count"],
        marker_color=[BLUE, GREEN],
        text=by_weekday["trip_count"].map("{:,}".format),
        textposition="outside",
        hovertemplate="%{x}<br>%{y:,} trips<extra></extra>",
    )
)
fig_weekday.update_layout(
    title="Trips: weekday vs. weekend",
    template="plotly_white",
    xaxis_title=None,
    yaxis_title="Trips",
    margin=dict(t=48, l=0, r=0, b=0),
    showlegend=False,
)
right2.plotly_chart(fig_weekday, use_container_width=True)

with st.expander("Show underlying data"):
    st.dataframe(filtered.sort_values("pickup_date", ascending=False), use_container_width=True)
