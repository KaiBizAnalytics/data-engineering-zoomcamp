"""
NBA Home Court Advantage — Streamlit Dashboard

Question: Does home court advantage still matter? Did COVID's bubble season
(no fans) reveal that crowd noise is the real driver?

Four tiles:
  1. Bar  — All-time home win % by team (categorical)
  2. Line — League-wide home win % by season (temporal, COVID highlighted)
  3. Line — Attendance vs home advantage over time (fan effect)
  4. Bar  — Bubble vs non-bubble home advantage delta by team
"""

import os
import json
import tempfile
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account

GCP_PROJECT = os.environ.get("GCP_PROJECT", "sanguine-mark-366002")
BQ_DATASET  = os.environ.get("BQ_DATASET",  "nba_dbt_marts")
MART_TABLE  = f"{GCP_PROJECT}.{BQ_DATASET}.mart_home_court_advantage"
BUBBLE_YEAR = 2019


def _get_bq_client() -> bigquery.Client:
    # Streamlit Cloud: credentials passed as JSON string in secrets
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON") or \
                 st.secrets.get("GOOGLE_APPLICATION_CREDENTIALS_JSON", None)
    if creds_json:
        info = json.loads(creds_json) if isinstance(creds_json, str) else creds_json
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=GCP_PROJECT, credentials=creds)
    # Local: use GOOGLE_APPLICATION_CREDENTIALS file path
    return bigquery.Client(project=GCP_PROJECT)


@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    client = _get_bq_client()
    query  = f"SELECT * FROM `{MART_TABLE}` ORDER BY season_year, team_abbr"
    return client.query(query).to_dataframe()


# ── Tile 1: Home win % by team (all-time) ─────────────────────────────────────
def tile_by_team(df: pd.DataFrame):
    st.subheader("Home Win % by Team — All Time")
    st.caption("All regular season games. Higher = stronger home court.")

    # Use most recent team name per abbreviation (teams that relocated/renamed)
    latest_names = (
        df.sort_values("season_year")
        .groupby("team_abbr")["team_name"]
        .last()
    )
    agg = (
        df.groupby("team_abbr")
        .agg(home_wins=("home_wins", "sum"), home_games=("home_games", "sum"))
        .reset_index()
    )
    agg["team_name"] = agg["team_abbr"].map(latest_names)
    agg["home_win_pct"] = agg["home_wins"] / agg["home_games"]
    agg = agg.sort_values("home_win_pct", ascending=False)
    league_avg = agg["home_wins"].sum() / agg["home_games"].sum()

    fig = px.bar(
        agg, x="team_abbr", y="home_win_pct",
        color="home_win_pct", color_continuous_scale="Blues",
        hover_data={"team_name": True, "home_games": True, "home_win_pct": ":.1%"},
        labels={"team_abbr": "Team", "home_win_pct": "Home Win %"},
        height=420,
    )
    fig.add_hline(y=league_avg, line_dash="dash", line_color="red",
                  annotation_text=f"League avg {league_avg:.1%}", annotation_position="top right")
    fig.update_layout(yaxis_tickformat=".0%", coloraxis_showscale=False, xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)


# ── Tile 2: Home win % over time ──────────────────────────────────────────────
def tile_over_time(df: pd.DataFrame):
    st.subheader("League-Wide Home Win % by Season")
    st.caption("Each point = one NBA season. Red star = 2019-20 COVID bubble (zero fans).")

    season = (
        df.groupby(["season_year", "is_bubble_season"])
        .agg(league_home_win_pct=("league_home_win_pct", "first"))
        .reset_index()
        .sort_values("season_year")
    )
    bubble     = season[season["is_bubble_season"]]
    non_bubble = season[~season["is_bubble_season"]]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=non_bubble["season_year"], y=non_bubble["league_home_win_pct"],
        mode="lines+markers", name="Regular seasons",
        line=dict(color="#1f77b4", width=2), marker=dict(size=5),
        hovertemplate="Season: %{x}<br>Home Win%%: %{y:.1%}<extra></extra>",
    ))
    if not bubble.empty:
        bval = bubble["league_home_win_pct"].values[0]
        fig.add_trace(go.Scatter(
            x=bubble["season_year"], y=bubble["league_home_win_pct"],
            mode="markers", name="COVID Bubble (2019-20)",
            marker=dict(color="red", size=14, symbol="star"),
        ))
        fig.add_annotation(x=BUBBLE_YEAR, y=bval, text="Bubble — no fans",
                           showarrow=True, arrowhead=2, ax=50, ay=-40,
                           font=dict(color="red"))
    fig.update_layout(yaxis_tickformat=".0%", xaxis_title="Season Start Year",
                      yaxis_title="Home Win %", height=420)
    st.plotly_chart(fig, use_container_width=True)


# ── Tile 3: Attendance vs home advantage ──────────────────────────────────────
def tile_attendance(df: pd.DataFrame):
    st.subheader("Fan Attendance vs Home Advantage Over Time")
    st.caption("Does the crowd drive home court advantage? Watch what happens when attendance hits zero.")

    season = (
        df.groupby(["season_year", "is_bubble_season"])
        .agg(
            league_avg_attendance=("league_avg_attendance", "first"),
            league_home_win_pct=("league_home_win_pct", "first"),
        )
        .reset_index()
        .sort_values("season_year")
        # Only seasons with meaningful attendance data (modern era)
        .query("season_year >= 1980")
    )

    fig = go.Figure()

    # Attendance bars (secondary axis)
    fig.add_trace(go.Bar(
        x=season["season_year"], y=season["league_avg_attendance"],
        name="Avg Attendance", marker_color=[
            "rgba(255,0,0,0.6)" if b else "rgba(100,149,237,0.4)"
            for b in season["is_bubble_season"]
        ], yaxis="y2",
    ))

    # Home win % line
    fig.add_trace(go.Scatter(
        x=season["season_year"], y=season["league_home_win_pct"],
        name="Home Win %", mode="lines+markers",
        line=dict(color="darkblue", width=2), yaxis="y1",
        hovertemplate="Season: %{x}<br>Home Win%%: %{y:.1%}<extra></extra>",
    ))

    fig.update_layout(
        yaxis=dict(title="Home Win %", tickformat=".0%", side="left"),
        yaxis2=dict(title="Avg Attendance", side="right", overlaying="y",
                    showgrid=False),
        xaxis_title="Season Start Year",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        height=420,
    )
    st.plotly_chart(fig, use_container_width=True)


# ── Tile 4: Bubble impact by team ─────────────────────────────────────────────
def tile_bubble_impact(df: pd.DataFrame):
    st.subheader("Bubble Season Impact by Team")
    st.caption(
        "Home advantage delta = home win% − away win%. "
        "Orange = bubble season (no fans). Blue = career average. "
        "Teams that rely on crowd noise show the biggest drop."
    )

    # All-time average delta per team
    alltime = (
        df[~df["is_bubble_season"]]
        .groupby("team_abbr")
        .agg(normal_delta=("home_advantage_delta", "mean"))
        .reset_index()
    )

    # Bubble delta per team
    bubble = (
        df[df["is_bubble_season"]]
        .groupby("team_abbr")
        .agg(bubble_delta=("home_advantage_delta", "mean"))
        .reset_index()
    )

    combined = alltime.merge(bubble, on="team_abbr", how="inner")
    combined["delta_change"] = combined["bubble_delta"] - combined["normal_delta"]
    combined = combined.sort_values("normal_delta", ascending=False)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=combined["team_abbr"], y=combined["normal_delta"],
        name="Normal seasons avg", marker_color="steelblue",
    ))
    fig.add_trace(go.Bar(
        x=combined["team_abbr"], y=combined["bubble_delta"],
        name="2019-20 Bubble", marker_color="orangered",
    ))
    fig.add_hline(y=0, line_dash="dash", line_color="black", line_width=1)
    fig.update_layout(
        barmode="group",
        yaxis_title="Home Advantage Delta (home% − away%)",
        yaxis_tickformat=".0%",
        xaxis_tickangle=-45,
        height=420,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig, use_container_width=True)


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    st.set_page_config(
        page_title="NBA Home Court Advantage",
        page_icon="🏀",
        layout="wide",
    )

    st.title("🏀 NBA Home Court Advantage — Does It Still Matter?")
    st.markdown(
        "Analyzing 70+ years of NBA home vs. away results across all 30 teams. "
        "The 2019-20 COVID bubble (zero fans at Disney World) acts as a natural experiment: "
        "**if crowd noise drives home court advantage, removing it should make the effect collapse.**"
    )

    with st.spinner("Loading data from BigQuery..."):
        df = load_data()

    if df.empty:
        st.error("No data found. Make sure the pipeline has run successfully.")
        return

    # Sidebar filters
    with st.sidebar:
        st.header("Filters")
        min_y, max_y = int(df["season_year"].min()), int(df["season_year"].max())
        yr = st.slider("Season Year Range", min_y, max_y, (min_y, max_y))
        df = df[(df["season_year"] >= yr[0]) & (df["season_year"] <= yr[1])]

        teams = sorted(df["team_abbr"].unique())
        sel = st.multiselect("Teams (blank = all)", teams)
        if sel:
            df = df[df["team_abbr"].isin(sel)]

    # Summary metrics
    c1, c2, c3, c4 = st.columns(4)
    overall = df["home_wins"].sum() / df["home_games"].sum()
    c1.metric("Overall Home Win %", f"{overall:.1%}")

    bub = df[df["is_bubble_season"]]
    if not bub.empty:
        bval = bub["home_wins"].sum() / bub["home_games"].sum()
        c2.metric("Bubble Home Win %", f"{bval:.1%}", delta=f"{bval - overall:.1%}")

    c3.metric("Avg Home Advantage Delta", f"{df['home_advantage_delta'].mean():+.1%}",
              help="home_win_pct − away_win_pct")

    att = df[df["avg_attendance"].notna() & (df["avg_attendance"] > 0)]
    if not att.empty:
        c4.metric("Avg Attendance (when known)", f"{int(att['avg_attendance'].mean()):,}")

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        tile_by_team(df)
    with col2:
        tile_over_time(df)

    st.divider()

    col3, col4 = st.columns(2)
    with col3:
        tile_attendance(df)
    with col4:
        tile_bubble_impact(df)

    with st.expander("Raw data"):
        st.dataframe(df, use_container_width=True)


if __name__ == "__main__":
    main()
