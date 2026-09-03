"""
Israeli NBA Players: 360° Performance Analytics Dashboard

Tracks every active Israeli NBA player — Deni Avdija, Ben Saraf, Danny Wolf and
Emanuel Sharp — with career trends, hexbin/zone shot maps, and an All-Star comparison lab.

Features:
- Multi-player switcher: every page re-renders for whichever player is selected.
- Smart Data Patching: updates the current-season career row from game logs if stale.
- Hexbin Shot Maps: advanced density visualization with fixed aspect ratios.
- 14-Zone Efficiency Map: shot chart aggregated by court zone, colored by FG%.
- Deep Dive: percentile ranking against the current NBA All-Star roster.
"""

import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import theme

# -----------------------------
# Config and constants
# -----------------------------
DATA_FILE = "nba_data.pkl"

PLAYER_ORDER = ["deni_avdija", "ben_saraf", "danny_wolf", "emanuel_sharp"]
PLAYER_NAMES = {
    "deni_avdija": "Deni Avdija",
    "ben_saraf": "Ben Saraf",
    "danny_wolf": "Danny Wolf",
    "emanuel_sharp": "Emanuel Sharp",
}

SCOUTING_REPORTS = {
    "deni_avdija": """
### 🧐 Analysis: The Expanded Role
Avdija's star-making season has been his capacity for scaling up his production to fit an
expanded role since arriving in Portland.
* **Pick-and-Roll Volume:** More possessions as a P&R initiator than in his full years 2 or 3.
* **Elite Driving:** No one in the league drives more often, and few pass out of drives more frequently.
* **Free Throw Rate:** His downhill speed has him getting to the line at a rate on par with **Shai Gilgeous-Alexander**.

---
### ⚡ Defining Trait: The One-Man Fast Break
Avdija has become a reliable one-man fast break — equally adept at finishing at full tilt or
shifting gears (Euro-step, shoulder bumps) to dislodge defenders.

---
### 🧬 Modern NBA Archetype: The Multidimensional Wing
Growth through years 4 and 5; vision and ballhandling got him noticed, defense and rebounding
instincts kept him on the floor long enough for it to pay off. Made the 2026 All-Star Game as
a Western Conference reserve.
""",
}
DEFAULT_SCOUTING = """
Rookie-season tracking is live for this player. As more games are logged, this section will
fill in with a detailed scouting breakdown — for now, check the Dashboard and Career Analysis
pages for up-to-date per-game trends.
"""

st.set_page_config(page_title="Israeli NBA Analytics", page_icon="🏀", layout="wide")
theme.inject_css()
theme.apply_plotly_theme()


# -----------------------------
# Data Loading & Patching
# -----------------------------
@st.cache_data(show_spinner=False)
def load_nba_data() -> dict:
    if not Path(DATA_FILE).exists():
        return {}
    try:
        with open(DATA_FILE, "rb") as f:
            return pickle.load(f)
    except Exception as e:
        st.error(f"❌ Error loading data file: {e}")
        return {}


def patch_career_stats(career_df: pd.DataFrame, game_logs: dict) -> pd.DataFrame:
    """Overwrite the latest-season career row with fresh aggregates from game logs, if the
    logs cover more games than the (possibly stale) career table — or append it if missing."""
    if not game_logs or career_df.empty:
        return career_df

    latest_season = max(game_logs.keys())
    logs = game_logs[latest_season]
    if logs.empty:
        return career_df

    career_df = career_df.copy()
    career_df["SEASON_ID"] = career_df["SEASON_ID"].astype(str)
    true_gp = len(logs)

    updated = {
        "SEASON_ID": latest_season,
        "GP": true_gp,
        "GS": true_gp,
        "MIN": logs["MIN"].mean() if "MIN" in logs.columns else 0,
        "PTS": logs["PTS"].mean(),
        "REB": logs["REB"].mean(),
        "AST": logs["AST"].mean(),
        "STL": logs["STL"].mean() if "STL" in logs.columns else 0,
        "BLK": logs["BLK"].mean() if "BLK" in logs.columns else 0,
        "TOV": logs["TOV"].mean() if "TOV" in logs.columns else 0,
        "FG_PCT": (logs["FGM"].sum() / logs["FGA"].sum()) if logs["FGA"].sum() > 0 else 0,
        "FG3_PCT": (logs["FG3M"].sum() / logs["FG3A"].sum()) if logs["FG3A"].sum() > 0 else 0,
        "FT_PCT": (logs["FTM"].sum() / logs["FTA"].sum()) if logs["FTA"].sum() > 0 else 0,
    }

    mask = career_df["SEASON_ID"] == latest_season
    if mask.any():
        idx = career_df.index[mask][0]
        if true_gp >= career_df.at[idx, "GP"]:
            for col, val in updated.items():
                if col in career_df.columns:
                    career_df.at[idx, col] = val
    else:
        career_df = pd.concat([career_df, pd.DataFrame([updated])], ignore_index=True)
    return career_df


def merge_career_frames(basic_df: pd.DataFrame, adv_df: pd.DataFrame) -> pd.DataFrame:
    if basic_df.empty:
        return pd.DataFrame()
    if adv_df.empty or "SEASON_ID" not in adv_df.columns:
        return basic_df
    adv_cols = ["SEASON_ID", "NET_RATING", "AST_TO", "TS_PCT", "USG_PCT"]
    adv_cols = [c for c in adv_cols if c in adv_df.columns]
    merged = basic_df.merge(adv_df[adv_cols], on="SEASON_ID", how="left")
    return merged.sort_values("SEASON_ID")


def latest_game_log_stats(game_logs: dict):
    """Per-game averages from the most recent season with logged games. Returns (stats, season)
    or (None, None) if the player hasn't taken the floor yet."""
    if not game_logs:
        return None, None
    season = max(game_logs.keys())
    logs = game_logs[season]
    if logs.empty:
        return None, None
    stats = {
        "PTS": logs["PTS"].mean(),
        "REB": logs["REB"].mean(),
        "AST": logs["AST"].mean(),
        "STL": logs["STL"].mean() if "STL" in logs.columns else 0,
        "BLK": logs["BLK"].mean() if "BLK" in logs.columns else 0,
        "TOV": logs["TOV"].mean() if "TOV" in logs.columns else 0,
    }
    return stats, season


# -----------------------------
# Visualization Helpers
# -----------------------------
def create_hexbin_heatmap(shot_df: pd.DataFrame, hex_size: float = 25.0) -> list:
    """Generate hexbin data for heatmap (smaller hex size for better detail)."""
    if shot_df.empty or "LOC_X" not in shot_df.columns or "LOC_Y" not in shot_df.columns:
        return []
    valid = shot_df.dropna(subset=["LOC_X", "LOC_Y"])
    if valid.empty:
        return []

    x, y = valid["LOC_X"].values, valid["LOC_Y"].values
    hex_bins = []

    h_spacing = hex_size * np.sqrt(3)
    v_spacing = hex_size * 1.5

    x_centers = np.arange(-250, 250 + h_spacing, h_spacing)
    y_centers = np.arange(-47.5, 422.5 + v_spacing, v_spacing)

    for i, yc in enumerate(y_centers):
        for j, xc in enumerate(x_centers):
            xo = (h_spacing / 2) if (i % 2 == 1) else 0
            xc_actual = xc + xo

            dists = np.sqrt((x - xc_actual) ** 2 + (y - yc) ** 2)
            count = np.sum(dists <= hex_size)

            if count > 0:
                vx, vy = [], []
                for k in range(6):
                    angle = k * np.pi / 3
                    vx.append(xc_actual + hex_size * np.cos(angle))
                    vy.append(yc + hex_size * np.sin(angle))
                vx.append(vx[0])
                vy.append(vy[0])
                hex_bins.append({"x": vx, "y": vy, "count": int(count)})
    return hex_bins


def get_court_zones() -> list:
    """Returns 14 polygons representing the NBA court zones (NBA standard geometry)."""
    zones = []

    R_HOOP = 40.0
    R_3PT = 237.5
    R_FAR = 500.0
    X_CORNER = 220.0
    Y_PAINT_TOP = 142.5

    y_break = np.sqrt(R_3PT ** 2 - X_CORNER ** 2)
    theta_break_r = np.arctan2(y_break, X_CORNER)
    theta_cut_r = np.radians(60)
    theta_cut_l = np.radians(120)
    theta_break_l = np.pi - theta_break_r

    def get_arc(r, theta1, theta2, steps=30):
        t = np.linspace(theta1, theta2, steps)
        return r * np.cos(t), r * np.sin(t)

    arc_3pt_r_x, arc_3pt_r_y = get_arc(R_3PT, theta_break_r, theta_cut_r)
    arc_3pt_c_x, arc_3pt_c_y = get_arc(R_3PT, theta_cut_r, theta_cut_l)
    arc_3pt_l_x, arc_3pt_l_y = get_arc(R_3PT, theta_cut_l, theta_break_l)

    ra_x, ra_y = get_arc(R_HOOP, 0, np.pi)
    zones.append({
        "name": "Restricted Area", "key": "Restricted Area_Center(C)",
        "x": np.concatenate(([40, 40, -40, -40], ra_x[::-1])),
        "y": np.concatenate(([-47.5, 0, 0, -47.5], ra_y[::-1])),
    })

    zones.append({
        "name": "Paint", "key": "In The Paint (Non-RA)_Center(C)",
        "x": np.concatenate(([80, 80, -80, -80, -40], ra_x, [40])),
        "y": np.concatenate(([-47.5, Y_PAINT_TOP, Y_PAINT_TOP, -47.5, -47.5], ra_y, [-47.5])),
    })

    zones.append({
        "name": "MR Right", "key": "Mid-Range_Right Side(R)",
        "x": [80, X_CORNER, X_CORNER, 80, 80],
        "y": [-47.5, -47.5, y_break, y_break, -47.5],
    })

    zones.append({
        "name": "MR RC", "key": "Mid-Range_Right Side Center(RC)",
        "x": np.concatenate(([80], arc_3pt_r_x[::-1], [X_CORNER, 80, 80])),
        "y": np.concatenate(([Y_PAINT_TOP], arc_3pt_r_y[::-1], [y_break, y_break, Y_PAINT_TOP])),
    })

    zones.append({
        "name": "MR Center", "key": "Mid-Range_Center(C)",
        "x": np.concatenate(([80], arc_3pt_c_x[::-1], [-80, 80])),
        "y": np.concatenate(([Y_PAINT_TOP], arc_3pt_c_y[::-1], [Y_PAINT_TOP, Y_PAINT_TOP])),
    })

    zones.append({
        "name": "MR LC", "key": "Mid-Range_Left Side Center(LC)",
        "x": np.concatenate(([-80, -X_CORNER], arc_3pt_l_x[::-1], [-80, -80])),
        "y": np.concatenate(([y_break, y_break], arc_3pt_l_y[::-1], [Y_PAINT_TOP, y_break])),
    })

    zones.append({
        "name": "MR Left", "key": "Mid-Range_Left Side(L)",
        "x": [-80, -X_CORNER, -X_CORNER, -80, -80],
        "y": [y_break, y_break, -47.5, -47.5, y_break],
    })

    zones.append({"name": "Right Corner 3", "key": "Right Corner 3_Right Side(R)",
                  "x": [X_CORNER, 250, 250, X_CORNER, X_CORNER], "y": [-47.5, -47.5, y_break, y_break, -47.5]})
    zones.append({"name": "Left Corner 3", "key": "Left Corner 3_Left Side(L)",
                  "x": [-X_CORNER, -250, -250, -X_CORNER, -X_CORNER], "y": [-47.5, -47.5, y_break, y_break, -47.5]})

    far_r_x, far_r_y = get_arc(R_FAR, theta_break_r, theta_cut_r, steps=5)
    zones.append({
        "name": "AB3 RC", "key": "Above the Break 3_Right Side Center(RC)",
        "x": np.concatenate((arc_3pt_r_x, far_r_x[::-1], [arc_3pt_r_x[0]])),
        "y": np.concatenate((arc_3pt_r_y, far_r_y[::-1], [arc_3pt_r_y[0]])),
    })

    far_c_x, far_c_y = get_arc(R_FAR, theta_cut_r, theta_cut_l, steps=5)
    zones.append({
        "name": "AB3 Center", "key": "Above the Break 3_Center(C)",
        "x": np.concatenate((arc_3pt_c_x, far_c_x[::-1], [arc_3pt_c_x[0]])),
        "y": np.concatenate((arc_3pt_c_y, far_c_y[::-1], [arc_3pt_c_y[0]])),
    })

    far_l_x, far_l_y = get_arc(R_FAR, theta_cut_l, theta_break_l, steps=5)
    zones.append({
        "name": "AB3 LC", "key": "Above the Break 3_Left Side Center(LC)",
        "x": np.concatenate((arc_3pt_l_x, far_l_x[::-1], [arc_3pt_l_x[0]])),
        "y": np.concatenate((arc_3pt_l_y, far_l_y[::-1], [arc_3pt_l_y[0]])),
    })

    zones.append({"name": "AB3 Right Strip", "key": "Above the Break 3_Right Side(R)",
                  "x": [X_CORNER, 250, 250, X_CORNER, X_CORNER], "y": [y_break, y_break, 422.5, 422.5, y_break]})
    zones.append({"name": "AB3 Left Strip", "key": "Above the Break 3_Left Side(L)",
                  "x": [-X_CORNER, -250, -250, -X_CORNER, -X_CORNER], "y": [y_break, y_break, 422.5, 422.5, y_break]})

    return zones


def draw_nba_court(fig=None):
    if fig is None:
        fig = go.Figure()
    line_c = theme.MUTED
    shapes = [
        dict(type="rect", x0=-250, y0=-47.5, x1=250, y1=422.5, line=dict(color=line_c, width=2)),
        dict(type="rect", x0=-80, y0=-47.5, x1=80, y1=142.5, line=dict(color=line_c, width=2)),
        dict(type="rect", x0=-60, y0=-47.5, x1=60, y1=142.5, line=dict(color=line_c, width=2)),
        dict(type="circle", x0=-7.5, y0=-7.5, x1=7.5, y1=7.5, line=dict(color=theme.ACCENT, width=2)),
        dict(type="line", x0=-30, y0=-40, x1=30, y1=-40, line=dict(color=line_c, width=2)),
        dict(type="line", x0=0, y0=-40, x1=0, y1=-7.5, line=dict(color=theme.ACCENT, width=2)),
        dict(type="line", x0=-220, y0=-47.5, x1=-220, y1=92.5, line=dict(color=line_c, width=2)),
        dict(type="line", x0=220, y0=-47.5, x1=220, y1=92.5, line=dict(color=line_c, width=2)),
    ]

    arc_x, arc_y = [], []
    for i in range(500):
        x = -220 + (i / 499) * 440
        if abs(x) <= 237.5:
            y = np.sqrt(237.5 ** 2 - x ** 2)
            if y > 92.5:
                arc_x.append(x)
                arc_y.append(y)
    fig.add_trace(go.Scatter(x=arc_x, y=arc_y, mode="lines", line=dict(color=line_c, width=2), showlegend=False, hoverinfo="skip"))

    cc_x = [60 * np.cos(t) for t in np.linspace(0, np.pi, 50)]
    cc_y = [422.5 + 60 * np.sin(t) for t in np.linspace(0, np.pi, 50)]
    fig.add_trace(go.Scatter(x=cc_x, y=cc_y, mode="lines", line=dict(color=line_c, width=2), showlegend=False, hoverinfo="skip"))

    fig.update_layout(
        shapes=shapes,
        xaxis=dict(range=[-250, 250], showgrid=False, zeroline=False, visible=False, fixedrange=True),
        yaxis=dict(range=[-47.5, 422.5], scaleanchor="x", scaleratio=1, showgrid=False, zeroline=False, visible=False, fixedrange=True),
        plot_bgcolor=theme.WELL,
        paper_bgcolor=theme.BG,
        margin=dict(l=0, r=0, t=25, b=0),
        width=650, height=600,
        autosize=False,
    )
    return fig


def create_clean_shot_chart(shot_df: pd.DataFrame, season: str) -> go.Figure:
    """Mode A: Clean Scatter Chart (Made=accent circle, Missed=red X)."""
    fig = draw_nba_court()

    clean_df = shot_df.dropna(subset=["LOC_X", "LOC_Y"])
    if clean_df.empty:
        fig.update_layout(title=f"{season} - No Data")
        return fig

    made = clean_df[clean_df["EVENT_TYPE"] == "Made Shot"]
    missed = clean_df[clean_df["EVENT_TYPE"] == "Missed Shot"]

    fig.add_trace(go.Scatter(
        x=made["LOC_X"], y=made["LOC_Y"], mode="markers",
        marker=dict(color=theme.ACCENT, size=6, opacity=0.85, line=dict(width=0)),
        name="Made", showlegend=True,
    ))
    fig.add_trace(go.Scatter(
        x=missed["LOC_X"], y=missed["LOC_Y"], mode="markers",
        marker=dict(color=theme.NEGATIVE, size=6, opacity=0.75, symbol="x"),
        name="Missed", showlegend=True,
    ))

    fig.update_layout(
        title=dict(text=f"{season} Shot Chart", x=0.5, xanchor="center"),
        legend=dict(orientation="h", y=1.02, x=0.5, xanchor="center"),
        height=650,
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig


def create_zone_efficiency_map(shot_df: pd.DataFrame, season: str) -> go.Figure:
    """Mode B: 14-Zone Efficiency Map (Polygons colored by FG%)."""
    fig = draw_nba_court()

    if shot_df.empty:
        fig.update_layout(title=f"{season} - No Data")
        return fig

    shot_df = shot_df.copy()
    if "SHOT_ZONE_BASIC" in shot_df.columns and "SHOT_ZONE_AREA" in shot_df.columns:
        shot_df["ZONE_GROUP"] = shot_df["SHOT_ZONE_BASIC"] + "_" + shot_df["SHOT_ZONE_AREA"]

    stats = shot_df.groupby("ZONE_GROUP").agg(
        FGM=("SHOT_MADE_FLAG", "sum"),
        FGA=("SHOT_ATTEMPTED_FLAG", "count"),
    ).reset_index()
    stats["PCT"] = (stats["FGM"] / stats["FGA"]).fillna(0)

    zones = get_court_zones()

    for z in zones:
        row = stats[stats["ZONE_GROUP"] == z["key"]]

        val_text, hover_text = "", z["name"]

        if not row.empty:
            fgm, fga, pct = int(row.iloc[0]["FGM"]), int(row.iloc[0]["FGA"]), row.iloc[0]["PCT"]

            if pct < 0.35:
                fill_color = "#1c3a5e"
            elif pct < 0.45:
                fill_color = "#3a3f4a"
            else:
                fill_color = "#7a1425"
            fill_color = f"rgba{tuple(int(fill_color.lstrip('#')[i:i + 2], 16) for i in (0, 2, 4)) + (0.9,)}"

            val_text = f"<b>{fgm}/{fga}</b>"
            pct_text = f"<b>{pct:.1%}</b>"
            hover_text += f"<br>{fgm}/{fga} ({pct:.1%})"
        else:
            fill_color = "rgba(28, 31, 38, 0.6)"

        fig.add_trace(go.Scatter(
            x=z["x"], y=z["y"], fill="toself", mode="lines",
            line=dict(color=fill_color, width=1), fillcolor=fill_color,
            hoverinfo="text", text=hover_text, showlegend=False,
        ))

        if val_text:
            cx, cy = np.mean(z["x"]), np.mean(z["y"])
            fig.add_trace(go.Scatter(
                x=[cx], y=[cy], mode="text",
                text=[f"{val_text}<br>{pct_text}"],
                textfont=dict(family="Barlow Condensed, Arial", size=11, color=theme.TEXT),
                showlegend=False, hoverinfo="skip",
            ))

    fig.update_layout(
        title=dict(text=f"{season} Zone Efficiency", x=0.5, xanchor="center"),
        height=650,
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig


# -----------------------------
# Deep Dive charts
# -----------------------------
def plot_allstar_thresh(player_name: str, player_stats: dict, allstar_df: pd.DataFrame) -> go.Figure:
    if allstar_df.empty:
        return go.Figure()

    avg_pts = (allstar_df["PTS"] * allstar_df["GP"]).sum() / allstar_df["GP"].sum()
    avg_reb = (allstar_df["REB"] * allstar_df["GP"]).sum() / allstar_df["GP"].sum()
    avg_ast = (allstar_df["AST"] * allstar_df["GP"]).sum() / allstar_df["GP"].sum()

    bottom = allstar_df.nsmallest(4, "PTS").sort_values("PTS")

    fig = go.Figure()
    cats = ["PTS", "REB", "AST"]

    d_vals = [player_stats.get("PTS", 0), player_stats.get("REB", 0), player_stats.get("AST", 0)]
    fig.add_trace(go.Bar(name=player_name, x=cats, y=d_vals, marker_color=theme.ACCENT, text=[f"{v:.1f}" for v in d_vals], textposition="outside"))

    a_vals = [avg_pts, avg_reb, avg_ast]
    fig.add_trace(go.Bar(name="All-Star Avg", x=cats, y=a_vals, marker_color=theme.INFO, text=[f"{v:.1f}" for v in a_vals], textposition="outside"))

    for _, row in bottom.iterrows():
        p_name = row["PLAYER_NAME"]
        vals = [row["PTS"], row["REB"], row["AST"]]
        fig.add_trace(go.Bar(name=f"{p_name} (Entry)", x=cats, y=vals, opacity=0.4, marker_color=theme.MUTED_2))

    fig.update_layout(title="The All-Star Threshold", barmode="group", yaxis_title="Per Game")
    return fig


def plot_triple_threat(allstar_df: pd.DataFrame, player_name: str, player_stats: dict, is_2d: bool = False) -> go.Figure:
    if allstar_df.empty:
        return go.Figure()

    fig = go.Figure()
    d_pts, d_reb, d_ast = player_stats["PTS"], player_stats["REB"], player_stats["AST"]

    if is_2d:
        fig.add_trace(go.Scatter(
            x=allstar_df["PTS"], y=allstar_df["AST"], mode="markers", name="All-Stars",
            marker=dict(size=allstar_df["REB"] * 3, color=theme.MUTED_2, opacity=0.5, line=dict(width=1, color=theme.TEXT)),
            text=allstar_df["PLAYER_NAME"], hovertemplate="%{text}<br>PTS: %{x}<br>AST: %{y}<br>REB: %{marker.size}<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=[d_pts], y=[d_ast], mode="markers+text", name=player_name, text=[player_name], textposition="top center",
            marker=dict(size=[d_reb * 3], color=theme.ACCENT, line=dict(width=2, color=theme.TEXT)),
        ))
        fig.update_layout(title="Triple Threat (2D): PTS vs AST (Size=REB)", xaxis_title="Points", yaxis_title="Assists")
    else:
        fig.add_trace(go.Scatter3d(
            x=allstar_df["PTS"], y=allstar_df["REB"], z=allstar_df["AST"], mode="markers", name="All-Stars",
            marker=dict(size=6, color=theme.MUTED_2, opacity=0.6), text=allstar_df["PLAYER_NAME"],
        ))
        fig.add_trace(go.Scatter3d(
            x=[d_pts], y=[d_reb], z=[d_ast], mode="markers+text", name=player_name, text=[player_name],
            marker=dict(size=12, color=theme.ACCENT, line=dict(width=2, color=theme.TEXT)),
        ))
        fig.update_layout(title="Triple Threat (3D)", scene=dict(xaxis_title="PTS", yaxis_title="REB", zaxis_title="AST"))

    return fig


def analytical_verdict(player_name: str, player_stats: dict, allstar_df: pd.DataFrame):
    if allstar_df.empty:
        return

    def get_ordinal(n):
        if 11 <= (n % 100) <= 13:
            suffix = "th"
        else:
            suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
        return f"{n}{suffix}"

    pts_p = int((allstar_df["PTS"] < player_stats["PTS"]).mean() * 100)
    reb_p = int((allstar_df["REB"] < player_stats["REB"]).mean() * 100)
    ast_p = int((allstar_df["AST"] < player_stats["AST"]).mean() * 100)

    st.markdown("### 🎯 The Analytical Verdict")

    c1, c2, c3 = st.columns(3)
    c1.metric("Scoring Percentile", get_ordinal(pts_p), help="Rank among the current All-Star roster")
    c2.metric("Rebounding Percentile", get_ordinal(reb_p), help="Rank among the current All-Star roster")
    c3.metric("Playmaking Percentile", get_ordinal(ast_p), help="Rank among the current All-Star roster")

    st.caption(f"""
    ℹ️ **Context:** These percentages compare {player_name} against the **{{season}} All-Star Roster**.
    For example, being in the **{get_ordinal(pts_p)} percentile** means {player_name.split()[0]} outscores {pts_p}% of the NBA's elite.
    """)

    avg_p = (pts_p + reb_p + ast_p) / 3
    if avg_p > 50:
        st.success(f"🏆 **All-Star Caliber**: {player_name} ranks in the top half of All-Stars ({get_ordinal(int(avg_p))} percentile avg).")
    elif avg_p > 30:
        st.warning(f"⚡ **Borderline**: {player_name} is competitive with lower-tier All-Stars ({get_ordinal(int(avg_p))} percentile avg).")
    else:
        st.info(f"📈 **Developing**: {player_name} shows flashes but trails the All-Star pack ({get_ordinal(int(avg_p))} percentile avg).")


def plot_versatility_radar(player_name: str, player_stats: dict, allstar_df: pd.DataFrame) -> go.Figure:
    if allstar_df.empty:
        return go.Figure()

    metrics = ["PTS", "REB", "AST", "STL", "BLK"]
    d_vals = [player_stats.get(m, 0) for m in metrics]
    avg_stats = allstar_df[metrics].mean().tolist()

    max_vals = []
    for i, m in enumerate(metrics):
        max_val = max(allstar_df[m].max(), d_vals[i])
        max_vals.append(max_val if max_val > 0 else 1)

    d_norm = [d / m for d, m in zip(d_vals, max_vals)]
    a_norm = [a / m for a, m in zip(avg_stats, max_vals)]

    metrics += [metrics[0]]
    d_norm += [d_norm[0]]
    a_norm += [a_norm[0]]

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=a_norm, theta=metrics, fill="toself", name="All-Star Avg",
        line=dict(color=theme.INFO, width=2), fillcolor="rgba(159, 208, 255, 0.18)",
    ))
    fig.add_trace(go.Scatterpolar(
        r=d_norm, theta=metrics, fill="toself", name=player_name,
        line=dict(color=theme.ACCENT, width=3), fillcolor="rgba(215, 255, 58, 0.25)",
    ))

    fig.update_layout(
        polar=dict(radialaxis=dict(visible=False, range=[0, 1])),
        showlegend=True,
        title="The Multidimensional Wing (Normalized)",
        height=500,
    )
    return fig


def plot_offensive_engine(player_name: str, player_stats: dict, allstar_df: pd.DataFrame) -> go.Figure:
    if allstar_df.empty:
        return go.Figure()

    df = allstar_df.copy()
    if player_name not in df["PLAYER_NAME"].values:
        d_row = {"PLAYER_NAME": player_name}
        for k, v in player_stats.items():
            if k in df.columns:
                d_row[k] = v
        df = pd.concat([df, pd.DataFrame([d_row])], ignore_index=True)

    df["PTS_CREATED"] = df["AST"] * 2.3
    df["TOTAL_OUTPUT"] = df["PTS"] + df["PTS_CREATED"]
    df = df.sort_values("TOTAL_OUTPUT", ascending=False)

    top_15 = df.head(15)
    if player_name not in top_15["PLAYER_NAME"].values:
        p_row = df[df["PLAYER_NAME"] == player_name]
        plot_df = pd.concat([top_15, p_row]).sort_values("TOTAL_OUTPUT", ascending=False)
    else:
        plot_df = top_15

    fig = go.Figure()
    names = plot_df["PLAYER_NAME"].tolist()

    fig.add_trace(go.Bar(
        name="Points Scored", x=names, y=plot_df["PTS"],
        marker_color=[theme.ACCENT if x == player_name else theme.MUTED_2 for x in names],
    ))
    fig.add_trace(go.Bar(
        name="Points Created (Est)", x=names, y=plot_df["PTS_CREATED"],
        marker_color=[theme.INFO if x == player_name else "#4a505b" for x in names],
    ))

    fig.update_layout(
        barmode="stack", title="The Offensive Engine (Scoring + Playmaking)",
        xaxis_tickangle=-45, yaxis_title="Total Points Production", height=500,
    )
    return fig


def render_scouting_report(player_key: str, player_name: str):
    body = SCOUTING_REPORTS.get(player_key, DEFAULT_SCOUTING)
    with st.expander(f"📋 READ: Scouting Report & Analysis — {player_name}", expanded=False):
        st.markdown(body)


# -----------------------------
# Main App
# -----------------------------
def main():
    st.sidebar.markdown(
        f"""
        <div style="padding:4px 0 14px 0;">
            <div style="font-family:{theme.FONT_DISPLAY};font-size:24px;font-weight:800;
                        font-style:italic;text-transform:uppercase;color:{theme.ACCENT};">
                Israeli NBA Watch
            </div>
            <div style="font-family:{theme.FONT_DISPLAY};font-size:11px;letter-spacing:0.2em;
                        text-transform:uppercase;color:{theme.MUTED_2};margin-top:2px;">
                360° Performance Analytics
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    player_key = st.sidebar.selectbox(
        "Player", options=PLAYER_ORDER, format_func=lambda k: PLAYER_NAMES[k], index=0,
    )
    player_name = PLAYER_NAMES[player_key]

    page = st.sidebar.radio(
        "Navigate", ["Dashboard", "Career Analysis", "Raw Data", "Shot Maps", "Research: Deep Dive", "About Me"],
    )

    data = load_nba_data()
    if not data:
        st.error("Missing data file. Run `python fetch_data.py`.")
        st.stop()

    players_blob = data.get("players", {})
    player_data = players_blob.get(player_key, {})

    if not player_data and page != "About Me":
        st.warning(f"No data fetched yet for {player_name}. Run `python fetch_data.py --player {player_key}`.")
        st.stop()

    career_basic = player_data.get("career_basic", pd.DataFrame())
    career_adv = player_data.get("career_advanced", pd.DataFrame())
    game_logs = player_data.get("game_logs", {})
    shot_charts = player_data.get("shot_charts", {})
    allstar = data.get("allstar_stats", pd.DataFrame())
    allstar_detailed = data.get("allstar_detailed_stats", pd.DataFrame())
    league_ft = data.get("league_ft_stats", pd.DataFrame())
    allstar_season = data.get("allstar_season", "current")

    # Exclude the selected player from their own comparison cohort (Deni is a 2026 All-Star).
    if not allstar.empty:
        allstar = allstar[allstar["PLAYER_NAME"] != player_name]
    if not allstar_detailed.empty:
        allstar_detailed = allstar_detailed[allstar_detailed["PLAYER_NAME"] != player_name]

    career_basic = patch_career_stats(career_basic, game_logs)
    career_df = merge_career_frames(career_basic, career_adv)
    seasons_sorted = sorted(game_logs.keys(), reverse=True)

    # -----------------------------
    # PAGE: Dashboard
    # -----------------------------
    if page == "Dashboard":
        st.title(f"{player_name} — Performance Dashboard")

        if not seasons_sorted:
            st.info(f"🏀 {player_name} hasn't logged an NBA game yet this season — check back once the season tips off.")
        else:
            for i, season in enumerate(seasons_sorted):
                df = game_logs[season].sort_values("GAME_DATE")
                if df.empty:
                    continue
                avg_pts, avg_min = df["PTS"].mean(), df["MIN"].mean()

                label = f"{season[2:4]}/{season[5:]} Impact"
                st.subheader(label)

                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=df["GAME_DATE"], y=df["PTS"],
                    marker_color=[theme.ACCENT if w == "W" else theme.NEGATIVE for w in df["WL"]],
                    name="PTS",
                ))
                fig.add_trace(go.Scatter(x=df["GAME_DATE"], y=df["MIN"], mode="lines", name="MIN", yaxis="y2", line=dict(color=theme.INFO, width=2)))
                fig.add_hline(y=avg_pts, line_dash="dash", line_color=theme.MUTED, annotation_text=f"Avg PTS: {avg_pts:.1f}", annotation_position="top left")
                fig.add_trace(go.Scatter(x=df["GAME_DATE"], y=[avg_min] * len(df), mode="lines", name=f"Avg MIN ({avg_min:.1f})", yaxis="y2", line=dict(color=theme.INFO, width=1, dash="dot")))

                fig.update_layout(
                    yaxis=dict(title="Points"),
                    yaxis2=dict(title="Minutes", overlaying="y", side="right", range=[0, 48]),
                    legend=dict(orientation="h", y=1.1), height=400,
                )
                st.plotly_chart(fig, width="stretch", theme=None)
                if i < len(seasons_sorted) - 1:
                    st.divider()

    # -----------------------------
    # PAGE: Career Analysis
    # -----------------------------
    elif page == "Career Analysis":
        st.title(f"{player_name} — Career Trajectory Analysis")
        if career_df.empty:
            st.info("Not enough career data yet to chart a trajectory.")
        else:
            st.subheader("Per Game Stats")
            fig = px.bar(career_df, x="SEASON_ID", y=["PTS", "REB", "AST"], barmode="group", title="PTS / REB / AST")
            fig.update_layout(xaxis=dict(showgrid=False, title="Season"), yaxis=dict(showgrid=True, dtick=5), margin=dict(t=40, l=40, r=40, b=40))
            st.plotly_chart(fig, width="stretch", theme=None)

            st.divider()

            st.subheader("Per 36 Minutes")
            df_36 = career_df.copy()
            for c in ["PTS", "REB", "AST", "STL", "TOV"]:
                if c in df_36.columns and "MIN" in df_36.columns:
                    df_36[f"{c}_36"] = df_36.apply(lambda r: (r[c] / r["MIN"] * 36) if r["MIN"] > 0 else 0, axis=1)
            fig = px.bar(df_36, x="SEASON_ID", y=[c + "_36" for c in ["PTS", "REB", "AST", "STL", "TOV"]], barmode="group", title="Per 36 Min")
            fig.update_layout(xaxis=dict(showgrid=False, title="Season"), yaxis=dict(showgrid=True, dtick=5), margin=dict(t=40, l=40, r=40, b=40))
            st.plotly_chart(fig, width="stretch", theme=None)

            st.divider()

            if "USG_PCT" in career_df.columns:
                st.subheader("Usage Rate")
                st.caption("**Definition:** Percentage of team plays used by the player while on floor. **High (>30%):** Primary Scorers | **Low (<15%):** Role Players")
                fig = px.line(career_df, x="SEASON_ID", y="USG_PCT", markers=True, title="Usage %", labels={"USG_PCT": "Usage Percentage", "SEASON_ID": "Season"})
                fig.add_hline(y=0.20, line_dash="dash", annotation_text="League Avg (20%)")
                st.plotly_chart(fig, width="stretch", theme=None)
                st.divider()

            if "TS_PCT" in career_df.columns:
                st.subheader("True Shooting %")
                st.caption("**Definition:** Shooting efficiency adjusting for 3-pointers (1.5x) and Free Throws. **Elite (>60%)** | **Avg (~58%)** | **Poor (<52%)**")
                fig = px.line(career_df, x="SEASON_ID", y="TS_PCT", markers=True, title="TS %", labels={"TS_PCT": "True Shooting Percentage", "SEASON_ID": "Season"})
                fig.add_hline(y=0.58, line_dash="dash", annotation_text="League Avg (58%)")
                st.plotly_chart(fig, width="stretch", theme=None)

    # -----------------------------
    # PAGE: Raw Data
    # -----------------------------
    elif page == "Raw Data":
        st.title(f"{player_name} — Raw Data & Custom Trends")

        if career_df.empty:
            st.info("No career data yet.")
        else:
            st.subheader("Interactive Trend Viewer")
            numeric_cols = [c for c in career_df.columns if career_df[c].dtype in ["float64", "int64"]]
            defaults = ["PTS", "REB"] if "PTS" in numeric_cols else []
            sel_metrics = st.multiselect("Select Metrics", numeric_cols, default=defaults)
            if sel_metrics:
                fig = px.line(career_df, x="SEASON_ID", y=sel_metrics, markers=True, title="Custom Trends")
                st.plotly_chart(fig, width="stretch", theme=None)

            st.divider()
            st.subheader("Career Data Table")

            df_display = career_df.copy()
            drop_cols = ["PLAYER_ID", "LEAGUE_ID", "TEAM_ID"]
            df_display = df_display.drop(columns=[c for c in drop_cols if c in df_display.columns])

            if "SEASON_ID" in df_display.columns:
                df_display["SEASON_ID"] = df_display["SEASON_ID"].apply(
                    lambda x: f"{x[2:4]}/{x[5:]}" if isinstance(x, str) and len(x) >= 7 else x
                )
            df_display = df_display.rename(columns={"SEASON_ID": "Season", "TEAM_ABBREVIATION": "TEAM", "PLAYER_AGE": "AGE"})

            for c in ["GP", "GS"]:
                if c in df_display.columns:
                    df_display[c] = pd.to_numeric(df_display[c], errors="coerce").fillna(0).astype(int)

            avg_cols = ["MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV", "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA", "PF", "USG_PCT", "TS_PCT"]
            col_config = {}
            for c in avg_cols:
                if c in df_display.columns:
                    col_config[c] = st.column_config.NumberColumn(format="%.1%") if "PCT" in c else st.column_config.NumberColumn(format="%.1f")

            st.dataframe(df_display, width="stretch", hide_index=True, column_config=col_config)

    # -----------------------------
    # PAGE: Shot Maps
    # -----------------------------
    elif page == "Shot Maps":
        st.title(f"{player_name} — Shot Analysis")

        available_seasons = sorted(shot_charts.keys(), reverse=True)
        if not available_seasons:
            st.info("No shot chart data available yet for this player.")
        else:
            c_ctrl, c_view = st.columns([1, 4])
            with c_ctrl:
                compare = st.checkbox("Compare Mode", value=False)
                s_a = st.selectbox("Season A", available_seasons, index=0)
                s_b = st.selectbox("Season B", available_seasons, index=min(1, len(available_seasons) - 1)) if compare else None
                view_type = st.radio("Map Style", ["Shot Chart", "14-Zone Efficiency"], index=0, horizontal=True)

            with c_view:
                df_a = shot_charts.get(s_a, pd.DataFrame())
                if not compare:
                    if view_type == "14-Zone Efficiency":
                        st.plotly_chart(create_zone_efficiency_map(df_a, s_a), width="content", theme=None)
                    else:
                        st.plotly_chart(create_clean_shot_chart(df_a, s_a), width="content", theme=None)
                else:
                    df_b = shot_charts.get(s_b, pd.DataFrame())
                    c1, c2 = st.columns(2)
                    with c1:
                        if view_type == "14-Zone Efficiency":
                            st.plotly_chart(create_zone_efficiency_map(df_a, s_a), width="content", theme=None)
                        else:
                            st.plotly_chart(create_clean_shot_chart(df_a, s_a), width="content", theme=None)
                    with c2:
                        if view_type == "14-Zone Efficiency":
                            st.plotly_chart(create_zone_efficiency_map(df_b, s_b), width="content", theme=None)
                        else:
                            st.plotly_chart(create_clean_shot_chart(df_b, s_b), width="content", theme=None)

    # -----------------------------
    # PAGE: Deep Dive
    # -----------------------------
    elif page == "Research: Deep Dive":
        st.title(f"{player_name} — All-Star Comparison")

        render_scouting_report(player_key, player_name)

        if not league_ft.empty:
            st.divider()
            st.subheader(f"🔥 {data.get('league_ft_season', '')} Season: Free Throw Leaders (Top 10)")

            ft_df = league_ft.sort_values("FTM", ascending=False).head(10).reset_index(drop=True)
            if player_name not in ft_df["PLAYER_NAME"].values:
                own_row = league_ft[league_ft["PLAYER_NAME"] == player_name]
                ft_df = pd.concat([ft_df, own_row]).reset_index(drop=True)

            sort_metric = st.selectbox("Sort Leaderboard By:", ["Total Attempts", "Total Made", "FT%"], index=0, key="ft_sort_box")
            col_map = {"Total Attempts": "FTA", "Total Made": "FTM", "FT%": "FT_PCT"}
            target_col = col_map[sort_metric]

            df_display = ft_df.sort_values(target_col, ascending=False).reset_index(drop=True)
            df_display.index += 1
            df_display.index.name = "Rank"

            def highlight_player(row):
                if row.get("PLAYER_NAME") == player_name:
                    return [f"background-color: {theme.ACCENT}; color: {theme.ACCENT_INK}"] * len(row)
                return [""] * len(row)

            st.dataframe(
                df_display.style.apply(highlight_player, axis=1),
                width="stretch",
                column_config={
                    "FT_PCT": st.column_config.NumberColumn("FT%", format="%.1%"),
                    "FTM": st.column_config.NumberColumn("Total Made", format="%d"),
                    "FTA": st.column_config.NumberColumn("Total Attempts", format="%d"),
                    "GP": st.column_config.NumberColumn("Games", format="%d"),
                },
                hide_index=False,
                key=f"ft_leaderboard_{sort_metric}",
            )

        player_stats, stats_season = latest_game_log_stats(game_logs)

        if player_stats is None:
            st.divider()
            st.info(f"{player_name} hasn't played an NBA game yet — the All-Star comparison lab activates once box scores are logged.")
        elif not allstar.empty:
            if not career_df.empty:
                cur = career_df[career_df["SEASON_ID"] == stats_season]
                if not cur.empty:
                    if "USG_PCT" in cur.columns:
                        player_stats["USG_PCT"] = cur.iloc[0]["USG_PCT"]
                    if "TS_PCT" in cur.columns:
                        player_stats["TS_PCT"] = cur.iloc[0]["TS_PCT"]

            st.subheader(f"1. The All-Star Threshold ({allstar_season})")
            c1, c2 = st.columns([2, 1])
            with c1:
                st.plotly_chart(plot_allstar_thresh(player_name, player_stats, allstar), width="stretch", theme=None)
            with c2:
                analytical_verdict(player_name, player_stats, allstar)

            st.divider()
            st.subheader("2. The Triple Threat")
            show_2d = st.toggle("Switch to 2D Bubble View", value=False)
            st.plotly_chart(plot_triple_threat(allstar, player_name, player_stats, show_2d), width="stretch", theme=None)

            if not allstar_detailed.empty and "USG_PCT" in player_stats:
                st.divider()
                st.subheader("3. Separation Chart (Usage vs Efficiency)")
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=allstar_detailed["USG_PCT"] * 100, y=allstar_detailed["TS_PCT"] * 100, mode="markers", name="All-Stars", text=allstar_detailed["PLAYER_NAME"], marker=dict(color=theme.MUTED_2, size=8, opacity=0.5)))
                fig.add_trace(go.Scatter(x=[player_stats["USG_PCT"] * 100], y=[player_stats.get("TS_PCT", 0) * 100], mode="markers+text", name=player_name, text=[player_name], marker=dict(color=theme.ACCENT, size=18, symbol="star", line=dict(width=2, color=theme.TEXT)), textposition="top center"))
                fig.update_layout(xaxis_title="Usage %", yaxis_title="True Shooting %")
                st.plotly_chart(fig, width="stretch", theme=None)

            st.divider()
            st.subheader("4. Full League Comparison Table")
            rank_metric = st.selectbox("🏆 Rank Players By:", ["PTS", "REB", "AST", "STL", "BLK", "TOV"], index=0)

            t_df = allstar[["PLAYER_NAME", "PTS", "REB", "AST", "STL", "BLK", "TOV", "GP"]].copy()
            d_row = {"PLAYER_NAME": player_name, "GP": len(game_logs.get(stats_season, []))}
            for k in ["PTS", "REB", "AST", "STL", "BLK", "TOV"]:
                d_row[k] = player_stats[k]
            t_df = pd.concat([t_df, pd.DataFrame([d_row])], ignore_index=True)
            t_df = t_df.sort_values(rank_metric, ascending=False).reset_index(drop=True)
            t_df.insert(0, "Rank", range(1, len(t_df) + 1))

            cfg = {c: st.column_config.NumberColumn(format="%.1f") for c in ["PTS", "REB", "AST", "STL", "BLK", "TOV"]}

            def hl_player(x):
                return [f"background-color: {theme.ACCENT}; color: {theme.ACCENT_INK}" if x["PLAYER_NAME"] == player_name else "" for _ in x]

            st.dataframe(t_df.style.apply(hl_player, axis=1), width="stretch", hide_index=True, column_config=cfg)

            st.divider()
            st.subheader("5. Advanced Case Studies")
            c_adv1, c_adv2 = st.columns(2)
            with c_adv1:
                st.plotly_chart(plot_versatility_radar(player_name, player_stats, allstar), width="stretch", theme=None)
            with c_adv2:
                st.plotly_chart(plot_offensive_engine(player_name, player_stats, allstar), width="stretch", theme=None)

    elif page == "About Me":
        st.title("About This Dashboard")
        st.markdown(
            """
            Tracking every active Israeli NBA player in one place:
            - **Deni Avdija** — Portland Trail Blazers, 2026 NBA All-Star
            - **Ben Saraf** — Brooklyn Nets
            - **Danny Wolf** — Brooklyn Nets
            - **Emanuel Sharp** — Sacramento Kings

            Data via the NBA Stats API (`nba_api`), refreshed by `fetch_data.py`.
            """
        )
        st.write("Created by Ram Shiri")


if __name__ == "__main__":
    main()
