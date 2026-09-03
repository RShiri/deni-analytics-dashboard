"""
Shared visual theme for the dashboard, ported from XLALIGA's "Broadcast Kinetic" skin
(carbon ground, chamfered plates, ONE signal colour). Two pieces:
  - inject_css(): CSS injected into the Streamlit page.
  - apply_plotly_theme(): registers a matching Plotly template as the default.
"""

import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st

# -----------------------------
# Design tokens (mirrors XLALIGA laliga_dashboard/styles.css :root)
# -----------------------------
BG = "#0c0d10"
WELL = "#090a0d"
PLATE = "#15171c"
PLATE_2 = "#1c1f26"
LINE = "rgba(255, 255, 255, 0.08)"
LINE_STRONG = "rgba(255, 255, 255, 0.18)"

TEXT = "#f5f7fa"
MUTED = "#b9bfc9"
MUTED_2 = "#737b88"

ACCENT = "#d7ff3a"          # lime — the one signal colour: ahead / active / positive
ACCENT_INK = "#0c0d10"
NEGATIVE = "#ff2a4d"        # red — the only other semantic colour
INFO = "#9fd0ff"            # pale blue — secondary / on-target / info
NEUTRAL = "#4a505b"

FONT_DISPLAY = "'Barlow Condensed', 'Barlow', 'Segoe UI', sans-serif"
FONT_BODY = "'Barlow', 'Segoe UI', sans-serif"

# Back-compat aliases so callers can keep using the old Deni-era names.
COLOR_POSITIVE = INFO
COLOR_NEGATIVE = NEGATIVE
COLOR_PLAYER = ACCENT
COLOR_HIGHLIGHT = INFO
COLOR_GRAY = MUTED_2


def inject_css() -> None:
    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:ital,wght@0,500;0,700;0,800;1,700;1,800&family=Barlow:wght@400;500;600;700&display=swap');

        html, body, .stApp {{
            background: {BG} !important;
            color: {TEXT} !important;
            font-family: {FONT_BODY} !important;
        }}
        .stApp {{
            background-image: repeating-linear-gradient(-55deg, rgba(255,255,255,0.022) 0 2px, transparent 2px 14px);
        }}

        h1, h2, h3, h4, h5, h6,
        [data-testid="stMarkdownContainer"] h1, [data-testid="stMarkdownContainer"] h2, [data-testid="stMarkdownContainer"] h3 {{
            font-family: {FONT_DISPLAY} !important;
            font-weight: 800 !important;
            font-style: italic;
            letter-spacing: 0.02em;
            text-transform: uppercase;
            color: {TEXT} !important;
        }}

        p, span, label, div {{ font-family: {FONT_BODY}; }}

        /* Sidebar */
        section[data-testid="stSidebar"] {{
            background: {PLATE} !important;
            border-right: 1px solid {LINE_STRONG};
        }}
        section[data-testid="stSidebar"] * {{ color: {TEXT}; }}
        section[data-testid="stSidebar"] h1 {{
            font-size: 22px !important;
            color: {ACCENT} !important;
            -webkit-text-fill-color: {ACCENT} !important;
        }}

        /* Radio nav styled like slanted tabs */
        section[data-testid="stSidebar"] [role="radiogroup"] label {{
            background: {PLATE_2};
            border: 1px solid {LINE};
            padding: 6px 12px;
            margin-bottom: 4px;
            font-family: {FONT_DISPLAY};
            font-weight: 700;
            letter-spacing: 0.05em;
            text-transform: uppercase;
        }}
        section[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {{
            background: {ACCENT};
            color: {ACCENT_INK} !important;
        }}
        section[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) p {{
            color: {ACCENT_INK} !important;
        }}

        /* Metric cards */
        div[data-testid="stMetric"] {{
            background: {PLATE_2};
            border: 1px solid {LINE_STRONG};
            padding: 12px 16px;
        }}
        div[data-testid="stMetricValue"] {{
            font-family: {FONT_DISPLAY} !important;
            color: {ACCENT} !important;
        }}
        div[data-testid="stMetricLabel"] {{
            font-family: {FONT_DISPLAY} !important;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            color: {MUTED} !important;
        }}

        /* Expanders / containers as plates */
        div[data-testid="stExpander"] {{
            background: {PLATE_2};
            border: 1px solid {LINE_STRONG};
            border-radius: 0 !important;
        }}

        /* Buttons, selects, radios */
        .stButton > button, .stDownloadButton > button {{
            background: {PLATE_2};
            color: {TEXT};
            border: 1px solid {LINE_STRONG};
            border-radius: 0 !important;
            font-family: {FONT_DISPLAY};
            font-weight: 700;
            letter-spacing: 0.05em;
            text-transform: uppercase;
        }}
        .stButton > button:hover {{ border-color: {ACCENT}; color: {ACCENT}; }}

        div[data-baseweb="select"] > div, .stSelectbox div[data-baseweb="select"] {{
            background: {PLATE_2} !important;
            border-color: {LINE_STRONG} !important;
            border-radius: 0 !important;
        }}
        div[data-baseweb="popover"] {{ background: {PLATE_2} !important; }}

        /* Tabs */
        button[data-baseweb="tab"] {{
            font-family: {FONT_DISPLAY};
            font-weight: 700;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            color: {MUTED};
        }}
        button[aria-selected="true"][data-baseweb="tab"] {{ color: {ACCENT} !important; }}
        div[data-baseweb="tab-highlight"] {{ background-color: {ACCENT} !important; }}

        /* Alerts */
        div[data-testid="stAlertContainer"] {{
            border-radius: 0 !important;
            border-left: 3px solid {ACCENT};
            background: {PLATE_2} !important;
        }}

        /* Dataframes */
        div[data-testid="stDataFrame"] {{ border: 1px solid {LINE_STRONG}; }}

        /* Dividers */
        hr {{ border-color: {LINE_STRONG} !important; }}

        /* Sliders / checkboxes accent */
        div[data-testid="stSlider"] [role="slider"] {{ background-color: {ACCENT} !important; }}
        .stCheckbox svg, .stToggle [data-checked="true"] {{ color: {ACCENT} !important; }}

        code, .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def apply_plotly_theme() -> None:
    """Registers + activates a Plotly template mirroring the site's dark, hi-signal look."""
    template = go.layout.Template()
    template.layout = go.Layout(
        paper_bgcolor=BG,
        plot_bgcolor=WELL,
        font=dict(family=FONT_BODY, color=TEXT, size=13),
        colorway=[ACCENT, INFO, NEGATIVE, MUTED, "#ffb020"],
        title=dict(font=dict(family=FONT_DISPLAY, size=18, color=TEXT)),
        legend=dict(font=dict(color=MUTED)),
        xaxis=dict(gridcolor=LINE_STRONG, zerolinecolor=LINE_STRONG, linecolor=LINE_STRONG, color=MUTED),
        yaxis=dict(gridcolor=LINE_STRONG, zerolinecolor=LINE_STRONG, linecolor=LINE_STRONG, color=MUTED),
        polar=dict(bgcolor=WELL),
        scene=dict(
            xaxis=dict(backgroundcolor=WELL, gridcolor=LINE_STRONG, color=MUTED),
            yaxis=dict(backgroundcolor=WELL, gridcolor=LINE_STRONG, color=MUTED),
            zaxis=dict(backgroundcolor=WELL, gridcolor=LINE_STRONG, color=MUTED),
        ),
        margin=dict(t=50, l=20, r=20, b=20),
    )
    pio.templates["broadcast_kinetic"] = template
    pio.templates.default = "broadcast_kinetic"
