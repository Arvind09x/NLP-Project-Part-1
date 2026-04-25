from __future__ import annotations

import html
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from fitness_reddit_analyzer.app_data import list_available_caches, load_dashboard_snapshot, resolve_cache_key
from fitness_reddit_analyzer.config import APP_TITLE


st.set_page_config(page_title=APP_TITLE, layout="wide")

PLOTLY_CONFIG = {"displayModeBar": False, "responsive": True}
HERO_IMAGE_CANDIDATES = (
    Path("/Users/arvindsrinivass/Downloads/4ea73c75319148e3b2275fcf102e36a3.jpg"),
    Path("/Users/arvindsrinivass/Downloads/2b530d0302e87d964541b0765ec5f52b.jpg"),
    Path("/Users/arvindsrinivass/Desktop/Screenshot 2026-04-22 at 12.52.07 PM.png"),
)
PAGES = ("Overview", "Topics", "Topic Detail", "Methods")

CHART_COLORS = {
    "ink": "#000000",
    "ink_soft": "#292421",
    "muted": "#6b6259",
    "grid": "#d5c9ba",
    "line_soft": "#8e847a",
    "accent": "#260e0e",
    "accent_soft": "#cfc5b8",
    "fill": "#ffffff",
}


def get_hero_image_path() -> Path | None:
    for candidate in HERO_IMAGE_CANDIDATES:
        if candidate.exists():
            return candidate
    return None


def get_image_data_uri(path: Path) -> str:
    import base64

    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    suffix = path.suffix.lstrip(".").lower() or "jpeg"
    return f"data:image/{suffix};base64,{encoded}"


def page_url(page: str) -> str:
    return f"?page={quote(page)}"


def apply_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@400;500;600;700;800&display=swap');
        :root {
            --main: #f4e9dc;
            --paper: #ffffff;
            --text: #000000;
            --muted: #5d554f;
            --muted-soft: #756c63;
            --line: rgba(31, 31, 31, 0.12);
            --line-strong: #1f1f1f;
            --shadow-soft: 0 2px 0 rgba(31, 31, 31, 0.12);
            --shadow-deep: 0 4px 4px rgba(0, 0, 0, 0.25);
        }
        html, body, [class*="css"]  {
            font-family: "Inter", "Segoe UI", sans-serif;
        }
        .stApp {
            background: var(--main);
            color: var(--text);
        }
        header[data-testid="stHeader"], [data-testid="stSidebar"], #MainMenu {
            display: none !important;
        }
        [data-testid="stAppViewContainer"] {
            background: transparent;
        }
        .block-container {
            max-width: 1512px;
            padding-top: 3.75rem;
            padding-bottom: 4rem;
            padding-left: clamp(1.4rem, 5.5vw, 5.25rem);
            padding-right: clamp(1.4rem, 5.5vw, 5.25rem);
        }
        h1, h2, h3, h4 {
            font-family: "Bebas Neue", "Arial Narrow", sans-serif;
            color: var(--text);
            letter-spacing: 0;
        }
        p, li, label, div, span {
            color: var(--text);
        }
        .home-frame {
            min-height: 48.5rem;
        }
        .home-title-card {
            width: min(100%, 16.5rem);
            height: 4rem;
            margin: 0 auto 3.15rem auto;
            display: grid;
            place-items: center;
            background: var(--paper);
            border: 1px solid var(--line);
            box-shadow: var(--shadow-soft);
            font-family: "Bebas Neue", "Arial Narrow", sans-serif;
            font-size: 2.5rem;
            line-height: 1;
            letter-spacing: 0.03em;
            text-transform: uppercase;
        }
        .home-grid {
            display: grid;
            grid-template-columns: minmax(18rem, 38.3rem) 25rem;
            column-gap: clamp(3rem, 11vw, 11.8rem);
            align-items: start;
        }
        .home-nav-stack {
            display: grid;
            gap: 0;
        }
        .home-nav-stack div[data-testid="stButton"] {
            margin-bottom: 3.35rem;
        }
        .home-nav-stack div[data-testid="stButton"] button {
            min-height: 5.7rem;
            font-size: 2rem;
        }
        .figma-link {
            width: 100%;
            min-height: 3.35rem;
            display: grid;
            place-items: center;
            border: 1px solid var(--line);
            background: var(--paper);
            color: var(--text) !important;
            box-shadow: var(--shadow-soft);
            font-family: "Bebas Neue", "Arial Narrow", sans-serif;
            font-size: 1.35rem;
            line-height: 1;
            letter-spacing: 0.02em;
            text-transform: uppercase;
            text-decoration: none !important;
            transition: transform 140ms ease, border-color 140ms ease, box-shadow 140ms ease;
        }
        .figma-link:hover {
            transform: translateY(-1px);
            border-color: var(--line-strong);
            color: var(--text) !important;
            box-shadow: var(--shadow-deep);
        }
        .figma-link.is-active {
            border-color: var(--line-strong);
        }
        .home-poster {
            width: min(100%, 25rem);
            height: 35rem;
            object-fit: cover;
            display: block;
        }
        .page-topbar {
            display: grid;
            grid-template-columns: minmax(13rem, 20rem) minmax(0, 1fr);
            gap: 1.5rem;
            align-items: start;
            margin-bottom: 2.2rem;
        }
        .page-title-card {
            min-height: 4rem;
            display: grid;
            place-items: center;
            background: var(--paper);
            border: 1px solid var(--line);
            box-shadow: var(--shadow-soft);
            font-family: "Bebas Neue", "Arial Narrow", sans-serif;
            font-size: 2.5rem;
            line-height: 1;
            letter-spacing: 0.03em;
            text-transform: uppercase;
        }
        .page-nav-row {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 0.75rem;
        }
        .rail-kicker, .eyebrow {
            text-transform: uppercase;
            letter-spacing: 0.06em;
            font-size: 0.78rem;
            font-weight: 700;
            color: var(--muted-soft);
            font-family: "Inter", "Segoe UI", sans-serif;
        }
        .rail-title {
            margin-top: 0.5rem;
            font-family: "Bebas Neue", "Arial Narrow", sans-serif;
            font-size: 1.25rem;
            line-height: 1;
            font-weight: 400;
            letter-spacing: 0.03em;
        }
        .rail-copy, .subtle-text, .stCaption {
            color: var(--muted) !important;
            line-height: 1.6;
            font-family: "Inter", "Segoe UI", sans-serif;
        }
        .hero-shell {
            padding: 1.25rem 0 0.75rem 0;
            margin-bottom: 1.2rem;
            border: 1px solid var(--line);
            border-left: 0;
            border-right: 0;
        }
        .hero-label {
            text-transform: uppercase;
            letter-spacing: 0.06em;
            font-size: 0.78rem;
            font-weight: 700;
            color: var(--muted-soft);
            font-family: "Inter", "Segoe UI", sans-serif;
        }
        .hero-title {
            margin: 0.45rem 0 0.65rem 0;
            font-family: "Bebas Neue", "Arial Narrow", sans-serif;
            font-size: clamp(2.35rem, 5vw, 4.25rem);
            line-height: 0.95;
            letter-spacing: 0.02em;
        }
        .hero-subtitle {
            max-width: 54rem;
            font-size: 0.98rem;
            color: var(--muted);
            line-height: 1.65;
            font-family: "Inter", "Segoe UI", sans-serif;
        }
        .hero-meta-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.55rem;
            margin-top: 1rem;
        }
        .hero-chip {
            border: 1px solid var(--line);
            color: var(--text);
            background: var(--paper);
            border-radius: 0;
            padding: 0.5rem 0.8rem;
            font-size: 0.82rem;
            font-weight: 600;
            font-family: "Inter", "Segoe UI", sans-serif;
        }
        .section-shell {
            padding: 1.2rem 0 0.2rem 0;
        }
        .section-title {
            font-family: "Bebas Neue", "Arial Narrow", sans-serif;
            font-size: 2rem;
            line-height: 1;
            letter-spacing: 0.02em;
            margin-bottom: 0.2rem;
        }
        .section-copy {
            color: var(--muted);
            max-width: 48rem;
            line-height: 1.7;
            margin-bottom: 1rem;
            font-family: "Inter", "Segoe UI", sans-serif;
        }
        .metric-card {
            min-height: 8.1rem;
            padding: 1rem 0.1rem 1.1rem 0.1rem;
            border-top: 1px solid var(--line);
        }
        .metric-label {
            color: var(--muted-soft);
            text-transform: uppercase;
            letter-spacing: 0.06em;
            font-size: 0.71rem;
            font-weight: 700;
            margin-bottom: 0.7rem;
            font-family: "Inter", "Segoe UI", sans-serif;
        }
        .metric-value {
            font-family: "Bebas Neue", "Arial Narrow", sans-serif;
            font-size: 2.65rem;
            line-height: 0.95;
            font-weight: 400;
            color: var(--text);
        }
        .metric-foot {
            margin-top: 0.75rem;
            color: var(--muted);
            font-size: 0.88rem;
            line-height: 1.55;
            font-family: "Inter", "Segoe UI", sans-serif;
        }
        .panel-card, .doc-card, .stance-card, .note-card {
            margin-bottom: 0.9rem;
            padding: 1rem 0 1rem 0;
            border-top: 1px solid var(--line);
        }
        .panel-title {
            font-family: "Bebas Neue", "Arial Narrow", sans-serif;
            font-size: 1.35rem;
            line-height: 1;
            font-weight: 400;
            letter-spacing: 0.02em;
            margin-bottom: 0.35rem;
        }
        .doc-card {
            border-top-width: 1px;
        }
        .doc-title {
            font-family: "Inter", "Segoe UI", sans-serif;
            font-size: 1rem;
            font-weight: 700;
            margin: 0.25rem 0 0.4rem 0;
            line-height: 1.35;
        }
        .doc-meta, .caption-line {
            color: var(--muted);
            font-size: 0.84rem;
            font-family: "Inter", "Segoe UI", sans-serif;
        }
        .doc-body {
            color: var(--text);
            line-height: 1.65;
            font-size: 0.95rem;
            font-family: "Inter", "Segoe UI", sans-serif;
        }
        .topic-anchor {
            color: var(--text);
            text-decoration: none;
        }
        .topic-anchor:hover {
            color: var(--accent);
        }
        .pill {
            display: inline-block;
            padding: 0.42rem 0.78rem;
            margin: 0 0.45rem 0.45rem 0;
            border-radius: 0;
            border: 1px solid var(--line);
            background: var(--paper);
            color: var(--text);
            font-size: 0.78rem;
            font-weight: 600;
            font-family: "Inter", "Segoe UI", sans-serif;
        }
        .topic-summary-line strong {
            color: var(--text);
        }
        .stPlotlyChart, div[data-testid="stDataFrame"] {
            background: var(--paper);
            border: 1px solid var(--line);
            border-radius: 0;
            padding: 0.9rem 0.9rem 0.25rem 0.9rem;
            box-shadow: var(--shadow-soft);
        }
        .topic-table-shell {
            background: var(--paper);
            border: 1px solid var(--line);
            border-radius: 0;
            padding: 1rem 1rem 0.8rem 1rem;
            overflow-x: auto;
            margin-bottom: 1rem;
            box-shadow: var(--shadow-soft);
        }
        .topic-table {
            width: 100%;
            min-width: 1180px;
            border-collapse: separate;
            border-spacing: 0;
            color: var(--text);
            font-size: 0.93rem;
        }
        .topic-table thead th {
            padding: 0.9rem 0.85rem;
            text-align: left;
            background: #f8f4ee;
            color: var(--text);
            font-weight: 700;
            border-bottom: 1px solid var(--line);
            white-space: nowrap;
            font-family: "Inter", "Segoe UI", sans-serif;
        }
        .topic-table thead th:first-child {
            border-top-left-radius: 0;
        }
        .topic-table thead th:last-child {
            border-top-right-radius: 0;
        }
        .topic-table tbody td {
            padding: 0.9rem 0.85rem;
            border-bottom: 1px solid #e6ddd1;
            vertical-align: top;
            background: #ffffff;
            font-family: "Inter", "Segoe UI", sans-serif;
        }
        .topic-table tbody tr:nth-child(even) td {
            background: #fbf8f4;
        }
        .topic-table tbody td.num {
            text-align: right;
            font-variant-numeric: tabular-nums;
            white-space: nowrap;
        }
        .topic-table tbody td.wrap {
            min-width: 220px;
        }
        .method-row {
            display: grid;
            grid-template-columns: 64px minmax(0, 1fr);
            gap: 1rem;
            padding: 1.25rem 0;
            border-top: 1px solid var(--line);
        }
        .method-index {
            width: 56px;
            height: 56px;
            border-radius: 0;
            display: grid;
            place-items: center;
            background: var(--paper);
            border: 1px solid var(--line-strong);
            color: var(--text);
            font-family: "Bebas Neue", "Arial Narrow", sans-serif;
            font-size: 1.5rem;
            font-weight: 400;
            letter-spacing: 0.02em;
            box-shadow: var(--shadow-soft);
        }
        .method-title {
            font-family: "Bebas Neue", "Arial Narrow", sans-serif;
            font-size: 1.45rem;
            line-height: 1;
            font-weight: 400;
            letter-spacing: 0.02em;
            margin-bottom: 0.35rem;
        }
        .method-list {
            margin: 0;
            padding-left: 1.05rem;
            color: var(--muted);
        }
        .method-list li {
            margin-bottom: 0.45rem;
            line-height: 1.65;
        }
        .note-grid {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 0.9rem 1.5rem;
        }
        .note-card {
            margin-bottom: 0;
        }
        div[data-testid="stButton"] button {
            width: 100%;
            min-height: 3.35rem;
            border-radius: 0;
            border: 1px solid var(--line) !important;
            background: var(--paper) !important;
            color: var(--text) !important;
            font-family: "Bebas Neue", "Arial Narrow", sans-serif;
            font-size: 1.35rem;
            font-weight: 400;
            letter-spacing: 0.02em;
            text-transform: uppercase;
            transition: transform 140ms ease, background 140ms ease, border-color 140ms ease, box-shadow 140ms ease;
            box-shadow: var(--shadow-soft);
        }
        div[data-testid="stButton"] button:hover {
            transform: translateY(-1px);
            border-color: var(--line-strong) !important;
            background: var(--paper) !important;
            box-shadow: var(--shadow-deep);
        }
        div[data-testid="stButton"] button * {
            color: inherit !important;
        }
        div[data-testid="stButton"] button[kind="primary"] {
            background: var(--paper) !important;
            color: var(--text) !important;
            border-color: var(--line-strong) !important;
            box-shadow: var(--shadow-soft);
        }
        div[data-testid="stButton"] button[kind="primary"] p,
        div[data-testid="stButton"] button[kind="primary"] span,
        div[data-testid="stButton"] button[kind="primary"] div {
            color: var(--text) !important;
        }
        div[data-testid="stButton"] button[kind="secondary"] p,
        div[data-testid="stButton"] button[kind="secondary"] span,
        div[data-testid="stButton"] button[kind="secondary"] div {
            color: var(--text) !important;
        }
        a, a:visited {
            color: var(--text);
        }
        a:hover {
            color: var(--accent);
        }
        .topic-detail-section {
            margin-top: 0.15rem;
            margin-bottom: 0.9rem;
        }
        .stSelectbox label {
            color: var(--muted-soft);
            text-transform: uppercase;
            font-size: 0.73rem;
            letter-spacing: 0.06em;
            font-weight: 700;
            font-family: "Inter", "Segoe UI", sans-serif;
        }
        .stSelectbox [data-baseweb="select"] > div {
            min-height: 3.05rem;
            border-radius: 0;
            border-color: var(--line-strong);
            background: var(--paper);
        }
        .stInfo {
            border-radius: 0;
            border: 1px solid var(--line);
            background: var(--paper);
            color: var(--text);
        }
        div[data-testid="stMarkdownContainer"] p,
        div[data-testid="stMarkdownContainer"] li,
        div[data-testid="stMarkdownContainer"] span:not(.hero-chip):not(.pill),
        div[data-testid="stMarkdownContainer"] label,
        div[data-testid="stMarkdownContainer"] td,
        div[data-testid="stMarkdownContainer"] th {
            font-family: "Inter", "Segoe UI", sans-serif;
        }
        @media (max-width: 1100px) {
            .block-container {
                padding-left: 1rem;
                padding-right: 1rem;
            }
            .home-grid,
            .page-topbar {
                grid-template-columns: 1fr;
            }
            .home-grid {
                row-gap: 2rem;
            }
            .home-poster {
                width: min(100%, 25rem);
                height: auto;
                justify-self: start;
            }
            .page-nav-row {
                grid-template-columns: repeat(2, minmax(0, 1fr));
            }
            .note-grid {
                grid-template-columns: 1fr;
            }
        }
        @media (max-width: 700px) {
            .block-container {
                padding-top: 2rem;
            }
            .home-title-card,
            .page-title-card {
                font-size: 2.15rem;
            }
            .home-nav-stack {
                gap: 1rem;
            }
            .home-nav-stack div[data-testid="stButton"] {
                margin-bottom: 0;
            }
            .home-nav-stack div[data-testid="stButton"] button {
                min-height: 4.5rem;
            }
            .page-nav-row {
                grid-template-columns: 1fr;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def metric_card(label: str, value: str, foot: str) -> None:
    label_text = html.escape(label)
    value_text = html.escape(value)
    foot_text = html.escape(foot)
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">{label_text}</div>
            <div class="metric-value">{value_text}</div>
            <div class="metric-foot">{foot_text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def panel_card(title: str, body: str) -> None:
    title_text = html.escape(title)
    body_text = html.escape(body)
    st.markdown(
        f"""
        <div class="panel-card">
            <div class="panel-title">{title_text}</div>
            <div class="subtle-text">{body_text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def doc_card(doc: dict, *, label: str | None = None) -> None:
    badge = html.escape(label or doc["source_type"].replace("_", " ").title())
    title_text = html.escape(doc["title"])
    author_text = html.escape(doc.get("author_name", ""))
    excerpt_text = html.escape(doc.get("excerpt", ""))
    created_text = html.escape(doc.get("created_at", ""))
    title_html = (
        f'<a class="topic-anchor" href="{html.escape(doc["reddit_url"])}" target="_blank">{title_text}</a>'
        if doc.get("reddit_url")
        else title_text
    )
    st.markdown(
        f"""
        <div class="doc-card">
            <span class="pill">{badge}</span>
            <div class="doc-title">{title_html}</div>
            <div class="doc-meta">{created_text} • {author_text} • score {doc.get("score", 0)}</div>
            <div class="doc-body">{excerpt_text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def format_int(value: int) -> str:
    return f"{value:,}"


def apply_chart_style(figure: go.Figure, *, height: int, legend_y: float = 1.1) -> go.Figure:
    figure.update_layout(
        template="simple_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": CHART_COLORS["ink_soft"], "family": "Inter, sans-serif"},
        margin={"l": 18, "r": 18, "t": 24, "b": 18},
        legend={
            "orientation": "h",
            "y": legend_y,
            "x": 0.0,
            "title": None,
            "font": {"size": 11, "color": CHART_COLORS["ink_soft"]},
        },
        height=height,
    )
    figure.update_xaxes(
        showgrid=False,
        linecolor=CHART_COLORS["grid"],
        tickfont={"color": CHART_COLORS["muted"]},
        title_font={"color": CHART_COLORS["ink_soft"]},
    )
    figure.update_yaxes(
        gridcolor=CHART_COLORS["grid"],
        zeroline=False,
        tickfont={"color": CHART_COLORS["muted"]},
        title_font={"color": CHART_COLORS["ink_soft"]},
    )
    return figure


def build_activity_chart(monthly_activity: list[dict]) -> go.Figure:
    frame = pd.DataFrame(monthly_activity)
    frame["month_start"] = pd.to_datetime(frame["month_start"])
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=frame["month_start"],
            y=frame["posts"],
            mode="lines+markers",
            name="Posts",
            line={"color": CHART_COLORS["ink"], "width": 3},
            marker={"size": 7, "color": CHART_COLORS["ink"]},
        )
    )
    figure.add_trace(
        go.Scatter(
            x=frame["month_start"],
            y=frame["comments"],
            mode="lines+markers",
            name="Comments",
            line={"color": CHART_COLORS["accent"], "width": 2.75},
            marker={"size": 6, "color": CHART_COLORS["accent"]},
        )
    )
    figure.add_trace(
        go.Scatter(
            x=frame["month_start"],
            y=frame["modeled_documents"],
            mode="lines+markers",
            name="Hybrid Topic Corpus",
            line={"color": CHART_COLORS["line_soft"], "width": 2.75, "dash": "dot"},
            marker={"size": 7, "color": CHART_COLORS["line_soft"]},
        )
    )
    figure.update_layout(xaxis_title="Month", yaxis_title="Count")
    return apply_chart_style(figure, height=360, legend_y=1.14)


def build_topic_share_chart(topics: list[dict]) -> go.Figure:
    frame = pd.DataFrame(
        [
            {
                "topic_label": f"{topic['topic_id']}: {topic['topic_label']}",
                "document_share_pct": topic["document_share_pct"],
                "major_topic": "Major stance topic" if topic["major_topic"] else "Other topic",
            }
            for topic in sorted(topics, key=lambda item: item["document_share_pct"], reverse=True)
        ]
    )
    figure = px.bar(
        frame,
        x="document_share_pct",
        y="topic_label",
        color="major_topic",
        orientation="h",
        color_discrete_map={
            "Major stance topic": CHART_COLORS["ink"],
            "Other topic": CHART_COLORS["accent_soft"],
        },
        labels={"document_share_pct": "Document share (%)", "topic_label": ""},
    )
    figure.update_layout(showlegend=False)
    figure.update_yaxes(categoryorder="total ascending", tickfont={"color": CHART_COLORS["ink_soft"]})
    figure.update_traces(
        hovertemplate="%{y}<br>Document share: %{x:.1f}%<extra></extra>",
        marker_line_color="#cbbeb0",
        marker_line_width=0.6,
    )
    return apply_chart_style(figure, height=430)


def build_topic_detail_chart(topic: dict) -> go.Figure:
    frame = pd.DataFrame(topic["monthly_trend"])
    frame["month_start"] = pd.to_datetime(frame["month_start"])
    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=frame["month_start"],
            y=frame["document_count"],
            name="Document count",
            marker_color=CHART_COLORS["fill"],
            marker_line={"color": "#c8baa9", "width": 1},
        )
    )
    figure.add_trace(
        go.Scatter(
            x=frame["month_start"],
            y=frame["document_share"],
            name="Document share (%)",
            mode="lines+markers",
            yaxis="y2",
            line={"color": CHART_COLORS["ink"], "width": 3},
            marker={"size": 7, "color": CHART_COLORS["accent"]},
        )
    )
    figure.update_layout(
        xaxis_title="Month",
        yaxis_title="Document count",
        yaxis2={
            "title": "Share (%)",
            "overlaying": "y",
            "side": "right",
            "tickfont": {"color": CHART_COLORS["muted"]},
            "title_font": {"color": CHART_COLORS["ink_soft"]},
        },
    )
    return apply_chart_style(figure, height=430, legend_y=1.14)


def build_source_mix_chart(topic: dict) -> go.Figure:
    frame = pd.DataFrame(
        [
            {"source": "Posts", "count": topic["post_documents"]},
            {"source": "Comments", "count": topic["comment_documents"]},
        ]
    )
    figure = px.pie(
        frame,
        values="count",
        names="source",
        hole=0.62,
        color="source",
        color_discrete_map={"Posts": CHART_COLORS["ink"], "Comments": CHART_COLORS["accent"]},
    )
    figure.update_layout(
        template="simple_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": CHART_COLORS["ink_soft"], "family": "Inter, sans-serif"},
        margin={"l": 0, "r": 0, "t": 18, "b": 12},
        showlegend=True,
        legend={"orientation": "h", "y": -0.08, "x": 0.06, "title": None, "font": {"size": 11}},
        height=340,
    )
    figure.update_traces(
        textposition="outside",
        textinfo="label+percent",
        textfont={"color": CHART_COLORS["ink"], "size": 12},
        marker={"line": {"color": "#f3eee7", "width": 2}},
        hovertemplate="%{label}: %{value:,} documents (%{percent})<extra></extra>",
    )
    return figure


def render_topic_table(rows: list[dict]) -> None:
    headers = [
        ("Topic", ""),
        ("Label", "wrap"),
        ("Top keywords", "wrap"),
        ("Document share (%)", "num"),
        ("Post share among modeled posts (%)", "num"),
        ("Post share among all scraped posts (%)", "num"),
        ("Documents", "num"),
        ("Posts", "num"),
        ("Comments", "num"),
        ("Trend", ""),
        ("Major topic", ""),
        ("Stance", ""),
    ]
    body_rows: list[str] = []
    for row in rows:
        cells = [
            ("num", str(row["topic_id"])),
            ("wrap", html.escape(str(row["topic_label"]))),
            ("wrap", html.escape(str(row["top_keywords"]))),
            ("num", f"{float(row['document_share_pct']):.1f}"),
            ("num", f"{float(row['modeled_post_share_pct']):.1f}"),
            ("num", f"{float(row['corpus_post_share_pct']):.2f}"),
            ("num", format_int(int(row["document_count"]))),
            ("num", format_int(int(row["post_documents"]))),
            ("num", format_int(int(row["comment_documents"]))),
            ("", html.escape(str(row["trend_label"]))),
            ("", "Yes" if bool(row["major_topic"]) else "No"),
            ("", html.escape(str(row["stance_status"]))),
        ]
        body_rows.append(
            "<tr>"
            + "".join(f'<td class="{klass}">{value}</td>' for klass, value in cells)
            + "</tr>"
        )

    header_html = "".join(f"<th>{html.escape(label)}</th>" for label, _ in headers)
    table_html = (
        '<div class="topic-table-shell"><table class="topic-table">'
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table></div>"
    )
    st.markdown(table_html, unsafe_allow_html=True)


def render_section_intro(title: str, copy: str) -> None:
    st.markdown(
        f"""
        <div class="section-shell">
            <div class="section-title">{html.escape(title)}</div>
            <div class="section-copy">{html.escape(copy)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_link_row(current_page: str) -> None:
    links = []
    for page in PAGES:
        active_class = " is-active" if page == current_page else ""
        links.append(f'<a class="figma-link{active_class}" href="{page_url(page)}" target="_self">{html.escape(page)}</a>')
    st.markdown(f'<div class="page-nav-row">{"".join(links)}</div>', unsafe_allow_html=True)


def render_cache_selector(available_caches: list, current_key: str) -> str:
    current = current_key
    labels = {option.key: option.label for option in available_caches}
    options = [option.key for option in available_caches]
    if len(options) <= 1:
        return current

    st.markdown('<div class="eyebrow">Era</div>', unsafe_allow_html=True)
    selector_cols = st.columns(len(options))
    for column, option_key in zip(selector_cols, options):
        with column:
            label = labels[option_key].replace(": ", "\n", 1)
            if st.button(
                label,
                key=f"cache_{option_key}",
                use_container_width=True,
                type="primary" if option_key == current_key else "secondary",
            ):
                current = option_key
    return current


def render_page_header(snapshot: dict, available_caches: list, selected_cache_key: str, page: str) -> str:
    cache_label = snapshot["meta"].get("cache_label", "Default cache")
    window_label = snapshot["stats"]["window_label"]
    source = snapshot["meta"].get("cache_source", "unknown")
    st.markdown(
        f"""
        <div class="page-topbar">
            <div class="page-title-card">{html.escape(page)}</div>
            <div class="page-nav-row">{''.join(
                f'<a class="figma-link{" is-active" if nav_page == page else ""}" href="{page_url(nav_page)}" target="_self">{html.escape(nav_page)}</a>'
                for nav_page in PAGES
            )}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    selected_cache_key = render_cache_selector(available_caches, selected_cache_key)
    st.markdown(
        f"""
        <div class="hero-shell">
            <div class="hero-label">Fitness Research Dashboard</div>
            <div class="hero-title">{html.escape(APP_TITLE)}</div>
            <div class="hero-subtitle">
                {html.escape(snapshot["meta"].get("cache_help_text", snapshot["stats"]["corpus_summary"]))}
            </div>
            <div class="hero-meta-row">
                <span class="hero-chip">{html.escape(cache_label)}</span>
                <span class="hero-chip">{html.escape(window_label)}</span>
                <span class="hero-chip">Source: {html.escape(source)}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    return selected_cache_key


def render_home() -> None:
    image_path = get_hero_image_path()
    poster_html = ""
    if image_path is not None:
        poster_html = (
            f'<img class="home-poster" alt="Yeah Buddy fitness poster" '
            f'src="{get_image_data_uri(image_path)}">'
        )
    else:
        poster_html = '<div class="home-poster"></div>'

    st.markdown('<div class="home-title-card">Fitness</div>', unsafe_allow_html=True)
    left, spacer, right = st.columns([613, 188, 400], gap="small")
    with left:
        st.markdown('<div class="home-nav-stack">', unsafe_allow_html=True)
        for page, label in (
            ("Overview", "Overview"),
            ("Topics", "Topics"),
            ("Topic Detail", "Topic Details"),
            ("Methods", "Methods"),
        ):
            if st.button(label, key=f"home_{page}", use_container_width=True):
                st.session_state.page = page
                st.query_params["page"] = page
                st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
    with spacer:
        st.empty()
    with right:
        st.markdown(poster_html, unsafe_allow_html=True)


def render_overview(snapshot: dict) -> None:
    if not snapshot["topics"] or not snapshot["overview"]["monthly_activity"]:
        st.info("Pipeline outputs are not available yet. Run the Part 1 stages and rebuild the app cache.")
        return

    stats = snapshot["stats"]
    overview = snapshot["overview"]
    render_section_intro(
        "Selected KPIs",
        "The first row now keeps only the counts that help you orient quickly before you move into the charts.",
    )
    metric_columns = st.columns(6)
    cards = [
        ("Posts", format_int(stats["total_posts"]), "Raw corpus posts in SQLite"),
        ("Comments", format_int(stats["total_comments"]), "Raw corpus comments in SQLite"),
        ("Authors", format_int(stats["total_authors"]), "Non-deleted authors in corpus"),
        ("Topics", str(stats["total_topics"]), "Final hybrid topic count"),
        ("Major Topics", str(stats["major_topic_count"]), "Validated for stance reading"),
        ("Hybrid Docs", format_int(stats["modeled_documents"]), "Documents used in the model space"),
    ]
    for column, card in zip(metric_columns, cards):
        with column:
            metric_card(*card)

    trend_col, insight_col = st.columns([1.55, 0.95], gap="large")
    with trend_col:
        render_section_intro(
            "Activity Across the Window",
            "Posts, comments, and the modeled hybrid corpus are separated more clearly so the signal stays readable against the warm background.",
        )
        st.plotly_chart(build_activity_chart(overview["monthly_activity"]), use_container_width=True, config=PLOTLY_CONFIG)
    with insight_col:
        render_section_intro(
            "What Stands Out",
            "A short interpretation panel replaces the older stack of heavy cards.",
        )
        highlights = overview["activity_highlights"]
        panel_card("Corpus summary", stats["corpus_summary"])
        panel_card("Peak posting month", f"{highlights['peak_posts_month']['month_start']}: {format_int(highlights['peak_posts_month']['count'])} posts")
        panel_card("Peak comment month", f"{highlights['peak_comments_month']['month_start']}: {format_int(highlights['peak_comments_month']['count'])} comments")
        panel_card(
            "Peak modeled month",
            f"{highlights['peak_modeled_month']['month_start']}: {format_int(highlights['peak_modeled_month']['count'])} modeled documents",
        )

    share_col, list_col = st.columns([1.2, 1.0], gap="large")
    with share_col:
        render_section_intro(
            "Topic Share",
            "Major stance topics are now visually distinct from the rest, while labels and axes stay darker for legibility.",
        )
        st.plotly_chart(build_topic_share_chart(snapshot["topics"]), use_container_width=True, config=PLOTLY_CONFIG)
    with list_col:
        render_section_intro(
            "Top Topics",
            "This summary keeps the comparison compact so you can scan prevalence, post share, and trend label in one pass.",
        )
        for topic in overview["top_topics"]:
            st.markdown(
                f"""
                <div class="panel-card">
                    <div class="panel-title">Topic {topic['topic_id']} • {html.escape(topic['topic_label'])}</div>
                    <div class="topic-summary-line">
                        <strong>{topic['document_share_pct']:.1f}%</strong> modeled documents •
                        <strong>{topic['corpus_post_share_pct']:.2f}%</strong> scraped posts •
                        <strong>{format_int(topic['post_documents'])}</strong> post docs •
                        <strong>{html.escape(topic['trend_label'])}</strong> •
                        <strong>{'major stance topic' if topic['major_topic'] else 'topic-only'}</strong>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_topics(snapshot: dict) -> None:
    if not snapshot["topics"]:
        st.info("No topic outputs are available yet.")
        return
    render_section_intro(
        "Topic Comparison",
        "Document share reflects the hybrid modeling space. Post-share columns stay visible so the assignment framing remains easy to compare.",
    )
    render_topic_table(snapshot["topic_table"])
    st.plotly_chart(build_topic_share_chart(snapshot["topics"]), use_container_width=True, config=PLOTLY_CONFIG)


def render_topic_detail(snapshot: dict) -> None:
    topics = snapshot["topics"]
    if not topics:
        st.info("No topic data is available yet.")
        return

    render_section_intro(
        "Topic Detail",
        "Choose one topic, then read its scale, monthly movement, source mix, and stance interpretation on a single consistent surface.",
    )
    options = {f"Topic {topic['topic_id']} • {topic['topic_label']}": topic for topic in topics}
    selected_key = st.selectbox("Select a topic", list(options.keys()))
    topic = options[selected_key]
    stance = topic["stance"]

    top_line = st.columns(5)
    metrics = [
        ("Document share", f"{topic['document_share_pct']:.1f}%", "Share of hybrid documents"),
        ("Corpus post share", f"{topic['corpus_post_share_pct']:.2f}%", "Share of all scraped posts"),
        ("Modeled post share", f"{topic['modeled_post_share_pct']:.1f}%", "Share of model-ready posts"),
        ("Posts", format_int(topic["post_documents"]), "Post documents inside topic"),
        ("Comments", format_int(topic["comment_documents"]), "Comment documents inside topic"),
    ]
    for column, item in zip(top_line, metrics):
        with column:
            metric_card(*item)

    st.markdown('<div class="topic-detail-section">', unsafe_allow_html=True)
    st.markdown(" ".join(f'<span class="pill">{html.escape(term)}</span>' for term in topic["keyword_terms"][:8]), unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    trend_col, semantics_col = st.columns([0.8, 1.2], gap="large")
    with trend_col:
        panel_card("Trend label", f"{topic['trend_label'].title()} • Coverage {topic['month_coverage_ratio']:.1f}% of months")
    with semantics_col:
        panel_card(
            "How to read the share values",
            (
                f"This topic contains {format_int(topic['document_count'])} modeled documents, including "
                f"{format_int(topic['post_documents'])} posts. Use document share for the actual modeling space "
                f"and post share when you want the post-prevalence framing from the assignment."
            ),
        )

    render_section_intro(
        "Monthly Trend",
        "Bars show volume. The line tracks share, which makes it easier to distinguish genuinely stronger months from noisy absolute counts.",
    )
    st.plotly_chart(build_topic_detail_chart(topic), use_container_width=True, config=PLOTLY_CONFIG)

    mix_col, semantics_col = st.columns([0.9, 1.1], gap="large")
    with mix_col:
        render_section_intro("Source Mix", "The donut labels sit outside the chart now so the proportions stay readable.")
        st.plotly_chart(build_source_mix_chart(topic), use_container_width=True, config=PLOTLY_CONFIG)
    with semantics_col:
        render_section_intro("Interpretation", "This keeps the statistical context near the chart instead of burying it below the fold.")
        panel_card(
            "Topic semantics",
            f"This topic is labeled as {topic['trend_label']}. Average assignment confidence is {topic['average_assignment_confidence']:.3f}, and the peak-to-median monthly ratio is {topic['peak_to_median_ratio']:.2f}.",
        )

    render_section_intro("Representative Documents", "These examples stay plain and readable so the text does the work.")
    for doc in topic["representative_documents"]:
        doc_card(doc)

    render_section_intro(
        "Stance View",
        "The stance section is folded into the same visual language rather than looking like a separate tool.",
    )
    if not stance["available"]:
        st.markdown(
            f"""
            <div class="stance-card">
                <div class="panel-title">No stance analysis for Topic {topic['topic_id']}</div>
                <div class="subtle-text">{html.escape(stance['detail_note'])}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    title = "Neutral discussion-pattern summaries" if stance["mode"] == "neutral_patterns" else "Cluster-style stance summaries"
    st.markdown(
        f"""
        <div class="stance-card">
            <div class="panel-title">{title}</div>
            <div class="subtle-text">{html.escape(stance['detail_note'])}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    outcome = stance["metrics"]
    summary_columns = st.columns(4)
    summary_cards = [
        ("Analyzed comments", format_int(outcome["analyzed_comments"]), "Comments clustered for stance"),
        ("Minority share", f"{outcome['minority_share'] * 100:.1f}%", "Smaller side of the split"),
        ("Silhouette", f"{outcome['silhouette_score']:.4f}", "Higher is cleaner separation"),
        ("Centroid similarity", f"{outcome['centroid_similarity']:.4f}", "Higher means more overlap"),
    ]
    for column, card in zip(summary_columns, summary_cards):
        with column:
            metric_card(*card)

    for summary in stance["summaries"]:
        panel_card(summary["stance_label"].replace("_", " ").title(), summary["summary_text"])
        for comment in summary["representative_comments"]:
            doc_card(comment, label="Representative comment")


def render_methods(snapshot: dict) -> None:
    if not snapshot["methods"]["sections"]:
        st.info("Methods content will appear here after the pipeline artifacts are available.")
        return

    methods = snapshot["methods"]
    render_section_intro(
        "Methods",
        "This page now reads like a short process walkthrough: what was collected, how the topic model was built, and how stance claims are kept cautious.",
    )
    for index, section in enumerate(methods["sections"], start=1):
        items_html = "".join(f"<li>{html.escape(item)}</li>" for item in section["items"])
        st.markdown(
            f"""
            <div class="method-row">
                <div class="method-index">{index}</div>
                <div>
                    <div class="method-title">{html.escape(section['title'])}</div>
                    <ul class="method-list">{items_html}</ul>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    render_section_intro(
        "Interpretation Notes",
        "These notes stay visible as caveats and reading guidance, not as dense blocks of repeated prose.",
    )
    notes_html = "".join(
        f'<div class="note-card"><div class="subtle-text">{html.escape(note)}</div></div>'
        for note in methods["notes"]
    )
    st.markdown(f'<div class="note-grid">{notes_html}</div>', unsafe_allow_html=True)


def main() -> None:
    apply_theme()
    requested_page = st.query_params.get("page")
    if isinstance(requested_page, list):
        requested_page = requested_page[0] if requested_page else None
    if requested_page not in PAGES:
        requested_page = None

    available_caches = list_available_caches()
    default_cache_key = resolve_cache_key()

    if "page" not in st.session_state:
        st.session_state.page = requested_page or ""
    elif requested_page is not None:
        st.session_state.page = requested_page
    else:
        st.session_state.page = ""
    if "cache_key" not in st.session_state:
        st.session_state.cache_key = default_cache_key

    available_keys = {option.key for option in available_caches}
    if st.session_state.cache_key not in available_keys:
        st.session_state.cache_key = default_cache_key

    snapshot = load_dashboard_snapshot(st.session_state.cache_key)

    if not st.session_state.page:
        render_home()
        return

    updated_cache_key = render_page_header(snapshot, available_caches, st.session_state.cache_key, st.session_state.page)
    if updated_cache_key != st.session_state.cache_key:
        st.session_state.cache_key = updated_cache_key
        snapshot = load_dashboard_snapshot(st.session_state.cache_key)

    if st.session_state.page == "Overview":
        render_overview(snapshot)
    elif st.session_state.page == "Topics":
        render_topics(snapshot)
    elif st.session_state.page == "Topic Detail":
        render_topic_detail(snapshot)
    else:
        render_methods(snapshot)


if __name__ == "__main__":
    main()
