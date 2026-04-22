from __future__ import annotations

import html
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from fitness_reddit_analyzer.app_data import list_available_caches, load_dashboard_snapshot, resolve_cache_key
from fitness_reddit_analyzer.config import APP_TITLE


st.set_page_config(page_title=APP_TITLE, layout="wide")

PLOTLY_CONFIG = {"displayModeBar": False, "responsive": True}
HERO_IMAGE_CANDIDATES = (
    Path("/Users/arvindsrinivass/Downloads/2b530d0302e87d964541b0765ec5f52b.jpg"),
    Path("/Users/arvindsrinivass/Desktop/Screenshot 2026-04-22 at 12.52.07 PM.png"),
)
PAGES = ("Overview", "Topics", "Topic Detail", "Methods")

CHART_COLORS = {
    "ink": "#181613",
    "ink_soft": "#403a35",
    "muted": "#7a726a",
    "grid": "#d9d0c4",
    "line_soft": "#8f877e",
    "accent": "#b34f35",
    "accent_soft": "#d8b7a8",
    "fill": "#ece4db",
}


def get_hero_image_path() -> Path | None:
    for candidate in HERO_IMAGE_CANDIDATES:
        if candidate.exists():
            return candidate
    return None


def apply_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=Sora:wght@600;700;800&display=swap');
        :root {
            --bg: #f3eee7;
            --bg-alt: #efe7dc;
            --paper: rgba(255, 251, 247, 0.82);
            --paper-strong: rgba(255, 251, 247, 0.94);
            --line: #d7cec1;
            --line-strong: #c2b6a8;
            --text: #171411;
            --muted: #6e665f;
            --muted-soft: #8b8278;
            --accent: #b34f35;
            --accent-soft: #edd4ca;
            --shadow: 0 18px 50px rgba(42, 30, 17, 0.08);
            --radius-lg: 28px;
            --radius-md: 18px;
        }
        html, body, [class*="css"]  {
            font-family: "Manrope", "Segoe UI", sans-serif;
        }
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(255,255,255,0.55), transparent 22%),
                radial-gradient(circle at 85% 20%, rgba(179,79,53,0.09), transparent 22%),
                linear-gradient(180deg, #f5f1ea 0%, var(--bg) 100%);
            color: var(--text);
        }
        header[data-testid="stHeader"], [data-testid="stSidebar"], #MainMenu {
            display: none !important;
        }
        [data-testid="stAppViewContainer"] {
            background: transparent;
        }
        .block-container {
            max-width: 1480px;
            padding-top: 1.3rem;
            padding-bottom: 2.8rem;
            padding-left: 1.6rem;
            padding-right: 1.6rem;
        }
        h1, h2, h3, h4 {
            font-family: "Sora", "Avenir Next", sans-serif;
            color: var(--text);
            letter-spacing: -0.03em;
        }
        p, li, label, div, span {
            color: var(--text);
        }
        .shell-main {
            padding-right: 1rem;
        }
        .app-shell-rail {
            position: sticky;
            top: 1.2rem;
            padding: 1rem;
            border-radius: 24px;
            background: linear-gradient(180deg, rgba(255,251,247,0.72), rgba(250,244,236,0.9));
            border: 1px solid var(--line);
            box-shadow: var(--shadow);
            backdrop-filter: blur(14px);
        }
        .rail-kicker, .eyebrow {
            text-transform: uppercase;
            letter-spacing: 0.14em;
            font-size: 0.72rem;
            font-weight: 800;
            color: var(--muted-soft);
        }
        .rail-title {
            margin-top: 0.5rem;
            font-family: "Sora", "Avenir Next", sans-serif;
            font-size: 1.25rem;
            line-height: 1.05;
            font-weight: 700;
        }
        .rail-copy, .subtle-text, .stCaption {
            color: var(--muted) !important;
            line-height: 1.6;
        }
        .hero-shell {
            position: relative;
            overflow: hidden;
            padding: 1.8rem;
            margin-bottom: 1.2rem;
            border-radius: 34px;
            background:
                linear-gradient(135deg, rgba(255,252,248,0.96), rgba(245,237,228,0.92)),
                var(--paper);
            border: 1px solid var(--line);
            box-shadow: var(--shadow);
        }
        .hero-shell::after {
            content: "";
            position: absolute;
            inset: auto -9rem -9rem auto;
            width: 20rem;
            height: 20rem;
            border-radius: 999px;
            background: radial-gradient(circle, rgba(179,79,53,0.12), transparent 64%);
            pointer-events: none;
        }
        .hero-label {
            font-size: 0.82rem;
            color: var(--muted-soft);
            text-transform: uppercase;
            letter-spacing: 0.16em;
            font-weight: 800;
        }
        .hero-title {
            margin: 0.45rem 0 0.55rem 0;
            font-size: clamp(2.5rem, 5vw, 5.4rem);
            line-height: 0.94;
            font-weight: 800;
        }
        .hero-image-replace {
            margin: 0.7rem 0 0.85rem 0;
            max-width: min(100%, 34rem);
        }
        .hero-image-replace img {
            display: block;
            width: 100%;
            height: auto;
            filter: contrast(1.02);
        }
        .hero-subtitle {
            max-width: 42rem;
            font-size: 1rem;
            color: var(--muted);
            line-height: 1.7;
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
            background: rgba(255,255,255,0.55);
            border-radius: 999px;
            padding: 0.5rem 0.8rem;
            font-size: 0.82rem;
            font-weight: 700;
        }
        .poster-frame {
            background: #12100e;
            border-radius: 24px;
            padding: 0.85rem;
            box-shadow: 0 18px 44px rgba(20, 16, 12, 0.18);
        }
        .section-shell {
            padding: 1.2rem 0 0.2rem 0;
        }
        .section-title {
            font-size: 1.32rem;
            margin-bottom: 0.2rem;
        }
        .section-copy {
            color: var(--muted);
            max-width: 48rem;
            line-height: 1.7;
            margin-bottom: 1rem;
        }
        .metric-card {
            min-height: 8.1rem;
            padding: 1rem 0.1rem 1.1rem 0.1rem;
            border-top: 1px solid var(--line);
        }
        .metric-label {
            color: var(--muted-soft);
            text-transform: uppercase;
            letter-spacing: 0.12em;
            font-size: 0.71rem;
            font-weight: 800;
            margin-bottom: 0.7rem;
        }
        .metric-value {
            font-size: 2.15rem;
            line-height: 0.98;
            font-weight: 800;
            color: var(--text);
        }
        .metric-foot {
            margin-top: 0.75rem;
            color: var(--muted);
            font-size: 0.88rem;
            line-height: 1.55;
        }
        .panel-card, .doc-card, .stance-card, .note-card {
            margin-bottom: 0.9rem;
            padding: 1rem 0 1rem 0;
            border-top: 1px solid var(--line);
        }
        .panel-title {
            font-size: 1rem;
            font-weight: 800;
            margin-bottom: 0.35rem;
        }
        .doc-card {
            border-top-width: 1px;
        }
        .doc-title {
            font-size: 1rem;
            font-weight: 800;
            margin: 0.25rem 0 0.4rem 0;
            line-height: 1.35;
        }
        .doc-meta, .caption-line {
            color: var(--muted);
            font-size: 0.84rem;
        }
        .doc-body {
            color: var(--text);
            line-height: 1.65;
            font-size: 0.95rem;
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
            border-radius: 999px;
            border: 1px solid var(--line);
            background: rgba(255,255,255,0.65);
            color: var(--text);
            font-size: 0.78rem;
            font-weight: 700;
        }
        .topic-summary-line strong {
            color: var(--text);
        }
        .stPlotlyChart, div[data-testid="stDataFrame"] {
            background: linear-gradient(180deg, rgba(255,251,247,0.68), rgba(248,241,232,0.92));
            border: 1px solid var(--line);
            border-radius: 24px;
            padding: 0.9rem 0.9rem 0.25rem 0.9rem;
        }
        .topic-table-shell {
            background: linear-gradient(180deg, rgba(255,251,247,0.72), rgba(248,241,232,0.92));
            border: 1px solid var(--line);
            border-radius: 28px;
            padding: 1rem 1rem 0.8rem 1rem;
            overflow-x: auto;
            margin-bottom: 1rem;
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
            background: #ece4db;
            color: var(--text);
            font-weight: 800;
            border-bottom: 1px solid var(--line);
            white-space: nowrap;
        }
        .topic-table thead th:first-child {
            border-top-left-radius: 18px;
        }
        .topic-table thead th:last-child {
            border-top-right-radius: 18px;
        }
        .topic-table tbody td {
            padding: 0.9rem 0.85rem;
            border-bottom: 1px solid #e6ddd1;
            vertical-align: top;
            background: rgba(255,255,255,0.55);
        }
        .topic-table tbody tr:nth-child(even) td {
            background: rgba(245,238,229,0.8);
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
            border-radius: 999px;
            display: grid;
            place-items: center;
            background: #161310;
            color: #fff7f1;
            font-family: "Sora", "Avenir Next", sans-serif;
            font-weight: 800;
            letter-spacing: -0.03em;
        }
        .method-title {
            font-size: 1.05rem;
            font-weight: 800;
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
            min-height: 2.95rem;
            border-radius: 16px;
            border: 1px solid var(--line) !important;
            background: rgba(255,255,255,0.55);
            color: var(--text) !important;
            font-weight: 800;
            letter-spacing: -0.01em;
            transition: transform 140ms ease, background 140ms ease, border-color 140ms ease, box-shadow 140ms ease;
            box-shadow: none;
        }
        div[data-testid="stButton"] button:hover {
            transform: translateY(-1px);
            border-color: var(--line-strong) !important;
            background: rgba(255,255,255,0.88);
        }
        div[data-testid="stButton"] button * {
            color: inherit !important;
        }
        div[data-testid="stButton"] button[kind="primary"] {
            background: #181613 !important;
            color: #fff7f1 !important;
            border-color: #181613 !important;
            box-shadow: 0 12px 24px rgba(24, 22, 19, 0.16);
        }
        div[data-testid="stButton"] button[kind="primary"] p,
        div[data-testid="stButton"] button[kind="primary"] span,
        div[data-testid="stButton"] button[kind="primary"] div {
            color: #fff7f1 !important;
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
            letter-spacing: 0.08em;
            font-weight: 800;
        }
        .stSelectbox [data-baseweb="select"] > div {
            min-height: 3.05rem;
            border-radius: 16px;
            border-color: var(--line-strong);
            background: rgba(255,255,255,0.82);
        }
        .stInfo {
            border-radius: 18px;
            border: 1px solid var(--line);
            background: rgba(255,251,247,0.8);
            color: var(--text);
        }
        @media (max-width: 1100px) {
            .block-container {
                padding-left: 1rem;
                padding-right: 1rem;
            }
            .shell-main {
                padding-right: 0;
            }
            .note-grid {
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
        font={"color": CHART_COLORS["ink_soft"], "family": "Manrope, sans-serif"},
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
        font={"color": CHART_COLORS["ink_soft"], "family": "Manrope, sans-serif"},
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


def render_page_selector(current_page: str) -> str:
    selected_page = current_page
    st.markdown('<div class="rail-kicker">Navigate</div>', unsafe_allow_html=True)
    st.markdown('<div class="rail-title">Analysis Views</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="rail-copy">Switch between the poster-like overview, topic breakdowns, detail view, and methods without losing context.</div>',
        unsafe_allow_html=True,
    )
    st.markdown("<div style='height:0.8rem'></div>", unsafe_allow_html=True)
    for page in PAGES:
        if st.button(page, key=f"page_{page}", use_container_width=True, type="primary" if page == current_page else "secondary"):
            selected_page = page
    return selected_page


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


def render_shell_header(snapshot: dict, available_caches: list, selected_cache_key: str) -> str:
    cache_label = snapshot["meta"].get("cache_label", "Default cache")
    window_label = snapshot["stats"]["window_label"]
    source = snapshot["meta"].get("cache_source", "unknown")
    selected_cache_key = render_cache_selector(available_caches, selected_cache_key)
    image_path = get_hero_image_path()
    hero_left, hero_right = st.columns([1.35, 0.8], gap="large")
    with hero_left:
        hero_image_markup = ""
        if image_path is not None:
            import base64

            encoded = base64.b64encode(image_path.read_bytes()).decode("ascii")
            hero_image_markup = (
                f'<div class="hero-image-replace"><img alt="Yeah Buddy poster" '
                f'src="data:image/{image_path.suffix.lstrip(".").lower()};base64,{encoded}"></div>'
            )
        st.markdown(
            f"""
            <div class="hero-shell">
                <div class="hero-label">Fitness Research Dashboard</div>
                {hero_image_markup}
                <div class="hero-subtitle">
                    A cleaner reading surface for the <strong>{html.escape(APP_TITLE)}</strong>:
                    compare eras, scan topic prevalence, inspect stance cautiously, and keep the whole study on one calm visual system.
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
    with hero_right:
        if image_path is not None:
            panel_card(
                "Reading surface",
                "The poster image now acts as the hero mark itself. This side panel stays lighter and is reserved for window context rather than repeating the same visual twice.",
            )
            panel_card("Selected window", f"{cache_label} • {window_label}")
            panel_card("Data source", source)
        else:
            panel_card(
                "Poster image unavailable",
                "The local reference image could not be found, so the layout falls back to the text-led composition.",
            )
    return selected_cache_key


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
    available_caches = list_available_caches()
    default_cache_key = resolve_cache_key()

    if "page" not in st.session_state:
        st.session_state.page = PAGES[0]
    if "cache_key" not in st.session_state:
        st.session_state.cache_key = default_cache_key

    available_keys = {option.key for option in available_caches}
    if st.session_state.cache_key not in available_keys:
        st.session_state.cache_key = default_cache_key

    snapshot = load_dashboard_snapshot(st.session_state.cache_key)

    shell_main, shell_rail = st.columns([4.9, 1.5], gap="large")
    with shell_rail:
        st.markdown('<div class="app-shell-rail">', unsafe_allow_html=True)
        st.session_state.page = render_page_selector(st.session_state.page)
        st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
        st.markdown('<div class="rail-kicker">Current Window</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="rail-title">{html.escape(snapshot["meta"].get("cache_label", "Default cache"))}</div>', unsafe_allow_html=True)
        st.markdown(
            f'<div class="rail-copy">{html.escape(snapshot["meta"].get("cache_help_text", snapshot["stats"]["corpus_summary"]))}</div>',
            unsafe_allow_html=True,
        )
        st.markdown("<div style='height:0.8rem'></div>", unsafe_allow_html=True)
        st.markdown('<div class="rail-kicker">Study Summary</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="rail-copy">{html.escape(snapshot["stats"]["corpus_summary"])}</div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with shell_main:
        st.markdown('<div class="shell-main">', unsafe_allow_html=True)
        updated_cache_key = render_shell_header(snapshot, available_caches, st.session_state.cache_key)
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
        st.markdown("</div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
