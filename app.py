from __future__ import annotations

import html

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from fitness_reddit_analyzer.app_data import load_dashboard_snapshot
from fitness_reddit_analyzer.config import APP_TITLE


st.set_page_config(page_title=APP_TITLE, layout="wide")

PLOTLY_CONFIG = {"displayModeBar": False, "responsive": True}


def apply_theme() -> None:
    st.markdown(
        """
        <style>
        :root {
            --bg: #070b09;
            --panel: #101513;
            --panel-2: #151d19;
            --panel-3: #1b2520;
            --border: #223029;
            --text: #edf5ef;
            --muted: #97a89f;
            --accent: #1ed760;
            --accent-2: #4ee28a;
            --warning: #f2c661;
        }
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(30, 215, 96, 0.14), transparent 28%),
                radial-gradient(circle at top right, rgba(78, 226, 138, 0.08), transparent 22%),
                linear-gradient(180deg, #070b09 0%, #0b120f 42%, #0f1613 100%);
            color: var(--text);
            font-family: "Avenir Next", "Trebuchet MS", sans-serif;
        }
        [data-testid="stSidebar"] {
            background: rgba(12, 18, 15, 0.92);
            border-right: 1px solid var(--border);
        }
        h1, h2, h3 {
            font-family: "Gill Sans", "Avenir Next", sans-serif;
            letter-spacing: -0.02em;
        }
        .hero-card, .panel-card, .metric-card, .note-card, .doc-card, .stance-card {
            background: linear-gradient(180deg, rgba(18, 24, 21, 0.96), rgba(14, 19, 17, 0.96));
            border: 1px solid rgba(53, 77, 66, 0.65);
            border-radius: 20px;
            box-shadow: 0 18px 40px rgba(0, 0, 0, 0.24);
        }
        .hero-card {
            padding: 1.4rem 1.5rem;
            margin-bottom: 1rem;
        }
        .hero-kicker {
            color: var(--accent);
            text-transform: uppercase;
            letter-spacing: 0.16em;
            font-size: 0.72rem;
            font-weight: 700;
        }
        .hero-title {
            font-size: 2.5rem;
            font-weight: 700;
            margin: 0.35rem 0 0.55rem 0;
        }
        .hero-subtitle, .subtle-text {
            color: var(--muted);
            font-size: 0.98rem;
            line-height: 1.55;
        }
        .metric-card {
            padding: 1rem 1rem 0.9rem 1rem;
            min-height: 7.25rem;
        }
        .metric-label {
            color: var(--muted);
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-size: 0.72rem;
            font-weight: 700;
            margin-bottom: 0.6rem;
        }
        .metric-value {
            color: var(--text);
            font-size: 2rem;
            font-weight: 700;
            line-height: 1;
        }
        .metric-foot {
            color: var(--muted);
            font-size: 0.84rem;
            margin-top: 0.7rem;
            line-height: 1.4;
        }
        .panel-card, .note-card, .stance-card {
            padding: 1.1rem 1.2rem;
            margin-bottom: 1rem;
        }
        .panel-title {
            font-size: 1.05rem;
            font-weight: 700;
            margin-bottom: 0.35rem;
        }
        .pill {
            display: inline-block;
            padding: 0.28rem 0.7rem;
            border-radius: 999px;
            background: rgba(30, 215, 96, 0.14);
            border: 1px solid rgba(30, 215, 96, 0.24);
            color: var(--accent-2);
            font-size: 0.78rem;
            font-weight: 700;
            margin-right: 0.45rem;
            margin-bottom: 0.45rem;
        }
        .doc-card {
            padding: 1rem 1rem 0.9rem 1rem;
            margin-bottom: 0.85rem;
        }
        .doc-title {
            font-size: 1rem;
            font-weight: 700;
            margin: 0.15rem 0 0.35rem 0;
        }
        .doc-meta {
            color: var(--muted);
            font-size: 0.82rem;
            margin-bottom: 0.55rem;
        }
        .doc-body {
            color: var(--text);
            font-size: 0.92rem;
            line-height: 1.5;
        }
        .topic-anchor {
            color: var(--accent-2);
            text-decoration: none;
            font-weight: 700;
        }
        .warning {
            color: var(--warning);
            font-weight: 700;
        }
        .caption-line {
            color: var(--muted);
            font-size: 0.8rem;
        }
        div[data-testid="stDataFrame"] {
            border: 1px solid rgba(53, 77, 66, 0.65);
            border-radius: 18px;
            overflow: hidden;
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


def build_activity_chart(monthly_activity: list[dict]) -> go.Figure:
    frame = pd.DataFrame(monthly_activity)
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=frame["month_start"],
            y=frame["posts"],
            mode="lines+markers",
            name="Posts",
            line={"color": "#4ee28a", "width": 3},
        )
    )
    figure.add_trace(
        go.Scatter(
            x=frame["month_start"],
            y=frame["comments"],
            mode="lines+markers",
            name="Comments",
            line={"color": "#c1f7d5", "width": 2},
        )
    )
    figure.add_trace(
        go.Scatter(
            x=frame["month_start"],
            y=frame["modeled_documents"],
            mode="lines+markers",
            name="Hybrid Topic Corpus",
            line={"color": "#1ed760", "width": 3, "dash": "dot"},
        )
    )
    figure.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin={"l": 20, "r": 20, "t": 10, "b": 20},
        legend={"orientation": "h", "y": 1.14, "x": 0.0},
        xaxis_title="Month",
        yaxis_title="Count",
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(gridcolor="rgba(255,255,255,0.08)")
    return figure


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
            "Major stance topic": "#1ed760",
            "Other topic": "#3a5446",
        },
        labels={"document_share_pct": "Document share (%)", "topic_label": ""},
    )
    figure.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin={"l": 20, "r": 20, "t": 20, "b": 20},
        legend={"orientation": "h", "y": 1.14, "x": 0.0, "title": None},
    )
    figure.update_xaxes(gridcolor="rgba(255,255,255,0.08)")
    figure.update_yaxes(categoryorder="total ascending")
    return figure


def build_topic_detail_chart(topic: dict) -> go.Figure:
    frame = pd.DataFrame(topic["monthly_trend"])
    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=frame["month_start"],
            y=frame["document_count"],
            name="Document count",
            marker_color="#214f35",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=frame["month_start"],
            y=frame["document_share"],
            name="Document share (%)",
            mode="lines+markers",
            yaxis="y2",
            line={"color": "#4ee28a", "width": 3},
        )
    )
    figure.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin={"l": 20, "r": 20, "t": 10, "b": 20},
        legend={"orientation": "h", "y": 1.14, "x": 0.0},
        xaxis_title="Month",
        yaxis_title="Document count",
        yaxis2={"title": "Share (%)", "overlaying": "y", "side": "right"},
    )
    figure.update_xaxes(showgrid=False)
    figure.update_yaxes(gridcolor="rgba(255,255,255,0.08)")
    return figure


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
        color_discrete_map={"Posts": "#4ee28a", "Comments": "#274233"},
    )
    figure.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin={"l": 0, "r": 0, "t": 10, "b": 10},
        showlegend=True,
        legend={"orientation": "h", "y": -0.1, "x": 0.1, "title": None},
    )
    figure.update_traces(textinfo="percent+label")
    return figure


def render_header(snapshot: dict) -> None:
    source = html.escape(snapshot["meta"].get("cache_source", "unknown"))
    window_label = html.escape(snapshot["stats"]["window_label"])
    st.markdown(
        f"""
        <div class="hero-card">
            <div class="hero-kicker">Part 1 Dashboard</div>
            <div class="hero-title">{APP_TITLE}</div>
            <div class="hero-subtitle">
                Interactive view of the completed <strong>r/fitness</strong> Part 1 pipeline across ingestion, hybrid topic modeling, and cautious stance analysis.
                Selected window: <strong>{window_label}</strong>. Data source: <strong>{source}</strong>.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_overview(snapshot: dict) -> None:
    render_header(snapshot)
    if not snapshot["topics"] or not snapshot["overview"]["monthly_activity"]:
        st.info("Pipeline outputs are not available yet. Run the Part 1 stages and rebuild the app cache.")
        return
    stats = snapshot["stats"]
    overview = snapshot["overview"]

    metric_columns = st.columns(6)
    cards = [
        ("Posts", format_int(stats["total_posts"]), "Raw corpus posts in SQLite"),
        ("Comments", format_int(stats["total_comments"]), "Raw corpus comments in SQLite"),
        ("Authors", format_int(stats["total_authors"]), "Non-deleted authors seen in corpus"),
        ("Topics", str(stats["total_topics"]), "Final topic count after hybrid modeling"),
        ("Major Topics", str(stats["major_topic_count"]), "Topics with validated stance pass"),
        ("Hybrid Docs", format_int(stats["modeled_documents"]), "Documents assigned across the 8 topics"),
    ]
    for column, card in zip(metric_columns, cards):
        with column:
            metric_card(*card)

    left, right = st.columns([1.65, 1.0], gap="large")
    with left:
        st.markdown("### Activity")
        st.plotly_chart(build_activity_chart(overview["monthly_activity"]), use_container_width=True, config=PLOTLY_CONFIG)
    with right:
        highlights = overview["activity_highlights"]
        panel_card("Corpus Summary", stats["corpus_summary"])
        panel_card(
            "Peak Posting Month",
            f"{highlights['peak_posts_month']['month_start']}: {format_int(highlights['peak_posts_month']['count'])} posts",
        )
        panel_card(
            "Peak Comment Month",
            f"{highlights['peak_comments_month']['month_start']}: {format_int(highlights['peak_comments_month']['count'])} comments",
        )
        panel_card(
            "Peak Hybrid Modeling Month",
            f"{highlights['peak_modeled_month']['month_start']}: {format_int(highlights['peak_modeled_month']['count'])} documents in the modeled hybrid slice",
        )

    bottom_left, bottom_right = st.columns([1.2, 1.0], gap="large")
    with bottom_left:
        st.markdown("### Topic Share")
        st.plotly_chart(build_topic_share_chart(snapshot["topics"]), use_container_width=True, config=PLOTLY_CONFIG)
    with bottom_right:
        st.markdown("### Top Topics")
        for topic in overview["top_topics"]:
            panel_card(
                f"Topic {topic['topic_id']} • {topic['topic_label']}",
                f"{topic['document_share_pct']:.1f}% of modeled documents • {format_int(topic['document_count'])} docs • {topic['trend_label']} • {'major stance topic' if topic['major_topic'] else 'topic-only'}",
            )


def render_topics(snapshot: dict) -> None:
    render_header(snapshot)
    if not snapshot["topics"]:
        st.info("No topic outputs are available yet.")
        return
    st.markdown("### Topic Comparison")
    st.caption("Document share reflects the hybrid topic corpus, not just posts.")
    frame = pd.DataFrame(snapshot["topic_table"]).rename(
        columns={
            "topic_id": "Topic",
            "topic_label": "Label",
            "top_keywords": "Top keywords",
            "document_share_pct": "Document share (%)",
            "document_count": "Documents",
            "post_documents": "Posts",
            "comment_documents": "Comments",
            "trend_label": "Trend",
            "major_topic": "Major topic",
            "stance_status": "Stance",
        }
    )
    st.dataframe(frame, use_container_width=True, hide_index=True)
    st.plotly_chart(build_topic_share_chart(snapshot["topics"]), use_container_width=True, config=PLOTLY_CONFIG)


def render_topic_detail(snapshot: dict) -> None:
    render_header(snapshot)
    topics = snapshot["topics"]
    if not topics:
        st.info("No topic data is available yet.")
        return

    options = {
        f"Topic {topic['topic_id']} • {topic['topic_label']}": topic
        for topic in topics
    }
    selected_key = st.selectbox("Select a topic", list(options.keys()))
    topic = options[selected_key]
    stance = topic["stance"]

    top_line = st.columns(5)
    metrics = [
        ("Document share", f"{topic['document_share_pct']:.1f}%", "Share of hybrid documents"),
        ("Documents", format_int(topic["document_count"]), "Assigned topic documents"),
        ("Posts", format_int(topic["post_documents"]), "Post documents inside topic"),
        ("Comments", format_int(topic["comment_documents"]), "Comment documents inside topic"),
        ("Trend", topic["trend_label"].title(), f"Coverage {topic['month_coverage_ratio']:.1f}%"),
    ]
    for column, item in zip(top_line, metrics):
        with column:
            metric_card(*item)

        st.markdown(
            " ".join(f'<span class="pill">{html.escape(term)}</span>' for term in topic["keyword_terms"][:8]),
            unsafe_allow_html=True,
        )

    chart_col, mix_col = st.columns([1.65, 1.0], gap="large")
    with chart_col:
        st.markdown("### Monthly Trend")
        st.caption("Bars show document count. Line shows monthly document share for this topic.")
        st.plotly_chart(build_topic_detail_chart(topic), use_container_width=True, config=PLOTLY_CONFIG)
    with mix_col:
        st.markdown("### Source Mix")
        st.plotly_chart(build_source_mix_chart(topic), use_container_width=True, config=PLOTLY_CONFIG)
        panel_card(
            "Topic Semantics",
            f"This topic is labeled as {topic['trend_label']}. Average assignment confidence is {topic['average_assignment_confidence']:.3f}, and the peak-to-median monthly ratio is {topic['peak_to_median_ratio']:.2f}.",
        )

    st.markdown("### Representative Documents")
    for doc in topic["representative_documents"]:
        doc_card(doc)

    st.markdown("### Stance View")
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
    render_header(snapshot)
    if not snapshot["methods"]["sections"]:
        st.info("Methods content will appear here after the pipeline artifacts are available.")
        return
    st.markdown("### Methods")
    methods = snapshot["methods"]
    for section in methods["sections"]:
        st.markdown(f"#### {section['title']}")
        for item in section["items"]:
            panel_card(section["title"], item)
    st.markdown("### Design Notes")
    for note in methods["notes"]:
        st.markdown(
            f"""
            <div class="note-card">
                <div class="subtle-text">{html.escape(note)}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def main() -> None:
    apply_theme()
    snapshot = load_dashboard_snapshot()

    pages = {
        "Overview": lambda: render_overview(snapshot),
        "Topics": lambda: render_topics(snapshot),
        "Topic Detail": lambda: render_topic_detail(snapshot),
        "Methods": lambda: render_methods(snapshot),
    }
    st.sidebar.markdown("## Explore")
    selection = st.sidebar.radio("Page", list(pages.keys()), label_visibility="collapsed")
    st.sidebar.caption(snapshot["stats"]["corpus_summary"])
    pages[selection]()


if __name__ == "__main__":
    main()
