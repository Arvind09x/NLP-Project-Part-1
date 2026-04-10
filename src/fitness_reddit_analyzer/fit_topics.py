from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from bertopic import BERTopic
from bertopic.dimensionality import BaseDimensionalityReduction
from sklearn.cluster import KMeans
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import CountVectorizer, ENGLISH_STOP_WORDS, TfidfVectorizer
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import Normalizer

from fitness_reddit_analyzer.config import (
    MAX_TOPIC_COUNT,
    METHODS_NOTES,
    MIN_TOPIC_COUNT,
    SUBREDDIT,
    TARGET_TOPIC_COUNT,
    TOPIC_MODEL_RANDOM_SEED,
    TOPIC_MODEL_TOTAL_DOC_TARGET,
    TOPIC_REPRESENTATIVE_DOCS,
    TOP_LEVEL_COMMENTS_PER_POST,
    checkpoint_path,
)
from fitness_reddit_analyzer.db import connect_db


TOPIC_STOPWORDS = sorted(
    ENGLISH_STOP_WORDS.union(
        {
            "amp",
            "arent",
            "cant",
            "couldnt",
            "didnt",
            "doesnt",
            "dont",
            "fit",
            "fitness",
            "going",
            "got",
            "im",
            "isnt",
            "ive",
            "just",
            "know",
            "like",
            "make",
            "need",
            "people",
            "really",
            "rfitness",
            "thats",
            "theres",
            "theyre",
            "ve",
            "want",
            "wasnt",
            "wont",
            "youre",
        }
    )
)


@dataclass
class TopicCheckpoint:
    stage: str
    generated_at_utc: int
    subreddit: str
    posts_included: int
    comments_included: int
    total_modeled_documents: int
    eligible_top_level_comments: int
    capped_comment_limit: int
    final_topic_count: int
    assignment_source: str
    topic_metric: str
    status: str


def run() -> None:
    checkpoint_file = checkpoint_path("fit_topics")
    ensure_prepare_features_complete()
    documents = load_topic_corpus()
    if documents.empty:
        raise RuntimeError("No modeling documents were selected for fit_topics.")
    topic_model, topics, confidences = fit_topic_model(documents)
    topic_records, topic_time_series, document_topics = build_topic_outputs(
        documents=documents,
        topic_model=topic_model,
        topics=topics,
        confidences=confidences,
    )
    checkpoint = persist_topic_outputs(documents, topic_records, topic_time_series, document_topics, checkpoint_file)
    print("fit_topics complete:", json.dumps(asdict(checkpoint), ensure_ascii=True))


def ensure_prepare_features_complete() -> None:
    with connect_db() as connection:
        checkpoint = connection.execute(
            "SELECT status FROM pipeline_checkpoints WHERE stage_name = 'prepare_features'"
        ).fetchone()
    if checkpoint is None or checkpoint["status"] != "completed":
        raise RuntimeError("Run prepare_features successfully before fit_topics.")


def load_topic_corpus() -> pd.DataFrame:
    posts = load_modeled_posts()
    comment_cap = max(TOPIC_MODEL_TOTAL_DOC_TARGET - len(posts), 0)
    comments = load_modeled_comments(comment_cap)
    documents = pd.concat([posts, comments], ignore_index=True)
    if documents.empty:
        return documents
    documents["month_start"] = (
        pd.to_datetime(documents["created_utc"], unit="s", utc=True).dt.strftime("%Y-%m-01")
    )
    documents["clean_text"] = documents["clean_text"].astype(str)
    documents = documents.sort_values(["created_utc", "document_id"], kind="stable").reset_index(drop=True)
    return documents


def load_modeled_posts() -> pd.DataFrame:
    with connect_db() as connection:
        frame = pd.read_sql_query(
            """
            SELECT
                d.document_id,
                d.source_type,
                d.source_id,
                p.post_id,
                p.post_id AS root_post_id,
                p.title,
                d.clean_text,
                d.created_utc,
                COALESCE(p.score, 0) AS score
            FROM documents d
            JOIN posts p ON p.post_id = d.source_id
            WHERE d.source_type = 'post'
              AND d.include_in_modeling = 1
              AND LOWER(d.subreddit) = LOWER(?)
            ORDER BY d.created_utc ASC, d.document_id ASC
            """,
            connection,
            params=(SUBREDDIT,),
        )
    return frame


def load_modeled_comments(comment_cap: int) -> pd.DataFrame:
    if comment_cap <= 0:
        return pd.DataFrame(
            columns=[
                "document_id",
                "source_type",
                "source_id",
                "post_id",
                "root_post_id",
                "title",
                "clean_text",
                "created_utc",
                "score",
            ]
        )
    with connect_db() as connection:
        frame = pd.read_sql_query(
            """
            WITH ranked_comments AS (
                SELECT
                    d.document_id,
                    d.source_type,
                    d.source_id,
                    c.comment_id,
                    c.post_id,
                    c.post_id AS root_post_id,
                    p.title,
                    d.clean_text,
                    d.created_utc,
                    COALESCE(c.score, 0) AS score,
                    ROW_NUMBER() OVER (
                        PARTITION BY c.post_id
                        ORDER BY COALESCE(c.score, 0) DESC, d.created_utc ASC, d.document_id ASC
                    ) AS within_post_rank
                FROM documents d
                JOIN comments c ON c.comment_id = d.source_id
                JOIN posts p ON p.post_id = c.post_id
                WHERE d.source_type = 'comment'
                  AND d.include_in_modeling = 1
                  AND LOWER(d.subreddit) = LOWER(?)
                  AND d.parent_id LIKE 't3_%'
            ),
            capped_comments AS (
                SELECT
                    document_id,
                    source_type,
                    source_id,
                    post_id,
                    root_post_id,
                    title,
                    clean_text,
                    created_utc,
                    score
                FROM ranked_comments
                WHERE within_post_rank <= ?
                ORDER BY score DESC, created_utc ASC, document_id ASC
                LIMIT ?
            )
            SELECT *
            FROM capped_comments
            ORDER BY created_utc ASC, document_id ASC
            """,
            connection,
            params=(SUBREDDIT, TOP_LEVEL_COMMENTS_PER_POST, comment_cap),
        )
    return frame


def fit_topic_model(documents: pd.DataFrame) -> tuple[BERTopic, list[int], np.ndarray]:
    texts = documents["clean_text"].tolist()
    embeddings = build_embeddings(texts)
    topic_count = min(max(TARGET_TOPIC_COUNT, MIN_TOPIC_COUNT), MAX_TOPIC_COUNT, len(documents))
    topic_model = BERTopic(
        embedding_model=None,
        umap_model=BaseDimensionalityReduction(),
        hdbscan_model=KMeans(
            n_clusters=topic_count,
            n_init=20,
            random_state=TOPIC_MODEL_RANDOM_SEED,
        ),
        vectorizer_model=CountVectorizer(
            stop_words=TOPIC_STOPWORDS,
            ngram_range=(1, 2),
            min_df=1,
            max_df=1.0,
        ),
        nr_topics=None,
        top_n_words=10,
        calculate_probabilities=False,
        verbose=False,
    )
    topics, _ = topic_model.fit_transform(texts, embeddings=embeddings)
    confidences = assignment_confidences(topic_model, embeddings)
    final_topic_count = count_topics(topics)
    if not MIN_TOPIC_COUNT <= final_topic_count <= MAX_TOPIC_COUNT:
        raise RuntimeError(
            f"fit_topics produced {final_topic_count} topics, outside the required {MIN_TOPIC_COUNT}-{MAX_TOPIC_COUNT} range."
        )
    return topic_model, list(topics), confidences


def build_embeddings(texts: list[str]) -> np.ndarray:
    tfidf = TfidfVectorizer(
        stop_words=TOPIC_STOPWORDS,
        ngram_range=(1, 2),
        min_df=5,
        max_df=0.6,
        max_features=50_000,
        sublinear_tf=True,
    )
    matrix = tfidf.fit_transform(texts)
    n_features = matrix.shape[1]
    if n_features < 2:
        raise RuntimeError("Not enough vocabulary to build topic embeddings.")
    n_components = min(100, n_features - 1, len(texts) - 1)
    if n_components < 2:
        n_components = min(n_features, len(texts))
    reducer = make_pipeline(
        TruncatedSVD(n_components=n_components, random_state=TOPIC_MODEL_RANDOM_SEED),
        Normalizer(copy=False),
    )
    return reducer.fit_transform(matrix)


def assignment_confidences(topic_model: BERTopic, embeddings: np.ndarray) -> np.ndarray:
    cluster_model = topic_model.hdbscan_model
    if not hasattr(cluster_model, "transform"):
        return np.ones(len(embeddings), dtype=float)
    distances = cluster_model.transform(embeddings)
    min_distances = distances.min(axis=1)
    confidences = 1.0 / (1.0 + min_distances)
    return confidences.astype(float)


def build_topic_outputs(
    *,
    documents: pd.DataFrame,
    topic_model: BERTopic,
    topics: list[int],
    confidences: np.ndarray,
) -> tuple[list[dict], list[dict], list[dict]]:
    frame = documents.copy()
    frame["topic_id"] = topics
    frame["assignment_confidence"] = confidences
    frame = frame[frame["topic_id"] >= 0].copy()
    if frame.empty:
        raise RuntimeError("All documents were assigned to outlier topics.")

    total_documents = len(frame)
    months = sorted(frame["month_start"].unique().tolist())
    topic_records: list[dict] = []
    document_topics: list[dict] = []
    topic_time_series: list[dict] = []

    topic_info = topic_model.get_topic_info()
    valid_topic_ids = [int(topic_id) for topic_id in topic_info["Topic"].tolist() if int(topic_id) >= 0]

    for topic_id in valid_topic_ids:
        topic_docs = frame[frame["topic_id"] == topic_id].copy()
        topic_docs = topic_docs.sort_values(
            ["assignment_confidence", "score", "created_utc", "document_id"],
            ascending=[False, False, True, True],
            kind="stable",
        )
        keywords = topic_model.get_topic(topic_id) or []
        top_keywords = [
            {"term": str(term), "weight": round(float(weight), 6)}
            for term, weight in keywords[:10]
        ]
        label_auto = build_topic_label(top_keywords)
        document_share = float(len(topic_docs) / total_documents)
        representative_docs = [
            {
                "document_id": row.document_id,
                "source_type": row.source_type,
                "source_id": row.source_id,
                "post_id": row.root_post_id,
                "title": row.title,
                "excerpt": row.clean_text[:280],
            }
            for row in topic_docs.head(TOPIC_REPRESENTATIVE_DOCS).itertuples(index=False)
        ]
        month_counts = (
            topic_docs.groupby("month_start")["document_id"]
            .count()
            .reindex(months, fill_value=0)
            .astype(int)
        )
        active_counts = month_counts[month_counts > 0]
        month_coverage_ratio = float((month_counts > 0).mean())
        peak_to_median_ratio = float(
            active_counts.max() / np.median(active_counts) if len(active_counts) else 1.0
        )
        topic_records.append(
            {
                "topic_id": topic_id,
                "topic_label": label_auto,
                "topic_label_auto": label_auto,
                "top_keywords_json": json.dumps(top_keywords, ensure_ascii=True),
                # Stored in the legacy share_of_posts column, but this is document share for the hybrid corpus.
                "share_of_posts": document_share,
                "topic_type": classify_topic_type(month_coverage_ratio, peak_to_median_ratio),
                "month_coverage_ratio": month_coverage_ratio,
                "peak_to_median_ratio": peak_to_median_ratio,
                "representative_posts_json": json.dumps(representative_docs, ensure_ascii=True),
                "notes": build_topic_notes(),
            }
        )
        for row in topic_docs.itertuples(index=False):
            document_topics.append(
                {
                    "document_id": row.document_id,
                    "topic_id": int(row.topic_id),
                    "assignment_source": "bertopic_hybrid_top_level_comments_v1",
                    "assignment_confidence": round(float(row.assignment_confidence), 6),
                    # Major-topic status is finalized after validation in fit_stance.
                    "is_major_topic": 0,
                }
            )

    monthly_totals = frame.groupby("month_start")["document_id"].count().to_dict()
    for topic_record in topic_records:
        topic_id = int(topic_record["topic_id"])
        topic_month_counts = (
            frame[frame["topic_id"] == topic_id]
            .groupby("month_start")["document_id"]
            .count()
            .to_dict()
        )
        for month in months:
            document_count = int(topic_month_counts.get(month, 0))
            month_total = int(monthly_totals[month])
            topic_time_series.append(
                {
                    "topic_id": topic_id,
                    "month_start": month,
                    # Stored in the legacy post_count/post_share columns, but these are document-level metrics.
                    "post_count": document_count,
                    "post_share": float(document_count / month_total) if month_total else 0.0,
                }
            )

    return topic_records, topic_time_series, document_topics


def count_topics(topics: list[int]) -> int:
    return len({int(topic_id) for topic_id in topics if int(topic_id) >= 0})


def build_topic_label(top_keywords: list[dict]) -> str:
    words = [item["term"] for item in top_keywords[:3] if item["term"]]
    if not words:
        return "Unlabeled Topic"
    return " / ".join(word.replace("_", " ").title() for word in words)


def classify_topic_type(month_coverage_ratio: float, peak_to_median_ratio: float) -> str:
    if month_coverage_ratio >= 0.75 and peak_to_median_ratio <= 1.75:
        return "persistent"
    if peak_to_median_ratio >= 2.5:
        return "trending"
    return "mixed"


def build_topic_notes() -> str:
    return " ".join(METHODS_NOTES)


def persist_topic_outputs(
    documents: pd.DataFrame,
    topic_records: list[dict],
    topic_time_series: list[dict],
    document_topics: list[dict],
    checkpoint_file: Path,
) -> TopicCheckpoint:
    now_utc = int(datetime.now(tz=UTC).timestamp())
    checkpoint = TopicCheckpoint(
        stage="fit_topics",
        generated_at_utc=now_utc,
        subreddit=SUBREDDIT,
        posts_included=int((documents["source_type"] == "post").sum()),
        comments_included=int((documents["source_type"] == "comment").sum()),
        total_modeled_documents=int(len(documents)),
        eligible_top_level_comments=count_eligible_top_level_comments(),
        capped_comment_limit=max(TOPIC_MODEL_TOTAL_DOC_TARGET - int((documents["source_type"] == "post").sum()), 0),
        final_topic_count=len(topic_records),
        assignment_source="bertopic_hybrid_top_level_comments_v1",
        topic_metric="document_share",
        status="completed",
    )
    checkpoint_file.write_text(json.dumps(asdict(checkpoint), indent=2), encoding="utf-8")
    with connect_db() as connection:
        connection.execute("DELETE FROM topic_argument_summaries")
        connection.execute("DELETE FROM comment_stances")
        connection.execute("DELETE FROM document_topics")
        connection.execute("DELETE FROM topic_time_series")
        connection.execute("DELETE FROM topic_definitions")
        connection.executemany(
            """
            INSERT INTO topic_definitions (
                topic_id,
                topic_label,
                topic_label_auto,
                top_keywords_json,
                share_of_posts,
                topic_type,
                month_coverage_ratio,
                peak_to_median_ratio,
                representative_posts_json,
                notes,
                created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["topic_id"],
                    row["topic_label"],
                    row["topic_label_auto"],
                    row["top_keywords_json"],
                    row["share_of_posts"],
                    row["topic_type"],
                    row["month_coverage_ratio"],
                    row["peak_to_median_ratio"],
                    row["representative_posts_json"],
                    row["notes"],
                    now_utc,
                )
                for row in topic_records
            ],
        )
        connection.executemany(
            """
            INSERT INTO document_topics (
                document_id,
                topic_id,
                assignment_source,
                assignment_confidence,
                is_major_topic
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    row["document_id"],
                    row["topic_id"],
                    row["assignment_source"],
                    row["assignment_confidence"],
                    row["is_major_topic"],
                )
                for row in document_topics
            ],
        )
        connection.executemany(
            """
            INSERT INTO topic_time_series (
                topic_id,
                month_start,
                post_count,
                post_share
            ) VALUES (?, ?, ?, ?)
            """,
            [
                (
                    row["topic_id"],
                    row["month_start"],
                    row["post_count"],
                    row["post_share"],
                )
                for row in topic_time_series
            ],
        )
        connection.execute(
            """
            INSERT INTO pipeline_checkpoints (stage_name, status, payload_json, updated_at_utc)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(stage_name) DO UPDATE SET
                status = excluded.status,
                payload_json = excluded.payload_json,
                updated_at_utc = excluded.updated_at_utc
            """,
            ("fit_topics", "completed", json.dumps(asdict(checkpoint), ensure_ascii=True), now_utc),
        )
    return checkpoint


def count_eligible_top_level_comments() -> int:
    with connect_db() as connection:
        return int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM documents d
                JOIN comments c ON c.comment_id = d.source_id
                WHERE d.source_type = 'comment'
                  AND d.include_in_modeling = 1
                  AND LOWER(d.subreddit) = LOWER(?)
                  AND d.parent_id LIKE 't3_%'
                """,
                (SUBREDDIT,),
            ).fetchone()[0]
        )
