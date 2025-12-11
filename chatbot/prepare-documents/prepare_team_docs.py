#!/usr/bin/env python
"""
prepare_team_docs.py (themes-only version)

Builds team-level summary docs directly from:
  - success-factors/team_data_with_themes_final.csv

Each JSONL line will look like:
{
  "id": "team-los-angeles-angels-2025",
  "type": "team_summary",
  "metadata": {...},
  "text": "Human-readable summary..."
}
"""

import os
import json
import argparse
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd


# =========================
# CONFIG / COLUMN CONSTANTS
# =========================

# ---- File paths (defaults; can override via CLI) ----
DEFAULT_THEMES_PATH = "success-factors/team_data_with_themes_final.csv"
DEFAULT_OUTPUT_PATH = "kb/team_docs.jsonl"

# ---- Key columns (tweak these to match your schema) ----
# In your themes CSV, the team name lives in `matched_keyword`
COL_TEAM_NAME = "matched_keyword"     # e.g., "Los Angeles Angels"
COL_SEASON = "year"                   # e.g., 2025

# Performance-related numeric columns in themes CSV
COL_WIN_PCT = "team_win_pct"
COL_RUNS_SCORED = "team_runs_scored"
COL_RUNS_ALLOWED = "team_runs_allowed"
COL_WINS = "team_wins"
COL_LOSSES = "team_losses"

# Sentiment / emotion / theme columns
COL_SENTIMENT_SCORE = "sentiment_score"
COL_SENTIMENT_LABEL = "sentiment_label"
COL_EMOTION_LABEL = "emotion_label"
COL_THEME_LABEL = "theme_label"


# =========================
# HELPER FUNCTIONS
# =========================

def slugify(text: str) -> str:
    """Simple slugify: lowercase, alnum + dashes."""
    if not isinstance(text, str):
        text = str(text)
    text = text.strip().lower()
    out = []
    for ch in text:
        if ch.isalnum():
            out.append(ch)
        elif ch in (" ", "-", "_"):
            out.append("-")
    slug = "".join(out)
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-") or "unknown"


def load_themes(themes_path: str) -> pd.DataFrame:
    """Load the themes/sentiment CSV into a DataFrame."""
    print(f"Loading themes/sentiment data from: {themes_path}")
    themes = pd.read_csv(themes_path, low_memory=False)
    print(f"  -> {len(themes):,} rows")

    if COL_TEAM_NAME not in themes.columns:
        raise KeyError(
            f"Expected '{COL_TEAM_NAME}' column in themes CSV "
            f"(current columns: {list(themes.columns)})"
        )

    return themes


def value_distribution(series: pd.Series, top_n: int = 5) -> List[Dict[str, Any]]:
    """
    Compute normalized distribution for a categorical series.

    Returns list of dicts: [{"label": "positive", "count": 120, "pct": 0.48}, ...]
    Only keeps top_n labels by frequency.
    """
    series = series.dropna()
    if series.empty:
        return []

    counts = series.value_counts()
    total = counts.sum()
    top_counts = counts.head(top_n)

    dist = []
    for label, count in top_counts.items():
        dist.append(
            {
                "label": label,
                "count": int(count),
                "pct": float(count / total) if total > 0 else 0.0,
            }
        )
    return dist


def aggregate_group(group: pd.DataFrame) -> Dict[str, Any]:
    """
    Given all rows for a (team, season) group, compute aggregated stats.

    Returns a dict with:
      - team_name, season
      - n_posts
      - performance stats (mean of team_* columns)
      - sentiment mean/std
      - sentiment/emotion/theme distributions
    """
    summary: Dict[str, Any] = {}

    team_name = group[COL_TEAM_NAME].iloc[0] if COL_TEAM_NAME in group.columns else None
    season = group[COL_SEASON].iloc[0] if COL_SEASON in group.columns else None

    summary["team_name"] = team_name
    summary["season"] = season
    summary["n_posts"] = int(len(group))

    # Performance metrics (only compute if columns exist)
    def safe_mean(col: str) -> Optional[float]:
        if col in group.columns:
            values = pd.to_numeric(group[col], errors="coerce")
            if values.notna().any():
                return float(values.mean())
        return None

    summary["win_pct_mean"] = safe_mean(COL_WIN_PCT)
    summary["runs_scored_mean"] = safe_mean(COL_RUNS_SCORED)
    summary["runs_allowed_mean"] = safe_mean(COL_RUNS_ALLOWED)
    summary["wins_mean"] = safe_mean(COL_WINS)
    summary["losses_mean"] = safe_mean(COL_LOSSES)

    # Sentiment stats
    if COL_SENTIMENT_SCORE in group.columns:
        scores = pd.to_numeric(group[COL_SENTIMENT_SCORE], errors="coerce")
        scores = scores.dropna()
        if not scores.empty:
            summary["sentiment_avg"] = float(scores.mean())
            summary["sentiment_std"] = float(scores.std(ddof=0))
        else:
            summary["sentiment_avg"] = None
            summary["sentiment_std"] = None
    else:
        summary["sentiment_avg"] = None
        summary["sentiment_std"] = None

    # Categorical distributions
    if COL_SENTIMENT_LABEL in group.columns:
        summary["sentiment_dist"] = value_distribution(group[COL_SENTIMENT_LABEL])
    else:
        summary["sentiment_dist"] = []

    if COL_EMOTION_LABEL in group.columns:
        summary["emotion_dist"] = value_distribution(group[COL_EMOTION_LABEL])
    else:
        summary["emotion_dist"] = []

    if COL_THEME_LABEL in group.columns:
        summary["theme_dist"] = value_distribution(group[COL_THEME_LABEL])
    else:
        summary["theme_dist"] = []

    return summary


def build_team_summary_text(agg: Dict[str, Any]) -> str:
    """
    Turn the aggregated dict for a team (optional season) into a short,
    human-readable summary string suitable for RAG.
    """
    team_name = agg.get("team_name") or "Unknown Team"
    season = agg.get("season")
    n_posts = agg.get("n_posts", 0)

    header = (
        f"{team_name} – Overall Team Summary"
        if season is None
        else f"{team_name} – {season} Season Summary"
    )

    lines = [header, "-" * len(header)]

    # Performance
    perf_bits = []
    win_pct = agg.get("win_pct_mean")
    if win_pct is not None:
        perf_bits.append(f"average win percentage of {win_pct:.3f}")
    runs_scored = agg.get("runs_scored_mean")
    runs_allowed = agg.get("runs_allowed_mean")
    if runs_scored is not None and runs_allowed is not None:
        perf_bits.append(
            f"typical runs scored of {runs_scored:.1f} and runs allowed of {runs_allowed:.1f}"
        )
    wins_mean = agg.get("wins_mean")
    losses_mean = agg.get("losses_mean")
    if wins_mean is not None and losses_mean is not None:
        perf_bits.append(
            f"approximate record around {wins_mean:.1f} wins and {losses_mean:.1f} losses"
        )

    if perf_bits:
        lines.append(
            "On-field performance metrics suggest the team had "
            + "; ".join(perf_bits)
            + "."
        )

    # Sentiment
    sentiment_avg = agg.get("sentiment_avg")
    sentiment_std = agg.get("sentiment_std")
    if sentiment_avg is not None:
        sentiment_line = (
            f"Across {n_posts:,} social posts, the average sentiment score "
            f"was {sentiment_avg:.3f}"
        )
        if sentiment_std is not None:
            sentiment_line += f" (volatility/std ≈ {sentiment_std:.3f})."
        else:
            sentiment_line += "."
        lines.append(sentiment_line)
    else:
        lines.append(f"The summary is based on {n_posts:,} social posts.")

    # Helper to describe distributions
    def describe_distribution(name: str, dist: List[Dict[str, Any]]) -> Optional[str]:
        if not dist:
            return None
        parts = []
        for item in dist:
            label = item["label"]
            pct = item["pct"]
            parts.append(f"{label} ({pct*100:.1f}%)")
        return f"{name} were dominated by " + ", ".join(parts) + "."

    # Sentiment label distribution
    sent_dist = describe_distribution("Sentiment labels", agg.get("sentiment_dist", []))
    if sent_dist:
        lines.append(sent_dist)

    # Emotion distribution
    emo_dist = describe_distribution("Emotional tone", agg.get("emotion_dist", []))
    if emo_dist:
        lines.append(emo_dist)

    # Theme distribution
    theme_dist = describe_distribution("Content themes", agg.get("theme_dist", []))
    if theme_dist:
        lines.append(theme_dist)

    lines.append(
        "This summary captures how team performance, fan sentiment, emotions, "
        "and post themes interact at a high level for retrieval-augmented analysis."
    )

    return "\n".join(lines)


def to_builtin(obj):
    """Recursively convert numpy / pandas types to plain Python types."""
    import numpy as np

    if isinstance(obj, dict):
        return {k: to_builtin(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_builtin(v) for v in obj]
    elif isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating,)):
        return float(obj)
    elif isinstance(obj, (np.bool_,)):
        return bool(obj)
    else:
        return obj


def group_and_summarize(
    themes: pd.DataFrame, group_by_season: bool = True
) -> List[Dict[str, Any]]:
    """
    Group the themes dataframe by team (and optionally season),
    aggregate stats, and build JSONL-ready docs.

    Returns a list of dicts with keys:
      - id
      - type
      - metadata
      - text
    """
    group_cols = [COL_TEAM_NAME]

    if group_by_season and COL_SEASON in themes.columns:
        group_cols.append(COL_SEASON)
    elif group_by_season:
        print(
            f"Warning: group_by_season=True but '{COL_SEASON}' column is missing; "
            "falling back to grouping by team only."
        )

    print(f"Grouping by columns: {group_cols}")
    docs: List[Dict[str, Any]] = []

    for keys, group in themes.groupby(group_cols):
        # keys is a scalar if only 1 group col; make it a tuple for uniform handling
        if not isinstance(keys, tuple):
            keys = (keys,)

        agg = aggregate_group(group)
        text = build_team_summary_text(agg)

        # Build a stable ID: team slug + optional season
        team_name = agg.get("team_name") or "unknown-team"
        team_slug = slugify(team_name)
        season = agg.get("season")

        if season is not None and not (isinstance(season, float) and np.isnan(season)):
            doc_id = f"team-{team_slug}-{season}"
        else:
            doc_id = f"team-{team_slug}"

        metadata = {
            "team_name": agg.get("team_name"),
            "season": agg.get("season"),
            "n_posts": agg.get("n_posts"),
            "win_pct_mean": agg.get("win_pct_mean"),
            "runs_scored_mean": agg.get("runs_scored_mean"),
            "runs_allowed_mean": agg.get("runs_allowed_mean"),
            "wins_mean": agg.get("wins_mean"),
            "losses_mean": agg.get("losses_mean"),
            "sentiment_avg": agg.get("sentiment_avg"),
            "sentiment_std": agg.get("sentiment_std"),
            "sentiment_dist": agg.get("sentiment_dist"),
            "emotion_dist": agg.get("emotion_dist"),
            "theme_dist": agg.get("theme_dist"),
        }

        doc = {
            "id": doc_id,
            "type": "team_summary",
            "metadata": metadata,
            "text": text,
        }
        docs.append(doc)

    print(f"Created {len(docs):,} team summary docs.")
    return docs


def write_jsonl(docs: List[Dict[str, Any]], output_path: str) -> None:
    """Write one JSON object per line to output_path."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    print(f"Writing JSONL to: {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        for doc in docs:
            safe_doc = to_builtin(doc)  # <-- convert numpy types to builtins
            line = json.dumps(safe_doc, ensure_ascii=False)
            f.write(line + "\n")
    print("Done.")



# =========================
# MAIN / CLI
# =========================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare team-level summary docs for RAG from themes/sentiment CSV."
    )
    parser.add_argument(
        "--themes-csv",
        default=DEFAULT_THEMES_PATH,
        help=f"Path to team themes/sentiment CSV (default: {DEFAULT_THEMES_PATH})",
    )
    parser.add_argument(
        "--output-jsonl",
        default=DEFAULT_OUTPUT_PATH,
        help=f"Output JSONL path (default: {DEFAULT_OUTPUT_PATH})",
    )
    parser.add_argument(
        "--no-season",
        action="store_true",
        help="If set, do NOT group by season; only group by team.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    themes = load_themes(args.themes_csv)
    docs = group_and_summarize(themes, group_by_season=not args.no_season)
    write_jsonl(docs, args.output_jsonl)


if __name__ == "__main__":
    main()