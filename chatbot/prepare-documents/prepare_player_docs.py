#!/usr/bin/env python
"""
prepare_player_docs.py

Builds player-level summary docs from:
  - combined-data/combined_team_and_player_data.csv

Each JSONL line will look like:
{
  "id": "player-shohei-ohtani-lad-2025",
  "type": "player_summary",
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
DEFAULT_COMBINED_PATH = "prepare-documents/combined_team_and_player_data.csv"
DEFAULT_OUTPUT_PATH = "documents/player_insights/player_docs.jsonl"

# ---- Identity columns (tweak to match your schema) ----
COL_PLAYER_NAME = "matched_player"
COL_PLAYER_NAME_FALLBACK = "player_name"      # e.g., "Shohei Ohtani"
COL_PLAYER_ID = "player_name_norm"          # if you have a stable id; else we’ll fall back to name
COL_TEAM_NAME = "team_name"          # team the player is associated with in that row
COL_TEAM_ID = "team_team_name"          # like "LAD", "ATL" if available
COL_SEASON = "year"                # e.g., 2025; set to "year" or similar if needed

# ---- Text / post-level info (optional) ----
COL_TEXT = "text_playerrec"          # social text column for player posts

# ---- Performance metric columns (batters) ----
COL_BATTING_AVG = "player_batting_avg"  # batting average
COL_HR = "player_home_runs"            # home runs
COL_RBI = "player_runs_batted_in"      # runs batted in
COL_OBP = "player_on_base_pct"         # on-base percentage
COL_SLG = "player_slugging_pct"        # slugging
COL_OPS = "player_on_base_plus_slugging"  # OPS
COL_HITS = "player_hits"
COL_RUNS = "player_runs_scored"
COL_WALKS = "player_walks"
COL_STRIKEOUTS = "player_strikeouts"
COL_WAR = "player_war"

# ---- Pitching metric columns (pitchers) ----
# Note: Pitching stats not available in current dataset
COL_ERA = None                        # earned run average
COL_WHIP = None                       # walks + hits per inning pitched
COL_K_PER_9 = None                    # strikeouts per 9
COL_BB_PER_9 = None                   # walks per 9

# ---- Sentiment / emotion / theme columns (only if present) ----
# Note: These don't exist in combined dataset - would need to merge from team_data_with_themes_final.csv
COL_SENTIMENT_SCORE = None
COL_SENTIMENT_LABEL = None
COL_EMOTION_LABEL = None
COL_THEME_LABEL = None


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


def load_combined(path: str) -> pd.DataFrame:
    print(f"Loading combined team + player data from: {path}")
    combined = pd.read_csv(path, low_memory=False)
    print(f"  -> {len(combined):,} rows")

    # Check for player name column (try matched_player first, then player_name)
    if COL_PLAYER_NAME not in combined.columns:
        if COL_PLAYER_NAME_FALLBACK in combined.columns:
            print(f"  -> Using '{COL_PLAYER_NAME_FALLBACK}' as player name column")
            # We'll handle this in the grouping logic
        else:
            raise KeyError(
                f"Expected '{COL_PLAYER_NAME}' or '{COL_PLAYER_NAME_FALLBACK}' column in combined CSV. "
                f"Current columns: {list(combined.columns)[:20]}..."
            )
    
    # Filter to only rows with player mentions
    if COL_PLAYER_NAME in combined.columns:
        player_rows = combined[combined[COL_PLAYER_NAME].notna()]
        print(f"  -> {len(player_rows):,} rows with player mentions")
        return player_rows
    elif COL_PLAYER_NAME_FALLBACK in combined.columns:
        player_rows = combined[combined[COL_PLAYER_NAME_FALLBACK].notna()]
        print(f"  -> {len(player_rows):,} rows with player mentions")
        return player_rows
    else:
        return combined


def value_distribution(series: pd.Series, top_n: int = 5) -> List[Dict[str, Any]]:
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


def safe_mean(group: pd.DataFrame, col: str) -> Optional[float]:
    if col in group.columns:
        values = pd.to_numeric(group[col], errors="coerce")
        values = values.dropna()
        if not values.empty:
            return float(values.mean())
    return None


def aggregate_player_group(group: pd.DataFrame) -> Dict[str, Any]:
    """
    Given all rows for a player group, compute aggregated stats.

    Returns dict with:
      - identity info (player_name, player_id, team_name, team_id, season)
      - n_posts
      - batting aggregates where available
      - sentiment mean/std (if present)
      - distributions for sentiment/emotion/theme (if present)
    """
    summary: Dict[str, Any] = {}

    # Identity - use matched_player or fallback to player_name
    if COL_PLAYER_NAME in group.columns:
        player_name = group[COL_PLAYER_NAME].iloc[0]
    elif COL_PLAYER_NAME_FALLBACK in group.columns:
        player_name = group[COL_PLAYER_NAME_FALLBACK].iloc[0]
    else:
        player_name = "Unknown Player"
    
    player_id = None
    if COL_PLAYER_ID in group.columns:
        player_id_vals = group[COL_PLAYER_ID].dropna()
        if not player_id_vals.empty:
            player_id = player_id_vals.iloc[0]
    
    # Get most common team (since a player might be mentioned across multiple teams)
    team_name = None
    if COL_TEAM_NAME in group.columns:
        team_counts = group[COL_TEAM_NAME].value_counts()
        if not team_counts.empty:
            team_name = team_counts.index[0]
    
    team_id = None
    if COL_TEAM_ID in group.columns:
        team_id_vals = group[COL_TEAM_ID].dropna()
        if not team_id_vals.empty:
            team_id = team_id_vals.iloc[0]
    
    season = None
    if COL_SEASON in group.columns:
        season_vals = group[COL_SEASON].dropna()
        if not season_vals.empty:
            season = int(season_vals.iloc[0]) if pd.notna(season_vals.iloc[0]) else None

    summary["player_name"] = player_name
    summary["player_id"] = player_id
    summary["team_name"] = team_name
    summary["team_id"] = team_id
    summary["season"] = season
    summary["n_rows"] = int(len(group))

    # Count posts from text_playerrec column
    if COL_TEXT in group.columns:
        summary["n_posts"] = int(group[COL_TEXT].notna().sum())
    else:
        summary["n_posts"] = int(len(group))

    # Batting stats
    summary["batting_avg_mean"] = safe_mean(group, COL_BATTING_AVG) if COL_BATTING_AVG else None
    summary["hr_mean"] = safe_mean(group, COL_HR) if COL_HR else None
    summary["rbi_mean"] = safe_mean(group, COL_RBI) if COL_RBI else None
    summary["obp_mean"] = safe_mean(group, COL_OBP) if COL_OBP else None
    summary["slg_mean"] = safe_mean(group, COL_SLG) if COL_SLG else None
    summary["ops_mean"] = safe_mean(group, COL_OPS) if COL_OPS else None
    summary["hits_mean"] = safe_mean(group, COL_HITS) if COL_HITS else None
    summary["runs_mean"] = safe_mean(group, COL_RUNS) if COL_RUNS else None
    summary["walks_mean"] = safe_mean(group, COL_WALKS) if COL_WALKS else None
    summary["strikeouts_mean"] = safe_mean(group, COL_STRIKEOUTS) if COL_STRIKEOUTS else None
    summary["war_mean"] = safe_mean(group, COL_WAR) if COL_WAR else None

    # Pitching stats (not available in current dataset)
    summary["era_mean"] = safe_mean(group, COL_ERA) if COL_ERA else None
    summary["whip_mean"] = safe_mean(group, COL_WHIP) if COL_WHIP else None
    summary["k_per_9_mean"] = safe_mean(group, COL_K_PER_9) if COL_K_PER_9 else None
    summary["bb_per_9_mean"] = safe_mean(group, COL_BB_PER_9) if COL_BB_PER_9 else None

    # Sentiment stats (not available in current dataset)
    summary["sentiment_avg"] = None
    summary["sentiment_std"] = None
    summary["sentiment_dist"] = []
    summary["emotion_dist"] = []
    summary["theme_dist"] = []

    return summary


def build_player_summary_text(agg: Dict[str, Any]) -> str:
    """
    Turn aggregated player dict into a human-readable summary.
    """
    player_name = agg.get("player_name") or "Unknown Player"
    team_name = agg.get("team_name")
    season = agg.get("season")
    n_posts = agg.get("n_posts", 0)

    if season is not None:
        header = f"{player_name} – {season} Player Summary"
    else:
        header = f"{player_name} – Overall Player Summary"

    if team_name:
        header += f" ({team_name})"

    lines = [header, "-" * len(header)]

    # Batting
    batting_bits = []
    avg = agg.get("batting_avg_mean")
    hr = agg.get("hr_mean")
    rbi = agg.get("rbi_mean")
    obp = agg.get("obp_mean")
    slg = agg.get("slg_mean")
    ops = agg.get("ops_mean")

    if avg is not None:
        batting_bits.append(f"batting average around {avg:.3f}")
    if hr is not None:
        batting_bits.append(f"typical home run total of {hr:.1f}")
    if rbi is not None:
        batting_bits.append(f"average RBI around {rbi:.1f}")
    hits = agg.get("hits_mean")
    runs = agg.get("runs_mean")
    if hits is not None:
        batting_bits.append(f"average hits around {hits:.1f}")
    if runs is not None:
        batting_bits.append(f"average runs scored around {runs:.1f}")
    if obp is not None or slg is not None or ops is not None:
        slash_parts = []
        if obp is not None:
            slash_parts.append(f"OBP {obp:.3f}")
        if slg is not None:
            slash_parts.append(f"SLG {slg:.3f}")
        if ops is not None:
            slash_parts.append(f"OPS {ops:.3f}")
        batting_bits.append(" / ".join(slash_parts))
    war = agg.get("war_mean")
    if war is not None:
        batting_bits.append(f"WAR around {war:.2f}")

    if batting_bits:
        lines.append("Offensively, the player shows " + "; ".join(batting_bits) + ".")

    # Pitching
    pitching_bits = []
    era = agg.get("era_mean")
    whip = agg.get("whip_mean")
    k9 = agg.get("k_per_9_mean")
    bb9 = agg.get("bb_per_9_mean")

    if era is not None:
        pitching_bits.append(f"ERA around {era:.2f}")
    if whip is not None:
        pitching_bits.append(f"WHIP near {whip:.2f}")
    if k9 is not None:
        pitching_bits.append(f"{k9:.1f} strikeouts per 9 innings")
    if bb9 is not None:
        pitching_bits.append(f"{bb9:.1f} walks per 9 innings")

    if pitching_bits:
        lines.append("On the mound, the player profiles with " + "; ".join(pitching_bits) + ".")

    # Sentiment
    sentiment_avg = agg.get("sentiment_avg")
    sentiment_std = agg.get("sentiment_std")
    if sentiment_avg is not None:
        line = (
            f"Across {n_posts:,} related social posts, the average sentiment score "
            f"was {sentiment_avg:.3f}"
        )
        if sentiment_std is not None:
            line += f" (volatility/std ≈ {sentiment_std:.3f})."
        else:
            line += "."
        lines.append(line)
    else:
        lines.append(f"The summary is based on {n_posts:,} rows in the integrated dataset.")

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
        "This summary captures how on-field performance and fan conversation "
        "around this player interact, for retrieval-augmented analysis."
    )

    return "\n".join(lines)


def group_and_summarize(
    combined: pd.DataFrame,
    group_by_team: bool = False,  # Changed default: group by player only to get ALL players
    group_by_season: bool = False,  # Changed default: don't split by season
) -> List[Dict[str, Any]]:
    """
    Group combined dataframe by player (optionally team + season),
    aggregate stats, and build JSONL-ready docs.
    
    By default, groups by player only to include ALL unique players mentioned.
    """
    # Determine which player name column to use
    if COL_PLAYER_NAME in combined.columns:
        player_col = COL_PLAYER_NAME
    elif COL_PLAYER_NAME_FALLBACK in combined.columns:
        player_col = COL_PLAYER_NAME_FALLBACK
    else:
        raise ValueError(f"Neither {COL_PLAYER_NAME} nor {COL_PLAYER_NAME_FALLBACK} found in columns")
    
    group_cols = [player_col]

    if group_by_team and COL_TEAM_NAME in combined.columns:
        group_cols.append(COL_TEAM_NAME)
    if group_by_season and COL_SEASON in combined.columns:
        group_cols.append(COL_SEASON)

    print(f"Grouping by columns: {group_cols}")
    print(f"  -> Total unique players: {combined[player_col].nunique():,}")
    docs: List[Dict[str, Any]] = []

    for keys, group in combined.groupby(group_cols):
        if not isinstance(keys, tuple):
            keys = (keys,)

        agg = aggregate_player_group(group)
        text = build_player_summary_text(agg)

        # Build ID: player slug only (no team/season to avoid duplicates)
        player_name = agg.get("player_name") or "unknown-player"
        player_slug = slugify(player_name)
        
        # Only include team/season in ID if grouping by them
        parts = ["player", player_slug]
        if group_by_team:
            team_id = agg.get("team_id") or agg.get("team_name")
            team_slug = slugify(team_id) if team_id is not None else None
            if team_slug:
                parts.append(team_slug)
        if group_by_season:
            season = agg.get("season")
            if season is not None and not (isinstance(season, float) and np.isnan(season)):
                parts.append(str(season))
        doc_id = "-".join(parts)

        metadata = {
            "player_name": agg.get("player_name"),
            "player_id": agg.get("player_id"),
            "team_name": agg.get("team_name"),
            "team_id": agg.get("team_id"),
            "season": agg.get("season"),
            "n_rows": agg.get("n_rows"),
            "n_posts": agg.get("n_posts"),
            "batting_avg_mean": agg.get("batting_avg_mean"),
            "hr_mean": agg.get("hr_mean"),
            "rbi_mean": agg.get("rbi_mean"),
            "obp_mean": agg.get("obp_mean"),
            "slg_mean": agg.get("slg_mean"),
            "ops_mean": agg.get("ops_mean"),
            "hits_mean": agg.get("hits_mean"),
            "runs_mean": agg.get("runs_mean"),
            "walks_mean": agg.get("walks_mean"),
            "strikeouts_mean": agg.get("strikeouts_mean"),
            "war_mean": agg.get("war_mean"),
            "era_mean": agg.get("era_mean"),
            "whip_mean": agg.get("whip_mean"),
            "k_per_9_mean": agg.get("k_per_9_mean"),
            "bb_per_9_mean": agg.get("bb_per_9_mean"),
            "sentiment_avg": agg.get("sentiment_avg"),
            "sentiment_std": agg.get("sentiment_std"),
            "sentiment_dist": agg.get("sentiment_dist"),
            "emotion_dist": agg.get("emotion_dist"),
            "theme_dist": agg.get("theme_dist"),
        }

        doc = {
            "id": doc_id,
            "type": "player_summary",
            "metadata": metadata,
            "text": text,
        }
        docs.append(doc)

    print(f"Created {len(docs):,} player summary docs.")
    return docs


def to_builtin(obj):
    """Recursively convert numpy / pandas types to plain Python types for JSON."""
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


def write_jsonl(docs: List[Dict[str, Any]], output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    print(f"Writing JSONL to: {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        for doc in docs:
            safe_doc = to_builtin(doc)
            line = json.dumps(safe_doc, ensure_ascii=False)
            f.write(line + "\n")
    print("Done.")


# =========================
# MAIN / CLI
# =========================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare player-level summary docs for RAG from combined CSV."
    )
    parser.add_argument(
        "--combined-csv",
        default=DEFAULT_COMBINED_PATH,
        help=f"Path to combined team+player+posts CSV (default: {DEFAULT_COMBINED_PATH})",
    )
    parser.add_argument(
        "--output-jsonl",
        default=DEFAULT_OUTPUT_PATH,
        help=f"Output JSONL path (default: {DEFAULT_OUTPUT_PATH})",
    )
    parser.add_argument(
        "--no-team",
        action="store_true",
        help="If set, do NOT group by team; group by player (and season) only.",
    )
    parser.add_argument(
        "--no-season",
        action="store_true",
        help="If set, do NOT group by season; group by player (and optional team) only.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    combined = load_combined(args.combined_csv)
    docs = group_and_summarize(
        combined,
        group_by_team=args.no_team,  # Inverted: --no-team means group_by_team=False
        group_by_season=args.no_season,  # Inverted: --no-season means group_by_season=False
    )
    write_jsonl(docs, args.output_jsonl)


if __name__ == "__main__":
    main()
