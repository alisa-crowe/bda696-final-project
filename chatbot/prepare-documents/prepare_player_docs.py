#!/usr/bin/env python
"""
prepare_player_docs.py

Builds clean player-level summary docs from combined_team_and_player_data.csv.

Each JSONL line represents ONE player with:
- player_name (from player_name_norm - the actual player ID)
- team_name (from player_team - the player's actual team)
- aggregated stats
- sentiment (if available)

No redundant fields like player_id vs player_name or team_id vs team_name.
"""
import os
import json
import argparse
from typing import List, Dict, Any, Optional
from pathlib import Path

import pandas as pd


# =========================
# CONFIG / COLUMN CONSTANTS
# =========================

# Get script directory for relative paths
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_COMBINED_PATH = str(SCRIPT_DIR / "combined_team_and_player_data.csv")
DEFAULT_OUTPUT_PATH = str(SCRIPT_DIR.parent / "documents" / "player_insights" / "player_docs.jsonl")

# Primary identity columns (use the actual player/team identifiers)
COL_PLAYER_NAME = "player_name_norm"  # This is the actual player ID/name
COL_TEAM_NAME = "team_name"  # Full team name (always populated for player rows)
COL_TEAM_NAME_FALLBACK = "team_team_name"  # Alternative full team name
COL_TEAM_ABBREV = "player_team"  # Team abbreviation (fallback if needed)
COL_SEASON = "year_playerrec"  # Season year

# Team abbreviation to full name mapping
TEAM_ABBREV_TO_FULL = {
    "LAD": "Los Angeles Dodgers",
    "TOR": "Toronto Blue Jays",
    "CLE": "Cleveland Guardians",
    "NYY": "New York Yankees",
    "SEA": "Seattle Mariners",
    "MIL": "Milwaukee Brewers",
    "CHC": "Chicago Cubs",
    "KCR": "Kansas City Royals",
    "NYM": "New York Mets",
    "LAA": "Los Angeles Angels",
    "PHI": "Philadelphia Phillies",
    "ARI": "Arizona Diamondbacks",
    "SDP": "San Diego Padres",
    "CIN": "Cincinnati Reds",
    "SFG": "San Francisco Giants",
    "ATL": "Atlanta Braves",
    "BOS": "Boston Red Sox",
    "TBR": "Tampa Bay Rays",
    "BAL": "Baltimore Orioles",
    "MIN": "Minnesota Twins",
    "DET": "Detroit Tigers",
    "HOU": "Houston Astros",
    "TEX": "Texas Rangers",
    "OAK": "Oakland Athletics",
    "STL": "St. Louis Cardinals",
    "PIT": "Pittsburgh Pirates",
    "COL": "Colorado Rockies",
    "MIA": "Miami Marlins",
    "WSN": "Washington Nationals",
    "CWS": "Chicago White Sox",
    "CHW": "Chicago White Sox",
}

# Text column for counting posts
COL_TEXT = "text_playerrec"

# Batting stats
COL_BATTING_AVG = "player_batting_avg"
COL_HR = "player_home_runs"
COL_RBI = "player_runs_batted_in"
COL_OBP = "player_on_base_pct"
COL_SLG = "player_slugging_pct"
COL_OPS = "player_on_base_plus_slugging"
COL_HITS = "player_hits"
COL_RUNS = "player_runs_scored"
COL_WALKS = "player_walks"
COL_STRIKEOUTS = "player_strikeouts"
COL_WAR = "player_war"

# Pitching stats (if available)
COL_ERA = None  # Not in current dataset
COL_WHIP = None
COL_K_PER_9 = None
COL_BB_PER_9 = None

# Sentiment columns (check if they exist)
COL_SENTIMENT_SCORE = None  # Check if exists
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
        elif ch in (" ", "-", "_", "."):
            out.append("-")
    slug = "".join(out)
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-") or "unknown"


def load_combined(path: str) -> pd.DataFrame:
    """Load and filter the combined CSV."""
    print(f"Loading combined data from: {path}")
    df = pd.read_csv(path, low_memory=False)
    print(f"  -> {len(df):,} total rows")
    
    # Filter to rows with player data
    if COL_PLAYER_NAME in df.columns:
        player_rows = df[df[COL_PLAYER_NAME].notna()].copy()
        print(f"  -> {len(player_rows):,} rows with player_name_norm")
    else:
        raise KeyError(f"Column '{COL_PLAYER_NAME}' not found in CSV")
    
    return player_rows


def safe_mean(series: pd.Series) -> Optional[float]:
    """Calculate mean, handling NaN and non-numeric values."""
    numeric = pd.to_numeric(series, errors='coerce')
    numeric = numeric.dropna()
    if len(numeric) > 0:
        return float(numeric.mean())
    return None


def safe_std(series: pd.Series) -> Optional[float]:
    """Calculate std dev, handling NaN and non-numeric values."""
    numeric = pd.to_numeric(series, errors='coerce')
    numeric = numeric.dropna()
    if len(numeric) > 1:
        return float(numeric.std())
    return None


def value_distribution(series: pd.Series, top_n: int = 5) -> List[Dict[str, Any]]:
    """Create distribution of values (for sentiment/emotion/theme labels)."""
    series = series.dropna()
    if series.empty:
        return []
    
    counts = series.value_counts()
    total = counts.sum()
    top_counts = counts.head(top_n)
    
    dist = []
    for label, count in top_counts.items():
        dist.append({
            "label": str(label),
            "count": int(count),
            "pct": float(count / total) if total > 0 else 0.0,
        })
    return dist


def aggregate_player_data(group: pd.DataFrame) -> Dict[str, Any]:
    """
    Aggregate all rows for a single player into summary stats.
    
    Returns clean dict with:
    - player_name (from player_name_norm)
    - team_name (from player_team - most common team)
    - season (most recent year)
    - n_posts (count of posts)
    - batting stats (means)
    - sentiment stats (if available)
    """
    summary: Dict[str, Any] = {}
    
    # Player name (primary identifier)
    player_name = group[COL_PLAYER_NAME].iloc[0]
    summary["player_name"] = str(player_name) if pd.notna(player_name) else None
    
    # Team name (prioritize full names, use most common)
    team_name = None
    
    # First try: full team name from team_name column
    if COL_TEAM_NAME in group.columns:
        team_counts = group[COL_TEAM_NAME].dropna()
        if not team_counts.empty:
            team_name = str(team_counts.value_counts().index[0])
    
    # Second try: full team name from team_team_name column
    if not team_name and COL_TEAM_NAME_FALLBACK in group.columns:
        team_counts = group[COL_TEAM_NAME_FALLBACK].dropna()
        if not team_counts.empty:
            team_name = str(team_counts.value_counts().index[0])
    
    # Third try: convert abbreviation to full name
    if not team_name and COL_TEAM_ABBREV in group.columns:
        abbrev_counts = group[COL_TEAM_ABBREV].dropna()
        if not abbrev_counts.empty:
            abbrev = str(abbrev_counts.value_counts().index[0]).strip()
            # Map abbreviation to full name
            if abbrev in TEAM_ABBREV_TO_FULL:
                team_name = TEAM_ABBREV_TO_FULL[abbrev]
            else:
                # If abbreviation not in mapping, use it as-is (better than null)
                team_name = abbrev
    
    summary["team_name"] = team_name
    
    # Season (most recent year)
    if COL_SEASON in group.columns:
        season_vals = group[COL_SEASON].dropna()
        if not season_vals.empty:
            # Get most recent season
            numeric_seasons = pd.to_numeric(season_vals, errors='coerce').dropna()
            if not numeric_seasons.empty:
                summary["season"] = int(numeric_seasons.max())
            else:
                summary["season"] = None
        else:
            summary["season"] = None
    else:
        summary["season"] = None
    
    # Count posts
    if COL_TEXT in group.columns:
        summary["n_posts"] = int(group[COL_TEXT].notna().sum())
    else:
        summary["n_posts"] = int(len(group))
    
    summary["n_rows"] = int(len(group))
    
    # Batting stats (aggregate means)
    summary["batting_avg"] = safe_mean(group[COL_BATTING_AVG]) if COL_BATTING_AVG in group.columns else None
    summary["home_runs"] = safe_mean(group[COL_HR]) if COL_HR in group.columns else None
    summary["rbi"] = safe_mean(group[COL_RBI]) if COL_RBI in group.columns else None
    summary["obp"] = safe_mean(group[COL_OBP]) if COL_OBP in group.columns else None
    summary["slg"] = safe_mean(group[COL_SLG]) if COL_SLG in group.columns else None
    summary["ops"] = safe_mean(group[COL_OPS]) if COL_OPS in group.columns else None
    summary["hits"] = safe_mean(group[COL_HITS]) if COL_HITS in group.columns else None
    summary["runs"] = safe_mean(group[COL_RUNS]) if COL_RUNS in group.columns else None
    summary["walks"] = safe_mean(group[COL_WALKS]) if COL_WALKS in group.columns else None
    summary["strikeouts"] = safe_mean(group[COL_STRIKEOUTS]) if COL_STRIKEOUTS in group.columns else None
    summary["war"] = safe_mean(group[COL_WAR]) if COL_WAR in group.columns else None
    
    # Pitching stats (if available)
    summary["era"] = safe_mean(group[COL_ERA]) if COL_ERA and COL_ERA in group.columns else None
    summary["whip"] = safe_mean(group[COL_WHIP]) if COL_WHIP and COL_WHIP in group.columns else None
    summary["k_per_9"] = safe_mean(group[COL_K_PER_9]) if COL_K_PER_9 and COL_K_PER_9 in group.columns else None
    summary["bb_per_9"] = safe_mean(group[COL_BB_PER_9]) if COL_BB_PER_9 and COL_BB_PER_9 in group.columns else None
    
    # Sentiment stats (check if columns exist)
    if COL_SENTIMENT_SCORE and COL_SENTIMENT_SCORE in group.columns:
        summary["sentiment_avg"] = safe_mean(group[COL_SENTIMENT_SCORE])
        summary["sentiment_std"] = safe_std(group[COL_SENTIMENT_SCORE])
    else:
        summary["sentiment_avg"] = None
        summary["sentiment_std"] = None
    
    # Sentiment/emotion/theme distributions (check if columns exist)
    if COL_SENTIMENT_LABEL and COL_SENTIMENT_LABEL in group.columns:
        summary["sentiment_dist"] = value_distribution(group[COL_SENTIMENT_LABEL])
    else:
        summary["sentiment_dist"] = []
    
    if COL_EMOTION_LABEL and COL_EMOTION_LABEL in group.columns:
        summary["emotion_dist"] = value_distribution(group[COL_EMOTION_LABEL])
    else:
        summary["emotion_dist"] = []
    
    if COL_THEME_LABEL and COL_THEME_LABEL in group.columns:
        summary["theme_dist"] = value_distribution(group[COL_THEME_LABEL])
    else:
        summary["theme_dist"] = []
    
    return summary


def build_player_summary_text(agg: Dict[str, Any]) -> str:
    """Build human-readable summary text from aggregated stats."""
    player_name = agg.get("player_name") or "Unknown Player"
    team_name = agg.get("team_name")
    season = agg.get("season")
    n_posts = agg.get("n_posts", 0)
    
    # Header
    header = f"{player_name}"
    if season:
        header += f" – {season}"
    header += " Player Summary"
    if team_name:
        header += f" ({team_name})"
    
    lines = [header, "-" * len(header)]
    
    # Batting stats
    batting_parts = []
    if agg.get("batting_avg") is not None:
        batting_parts.append(f"batting average of {agg['batting_avg']:.3f}")
    if agg.get("home_runs") is not None:
        batting_parts.append(f"{agg['home_runs']:.1f} home runs")
    if agg.get("rbi") is not None:
        batting_parts.append(f"{agg['rbi']:.1f} RBI")
    if agg.get("hits") is not None:
        batting_parts.append(f"{agg['hits']:.1f} hits")
    if agg.get("runs") is not None:
        batting_parts.append(f"{agg['runs']:.1f} runs scored")
    if agg.get("walks") is not None:
        batting_parts.append(f"{agg['walks']:.1f} walks")
    if agg.get("strikeouts") is not None:
        batting_parts.append(f"{agg['strikeouts']:.1f} strikeouts")
    
    if batting_parts:
        lines.append("Batting: " + ", ".join(batting_parts) + ".")
    
    # Slash line
    slash_parts = []
    if agg.get("obp") is not None:
        slash_parts.append(f"OBP {agg['obp']:.3f}")
    if agg.get("slg") is not None:
        slash_parts.append(f"SLG {agg['slg']:.3f}")
    if agg.get("ops") is not None:
        slash_parts.append(f"OPS {agg['ops']:.3f}")
    
    if slash_parts:
        lines.append("Slash line: " + " / ".join(slash_parts) + ".")
    
    # WAR
    if agg.get("war") is not None:
        lines.append(f"WAR: {agg['war']:.2f}.")
    
    # Pitching stats (if available)
    pitching_parts = []
    if agg.get("era") is not None:
        pitching_parts.append(f"ERA {agg['era']:.2f}")
    if agg.get("whip") is not None:
        pitching_parts.append(f"WHIP {agg['whip']:.2f}")
    if agg.get("k_per_9") is not None:
        pitching_parts.append(f"{agg['k_per_9']:.1f} K/9")
    if agg.get("bb_per_9") is not None:
        pitching_parts.append(f"{agg['bb_per_9']:.1f} BB/9")
    
    if pitching_parts:
        lines.append("Pitching: " + ", ".join(pitching_parts) + ".")
    
    # Post count
    lines.append(f"Based on {n_posts:,} social media posts mentioning this player.")
    
    # Sentiment (if available)
    if agg.get("sentiment_avg") is not None:
        sentiment_avg = agg["sentiment_avg"]
        sentiment_std = agg.get("sentiment_std")
        if sentiment_std is not None:
            lines.append(f"Average sentiment: {sentiment_avg:.3f} (std: {sentiment_std:.3f}).")
        else:
            lines.append(f"Average sentiment: {sentiment_avg:.3f}.")
    
    # Distributions (if available)
    if agg.get("sentiment_dist"):
        dist = agg["sentiment_dist"]
        parts = [f"{item['label']} ({item['pct']*100:.1f}%)" for item in dist[:3]]
        lines.append(f"Sentiment distribution: {', '.join(parts)}.")
    
    if agg.get("emotion_dist"):
        dist = agg["emotion_dist"]
        parts = [f"{item['label']} ({item['pct']*100:.1f}%)" for item in dist[:3]]
        lines.append(f"Emotion distribution: {', '.join(parts)}.")
    
    if agg.get("theme_dist"):
        dist = agg["theme_dist"]
        parts = [f"{item['label']} ({item['pct']*100:.1f}%)" for item in dist[:3]]
        lines.append(f"Theme distribution: {', '.join(parts)}.")
    
    lines.append(
        "This summary provides player performance metrics and fan sentiment "
        "for retrieval-augmented analysis."
    )
    
    return "\n".join(lines)


def to_builtin(obj):
    """Convert numpy/pandas types to plain Python types for JSON."""
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
    elif pd.isna(obj):
        return None
    else:
        return obj


def generate_player_docs(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Generate one document per unique player.
    
    Groups by player_name_norm (the actual player identifier) and aggregates all their data.
    """
    print(f"\nGenerating player documents...")
    print(f"  -> Grouping by: {COL_PLAYER_NAME}")
    
    # Group by player name only (one doc per player)
    grouped = df.groupby(COL_PLAYER_NAME)
    print(f"  -> Found {len(grouped):,} unique players")
    
    docs: List[Dict[str, Any]] = []
    skipped = 0
    
    for player_name, group in grouped:
        # Skip if player name is invalid
        if pd.isna(player_name) or str(player_name).strip() == "":
            skipped += 1
            continue
        
        # Aggregate stats for this player
        agg = aggregate_player_data(group)
        
        # Build summary text
        text = build_player_summary_text(agg)
        
        # Generate document ID
        player_slug = slugify(str(player_name))
        doc_id = f"player-{player_slug}"
        
        # Clean metadata (only include non-None values)
        metadata = {
            "player_name": agg["player_name"],
            "team_name": agg["team_name"],
            "season": agg["season"],
            "n_posts": agg["n_posts"],
            "n_rows": agg["n_rows"],
        }
        
        # Add stats (only if not None)
        if agg.get("batting_avg") is not None:
            metadata["batting_avg"] = agg["batting_avg"]
        if agg.get("home_runs") is not None:
            metadata["home_runs"] = agg["home_runs"]
        if agg.get("rbi") is not None:
            metadata["rbi"] = agg["rbi"]
        if agg.get("obp") is not None:
            metadata["obp"] = agg["obp"]
        if agg.get("slg") is not None:
            metadata["slg"] = agg["slg"]
        if agg.get("ops") is not None:
            metadata["ops"] = agg["ops"]
        if agg.get("hits") is not None:
            metadata["hits"] = agg["hits"]
        if agg.get("runs") is not None:
            metadata["runs"] = agg["runs"]
        if agg.get("walks") is not None:
            metadata["walks"] = agg["walks"]
        if agg.get("strikeouts") is not None:
            metadata["strikeouts"] = agg["strikeouts"]
        if agg.get("war") is not None:
            metadata["war"] = agg["war"]
        
        # Add sentiment (if available)
        if agg.get("sentiment_avg") is not None:
            metadata["sentiment_avg"] = agg["sentiment_avg"]
        if agg.get("sentiment_std") is not None:
            metadata["sentiment_std"] = agg["sentiment_std"]
        if agg.get("sentiment_dist"):
            metadata["sentiment_dist"] = agg["sentiment_dist"]
        if agg.get("emotion_dist"):
            metadata["emotion_dist"] = agg["emotion_dist"]
        if agg.get("theme_dist"):
            metadata["theme_dist"] = agg["theme_dist"]
        
        doc = {
            "id": doc_id,
            "type": "player_summary",
            "metadata": metadata,
            "text": text,
        }
        
        docs.append(doc)
    
    print(f"  -> Created {len(docs):,} player documents")
    if skipped > 0:
        print(f"  -> Skipped {skipped:,} invalid entries")
    
    return docs


def write_jsonl(docs: List[Dict[str, Any]], output_path: str) -> None:
    """Write documents to JSONL file."""
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"\nWriting JSONL to: {output_path}")
    with open(output_file, "w", encoding="utf-8") as f:
        for doc in docs:
            safe_doc = to_builtin(doc)
            line = json.dumps(safe_doc, ensure_ascii=False)
            f.write(line + "\n")
    print(f"Done. Wrote {len(docs):,} documents.")


# =========================
# MAIN
# =========================

def main():
    parser = argparse.ArgumentParser(
        description="Generate clean player summary documents from combined CSV."
    )
    parser.add_argument(
        "--combined-csv",
        default=DEFAULT_COMBINED_PATH,
        help=f"Path to combined CSV (default: {DEFAULT_COMBINED_PATH})",
    )
    parser.add_argument(
        "--output-jsonl",
        default=DEFAULT_OUTPUT_PATH,
        help=f"Output JSONL path (default: {DEFAULT_OUTPUT_PATH})",
    )
    args = parser.parse_args()
    
    # Load data
    df = load_combined(args.combined_csv)
    
    # Generate documents
    docs = generate_player_docs(df)
    
    # Write output
    write_jsonl(docs, args.output_jsonl)
    
    print("\n✅ Player documents generated successfully!")


if __name__ == "__main__":
    main()
