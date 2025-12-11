#!/usr/bin/env python
"""
prepare_example_docs.py

Extracts diverse example posts/quotes from the datasets for RAG knowledge base.
Samples across teams, players, sentiments, emotions, and themes to provide
comprehensive examples for the chatbot.

Outputs JSONL format with rich metadata.
"""

import os
import json
import argparse
import random
from typing import List, Dict, Any, Optional
from collections import defaultdict

import pandas as pd


# =========================
# CONFIG / FILE PATHS
# =========================

DEFAULT_THEMES_PATH = "prepare-documents/team_data_with_themes_final.csv"
DEFAULT_COMBINED_PATH = "prepare-documents/combined_team_and_player_data.csv"
DEFAULT_OUTPUT_PATH = "documents/examples/example_posts.jsonl"
DEFAULT_N_EXAMPLES = 2000  # Total examples to extract


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


def clean_text(text: Any) -> str:
    """Clean and validate text."""
    if pd.isna(text) or text is None:
        return ""
    text = str(text).strip()
    # Remove very short or very long posts
    if len(text) < 10 or len(text) > 2000:
        return ""
    return text


def sample_stratified(
    df: pd.DataFrame,
    group_col: str,
    n_per_group: int,
    min_per_group: int = 1,
) -> pd.DataFrame:
    """
    Sample n_per_group examples from each group, with minimum guarantee.
    """
    sampled = []
    groups = df.groupby(group_col)
    
    for group_name, group_df in groups:
        n_available = len(group_df)
        n_sample = min(n_per_group, max(min_per_group, n_available))
        if n_sample > 0:
            sampled_group = group_df.sample(n=n_sample, replace=False, random_state=42)
            sampled.append(sampled_group)
    
    if sampled:
        return pd.concat(sampled, ignore_index=True)
    return pd.DataFrame()


# =========================
# LOAD DATA
# =========================

def load_team_posts(themes_path: str) -> pd.DataFrame:
    """Load team posts with sentiment/emotion/theme labels."""
    print(f"Loading team posts from: {themes_path}")
    df = pd.read_csv(themes_path, low_memory=False)
    print(f"  -> {len(df):,} team posts loaded")
    
    # Ensure text column exists
    if "text" not in df.columns:
        raise ValueError("Expected 'text' column in themes CSV")
    
    # Filter to valid posts
    df = df[df["text"].notna()]
    df = df[df["text"].astype(str).str.len() >= 10]
    
    print(f"  -> {len(df):,} valid team posts")
    return df


def load_player_posts(combined_path: str) -> pd.DataFrame:
    """Load player posts from combined dataset."""
    print(f"Loading player posts from: {combined_path}")
    df = pd.read_csv(combined_path, low_memory=False)
    print(f"  -> {len(df):,} total rows loaded")
    
    # Filter to rows with player posts
    if "text_playerrec" in df.columns:
        player_df = df[df["text_playerrec"].notna()].copy()
        player_df = player_df[player_df["text_playerrec"].astype(str).str.len() >= 10]
        print(f"  -> {len(player_df):,} valid player posts")
        return player_df
    else:
        print("  -> No text_playerrec column found, skipping player posts")
        return pd.DataFrame()


# =========================
# EXTRACT EXAMPLES
# =========================

def extract_team_examples(
    df: pd.DataFrame,
    n_total: int,
) -> List[Dict[str, Any]]:
    """
    Extract diverse team post examples with stratification.
    """
    print(f"\nExtracting team examples (target: {n_total})...")
    examples = []
    
    # Calculate sampling strategy
    n_sentiment = n_total // 3  # Split across sentiment labels
    n_emotion = n_total // 5    # Split across emotion labels
    n_theme = n_total // 6      # Split across theme labels
    n_team = n_total // 30      # Split across teams
    
    # Strategy 1: Sample by sentiment label
    if "sentiment_label" in df.columns:
        sentiment_df = sample_stratified(df, "sentiment_label", n_sentiment, min_per_group=5)
        print(f"  -> Sampled {len(sentiment_df):,} by sentiment")
        for _, row in sentiment_df.iterrows():
            examples.append(create_team_example(row))
    
    # Strategy 2: Sample by emotion label
    if "emotion_label" in df.columns:
        emotion_df = sample_stratified(df, "emotion_label", n_emotion, min_per_group=3)
        print(f"  -> Sampled {len(emotion_df):,} by emotion")
        for _, row in emotion_df.iterrows():
            examples.append(create_team_example(row))
    
    # Strategy 3: Sample by theme label
    if "theme_label" in df.columns:
        theme_df = sample_stratified(df, "theme_label", n_theme, min_per_group=3)
        print(f"  -> Sampled {len(theme_df):,} by theme")
        for _, row in theme_df.iterrows():
            examples.append(create_team_example(row))
    
    # Strategy 4: Sample by team
    if "matched_keyword" in df.columns:
        team_df = sample_stratified(df, "matched_keyword", n_team, min_per_group=2)
        print(f"  -> Sampled {len(team_df):,} by team")
        for _, row in team_df.iterrows():
            examples.append(create_team_example(row))
    
    # Strategy 5: Sample diverse sentiment scores (positive, neutral, negative ranges)
    if "sentiment_score" in df.columns:
        df_pos_filtered = df[df["sentiment_score"] > 0.3]
        df_neg_filtered = df[df["sentiment_score"] < -0.3]
        df_neut_filtered = df[(df["sentiment_score"] >= -0.1) & (df["sentiment_score"] <= 0.1)]
        
        df_pos = df_pos_filtered.sample(min(100, len(df_pos_filtered)), random_state=42)
        df_neg = df_neg_filtered.sample(min(100, len(df_neg_filtered)), random_state=42)
        df_neut = df_neut_filtered.sample(min(100, len(df_neut_filtered)), random_state=42)
        
        for _, row in pd.concat([df_pos, df_neg, df_neut]).iterrows():
            examples.append(create_team_example(row))
    
    # Remove duplicates (by text)
    seen_texts = set()
    unique_examples = []
    for ex in examples:
        text_key = ex.get("text", "").lower()[:100]  # First 100 chars as key
        if text_key not in seen_texts:
            seen_texts.add(text_key)
            unique_examples.append(ex)
    
    print(f"  -> {len(unique_examples):,} unique team examples")
    return unique_examples


def create_team_example(row: pd.Series) -> Dict[str, Any]:
    """Create a team post example document."""
    text = clean_text(row.get("text", ""))
    if not text:
        return None
    
    doc = {
        "id": f"example-team-{slugify(str(row.get('matched_keyword', 'unknown')))}-{hash(text) % 100000}",
        "type": "example_post",
        "category": "team_discussion",
        "text": text,
        "metadata": {
            "team": str(row.get("matched_keyword", "")) if pd.notna(row.get("matched_keyword")) else None,
            "source": str(row.get("source", "")) if pd.notna(row.get("source")) else None,
            "subreddit": str(row.get("subreddit", "")) if pd.notna(row.get("subreddit")) else None,
            "author": str(row.get("author", "")) if pd.notna(row.get("author")) else None,
            "permalink": str(row.get("permalink", "")) if pd.notna(row.get("permalink")) else None,
            "created_utc": str(row.get("created_utc", "")) if pd.notna(row.get("created_utc")) else None,
            "year": int(row.get("year", 2025)) if pd.notna(row.get("year")) else None,
            "sentiment_score": float(row.get("sentiment_score", 0.0)) if pd.notna(row.get("sentiment_score")) else None,
            "sentiment_label": str(row.get("sentiment_label", "")) if pd.notna(row.get("sentiment_label")) else None,
            "emotion_label": str(row.get("emotion_label", "")) if pd.notna(row.get("emotion_label")) else None,
            "theme_label": str(row.get("theme_label", "")) if pd.notna(row.get("theme_label")) else None,
            "char_len": int(row.get("char_len", len(text))) if pd.notna(row.get("char_len")) else len(text),
            "team_wins": int(row.get("team_wins", 0)) if pd.notna(row.get("team_wins")) else None,
            "team_losses": int(row.get("team_losses", 0)) if pd.notna(row.get("team_losses")) else None,
            "team_win_pct": float(row.get("team_win_pct", 0.0)) if pd.notna(row.get("team_win_pct")) else None,
            "team_make_playoffs": str(row.get("team_make_playoffs", "")) if pd.notna(row.get("team_make_playoffs")) else None,
        }
    }
    
    return doc


def extract_player_examples(
    df: pd.DataFrame,
    n_total: int,
) -> List[Dict[str, Any]]:
    """
    Extract diverse player post examples.
    """
    print(f"\nExtracting player examples (target: {n_total})...")
    examples = []
    
    if df.empty or "text_playerrec" not in df.columns:
        print("  -> No player posts available")
        return examples
    
    # Filter to valid posts
    valid_df = df[df["text_playerrec"].notna()].copy()
    valid_df = valid_df[valid_df["text_playerrec"].astype(str).str.len() >= 10]
    
    if len(valid_df) == 0:
        print("  -> No valid player posts")
        return examples
    
    # Sample by player
    if "matched_player" in valid_df.columns:
        n_per_player = max(1, n_total // valid_df["matched_player"].nunique())
        player_df = sample_stratified(valid_df, "matched_player", n_per_player, min_per_group=1)
        print(f"  -> Sampled {len(player_df):,} by player")
        for _, row in player_df.iterrows():
            ex = create_player_example(row)
            if ex:
                examples.append(ex)
    
    # Sample by team
    if "team_name" in valid_df.columns:
        n_per_team = max(1, n_total // (valid_df["team_name"].nunique() * 2))
        team_df = sample_stratified(valid_df, "team_name", n_per_team, min_per_group=1)
        print(f"  -> Sampled {len(team_df):,} by team")
        for _, row in team_df.iterrows():
            ex = create_player_example(row)
            if ex:
                examples.append(ex)
    
    # Remove duplicates
    seen_texts = set()
    unique_examples = []
    for ex in examples:
        text_key = ex.get("text", "").lower()[:100]
        if text_key not in seen_texts:
            seen_texts.add(text_key)
            unique_examples.append(ex)
    
    print(f"  -> {len(unique_examples):,} unique player examples")
    return unique_examples


def create_player_example(row: pd.Series) -> Optional[Dict[str, Any]]:
    """Create a player post example document."""
    text = clean_text(row.get("text_playerrec", ""))
    if not text:
        return None
    
    player_name = str(row.get("matched_player", "")) if pd.notna(row.get("matched_player")) else None
    team_name = str(row.get("team_name", "")) if pd.notna(row.get("team_name")) else None
    
    doc = {
        "id": f"example-player-{slugify(player_name or 'unknown')}-{hash(text) % 100000}",
        "type": "example_post",
        "category": "player_discussion",
        "text": text,
        "metadata": {
            "player_name": player_name,
            "team_name": team_name,
            "source": str(row.get("source_playerrec", "")) if pd.notna(row.get("source_playerrec")) else None,
            "subreddit": str(row.get("subreddit_playerrec", "")) if pd.notna(row.get("subreddit_playerrec")) else None,
            "author": str(row.get("author_playerrec", "")) if pd.notna(row.get("author_playerrec")) else None,
            "created_utc": str(row.get("created_utc_playerrec", "")) if pd.notna(row.get("created_utc_playerrec")) else None,
            "char_len": int(row.get("char_len_playerrec", len(text))) if pd.notna(row.get("char_len_playerrec")) else len(text),
            "player_batting_avg": float(row.get("player_batting_avg", 0.0)) if pd.notna(row.get("player_batting_avg")) else None,
            "player_home_runs": int(row.get("player_home_runs", 0)) if pd.notna(row.get("player_home_runs")) else None,
            "player_runs_batted_in": int(row.get("player_runs_batted_in", 0)) if pd.notna(row.get("player_runs_batted_in")) else None,
            "player_war": float(row.get("player_war", 0.0)) if pd.notna(row.get("player_war")) else None,
        }
    }
    
    return doc


# =========================
# WRITE OUTPUT
# =========================

def to_builtin(obj):
    """Recursively convert numpy / pandas types to plain Python types for JSON."""
    if isinstance(obj, dict):
        return {k: to_builtin(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_builtin(v) for v in obj]
    elif isinstance(obj, (int, float, bool, str)):
        return obj
    elif pd.isna(obj):
        return None
    else:
        # Handle numpy/pandas types
        try:
            if hasattr(obj, 'item'):
                return obj.item()
            return obj
        except:
            return str(obj)


def write_jsonl(examples: List[Dict[str, Any]], output_path: str) -> None:
    """Write examples to JSONL file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    print(f"\nWriting {len(examples):,} examples to: {output_path}")
    
    with open(output_path, "w", encoding="utf-8") as f:
        for doc in examples:
            if doc is None:
                continue
            safe_doc = to_builtin(doc)
            line = json.dumps(safe_doc, ensure_ascii=False)
            f.write(line + "\n")
    
    print("Done.")


# =========================
# MAIN
# =========================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract diverse example posts/quotes for RAG knowledge base."
    )
    parser.add_argument(
        "--themes-csv",
        default=DEFAULT_THEMES_PATH,
        help=f"Path to team data with themes CSV (default: {DEFAULT_THEMES_PATH})",
    )
    parser.add_argument(
        "--combined-csv",
        default=DEFAULT_COMBINED_PATH,
        help=f"Path to combined team+player CSV (default: {DEFAULT_COMBINED_PATH})",
    )
    parser.add_argument(
        "--output-jsonl",
        default=DEFAULT_OUTPUT_PATH,
        help=f"Output JSONL path (default: {DEFAULT_OUTPUT_PATH})",
    )
    parser.add_argument(
        "--n-examples",
        type=int,
        default=DEFAULT_N_EXAMPLES,
        help=f"Target number of examples to extract (default: {DEFAULT_N_EXAMPLES})",
    )
    parser.add_argument(
        "--team-ratio",
        type=float,
        default=0.7,
        help="Ratio of team examples vs player examples (default: 0.7)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Load data
    team_df = load_team_posts(args.themes_csv)
    player_df = load_player_posts(args.combined_csv)
    
    # Calculate targets
    n_team = int(args.n_examples * args.team_ratio)
    n_player = args.n_examples - n_team
    
    # Extract examples
    team_examples = extract_team_examples(team_df, n_team)
    player_examples = extract_player_examples(player_df, n_player)
    
    # Combine and shuffle
    all_examples = team_examples + player_examples
    random.seed(42)
    random.shuffle(all_examples)
    
    # Limit to target (in case we got more)
    all_examples = all_examples[:args.n_examples]
    
    print(f"\n=== SUMMARY ===")
    print(f"Total examples extracted: {len(all_examples):,}")
    print(f"  -> Team examples: {len(team_examples):,}")
    print(f"  -> Player examples: {len(player_examples):,}")
    
    # Write output
    write_jsonl(all_examples, args.output_jsonl)
    
    # Print some statistics
    if team_examples:
        sentiment_counts = defaultdict(int)
        emotion_counts = defaultdict(int)
        theme_counts = defaultdict(int)
        
        for ex in team_examples:
            if ex and "metadata" in ex:
                meta = ex["metadata"]
                if meta.get("sentiment_label"):
                    sentiment_counts[meta["sentiment_label"]] += 1
                if meta.get("emotion_label"):
                    emotion_counts[meta["emotion_label"]] += 1
                if meta.get("theme_label"):
                    theme_counts[meta["theme_label"]] += 1
        
        print(f"\n=== DISTRIBUTION ===")
        if sentiment_counts:
            print(f"Sentiment labels: {dict(sentiment_counts)}")
        if emotion_counts:
            print(f"Emotion labels: {dict(emotion_counts)}")
        if theme_counts:
            print(f"Theme labels: {dict(theme_counts)}")


if __name__ == "__main__":
    main()
