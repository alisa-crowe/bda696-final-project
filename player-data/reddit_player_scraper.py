import argparse
import datetime as dt
import re
import time
from pathlib import Path
from typing import Dict, List

import pandas as pd
import praw
from prawcore.exceptions import TooManyRequests, NotFound, Forbidden, Redirect

SUBREDDITS = [
    "baseball", "mlb", "fantasybaseball",
    "nyyankees","NewYorkMets","redsox","Orioles","TampaBayRays",
    "phillies","Braves","Nationals","letsgofish",
    "Dodgers","SFGiants","Padres","azdiamondbacks","ColoradoRockies",
    "Astros","TexasRangers","Mariners","OaklandAthletics","angelsbaseball",
    "minnesotatwins","kansascityroyals","clevelandguardians","whitesox","motorcitykitties",
    "chicubs","cardinals","Brewers","buccos","reds",
]

def clean_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()

def utc_iso(ts_utc: float) -> str:
    return dt.datetime.fromtimestamp(ts_utc, tz=dt.timezone.utc).isoformat()

def search_query_for_player(name: str) -> str:
    # exact phrase match in title or body
    return f'title:"{name}" OR selftext:"{name}"'

def fetch_reddit_players(
    client_id: str,
    client_secret: str,
    user_agent: str,
    keywords_by_player: Dict[str, List[str]],
    per_player: int = 100,
    time_filter: str = "week",
    include_comments: bool = True,
    subreddits: List[str] | None = None,
    out_path: str | None = None,
    comments_per_post: int = 20,
):
    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
        requestor_kwargs={"timeout": 15},
    )

    if not subreddits:
        subreddits = SUBREDDITS

    rows = []
    total = 0

    for player_label, kw_list in keywords_by_player.items():
        collected_for_player = 0
        print(f"\n=== Player: {player_label} (target {per_player}) ===", flush=True)

        # iterate through keywords for this player until we hit the cap
        for kw in kw_list:
            if collected_for_player >= per_player:
                break

            for sub in subreddits:
                if collected_for_player >= per_player:
                    break

                sr = reddit.subreddit(sub)
                query = search_query_for_player(kw)
                print(f"[{sub}] searching: {query}", flush=True)

                backoff = 5
                while True:
                    try:
                        for submission in sr.search(query=query, sort="new",
                                                    time_filter=time_filter,
                                                    limit=per_player*2):
                            if collected_for_player >= per_player:
                                break
                            body = clean_text(f"{submission.title} {submission.selftext or ''}")
                            if not body:
                                continue
                            rows.append({
                                "entity_type": "player",
                                "matched_player": player_label,   # keep
                                "matched_keyword": kw,            # keep
                                "source": "reddit_post",
                                "subreddit": sub,
                                "author": str(submission.author) if submission.author else None,
                                "text": body,
                                "permalink": f"https://reddit.com{submission.permalink}",
                                "created_utc": utc_iso(submission.created_utc),
                            })
                            collected_for_player += 1
                            total += 1

                            if include_comments and collected_for_player < per_player:
                                submission.comments.replace_more(limit=0)
                                for c in submission.comments[:comments_per_post]:
                                    if collected_for_player >= per_player:
                                        break
                                    c_body = clean_text(getattr(c, "body", "") or "")
                                    if not c_body:
                                        continue
                                    rows.append({
                                        "entity_type": "player",
                                        "matched_player": player_label,
                                        "matched_keyword": kw,
                                        "source": "reddit_comment",
                                        "subreddit": sub,
                                        "author": str(c.author) if c.author else None,
                                        "text": c_body,
                                        "permalink": f"https://reddit.com{c.permalink}",
                                        "created_utc": utc_iso(c.created_utc),
                                    })
                                    collected_for_player += 1
                                    total += 1

                            # gentle throttle between API calls
                            time.sleep(0.25)

                        break  # search ok, leave backoff loop

                    except (Redirect, NotFound, Forbidden):
                        print(f"[skip] subreddit '{sub}' not accessible; skipping.", flush=True)
                        break  # stop trying this subreddit

                    except TooManyRequests as e:
                        wait = getattr(e, "seconds_to_reset", None) or backoff
                        print(f"[rate limit] sleeping {wait}s...", flush=True)
                        time.sleep(wait)
                        backoff = min(backoff * 2, 120)

            # (optional) tiny pause between keywords to be gentle
            time.sleep(0.2)

        print(f"[done] {player_label}: collected {collected_for_player}", flush=True)

        if out_path and (len(rows) % 1000 != 0):  # occasional flush
            pd.DataFrame(rows).drop_duplicates(subset=["text","permalink"]).to_csv(out_path, index=False)

    df = pd.DataFrame(rows).drop_duplicates(subset=["text","permalink"])
    if not df.empty:
        df["char_len"] = df["text"].str.len()
    return df

def main():
    ap = argparse.ArgumentParser(description="Collect Reddit posts/comments mentioning MLB players from a CSV with player_label & keyword.")
    ap.add_argument("--client-id", required=True)
    ap.add_argument("--client-secret", required=True)
    ap.add_argument("--user-agent", required=True, help='e.g. "mlb-sentiment by u/YOURNAME"')

    ap.add_argument("--players-csv", required=True, help="CSV file with columns: player_label, keyword")
    ap.add_argument("--player-col", default="keyword", help="Column name containing the keyword strings")
    ap.add_argument("--per-player", type=int, default=75, help="Target items (posts+comments) per player (50–100 recommended)")
    ap.add_argument("--time-filter", default="week", choices=["all","day","hour","month","week","year"])
    ap.add_argument("--include-comments", action="store_true")
    ap.add_argument("--subs", nargs="+", help="Override subreddit list (e.g., --subs baseball mlb)")
    ap.add_argument("--out", default="reddit-players.csv", help="Output CSV path")
    ap.add_argument("--comments-per-post", type=int, default=20, help="Max comments to pull per matched post")

    # batching by players (Approach A)
    ap.add_argument("--start-player", type=int, default=0, help="Start index in the sorted unique player list")
    ap.add_argument("--num-players", type=int, default=None, help="Number of players to process from start")
    args = ap.parse_args()

    kw_df = pd.read_csv(args.players_csv)
    if "player_label" not in kw_df.columns or args.player_col not in kw_df.columns:
        raise ValueError(f"CSV must include both 'player_label' and '{args.player_col}' columns.")

    # Build player list, slice by players
    all_players = sorted(kw_df["player_label"].dropna().unique().tolist())
    total_players = len(all_players)

    start = max(args.start_player, 0)
    end = None if args.num_players is None else start + max(args.num_players, 0)
    if start >= total_players:
        raise SystemExit(f"--start-player {start} >= total players {total_players}")

    run_players = set(all_players[start:end])
    print(f"[slice] processing players[{start}:{'end' if end is None else end}] "
          f"= {len(run_players)} players (of {total_players} total)")

    # Filter to selected players and build mapping: player -> list of keywords
    batch_df = kw_df[kw_df["player_label"].isin(run_players)].copy()
    keywords_by_player = {
        player: sub[args.player_col].dropna().astype(str).str.strip().tolist()
        for player, sub in batch_df.groupby("player_label")
    }

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)

    df = fetch_reddit_players(
        client_id=args.client_id,
        client_secret=args.client_secret,
        user_agent=args.user_agent,
        keywords_by_player=keywords_by_player,
        per_player=args.per_player,
        time_filter=args.time_filter,
        include_comments=args.include_comments,
        subreddits=args.subs,
        out_path=args.out,
        comments_per_post=args.comments_per_post,
    )

    df.to_csv(args.out, index=False)
    print(f"Wrote {args.out} with {len(df)} rows.")

if __name__ == "__main__":
    main()