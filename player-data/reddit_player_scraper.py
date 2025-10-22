import argparse
import datetime as dt
import json
import re
from pathlib import Path
from typing import List, Dict
import time

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
    text = re.sub(r"\s+", " ", text).strip()
    return text

def utc_iso(ts_utc: float) -> str:
    return dt.datetime.fromtimestamp(ts_utc, tz=dt.timezone.utc).isoformat()

def load_players(csv_path: str, name_col: str) -> List[str]:
    df = pd.read_csv(csv_path)
    if name_col not in df.columns:
        raise ValueError(f"Column '{name_col}' not in {csv_path}. Columns: {df.columns.tolist()}")
    names = (
        df[name_col]
        .astype(str)
        .str.strip()
        .dropna()
        .drop_duplicates()
        .tolist()
    )
    return [n for n in names if n]  # no empties

def search_query_for_player(name: str) -> str:
    # exact phrase match in title or body
    return f'title:"{name}" OR selftext:"{name}"'

def fetch_reddit_players(
    client_id: str,
    client_secret: str,
    user_agent: str,
    players: List[str],
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

    for pname in players:
        collected_for_player = 0
        print(f"\n=== Player: {pname} (target {per_player}) ===", flush=True)

        for sub in subreddits:
            if collected_for_player >= per_player:
                break

            try:
                sr = reddit.subreddit(sub)
                _ = next(sr.new(limit=1), None)  # validate access
            except Redirect:
                print(f"[skip] subreddit '{sub}' redirected.", flush=True)
                continue
            except (NotFound, Forbidden):
                print(f"[skip] subreddit '{sub}' 404/403.", flush=True)
                continue

            query = search_query_for_player(pname)
            print(f"[{sub}] searching: {query}", flush=True)

            backoff = 5
            while True:
                try:
                    # stop once per-player cap hits
                    for submission in sr.search(query=query, sort="new", time_filter=time_filter, limit=per_player*2):
                        if collected_for_player >= per_player:
                            break

                        body = clean_text(f"{submission.title} {submission.selftext or ''}")
                        if not body:
                            continue

                        rows.append({
                            "entity_type": "player",
                            "matched_player": pname,
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
                            # cap comments so we don't explode per-player cap
                            for c in submission.comments[:comments_per_post]:
                                if collected_for_player >= per_player:
                                    break
                                c_body = clean_text(getattr(c, "body", "") or "")
                                if not c_body:
                                    continue
                                rows.append({
                                    "entity_type": "player",
                                    "matched_player": pname,
                                    "source": "reddit_comment",
                                    "subreddit": sub,
                                    "author": str(c.author) if c.author else None,
                                    "text": c_body,
                                    "permalink": f"https://reddit.com{c.permalink}",
                                    "created_utc": utc_iso(c.created_utc),
                                })
                                collected_for_player += 1
                                total += 1

                        if out_path and (len(rows) % 1000 == 0):
                            pd.DataFrame(rows).drop_duplicates(subset=["text","permalink"]).to_csv(out_path, index=False)
                            print(f"[checkpoint] wrote {len(rows)} raw rows ({total} kept so far)", flush=True)

                    break  # search loop OK, leave backoff while

                except TooManyRequests as e:
                    # respect rate limits
                    wait = getattr(e, "seconds_to_reset", None) or backoff
                    print(f"[rate limit] sleeping {wait}s...", flush=True)
                    time.sleep(wait)
                    backoff = min(backoff * 2, 120)  # cap backoff

        print(f"[done] {pname}: collected {collected_for_player}", flush=True)

        if out_path and (len(rows) % 1000 != 0):  # occasional flush
            pd.DataFrame(rows).drop_duplicates(subset=["text","permalink"]).to_csv(out_path, index=False)

    df = pd.DataFrame(rows).drop_duplicates(subset=["text","permalink"])
    if not df.empty:
        df["char_len"] = df["text"].str.len()
    return df

def main():
    ap = argparse.ArgumentParser(description="Collect Reddit posts/comments mentioning MLB players from a CSV of names.")
    ap.add_argument("--client-id", required=True)
    ap.add_argument("--client-secret", required=True)
    ap.add_argument("--user-agent", required=True, help='e.g. "mlb-sentiment by u/YOURNAME"')

    ap.add_argument("--players-csv", default="players_nonduplicates.csv", help="CSV file with player names")
    ap.add_argument("--player-col", default="nameGiven", help="Column name containing player names")
    ap.add_argument("--per-player", type=int, default=75, help="Target items (posts+comments) per player (50–100 recommended)")
    ap.add_argument("--time-filter", default="week", choices=["all","day","hour","month","week","year"])
    ap.add_argument("--include-comments", action="store_true")
    ap.add_argument("--subs", nargs="+", help="Override subreddit list (e.g., --subs baseball mlb)")
    ap.add_argument("--out", default="reddit-players.csv", help="Output CSV path")
    ap.add_argument("--comments-per-post", type=int, default=20, help="Max comments to pull per matched post")

    args = ap.parse_args()

    players = load_players(args.players_csv, args.player_col)

    df = fetch_reddit_players(
        client_id=args.client_id,
        client_secret=args.client_secret,
        user_agent=args.user_agent,
        players=players,
        per_player=args.per_player,
        time_filter=args.time_filter,
        include_comments=args.include_comments,
        subreddits=args.subs,
        out_path=args.out,
        comments_per_post=args.comments_per_post,
    )

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    print(f"Wrote {args.out} with {len(df)} rows.")

if __name__ == "__main__":
    main()