import argparse
import datetime as dt
import json
import re
from pathlib import Path
from typing import Dict, List
import praw
import pandas as pd

# keywords to search for

# SUBREDDITS = [
#     "baseball", "mlb", "fantasybaseball",
#     "nyyankees","Mets","redsox","orioles","raysbaseball",
#     "phillies","Braves","Nats","letsgofish",
#     "Dodgers","SFGiants","Padres","azdiamondbacks","ColoradoRockies",
#     "Astros","TexasRangers","Mariners","OaklandAthletics","angelsbaseball",
#     "minnesotatwins","kansascityroyals","clevelandguardians","ChWhiteSox","motorcitykitties",
#     "chicubs","cardinals","Brewers","bucs","reds",
# ]

SUBREDDITS = ["baseball"]  # TEMP: just one

TEAMS = {
  "NYY": ["Yankees","NYY","NY Yankees","Bronx Bombers"],
  "NYM": ["Mets","NYM","NY Mets","Amazins"],
  "BOS": ["Red Sox","BOS","BoSox","Sox (Boston)"],
  "BAL": ["Orioles","BAL","O's","Birdland"],
  "TBR": ["Rays","Tampa Bay Rays","TBR"],
  "TOR": ["Blue Jays","Jays","TOR"],
  "PHI": ["Phillies","PHI","Phils"],
  "ATL": ["Braves","ATL","Atlanta Braves"],
  "MIA": ["Marlins","MIA","Miami Marlins","Fish"],
  "WSN": ["Nationals","Nats","WSH","WSN"],
  "CHC": ["Cubs","CHC","Chicago Cubs","Cubbies"],
  "STL": ["Cardinals","STL","Cards","Redbirds"],
  "MIL": ["Brewers","MIL","Crew","Brew Crew"],
  "PIT": ["Pirates","PIT","Bucs","Buccos"],
  "CIN": ["Reds","CIN","Cincinnati Reds","Redlegs"],
  "LAD": ["Dodgers","LAD","LA Dodgers","Blue Crew"],
  "SFG": ["Giants","SFG","SF Giants"],
  "SDP": ["Padres","SDP","Friars"],
  "ARI": ["Diamondbacks","D-backs","ARI","Snakes"],
  "COL": ["Rockies","COL","Colorado Rockies","Rox"],
  "HOU": ["Astros","HOU","Houston Astros","Stros"],
  "TEX": ["Rangers","TEX","Texas Rangers"],
  "SEA": ["Mariners","SEA","Ms","M's"],
  "OAK": ["Athletics","A's","OAK","Oakland A's"],
  "LAA": ["Angels","LAA","LA Angels","Halos"],
  "MIN": ["Twins","MIN","Minnesota Twins"],
  "KCR": ["Royals","KCR","KC Royals"],
  "CLE": ["Guardians","CLE","Cleveland Guardians"],
  "CHW": ["White Sox","CHW","ChiSox","Sox (Chicago)"],
  "DET": ["Tigers","DET","Detroit Tigers"],
}

# helper functions

def clean_text(text):
    if not text:
        return ""  # handle None or empty strings
    text = re.sub(r"\s+", " ", text).strip()  # collapse whitespace
    return text

def utc_iso(timestamp_utc):
    return dt.datetime.utcfromtimestamp(timestamp_utc).replace(tzinfo=dt.timezone.utc).isoformat()

def all_keywords():
    seen, kws = set(), []
    for arr in TEAMS.values():
        for k in arr:
            kl = k.lower()
            if kl not in seen:
                seen.add(kl)
                kws.append(k)
    return kws

def fetch_reddit(client_id: str, client_secret: str, user_agent: str,
                 limit: int, time_filter: str, include_comments: bool):

    reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)

    # keywords = all_keywords()
    keywords = ["Yankees"]  # TEMP: just one

    rows = []
    for sub in SUBREDDITS:
        sr = reddit.subreddit(sub)
        for kw in keywords:
            query = f'title:"{kw}" OR selftext:"{kw}"'
            for submission in sr.search(query=query, sort="new", time_filter=time_filter, limit=limit):
                body = clean_text(f"{submission.title} {submission.selftext or ''}")
                if not body:
                    continue
                rows.append({
                    "source": "reddit_post",
                    "subreddit": sub,
                    "author": str(submission.author) if submission.author else None,
                    "text": body,
                    "permalink": f"https://reddit.com{submission.permalink}",
                    "created_utc": utc_iso(submission.created_utc),
                    # store which keyword matched to filter later
                    "matched_keyword": kw,
                })
                if include_comments:
                    submission.comments.replace_more(limit=0)
                    for c in submission.comments.list():
                        c_body = clean_text(getattr(c, "body", "") or "")
                        if not c_body:
                            continue
                        rows.append({
                            "source": "reddit_comment",
                            "subreddit": sub,
                            "author": str(c.author) if c.author else None,
                            "text": c_body,
                            "permalink": f"https://reddit.com{c.permalink}",
                            "created_utc": utc_iso(c.created_utc),
                            "matched_keyword": kw,
                        })

    df = pd.DataFrame(rows).drop_duplicates(subset=["text","permalink"])
    # Optionally record length for quick QA
    if not df.empty:
        df["char_len"] = df["text"].str.len()
    return df

def main():
    ap = argparse.ArgumentParser(description="Collect Reddit posts/comments mentioning any MLB team keyword.")
    ap.add_argument("--client-id", required=True)
    ap.add_argument("--client-secret", required=True)
    ap.add_argument("--user-agent", required=True, help='e.g. "mlb-sentiment by u/YOURNAME"')
    ap.add_argument("--limit", type=int, default=50, help="Max posts per subreddit+keyword")
    ap.add_argument("--time-filter", default="week",
                    choices=["all","day","hour","month","week","year"])
    ap.add_argument("--include-comments", action="store_true")
    ap.add_argument("--out", default="reddit-teams.csv", help="Output CSV path")
    args = ap.parse_args()

    df = fetch_reddit(
        client_id=args.client_id,
        client_secret=args.client_secret,
        user_agent=args.user_agent,
        limit=args.limit,
        time_filter=args.time_filter,
        include_comments=args.include_comments,
    )

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    print(f"Wrote {args.out} with {len(df)} rows.")

if __name__ == "__main__":
    main()