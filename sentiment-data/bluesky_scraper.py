from atproto import Client
import pandas as pd
import datetime as dt
import re

# api info
client = Client()
client.login("acrowe3069.bsky.social", "5bfw-b2i5-yv6j-i2m6")  # bluesky handle, app password

# search config

SUBREDDITS = [
    "baseball","mlb","fantasybaseball","nyyankees","NewYorkMets","redsox","Orioles","TampaBayRays",
    "phillies","Braves","Nationals","letsgofish","Dodgers","SFGiants","Padres","azdiamondbacks",
    "ColoradoRockies","Astros","TexasRangers","Mariners","OaklandAthletics","angelsbaseball",
    "minnesotatwins","kansascityroyals","clevelandguardians","whitesox","motorcitykitties",
    "chicubs","cardinals","Brewers","buccos","reds",
]
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
def all_keywords():
    seen, kws = set(), []
    # include subreddit names as keywords (your request)
    for s in SUBREDDITS:
        sl = s.lower()
        if sl not in seen:
            seen.add(sl); kws.append(s)
    for arr in TEAMS.values():
        for k in arr:
            kl = k.lower()
            if kl not in seen:
                seen.add(kl); kws.append(k)
    return kws

# helper functions

def clean_text(text: str) -> str:
    if not text:
        return ""
    # collapse whitespace
    return re.sub(r"\s+", " ", text).strip()

def utc_iso_from_str(timestr: str) -> str:
    # timestr is something like "2025-10-09T12:34:56Z"
    # convert to ISO with timezone
    return pd.to_datetime(timestr, utc=True).isoformat()

def fetch_bluesky(handle: str, password: str,
                   keywords: list[str],
                   limit_per_kw: int = 100) -> pd.DataFrame:
    """
    Fetch Bluesky posts matching any of the keywords.
    Returns a DataFrame with columns similar to your Reddit fetch: 
    source, subreddit, author, text, permalink, created_utc, matched_keyword.
    """
    client = Client()
    client.login(handle, password)

    rows = []
    for kw in keywords:
        # Use the search posts API (bsky.feed.searchPosts or equivalent)
        resp = client.app.bsky.feed.search_posts(q=kw, sort="latest", limit=limit_per_kw)
        # `resp` will contain posts with metadata
        for post in resp.posts:
            rec = post.record
            text = clean_text(rec.text or "")
            if not text:
                continue
            # The post.uri is something like "at://did:plc:.../app.bsky.feed.post/…” 
            uri = post.uri
            # To build a permalink human-clickable, you can map `did` and `rkey`
            # but many clients accept the at:// URI directly or as a web URL.
            # For simplicity, we use `post.uri` as the permalink column.
            rows.append({
                "source": "bluesky_post",
                "subreddit": "bluesky",  # for compatibility with your pipeline
                "author": post.author.handle,
                "text": text,
                "permalink": uri,
                "created_utc": utc_iso_from_str(post.indexed_at),
                "matched_keyword": kw,
            })
    df = pd.DataFrame(rows).drop_duplicates(subset=["text", "permalink"])
    if not df.empty:
        df["char_len"] = df["text"].str.len()
    return df