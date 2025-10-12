import argparse
import re
import pandas as pd
from atproto import Client
from atproto import models as AtpModels

# config (same as Reddit scraper)
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
    return re.sub(r"\s+", " ", text).strip()

def utc_iso_from_str(ts: str) -> str:
    return pd.to_datetime(ts, utc=True).isoformat()

def uri_to_web(uri: str, did: str) -> str:
    # at://did:plc:.../app.bsky.feed.post/<rkey> -> https://bsky.app/profile/<did>/post/<rkey>
    try:
        rkey = uri.split("/")[-1]
        return f"https://bsky.app/profile/{did}/post/{rkey}"
    except Exception:
        return uri

def fetch_bluesky(handle: str, password: str, keywords: list[str], limit_per_kw: int = 50) -> pd.DataFrame:
    client = Client()
    client.login(handle, password)

    rows = []
    for kw in keywords:
        fetched = 0
        cursor = None
        while fetched < limit_per_kw:
            params = AtpModels.AppBskyFeedSearchPosts.Params(
                q=kw,
                sort="latest",
                limit=min(100, limit_per_kw - fetched),
                cursor=cursor,
            )
            resp = client.app.bsky.feed.search_posts(params)

            # SDK response can be resp.posts or resp.data.posts depending on version
            posts = getattr(resp, "posts", None)
            if posts is None:
                data = getattr(resp, "data", None)
                posts = getattr(data, "posts", []) if data else []

            if not posts:
                break

            for post in posts:
                rec = getattr(post, "record", None)
                if not rec:
                    continue
                text = clean_text(getattr(rec, "text", "") or "")
                if not text:
                    continue

                author = getattr(post, "author", None)
                handle_out = getattr(author, "handle", None) if author else None
                did = getattr(author, "did", None) if author else None
                uri = getattr(post, "uri", None)
                permalink = uri_to_web(uri, did) if (uri and did) else (uri or "")

                created = getattr(post, "indexed_at", None) or getattr(rec, "created_at", None)
                created_iso = utc_iso_from_str(created) if created else None

                rows.append({
                    "source": "bluesky_post",
                    "subreddit": "bluesky",
                    "author": handle_out,
                    "text": text,
                    "permalink": permalink,
                    "created_utc": created_iso,
                    "matched_keyword": kw,
                })

                fetched += 1
                if fetched >= limit_per_kw:
                    break

            # advance cursor (SDK may put it on resp.cursor or resp.data.cursor)
            cursor = getattr(resp, "cursor", None)
            if cursor is None:
                data = getattr(resp, "data", None)
                cursor = getattr(data, "cursor", None)
            if not cursor:
                break

    df = pd.DataFrame(rows).drop_duplicates(subset=["text", "permalink"])
    if not df.empty:
        df["char_len"] = df["text"].str.len()
    return df


def main():
    ap = argparse.ArgumentParser(description="Collect Bluesky posts for MLB sentiment.")
    ap.add_argument("--handle", required=True, help="acrowe3069.bsky.social")
    ap.add_argument("--app-password", required=True, help="Bluesky app password")
    ap.add_argument("--limit-per-kw", type=int, default=25)
    ap.add_argument("--out", default="bluesky-mlb.csv")
    ap.add_argument("--keywords", nargs="+", help="Optional override keyword list")
    args = ap.parse_args()

    kws = args.keywords if args.keywords else all_keywords()
    print(f"[info] keywords: {len(kws)} total")
    df = fetch_bluesky(args.handle, args.app_password, kws, limit_per_kw=args.limit_per_kw)
    df.to_csv(args.out, index=False)
    print(f"[done] wrote {args.out} with {len(df)} rows.")

if __name__ == "__main__":
    main()