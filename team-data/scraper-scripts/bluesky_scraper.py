import argparse
import re
import pandas as pd
from atproto import Client
from atproto import models as AtpModels
import json, os, time
from pathlib import Path

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
    
def load_state(path: str) -> dict:
    if path and os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {}

def save_state(path: str, state: dict):
    if path:
        with open(path, "w") as f:
            json.dump(state, f)

def checkpoint_write(df: pd.DataFrame, out_path: str):
    tmp = out_path + ".tmp"
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(tmp, index=False)
    os.replace(tmp, out_path)

def try_get_replies(client: Client, uri: str, did: str, kw: str) -> list[dict]:
    rows = []
    try:
        thread = client.app.bsky.feed.get_post_thread({'uri': uri, 'depth': 2})
        root = getattr(thread, "thread", None) or getattr(getattr(thread, "data", None), "thread", None)
        if not root:
            return rows
        stack, seen = [root], set()
        while stack and len(rows) < 50:
            node = stack.pop()
            post = getattr(node, "post", None)
            if not post:
                continue
            post_uri = getattr(post, "uri", None)
            if not post_uri or post_uri in seen:
                continue
            seen.add(post_uri)
            arec = getattr(post, "record", None)
            text = clean_text(getattr(arec, "text", "") or "")
            author = getattr(post, "author", None)
            handle_out = getattr(author, "handle", None) if author else None
            did_out = getattr(author, "did", None) if author else None
            created = getattr(post, "indexed_at", None) or getattr(arec, "created_at", None)
            created_iso = utc_iso_from_str(created) if created else None
            permalink = uri_to_web(post_uri, did_out) if (post_uri and did_out) else (post_uri or "")
            if text:
                rows.append({
                    "source": "bluesky_reply",
                    "subreddit": "bluesky",
                    "author": handle_out,
                    "text": text,
                    "permalink": permalink,
                    "created_utc": created_iso,
                    "matched_keyword": kw,
                })
            # children may be under replies/children depending on SDK version
            children = getattr(node, "replies", []) or getattr(node, "children", [])
            for ch in children:
                stack.append(ch)
    except Exception:
        pass
    return rows

def fetch_bluesky(
    handle: str,
    password: str,
    keywords: list[str],
    limit_per_kw: int = 50,
    sleep_ms: int = 200,
    checkpoint_every: int = 1000,
    out_path: str | None = None,
    state_path: str | None = None,
    include_replies: bool = False,
) -> pd.DataFrame:
    client = Client()
    client.login(handle, password)

    rows: list[dict] = []
    state = load_state(state_path) if state_path else {}

    for kw in keywords:
        fetched = int(state.get(kw, {}).get("fetched", 0))
        cursor = state.get(kw, {}).get("cursor", None)

        while fetched < limit_per_kw:
            params = AtpModels.AppBskyFeedSearchPosts.Params(
                q=kw, sort="latest",
                limit=min(100, limit_per_kw - fetched),
                cursor=cursor,
            )
            # basic retry
            for attempt in range(3):
                try:
                    resp = client.app.bsky.feed.search_posts(params)
                    break
                except Exception:
                    if attempt == 2:
                        raise
                    time.sleep(0.5 * (attempt + 1))

            data = getattr(resp, "data", None)
            posts = getattr(resp, "posts", None) or (getattr(data, "posts", []) if data else [])
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

                if include_replies and uri and did:
                    rows.extend(try_get_replies(client, uri, did, kw))

                if out_path and (len(rows) % checkpoint_every == 0):
                    df_ck = pd.DataFrame(rows).drop_duplicates(subset=["text", "permalink"])
                    checkpoint_write(df_ck, out_path)
                    state[kw] = {"cursor": cursor, "fetched": fetched}
                    save_state(state_path, state)

                if fetched >= limit_per_kw:
                    break

            # advance cursor
            cursor = getattr(resp, "cursor", None)
            if cursor is None:
                cursor = getattr(getattr(resp, "data", None), "cursor", None)

            # persist progress for this keyword
            state[kw] = {"cursor": cursor, "fetched": fetched}
            save_state(state_path, state)

            time.sleep(max(0, sleep_ms) / 1000.0)

    df = pd.DataFrame(rows).drop_duplicates(subset=["text", "permalink"])
    if not df.empty:
        df["char_len"] = df["text"].str.len()
    return df

def main():
    ap = argparse.ArgumentParser(description="Collect Bluesky posts for MLB sentiment.")
    ap.add_argument("--handle", required=True, help="e.g., acrowe3069.bsky.social")
    ap.add_argument("--app-password", required=True, help="Bluesky app password")
    ap.add_argument("--limit-per-kw", type=int, default=25)
    ap.add_argument("--out", default="bluesky-mlb.csv")
    ap.add_argument("--keywords", nargs="+", help="Optional override keyword list")

    # New: scale + robustness
    ap.add_argument("--checkpoint-every", type=int, default=1000, help="write interim checkpoint every N rows")
    ap.add_argument("--state", default="bluesky_state.json", help="resume state file (cursor/fetched per keyword)")
    ap.add_argument("--sleep-ms", type=int, default=200, help="sleep between pages to be polite")
    ap.add_argument("--include-replies", action="store_true", help="also fetch replies to matched posts")

    # Output options
    ap.add_argument("--format", choices=["csv","parquet"], default="csv")
    ap.add_argument("--append", action="store_true", help="append to existing output (dedup at end)")
    args = ap.parse_args()

    kws = args.keywords if args.keywords else all_keywords()
    print(f"[info] keywords: {len(kws)} total")

    # fetch with scaling/resume knobs
    df = fetch_bluesky(
        args.handle,
        args.app_password,
        kws,
        limit_per_kw=args.limit_per_kw,
        sleep_ms=args.sleep_ms,
        checkpoint_every=args.checkpoint_every,
        out_path=args.out,          # enables periodic checkpoint writes
        state_path=args.state,      # enables resume
        include_replies=args.include_replies,
    )

    # Append or write fresh
    out = Path(args.out)
    if args.append and out.exists():
        if args.format == "csv":
            prev = pd.read_csv(out)
        else:
            prev = pd.read_parquet(out)
        df = pd.concat([prev, df], ignore_index=True).drop_duplicates(subset=["text","permalink"])

    if args.format == "csv":
        df.to_csv(out, index=False)
    else:
        df.to_parquet(out, index=False)

    print(f"[done] wrote {out} with {len(df)} rows.")

if __name__ == "__main__":
    main()