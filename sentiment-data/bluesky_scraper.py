from atproto import Client
import pandas as pd
import datetime as dt
import re

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