"""
fetch_x_metrics.py
==================
Scans all news project folders for x_post.json files, fetches engagement
metrics (views, likes, reposts, replies, bookmarks) for each post, writes:

  Per-folder:   news/.../x_metrics.json
  Summary:      reports/x_metrics_YYYY_MM.md   (+ uploaded to Drive)
  Raw cache:    reports/x_metrics_YYYY_MM.jsonl (one line per post)

Metric sources (tried in order):
  1. X API v2  /tweets/:id?tweet.fields=...   (needs X_BEARER_TOKEN secret)
  2. X guest token API                         (no auth needed, rate-limited)
  3. Syndication API  cdn.syndication.twimg.com (public, often works)

Usage:
    python fetch_x_metrics.py                  # current month
    python fetch_x_metrics.py --month 2026-03  # specific month
    python fetch_x_metrics.py --refetch        # re-fetch even if cached
    python fetch_x_metrics.py --no-drive       # skip Drive upload
"""

import os
import sys
import re
import json
import time
import urllib.request
import urllib.parse
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import defaultdict

# ── Paths ─────────────────────────────────────────────────────────────────────
_SCRIPTS_DIR = Path(__file__).resolve().parent
_REPO_ROOT   = _SCRIPTS_DIR.parent
NEWS_DIR     = _REPO_ROOT / "news"
REPORTS_DIR  = _REPO_ROOT / "reports"
DRIVE_ROOT_ID = "1tnTb4BjVjOARRKaQjmrse4kddddj9ogj"

# ── Credentials ───────────────────────────────────────────────────────────────
X_BEARER_TOKEN = os.environ.get("X_BEARER_TOKEN", "")   # secret: X_BEARER_TOKEN
X_SESSION_JSON = os.environ.get("X_SESSION_JSON", "")   # secret: X_SESSION_JSON (cookies)
X_CSRF_TOKEN   = os.environ.get("X_CSRF_TOKEN",   "")   # optional, extracted from session


def get_pacific_time():
    return datetime.now(timezone(timedelta(hours=-7)))


# ── Tweet ID extraction ───────────────────────────────────────────────────────
def extract_tweet_id(url: str) -> str | None:
    """Extract numeric tweet ID from any x.com / twitter.com URL."""
    m = re.search(r"/status/(\d+)", url)
    return m.group(1) if m else None


def extract_account_handle(url: str) -> str:
    """Extract @handle from post URL.  https://x.com/harrison16899/status/... → @harrison16899"""
    m = re.match(r"https?://(?:x|twitter)\.com/([^/]+)/status/", url)
    return f"@{m.group(1)}" if m else "unknown"


# ── Metric fetchers ───────────────────────────────────────────────────────────
def _fetch_via_api_v2(tweet_id: str) -> dict | None:
    """
    X API v2 — requires Bearer token (free tier gives read access).
    Returns normalised metrics dict or None on failure.
    """
    if not X_BEARER_TOKEN:
        return None
    url = (
        f"https://api.twitter.com/2/tweets/{tweet_id}"
        f"?tweet.fields=public_metrics,non_public_metrics,organic_metrics,created_at,text"
    )
    headers = {"Authorization": f"Bearer {X_BEARER_TOKEN}"}
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        tweet = data.get("data", {})
        pm = tweet.get("public_metrics", {})
        om = tweet.get("organic_metrics", {})     # only with Elevated access
        npm = tweet.get("non_public_metrics", {}) # only with Elevated access
        return {
            "source":      "api_v2",
            "views":       pm.get("impression_count")
                           or om.get("impression_count")
                           or npm.get("impression_count"),
            "likes":       pm.get("like_count"),
            "reposts":     pm.get("retweet_count"),
            "replies":     pm.get("reply_count"),
            "quotes":      pm.get("quote_count"),
            "bookmarks":   pm.get("bookmark_count"),
            "created_at":  tweet.get("created_at", ""),
        }
    except Exception as e:
        print(f"      [API v2] failed for {tweet_id}: {e}")
        return None


def _fetch_guest_token() -> str | None:
    """Obtain a guest token from X (no auth required)."""
    url = "https://api.twitter.com/1.1/guest/activate.json"
    headers = {
        # Public bearer token used by the X web app (stable for years)
        "Authorization": (
            "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs"
            "%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
        ),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    try:
        req = urllib.request.Request(url, data=b"", headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return data.get("guest_token")
    except Exception:
        return None


_guest_token_cache: dict = {"token": None, "fetched_at": 0}

def _get_guest_token() -> str | None:
    now = time.time()
    if _guest_token_cache["token"] and now - _guest_token_cache["fetched_at"] < 1800:
        return _guest_token_cache["token"]
    token = _fetch_guest_token()
    if token:
        _guest_token_cache["token"] = token
        _guest_token_cache["fetched_at"] = now
    return token


def _fetch_via_guest_api(tweet_id: str) -> dict | None:
    """
    X TweetDetail GraphQL via guest token — gives view count + public metrics.
    No auth required but rate-limited (~180 req/15 min per guest token).
    """
    guest_token = _get_guest_token()
    if not guest_token:
        return None

    # GraphQL variables
    variables = json.dumps({
        "focalTweetId": tweet_id,
        "count": 1,
        "includePromotedContent": False,
        "withCommunity": False,
        "withQuickPromoteEligibilityTweetFields": False,
        "withBirdwatchNotes": False,
        "withVoice": False,
        "withV2Timeline": True,
    })
    features = json.dumps({
        "rweb_lists_timeline_redesign_enabled": True,
        "responsive_web_graphql_exclude_directive_enabled": True,
        "verified_phone_label_enabled": False,
        "creator_subscriptions_tweet_preview_api_enabled": True,
        "responsive_web_graphql_timeline_navigation_enabled": True,
        "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
        "tweetypie_unmention_optimization_enabled": True,
        "responsive_web_edit_tweet_api_enabled": True,
        "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
        "view_counts_everywhere_api_enabled": True,
        "longform_notetweets_consumption_enabled": True,
        "tweet_awards_web_tipping_enabled": False,
        "freedom_of_speech_not_reach_fetch_enabled": True,
        "standardized_nudges_misinfo": True,
        "tweet_with_visibility_results_feature_enabled": True,
        "longform_notetweets_rich_text_read_enabled": True,
        "longform_notetweets_inline_media_enabled": False,
        "responsive_web_enhance_cards_enabled": False,
    })
    params = urllib.parse.urlencode({"variables": variables, "features": features})
    url = f"https://twitter.com/i/api/graphql/0hWvDhmW8YQ-S_ib3azIrw/TweetDetail?{params}"

    headers = {
        "Authorization": (
            "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs"
            "%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
        ),
        "x-guest-token":  guest_token,
        "Content-Type":   "application/json",
        "User-Agent":     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "x-twitter-client-language": "en",
        "x-twitter-active-user": "yes",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        # Navigate the GraphQL response tree to the focal tweet
        instructions = (
            data.get("data", {})
                .get("threaded_conversation_with_injections_v2", {})
                .get("instructions", [])
        )
        for instr in instructions:
            for entry in instr.get("entries", []):
                content = entry.get("content", {})
                item_content = (
                    content.get("itemContent") or
                    content.get("items", [{}])[0].get("item", {}).get("itemContent", {})
                )
                tweet_results = item_content.get("tweet_results", {}).get("result", {})
                legacy = tweet_results.get("legacy", {})
                if legacy.get("id_str") == tweet_id or tweet_results.get("rest_id") == tweet_id:
                    pm = legacy.get("public_metrics") or {}
                    views = (tweet_results.get("views", {}).get("count")
                             or tweet_results.get("view_count"))
                    return {
                        "source":    "guest_api",
                        "views":     int(views) if views else None,
                        "likes":     legacy.get("favorite_count"),
                        "reposts":   legacy.get("retweet_count"),
                        "replies":   legacy.get("reply_count"),
                        "quotes":    legacy.get("quote_count"),
                        "bookmarks": legacy.get("bookmark_count"),
                        "created_at": legacy.get("created_at", ""),
                    }
        return None
    except Exception as e:
        print(f"      [Guest API] failed for {tweet_id}: {e}")
        return None


def _fetch_via_syndication(tweet_id: str) -> dict | None:
    """
    Twitter Syndication API — public endpoint, no auth.
    Returns limited metrics but reliable for likes/reposts/replies.
    Does NOT return view count.
    """
    url = f"https://cdn.syndication.twimg.com/tweet-result?id={tweet_id}&lang=en"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer":    "https://platform.twitter.com/",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if not data or data.get("errors"):
            return None
        return {
            "source":     "syndication",
            "views":      None,   # not available via syndication
            "likes":      data.get("favorite_count"),
            "reposts":    data.get("retweet_count"),
            "replies":    data.get("reply_count"),
            "quotes":     data.get("quote_count"),
            "bookmarks":  None,
            "created_at": data.get("created_at", ""),
        }
    except Exception as e:
        print(f"      [Syndication] failed for {tweet_id}: {e}")
        return None


def fetch_metrics(tweet_id: str, post_url: str) -> dict:
    """
    Try all sources in order, return the best result.
    Always returns a dict (with None values if everything fails).
    """
    for name, fn in [
        ("API v2",       lambda: _fetch_via_api_v2(tweet_id)),
        ("Guest API",    lambda: _fetch_via_guest_api(tweet_id)),
        ("Syndication",  lambda: _fetch_via_syndication(tweet_id)),
    ]:
        result = fn()
        if result:
            print(f"      ✅ [{name}] views={result.get('views')} likes={result.get('likes')} reposts={result.get('reposts')}")
            return result
        time.sleep(0.3)

    print(f"      ⚠️  All sources failed for {tweet_id}")
    return {
        "source": "failed", "views": None, "likes": None,
        "reposts": None, "replies": None, "quotes": None,
        "bookmarks": None, "created_at": "",
    }


# ── Folder scanner ────────────────────────────────────────────────────────────
def find_x_post_files(year_str: str, month_str: str) -> list[Path]:
    """
    Find all x_post.json files under news/YYYY/MM/
    """
    base = NEWS_DIR / year_str / month_str
    if not base.exists():
        # Also try flat news/ structure
        base = NEWS_DIR
    results = sorted(base.rglob("x_post.json"))
    print(f"   📂 Found {len(results)} x_post.json files under {base}")
    return results


def load_x_post(path: Path) -> dict | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and data.get("post_url"):
            return data
    except Exception:
        pass
    return None


def load_cached_metrics(path: Path) -> dict | None:
    """Load x_metrics.json from the same folder as x_post.json."""
    metrics_path = path.parent / "x_metrics.json"
    if metrics_path.exists():
        try:
            return json.loads(metrics_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return None


def save_metrics(path: Path, metrics: dict):
    """Write x_metrics.json alongside x_post.json."""
    out = path.parent / "x_metrics.json"
    out.write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8")


# ── Drive ─────────────────────────────────────────────────────────────────────
def get_drive_service():
    try:
        import pickle
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
    except ImportError:
        return None

    for p in ["token.json", os.path.expanduser("~/.api_tools/token.json")]:
        if os.path.exists(p):
            try:
                raw = open(p, "rb").read()
                if raw.startswith(b"\x80"):
                    pkl = pickle.loads(raw)
                    d = {
                        "token": getattr(pkl, "token", None),
                        "refresh_token": getattr(pkl, "_refresh_token", None),
                        "token_uri": getattr(pkl, "_token_uri", "https://oauth2.googleapis.com/token"),
                        "client_id": getattr(pkl, "_client_id", None),
                        "client_secret": getattr(pkl, "_client_secret", None),
                        "scopes": list(getattr(pkl, "_scopes", []) or ["https://www.googleapis.com/auth/drive.file"]),
                    }
                    json.dump(d, open(p, "w"))
                    creds = Credentials.from_authorized_user_info(d, d["scopes"])
                else:
                    d = json.loads(raw)
                    scopes = d.get("scopes", ["https://www.googleapis.com/auth/drive.file"])
                    if isinstance(scopes, str): scopes = scopes.split()
                    creds = Credentials.from_authorized_user_info(d, scopes)
                if creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                    open(p, "w").write(creds.to_json())
                if creds.valid:
                    return build("drive", "v3", credentials=creds)
            except Exception:
                pass
    return None


def drive_upload_text(service, folder_id: str, filename: str, content: str):
    from googleapiclient.http import MediaIoBaseUpload
    import io
    media = MediaIoBaseUpload(io.BytesIO(content.encode("utf-8")), mimetype="text/markdown")
    q = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    existing = service.files().list(q=q, fields="files(id)").execute().get("files", [])
    if existing:
        service.files().update(fileId=existing[0]["id"], media_body=media).execute()
    else:
        service.files().create(
            body={"name": filename, "parents": [folder_id]},
            media_body=media, fields="id",
        ).execute()


# ── Report builder ────────────────────────────────────────────────────────────
def build_report(rows: list[dict], year_str: str, month_str: str, generated_at: str) -> str:
    month_name = datetime.strptime(f"{year_str}-{month_str}-01", "%Y-%m-%d").strftime("%B %Y")

    # Sort by date desc, then by views desc
    rows_sorted = sorted(
        rows,
        key=lambda r: (r.get("date", ""), r.get("metrics", {}).get("views") or 0),
        reverse=True,
    )

    # Per-account totals
    by_account: dict[str, dict] = defaultdict(lambda: {
        "posts": 0, "views": 0, "likes": 0, "reposts": 0, "replies": 0, "quotes": 0, "bookmarks": 0
    })
    for r in rows:
        acc = r.get("account", "unknown")
        m   = r.get("metrics", {})
        by_account[acc]["posts"]     += 1
        by_account[acc]["views"]     += m.get("views")     or 0
        by_account[acc]["likes"]     += m.get("likes")     or 0
        by_account[acc]["reposts"]   += m.get("reposts")   or 0
        by_account[acc]["replies"]   += m.get("replies")   or 0
        by_account[acc]["quotes"]    += m.get("quotes")    or 0
        by_account[acc]["bookmarks"] += m.get("bookmarks") or 0

    # Per-day totals
    by_day: dict[str, dict] = defaultdict(lambda: {
        "posts": 0, "views": 0, "likes": 0, "reposts": 0
    })
    for r in rows:
        date = r.get("date", "undated")
        m    = r.get("metrics", {})
        by_day[date]["posts"]   += 1
        by_day[date]["views"]   += m.get("views")   or 0
        by_day[date]["likes"]   += m.get("likes")   or 0
        by_day[date]["reposts"] += m.get("reposts") or 0

    grand = {
        "posts":     len(rows),
        "views":     sum(r.get("metrics", {}).get("views")     or 0 for r in rows),
        "likes":     sum(r.get("metrics", {}).get("likes")     or 0 for r in rows),
        "reposts":   sum(r.get("metrics", {}).get("reposts")   or 0 for r in rows),
        "replies":   sum(r.get("metrics", {}).get("replies")   or 0 for r in rows),
        "quotes":    sum(r.get("metrics", {}).get("quotes")    or 0 for r in rows),
        "bookmarks": sum(r.get("metrics", {}).get("bookmarks") or 0 for r in rows),
    }

    def fmt(v):
        if v is None: return "—"
        if v >= 1_000_000: return f"{v/1_000_000:.1f}M"
        if v >= 1_000:     return f"{v/1_000:.1f}K"
        return str(v)

    lines = [
        f"# X Post Metrics Report — {month_name}",
        "",
        f"*Generated: {generated_at}  |  {len(rows)} posts tracked*",
        "",
        "---",
        "",
        "## Month Totals",
        "",
        "| Metric | Value |",
        "|--------|------:|",
        f"| Posts | {grand['posts']} |",
        f"| Total views | {fmt(grand['views'])} |",
        f"| Total likes | {fmt(grand['likes'])} |",
        f"| Total reposts | {fmt(grand['reposts'])} |",
        f"| Total replies | {fmt(grand['replies'])} |",
        f"| Total quotes | {fmt(grand['quotes'])} |",
        f"| Total bookmarks | {fmt(grand['bookmarks'])} |",
        "",
        "---",
        "",
        "## By Account",
        "",
        "| Account | Posts | Views | Likes | Reposts | Replies | Quotes | Bookmarks |",
        "|---------|------:|------:|------:|--------:|--------:|-------:|----------:|",
    ]
    for acc in sorted(by_account):
        a = by_account[acc]
        lines.append(
            f"| `{acc}` | {a['posts']} | {fmt(a['views'])} | {fmt(a['likes'])} "
            f"| {fmt(a['reposts'])} | {fmt(a['replies'])} | {fmt(a['quotes'])} | {fmt(a['bookmarks'])} |"
        )
    lines += [
        "",
        "---",
        "",
        "## By Day",
        "",
        "| Date | Posts | Views | Likes | Reposts |",
        "|------|------:|------:|------:|--------:|",
    ]
    for date in sorted(by_day.keys(), reverse=True):
        d = by_day[date]
        lines.append(
            f"| {date} | {d['posts']} | {fmt(d['views'])} "
            f"| {fmt(d['likes'])} | {fmt(d['reposts'])} |"
        )

    lines += [
        "",
        "---",
        "",
        "## All Posts (newest first)",
        "",
        "| Date | Account | Views | Likes | Reposts | Replies | Bookmarks | Title | URL |",
        "|------|---------|------:|------:|--------:|--------:|----------:|-------|-----|",
    ]
    for r in rows_sorted:
        m     = r.get("metrics", {})
        title = (r.get("title") or r.get("post_text") or "")[:60].replace("|", "∣")
        url   = r.get("post_url", "")
        short_url = re.sub(r"https?://(?:x|twitter)\.com", "x.com", url)
        lines.append(
            f"| {r.get('date','—')} | `{r.get('account','?')}` "
            f"| {fmt(m.get('views'))} | {fmt(m.get('likes'))} "
            f"| {fmt(m.get('reposts'))} | {fmt(m.get('replies'))} "
            f"| {fmt(m.get('bookmarks'))} "
            f"| {title} | [{short_url}]({url}) |"
        )

    # Top 10 by views
    top_views = sorted(
        [r for r in rows if r.get("metrics", {}).get("views")],
        key=lambda r: r["metrics"]["views"],
        reverse=True,
    )[:10]
    if top_views:
        lines += [
            "",
            "---",
            "",
            "## Top 10 Posts by Views",
            "",
            "| # | Views | Likes | Reposts | Account | Title |",
            "|---|------:|------:|--------:|---------|-------|",
        ]
        for i, r in enumerate(top_views, 1):
            m     = r["metrics"]
            title = (r.get("title") or r.get("post_text") or "")[:55].replace("|", "∣")
            lines.append(
                f"| {i} | {fmt(m.get('views'))} | {fmt(m.get('likes'))} "
                f"| {fmt(m.get('reposts'))} | `{r.get('account','?')}` | {title} |"
            )

    lines.append("")
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Fetch X post engagement metrics")
    parser.add_argument("--month",    default=None, help="YYYY-MM (default: current month)")
    parser.add_argument("--refetch",  action="store_true", help="Re-fetch even if x_metrics.json exists")
    parser.add_argument("--no-drive", action="store_true", help="Skip Drive upload")
    parser.add_argument("--delay",    type=float, default=1.0,
                        help="Seconds between API calls (default: 1.0)")
    args = parser.parse_args()

    pt_now = get_pacific_time()
    if args.month:
        parts = args.month.split("-")
        year_str, month_str = parts[0], parts[1].zfill(2)
    else:
        year_str  = pt_now.strftime("%Y")
        month_str = pt_now.strftime("%m")

    month_label = f"{year_str}-{month_str}"
    generated_at = pt_now.strftime("%Y-%m-%d %H:%M PT")
    print(f"\n📊 Fetching X metrics for {month_label}...")
    print(f"   Bearer token: {'✅ set' if X_BEARER_TOKEN else '⚠️  not set — using guest API + syndication'}")

    # ── Scan folders ──────────────────────────────────────────────────────────
    x_post_files = find_x_post_files(year_str, month_str)
    if not x_post_files:
        print(f"   ℹ️  No x_post.json files found under news/{year_str}/{month_str}/")
        return

    rows: list[dict] = []
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    jsonl_path = REPORTS_DIR / f"x_metrics_{year_str}_{month_str}.jsonl"
    jsonl_fh   = open(jsonl_path, "a", encoding="utf-8")

    for i, post_file in enumerate(x_post_files, 1):
        post_data = load_x_post(post_file)
        if not post_data:
            continue

        post_url = post_data.get("post_url", "")
        post_text = post_data.get("post_text", "")
        tweet_id  = extract_tweet_id(post_url)
        account   = extract_account_handle(post_url)
        # Try to get the news title from the parent folder's lyrics_with_prompts.md
        title = post_data.get("title", "")
        if not title:
            lyrics_file = post_file.parent / "lyrics_with_prompts.md"
            if lyrics_file.exists():
                first_line = lyrics_file.read_text(encoding="utf-8", errors="ignore").split("\n")[0]
                title = re.sub(r"^Song Title:\s*", "", first_line).strip()
        if not title:
            title = post_text[:80]

        # Infer date from folder name or post file mtime
        date = ""
        # Folder path: news/2026/04/2026-04-15/1030-SomeTitle/x_post.json
        for part in post_file.parts:
            if re.match(r"\d{4}-\d{2}-\d{2}", part):
                date = part
                break
        if not date:
            mtime = post_file.stat().st_mtime
            date  = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d")

        if not tweet_id:
            print(f"   ⚠️  No tweet ID in URL: {post_url}")
            continue

        print(f"   [{i}/{len(x_post_files)}] {account} — {tweet_id} — {title[:40]}...")

        # Check cache
        cached = None if args.refetch else load_cached_metrics(post_file)
        if cached and cached.get("metrics", {}).get("source") not in (None, "failed"):
            print(f"      ✅ [cached] views={cached['metrics'].get('views')} likes={cached['metrics'].get('likes')}")
            metrics = cached["metrics"]
        else:
            metrics = fetch_metrics(tweet_id, post_url)
            time.sleep(args.delay)

        row = {
            "tweet_id":   tweet_id,
            "post_url":   post_url,
            "account":    account,
            "date":       date,
            "title":      title,
            "post_text":  post_text[:120],
            "metrics":    metrics,
            "fetched_at": generated_at,
        }

        # Save per-folder x_metrics.json
        save_metrics(post_file, row)

        # Append to JSONL cache
        jsonl_fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        jsonl_fh.flush()

        rows.append(row)

    jsonl_fh.close()
    print(f"\n   📝 {len(rows)} posts processed  |  JSONL cache → {jsonl_path.name}")

    if not rows:
        print("   ℹ️  Nothing to report.")
        return

    # ── Build + save report ───────────────────────────────────────────────────
    report_md       = build_report(rows, year_str, month_str, generated_at)
    report_filename = f"x_metrics_{year_str}_{month_str}.md"
    local_path      = REPORTS_DIR / report_filename
    local_path.write_text(report_md, encoding="utf-8")
    print(f"   💾 Report written → {local_path}")

    # ── Upload to Drive ───────────────────────────────────────────────────────
    if not args.no_drive:
        service = get_drive_service()
        if service:
            try:
                drive_upload_text(service, DRIVE_ROOT_ID, report_filename, report_md)
                print(f"   ☁️  Uploaded to Drive → {report_filename}")
            except Exception as e:
                print(f"   ⚠️  Drive upload failed: {e}")
        else:
            print("   ℹ️  Drive not available — local only")

    # ── Console summary ───────────────────────────────────────────────────────
    def fmt(v):
        if v is None: return "—"
        if v >= 1_000_000: return f"{v/1_000_000:.1f}M"
        if v >= 1_000:     return f"{v/1_000:.1f}K"
        return str(v)

    total_views    = sum(r["metrics"].get("views")   or 0 for r in rows)
    total_likes    = sum(r["metrics"].get("likes")   or 0 for r in rows)
    total_reposts  = sum(r["metrics"].get("reposts") or 0 for r in rows)

    print(f"\n{'='*58}")
    print(f"  X METRICS — {month_label}")
    print(f"{'='*58}")
    print(f"  Posts tracked : {len(rows)}")
    print(f"  Total views   : {fmt(total_views)}")
    print(f"  Total likes   : {fmt(total_likes)}")
    print(f"  Total reposts : {fmt(total_reposts)}")

    # Per-account breakdown
    by_acc: dict[str, dict] = defaultdict(lambda: {"posts":0,"views":0,"likes":0,"reposts":0})
    for r in rows:
        a = r["account"]
        by_acc[a]["posts"]   += 1
        by_acc[a]["views"]   += r["metrics"].get("views")   or 0
        by_acc[a]["likes"]   += r["metrics"].get("likes")   or 0
        by_acc[a]["reposts"] += r["metrics"].get("reposts") or 0
    print(f"  {'─'*52}")
    for acc in sorted(by_acc):
        a = by_acc[acc]
        print(f"  {acc:<22}  {a['posts']:>3} posts  views={fmt(a['views'])}  likes={fmt(a['likes'])}")
    print(f"{'='*58}\n")


if __name__ == "__main__":
    main()