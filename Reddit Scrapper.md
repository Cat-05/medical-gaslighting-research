#!/usr/bin/env python3
# reddit_scraper.py
# Scrapes posts + comments via Reddit's public JSON endpoints (no API keys required).
# Outputs: reddit_combined.csv with columns matching project spreadsheet format.

import sys, json, csv, time, re
from datetime import datetime, timezone
from urllib import request, parse, error

BASE = "https://www.reddit.com"

# ------------ Config ------------
SUBREDDITS = [
    "AskDocs", "TwoXChromosomes", "Endometriosis", "Menopause", "Perimenopause",
    "WomensHealth", "ChronicIllness", "PatientExperience", "TrueOffMyChest", "relationships",
    "medicalgaslighting"
]
MAX_POSTS_PER_SUB   = 250
COMMENTS_PER_POST   = 400
SLEEP_BETWEEN_CALLS = 0.6
USER_AGENT = "Mozilla/5.0 (research scraper for academic use; contact: calarara@ttu.edu)"

# ------------ Themes ------------
# Column names match spreadsheet exactly
THEME_KEYWORDS = {
    "Consumer_Vulnerability": [
        "vulnerable", "powerless", "disadvantaged", "marginalized", "ignored", "dismissed",
        "not believed", "discredited", "patronized", "treated like crazy", "overreacting",
        "woman problem", "just anxiety", "overweight bias", "medical gaslighting",
        "doctor doesn't listen", "doctor did not listen", "not taken seriously"
    ],
    "Service_Failure_and_Recovery": [
        "service failure", "provider failed", "bad experience", "misdiagnosed", "sent home",
        "refused treatment", "delayed diagnosis", "incorrect diagnosis", "no apology",
        "never followed up", "had to switch doctor", "changed provider", "left clinic",
        "second opinion", "switched doctor", "moved to new provider"
    ],
    "Trust_and_Relationship_Breakdown": [
        "lost trust", "no longer trust", "broke my trust", "don't trust doctors", "mistrust healthcare",
        "lied to me", "betrayed", "never listen", "trust issues", "won't go back", "avoiding care",
        "loyal patient", "warn others", "negative review", "word of mouth"
    ],
    "Marketplace_Exclusion_and_Identity": [
        "excluded from care", "denied service", "couldn't get treatment", "gatekeeping",
        "refused referral", "ignored symptoms", "insurance denied",
        "racial bias", "gender bias", "intersectionality", "disability bias", "low income",
        "minority woman", "black woman", "latina", "trans woman", "medical discrimination", "not equal"
    ],
    "Psychological_Trauma": [
        "psychological trauma", "medical trauma", "trauma response", "traumatic",
        "ptsd", "flashbacks", "nightmares", "hypervigilance", "panic attacks", "panic attack",
        "depression", "anxiety", "avoidance", "avoid care", "mistrust", "fear of doctors"
    ],
}

# ------------ Helpers ------------
def compile_patterns(theme_map):
    out = {}
    for theme, kws in theme_map.items():
        pats = [(kw, re.compile(r'(?i)\b' + re.escape(kw) + r'\b')) for kw in kws]
        out[theme] = pats
    return out

COMPILED = compile_patterns(THEME_KEYWORDS)

def match_themes(text):
    hits = {}
    if not text: return hits
    for theme, pats in COMPILED.items():
        found = []
        for kw, p in pats:
            if p.search(text): found.append(kw)
        if found: hits[theme] = sorted(set(found))
    return hits

def to_iso(utc_ts):
    try:
        return datetime.fromtimestamp(float(utc_ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return ""

def get(url, params=None, max_retries=4):
    params = params or {}
    qs = "?" + parse.urlencode(params) if params else ""
    headers = {"User-Agent": USER_AGENT}
    req = request.Request(url + qs, headers=headers)
    last_err = None
    for i in range(max_retries):
        try:
            with request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except error.HTTPError as e:
            if e.code == 429:
                time.sleep(2.0 * (i + 1))
            else:
                time.sleep(1.0 + i * 0.5)
            last_err = e
        except Exception as e:
            time.sleep(1.0 + i * 0.5)
            last_err = e
    raise last_err

# ------------ Scrapers ------------
def fetch_sub_posts(subreddit, max_posts):
    posts, after = [], None
    while len(posts) < max_posts:
        params = {"limit": 100}
        if after: params["after"] = after
        data = get(f"{BASE}/r/{subreddit}/new.json", params=params)
        children = data.get("data", {}).get("children", [])
        if not children: break
        for ch in children:
            d = ch.get("data", {})
            if d: posts.append(d)
            if len(posts) >= max_posts: break
        after = data.get("data", {}).get("after")
        if not after: break
        time.sleep(SLEEP_BETWEEN_CALLS)
    return posts

def fetch_post_comments(permalink, max_comments):
    url = f"{BASE}{permalink}.json"
    data = get(url, params={"limit": 500, "depth": 2})
    if not isinstance(data, list) or len(data) < 2:
        return []
    listing = data[1].get("data", {}).get("children", [])
    out = []
    def walk(nodes):
        for n in nodes:
            if len(out) >= max_comments: return
            kind = n.get("kind")
            d = n.get("data", {})
            if kind == "t1":
                out.append(d)
                replies = d.get("replies")
                if isinstance(replies, dict):
                    walk(replies.get("data", {}).get("children", []))
    walk(listing)
    return out[:max_comments]

def build_row(content_type, author, subreddit, score, created, text, num_comments, url, hits):
    row = {
        "content_type": content_type,
        "author": author,
        "subreddit": subreddit,
        "score": score,
        "created": created,
        "text": text,
        "num_comments": num_comments,
        "url": url,
    }
    # Add one column per theme with matched keywords (empty string if no match)
    for theme in THEME_KEYWORDS:
        row[theme] = "; ".join(hits.get(theme, []))
    return row

# ------------ Main ------------
if __name__ == "__main__":
    print("Starting Reddit scrape via public JSON...")

    all_rows = []

    for sub in SUBREDDITS:
        print(f"\nSubreddit: r/{sub}")
        raw_posts = fetch_sub_posts(sub, MAX_POSTS_PER_SUB)
        print(f"  {len(raw_posts)} posts fetched")

        for d in raw_posts:
            title    = (d.get("title") or "").strip()
            selftext = (d.get("selftext") or "").strip()
            # Combine title + body into single text field to match spreadsheet
            text = f"{title}\n\n{selftext}".strip() if selftext else title
            author      = d.get("author", "")
            score       = d.get("score", "")
            num_comments= d.get("num_comments", "")
            created     = to_iso(d.get("created_utc"))
            permalink   = d.get("permalink", "")
            url         = f"{BASE}{permalink}" if permalink else ""
            hits        = match_themes(text)

            all_rows.append(build_row(
                content_type="post",
                author=author,
                subreddit=sub,
                score=score,
                created=created,
                text=text,
                num_comments=num_comments,
                url=url,
                hits=hits
            ))

            # Fetch comments for this post
            if permalink:
                try:
                    comments = fetch_post_comments(permalink, COMMENTS_PER_POST)
                    for c in comments:
                        body    = (c.get("body") or "").strip()
                        c_hits  = match_themes(body)
                        c_url   = f"{BASE}{c.get('permalink', '')}" if c.get("permalink") else url
                        all_rows.append(build_row(
                            content_type="comment",
                            author=c.get("author", ""),
                            subreddit=sub,
                            score=c.get("score", ""),
                            created=to_iso(c.get("created_utc")),
                            text=body,
                            num_comments="",
                            url=c_url,
                            hits=c_hits
                        ))
                    time.sleep(SLEEP_BETWEEN_CALLS)
                except Exception as e:
                    time.sleep(SLEEP_BETWEEN_CALLS)
                    continue

        time.sleep(SLEEP_BETWEEN_CALLS)

    # Save to CSV
    if all_rows:
        fieldnames = [
            "content_type", "author", "subreddit", "score", "created",
            "text", "num_comments", "url",
            "Consumer_Vulnerability", "Service_Failure_and_Recovery",
            "Trust_and_Relationship_Breakdown", "Marketplace_Exclusion_and_Identity",
            "Psychological_Trauma"
        ]
        with open("reddit_combined.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(all_rows)

        posts_count    = sum(1 for r in all_rows if r["content_type"] == "post")
        comments_count = sum(1 for r in all_rows if r["content_type"] == "comment")
        print(f"\nDone! Saved {posts_count} posts and {comments_count} comments → reddit_combined.csv ({len(all_rows)} total rows)")
    else:
        print("No data collected.")
