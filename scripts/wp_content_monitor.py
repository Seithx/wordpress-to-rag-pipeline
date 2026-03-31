#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WordPress Content Monitor (Polling Server)
============================================
Polls a WordPress site's RSS/REST API for new posts and comments,
stores state locally, exposes an HTTP API for querying.

Endpoints:
  GET  /posts          - all tracked posts (newest first)
  GET  /posts/new      - only unseen posts since last check
  GET  /comments       - all tracked comments (newest first)
  GET  /comments/new   - only unseen comments since last check
  GET  /status         - polling status and stats
  POST /mark-seen      - mark everything as seen
  POST /check-now      - trigger an immediate poll

Optional webhook: set WEBHOOK_URL env var to POST new items automatically.

Usage:
  py wp_content_monitor.py --site https://example.com
  WEBHOOK_URL=http://localhost:5000/hook py wp_content_monitor.py --site https://example.com
  POLL_INTERVAL=120 py wp_content_monitor.py --site https://example.com
"""

import argparse
import hashlib
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BROWSER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,'
              'application/rss+xml,application/atom+xml,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("monitor")


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def load_state(path):
    if path.exists():
        return json.loads(path.read_text(encoding='utf-8'))
    return {
        'seen_post_ids': [], 'seen_comment_ids': [],
        'posts': [], 'comments': [],
        'new_posts': [], 'new_comments': [],
        'last_poll': None, 'poll_count': 0, 'errors': [],
    }


def save_state(state, path):
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding='utf-8')


# ---------------------------------------------------------------------------
# Fetching (WP REST API first, RSS fallback)
# ---------------------------------------------------------------------------

def _item_id(item):
    raw = item.get('id') or item.get('link') or item.get('title', '')
    return hashlib.sha256(str(raw).encode()).hexdigest()[:16]


def fetch_wp_rest(url):
    try:
        r = requests.get(url, headers=BROWSER_HEADERS, timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        log.debug(f"WP REST failed: {e}")
    return None


def fetch_rss(url):
    try:
        r = requests.get(url, headers=BROWSER_HEADERS, timeout=15)
        if r.status_code != 200:
            return None
        feed = feedparser.parse(r.text)
        return [{
            'id': e.get('id', e.get('link', '')),
            'title': e.get('title', ''),
            'link': e.get('link', ''),
            'published': e.get('published', ''),
            'summary': e.get('summary', '')[:500],
            'author': e.get('author', ''),
        } for e in feed.entries]
    except Exception as e:
        log.warning(f"RSS error: {e}")
    return None


def fetch_items(site_url, kind):
    """Fetch posts or comments. Tries REST API, then RSS."""
    base = site_url.rstrip('/')

    if kind == 'posts':
        rest_url = f"{base}/wp-json/wp/v2/posts?per_page=20&_fields=id,date,title,link,excerpt"
        rss_url = f"{base}/feed/"
    else:
        rest_url = f"{base}/wp-json/wp/v2/comments?per_page=50&_fields=id,date,post,author_name,content,link"
        rss_url = f"{base}/comments/feed/"

    # Try REST API
    data = fetch_wp_rest(rest_url)
    if data:
        log.info(f"  {kind}: {len(data)} items via REST API")
        items = []
        for d in data:
            title_val = d.get('title', '')
            if isinstance(title_val, dict):
                title_val = title_val.get('rendered', '')
            content_val = d.get('content') or d.get('excerpt', '')
            if isinstance(content_val, dict):
                content_val = content_val.get('rendered', '')
            items.append({
                'id': d.get('id', ''), 'title': title_val,
                'link': d.get('link', ''), 'date': d.get('date', ''),
                'author': d.get('author_name', ''),
                'excerpt': (content_val or '')[:500],
                'post_id': d.get('post', ''), 'source': 'wp-rest',
            })
        return items

    # RSS fallback
    data = fetch_rss(rss_url)
    if data:
        log.info(f"  {kind}: {len(data)} items via RSS")
        for d in data:
            d['source'] = 'rss'
        return data

    log.warning(f"  {kind}: all fetch methods failed")
    return []


# ---------------------------------------------------------------------------
# Polling
# ---------------------------------------------------------------------------

def poll_once(site_url, state, webhook_url=''):
    now = datetime.now(timezone.utc).isoformat()
    log.info(f"Polling {site_url} ...")

    new_posts, new_comments = [], []

    for p in fetch_items(site_url, 'posts'):
        uid = _item_id(p)
        p['_uid'] = uid
        if uid not in state['seen_post_ids']:
            new_posts.append(p)
            state['seen_post_ids'].append(uid)

    for c in fetch_items(site_url, 'comments'):
        uid = _item_id(c)
        c['_uid'] = uid
        if uid not in state['seen_comment_ids']:
            new_comments.append(c)
            state['seen_comment_ids'].append(uid)

    state['new_posts'] = (state.get('new_posts', []) + new_posts)[-200:]
    state['new_comments'] = (state.get('new_comments', []) + new_comments)[-200:]
    state['seen_post_ids'] = state['seen_post_ids'][-500:]
    state['seen_comment_ids'] = state['seen_comment_ids'][-2000:]
    state['last_poll'] = now
    state['poll_count'] = state.get('poll_count', 0) + 1

    log.info(f"  => {len(new_posts)} new posts, {len(new_comments)} new comments")

    if (new_posts or new_comments) and webhook_url:
        try:
            r = requests.post(webhook_url, json={
                'timestamp': now, 'new_posts': new_posts, 'new_comments': new_comments,
            }, timeout=10)
            log.info(f"  Webhook -> {r.status_code}")
        except Exception as e:
            log.error(f"  Webhook failed: {e}")

    return state


def polling_loop(site_url, state, state_path, interval, webhook_url):
    poll_once(site_url, state, webhook_url)
    save_state(state, state_path)
    while True:
        time.sleep(interval)
        try:
            poll_once(site_url, state, webhook_url)
            save_state(state, state_path)
        except Exception as e:
            log.error(f"Poll error: {e}")
            state.setdefault('errors', []).append({
                'time': datetime.now(timezone.utc).isoformat(), 'error': str(e),
            })
            state['errors'] = state['errors'][-20:]


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------

app = FastAPI(title="WP Content Monitor", version="1.0")
_state = {}
_config = {}


@app.get("/posts")
def get_posts():
    return JSONResponse({"count": len(_state.get("posts", [])), "posts": _state.get("posts", [])})

@app.get("/posts/new")
def get_new_posts():
    return JSONResponse({"count": len(_state.get("new_posts", [])), "posts": _state.get("new_posts", [])})

@app.get("/comments")
def get_comments():
    return JSONResponse({"count": len(_state.get("comments", [])), "comments": _state.get("comments", [])})

@app.get("/comments/new")
def get_new_comments():
    return JSONResponse({"count": len(_state.get("new_comments", [])), "comments": _state.get("new_comments", [])})

@app.get("/status")
def get_status():
    return JSONResponse({
        "last_poll": _state.get("last_poll"),
        "poll_count": _state.get("poll_count", 0),
        "poll_interval": _config.get("interval"),
        "tracked_posts": len(_state.get("seen_post_ids", [])),
        "tracked_comments": len(_state.get("seen_comment_ids", [])),
        "pending_new_posts": len(_state.get("new_posts", [])),
        "pending_new_comments": len(_state.get("new_comments", [])),
        "recent_errors": _state.get("errors", [])[-5:],
    })

@app.post("/mark-seen")
def mark_seen():
    _state["new_posts"] = []
    _state["new_comments"] = []
    return JSONResponse({"status": "ok"})

@app.post("/check-now")
def check_now():
    poll_once(_config["site_url"], _state, _config.get("webhook_url", ""))
    return JSONResponse({
        "status": "ok",
        "new_posts": len(_state.get("new_posts", [])),
        "new_comments": len(_state.get("new_comments", [])),
    })


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global _state, _config

    parser = argparse.ArgumentParser(description='WordPress content monitor')
    parser.add_argument('--site', required=True, help='WordPress site URL')
    parser.add_argument('--port', type=int, default=int(os.environ.get('PORT', 8400)))
    parser.add_argument('--interval', type=int, default=int(os.environ.get('POLL_INTERVAL', 300)))
    parser.add_argument('--state-dir', default='.', help='Directory for state file')
    args = parser.parse_args()

    state_path = Path(args.state_dir) / 'monitor_state.json'
    webhook_url = os.environ.get('WEBHOOK_URL', '')

    _config = {'site_url': args.site, 'interval': args.interval, 'webhook_url': webhook_url}
    _state = load_state(state_path)

    print(f"\n  WP Content Monitor")
    print(f"  Site:     {args.site}")
    print(f"  API:      http://127.0.0.1:{args.port}")
    print(f"  Interval: {args.interval}s")
    print(f"  Webhook:  {webhook_url or '(none)'}\n")

    t = threading.Thread(
        target=polling_loop,
        args=(args.site, _state, state_path, args.interval, webhook_url),
        daemon=True,
    )
    t.start()
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
