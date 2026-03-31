#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WordPress Incremental Content Updater
======================================
Detects new posts on a WordPress site via WP REST API (RSS fallback),
downloads HTML, converts to Markdown using the existing pipeline.

Comparison logic:
  - Fetches recent posts from /wp-json/wp/v2/posts (paginated)
  - Falls back to RSS feed if REST API unavailable
  - Compares against local URL database + existing HTML files
  - Downloads only genuinely new content

Usage:
  py wp_content_updater.py --site https://example.com --output project/
  py wp_content_updater.py --site https://example.com --output project/ --dry-run
  py wp_content_updater.py --site https://example.com --output project/ --max 5
"""

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

import feedparser
import requests
from bs4 import BeautifulSoup

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BROWSER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
}

RATE_LIMIT = 1.5


def log(msg, log_file=None):
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode('ascii', errors='replace').decode())
    if log_file:
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

def load_state(state_file):
    if os.path.exists(state_file):
        with open(state_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {'known_urls': [], 'last_check': None, 'updates': []}


def save_state(state, state_file):
    with open(state_file, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def load_known_urls(url_list_file):
    """Build set of known URLs from the master URL list."""
    urls = set()
    if os.path.exists(url_list_file):
        with open(url_list_file, 'r', encoding='utf-8') as f:
            for line in f:
                url = line.strip()
                if url:
                    urls.add(url)
                    urls.add(unquote(url).split('?')[0].rstrip('/'))
    return urls


# ---------------------------------------------------------------------------
# Fetch from WordPress
# ---------------------------------------------------------------------------

def fetch_posts_wp_rest(site_url, per_page=20, page=1):
    """Fetch posts via WP REST API."""
    api_url = f"{site_url.rstrip('/')}/wp-json/wp/v2/posts"
    params = {
        'per_page': per_page, 'page': page,
        '_fields': 'id,date,title,link,slug',
        'orderby': 'date', 'order': 'desc',
    }
    try:
        r = requests.get(api_url, params=params, headers=BROWSER_HEADERS, timeout=15)
        if r.status_code == 200:
            results = []
            for p in r.json():
                title = p.get('title', {})
                if isinstance(title, dict):
                    title = title.get('rendered', '')
                results.append({
                    'id': p.get('id'), 'link': p.get('link', ''),
                    'title': title, 'date': p.get('date', ''),
                })
            return results
    except Exception:
        pass
    return None


def fetch_posts_rss(site_url):
    """Fallback: fetch from RSS feed."""
    feed_url = f"{site_url.rstrip('/')}/feed/"
    try:
        r = requests.get(feed_url, headers=BROWSER_HEADERS, timeout=15)
        if r.status_code != 200:
            return None
        feed = feedparser.parse(r.text)
        return [{
            'id': e.get('id', e.get('link', '')),
            'link': e.get('link', ''),
            'title': e.get('title', ''),
            'date': e.get('published', ''),
        } for e in feed.entries]
    except Exception:
        return None


def fetch_recent_posts(site_url, pages=3):
    """Try WP REST API first (multiple pages), then RSS."""
    all_posts = []
    for page in range(1, pages + 1):
        posts = fetch_posts_wp_rest(site_url, per_page=20, page=page)
        if posts is None:
            break
        all_posts.extend(posts)
        if len(posts) < 20:
            break
        time.sleep(0.5)

    if all_posts:
        return all_posts, 'wp-rest-api'

    posts = fetch_posts_rss(site_url)
    if posts:
        return posts, 'rss'

    return [], 'none'


# ---------------------------------------------------------------------------
# Download + convert
# ---------------------------------------------------------------------------

def url_to_filename(url):
    """Convert URL to safe filename."""
    parsed = urlparse(url)
    path = unquote(parsed.path).strip('/')
    safe = re.sub(r'[<>:"/\\|?*\n\r]', '_', path)
    safe = re.sub(r'_+', '_', safe).strip('_')
    if not safe:
        safe = hashlib.md5(url.encode()).hexdigest()[:16]
    if len(safe) > 180:
        safe = safe[:160] + '_' + hashlib.md5(url.encode()).hexdigest()[:8]
    return safe + '.html'


def download_and_convert(url, html_dir, md_dir, session):
    """Download one page and convert to markdown. Returns status dict."""
    html_fname = url_to_filename(url)
    html_path = os.path.join(html_dir, html_fname)
    md_fname = html_fname.replace('.html', '.md')
    md_path = os.path.join(md_dir, md_fname)

    # Download
    try:
        r = session.get(url, headers=BROWSER_HEADERS, timeout=30)
        r.raise_for_status()
        r.encoding = r.apparent_encoding or 'utf-8'
    except Exception as e:
        return {'url': url, 'status': 'download_error', 'error': str(e)}

    os.makedirs(html_dir, exist_ok=True)
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(r.text)

    # Convert using the converter module
    try:
        from convert_html_to_md import convert_single_file
        md_content, result = convert_single_file(html_path)
    except ImportError:
        # Inline minimal conversion if converter not on path
        md_content, result = _minimal_convert(r.text, url)

    if md_content is None:
        return {'url': url, 'status': 'empty', 'html': html_fname}

    os.makedirs(md_dir, exist_ok=True)
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(md_content)

    return {'url': url, 'status': 'ok', 'html': html_fname, 'md': md_fname, 'words': result}


def _minimal_convert(html_text, url):
    """Minimal HTML->MD fallback if full converter not importable."""
    soup = BeautifulSoup(html_text, 'html.parser')
    title_tag = soup.find('title')
    title = title_tag.get_text(strip=True) if title_tag else 'Untitled'
    body = soup.find('article') or soup.find('div', class_='entry-content')
    text = body.get_text(separator='\n\n', strip=True) if body else ''
    if not text:
        return None, 'empty'
    md = f"---\ntitle: \"{title}\"\nsource: {url}\n---\n\n# {title}\n\n{text}\n"
    return md, len(md.split())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='WordPress incremental content updater')
    parser.add_argument('--site', required=True, help='WordPress site URL (e.g. https://example.com)')
    parser.add_argument('--output', required=True, help='Project output directory')
    parser.add_argument('--dry-run', action='store_true', help='Preview without downloading')
    parser.add_argument('--max', type=int, default=None, help='Limit new articles to process')
    parser.add_argument('--pages', type=int, default=3, help='API pages to scan (20 posts each)')
    args = parser.parse_args()

    html_dir = os.path.join(args.output, 'html_raw')
    md_dir = os.path.join(args.output, 'md_output')
    data_dir = os.path.join(args.output, 'data')
    os.makedirs(data_dir, exist_ok=True)

    state_file = os.path.join(data_dir, 'update_state.json')
    url_list_file = os.path.join(data_dir, 'known_urls.txt')
    log_file = os.path.join(data_dir, 'update_log.txt')

    log(f"=== WordPress Content Updater ===", log_file)
    log(f"Site: {args.site}", log_file)

    known_urls = load_known_urls(url_list_file)
    log(f"Known URLs: {len(known_urls)}", log_file)

    log("Fetching recent posts...", log_file)
    posts, source = fetch_recent_posts(args.site, pages=args.pages)
    if not posts:
        log("[ERROR] Could not fetch posts from site", log_file)
        return
    log(f"Fetched {len(posts)} posts via {source}", log_file)

    # Find new posts
    new_posts = []
    for p in posts:
        link = p['link'].rstrip('/')
        decoded = unquote(link).split('?')[0].rstrip('/')
        if link not in known_urls and decoded not in known_urls:
            html_fname = url_to_filename(link)
            if not os.path.exists(os.path.join(html_dir, html_fname)):
                new_posts.append(p)

    if not new_posts:
        log("[OK] No new posts -- up to date", log_file)
        return

    log(f"Found {len(new_posts)} new posts:", log_file)
    for p in new_posts:
        log(f"  - {p['title'][:60]}  ({p['date'][:10]})", log_file)

    if args.max:
        new_posts = new_posts[:args.max]

    if args.dry_run:
        log(f"\n[DRY RUN] Would download {len(new_posts)} articles", log_file)
        return

    # Process
    session = requests.Session()
    results = []
    for i, p in enumerate(new_posts):
        log(f"\n[{i+1}/{len(new_posts)}] {p['title'][:60]}", log_file)
        result = download_and_convert(p['link'], html_dir, md_dir, session)
        results.append(result)
        log(f"  [{result['status'].upper()}]", log_file)

        # Append to URL list (with newline safety)
        with open(url_list_file, 'rb+') as f:
            f.seek(0, 2)
            if f.tell() > 0:
                f.seek(f.tell() - 1)
                if f.read(1) != b'\n':
                    f.write(b'\n')
            else:
                pass
        with open(url_list_file, 'a', encoding='utf-8') as f:
            f.write(p['link'] + '\n')

        time.sleep(RATE_LIMIT)

    ok = sum(1 for r in results if r['status'] == 'ok')
    errors = sum(1 for r in results if 'error' in r.get('status', ''))

    log(f"\n{'='*50}", log_file)
    log(f"UPDATE COMPLETE", log_file)
    log(f"[OK] {ok} downloaded + converted | [ERROR] {errors} errors", log_file)

    state = load_state(state_file)
    state['last_check'] = datetime.now().isoformat()
    state['updates'] = (state.get('updates', []) + [{
        'date': datetime.now().isoformat(),
        'new_found': len(new_posts), 'downloaded': ok, 'errors': errors,
    }])[-50:]
    save_state(state, state_file)


if __name__ == '__main__':
    main()
