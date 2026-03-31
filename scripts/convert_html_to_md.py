#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HTML to RAG-Ready Markdown Converter (Multiprocessing)
======================================================
Converts WordPress HTML pages to structured Markdown with:
  - YAML frontmatter (title, author, date, tags, source URL, media refs)
  - Article body with proper heading hierarchy
  - Threaded comment extraction with depth, author roles, reply chains
  - Site team / admin identification
  - Multiprocessing for throughput (default 8 workers)
  - Resume support (skip already-converted files)

Strips: navigation, sidebars, footers, ads, scripts, CSS, social buttons,
        voting/rating widgets.

Usage:
  py convert_html_to_md.py --input html_raw/ --output md_output/ --all
  py convert_html_to_md.py --input html_raw/ --output md_output/ --sample 10
  py convert_html_to_md.py --input html_raw/ --output md_output/ --all --resume
  py convert_html_to_md.py --input html_raw/ --output md_output/ --file page.html
"""

import argparse
import json
import os
import random
import re
import sys
from datetime import datetime
from multiprocessing import Pool, cpu_count
from pathlib import Path
from urllib.parse import unquote

from bs4 import BeautifulSoup

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')


# ---------------------------------------------------------------------------
# Metadata extraction
# ---------------------------------------------------------------------------

def extract_title(soup):
    """Extract page title (tries common WordPress patterns)."""
    # Elementor theme
    el = soup.select_one('h1.elementor-heading-title')
    if el:
        return el.get_text(strip=True)
    h1 = soup.find('h1')
    if h1:
        return h1.get_text(strip=True)
    title = soup.find('title')
    if title:
        return title.get_text(strip=True).split('|')[0].strip()
    return ''


def extract_author(soup):
    """Extract author from common WordPress author elements."""
    for selector in ['h4.elementor-author-box__name', '.author-name',
                     '.entry-author-name', 'span.author']:
        el = soup.select_one(selector)
        if el:
            return el.get_text(strip=True)
    return ''


def extract_date(soup):
    """Extract publication date."""
    # Structured data
    el = soup.select_one('[itemprop="datePublished"] time')
    if el:
        return el.get_text(strip=True)
    el = soup.select_one('time.entry-date')
    if el:
        return el.get('datetime', el.get_text(strip=True))
    # Meta tag fallback
    meta = soup.find('meta', property='article:published_time')
    if meta and meta.get('content'):
        return meta['content'][:10]
    return ''


def extract_tags(soup):
    """Extract article tags/categories."""
    tags = []
    for selector in ['span.elementor-post-info__terms-list a',
                     'a[rel="tag"]', '.post-tags a', '.entry-tags a']:
        for el in soup.select(selector):
            t = el.get_text(strip=True)
            if t:
                tags.append(t)
        if tags:
            break
    return tags


def extract_source_url(soup):
    """Extract canonical URL."""
    canonical = soup.find('link', rel='canonical')
    if canonical and canonical.get('href'):
        return canonical['href']
    og = soup.find('meta', property='og:url')
    if og and og.get('content'):
        return og['content']
    return ''


# ---------------------------------------------------------------------------
# Body extraction
# ---------------------------------------------------------------------------

def extract_body(soup):
    """Extract article body as markdown text, stripping boilerplate."""
    # Remove voting/rating widgets
    for junk_sel in ['div.wpd-rating-wrap', 'span.wpdrt', 'div.wpd-rating-data',
                     'div.sharedaddy', 'div.jp-relatedposts']:
        for junk in soup.select(junk_sel):
            junk.decompose()

    # Try common WordPress content containers
    container = None
    for sel in ['div.elementor-widget-theme-post-content .elementor-widget-container',
                'div.entry-content', 'article .post-content', 'article']:
        container = soup.select_one(sel)
        if container:
            break
    if not container:
        container = soup.find('article')
    if not container:
        return ''

    parts = []
    for el in container.children:
        if not hasattr(el, 'name') or el.name is None:
            text = str(el).strip()
            if text:
                parts.append(text)
            continue

        if el.name == 'p':
            text = el.get_text(strip=True)
            if text:
                parts.append(text)
        elif el.name in ('h1', 'h2', 'h3', 'h4', 'h5', 'h6'):
            level = int(el.name[1])
            text = el.get_text(strip=True)
            if text:
                parts.append(f"\n{'#' * (level + 1)} {text}\n")
        elif el.name == 'blockquote':
            text = el.get_text(strip=True)
            if text:
                lines = text.split('\n')
                parts.append('\n'.join(f'> {line}' for line in lines))
        elif el.name == 'ul':
            for li in el.find_all('li', recursive=False):
                parts.append(f"- {li.get_text(strip=True)}")
        elif el.name == 'ol':
            for i, li in enumerate(el.find_all('li', recursive=False), 1):
                parts.append(f"{i}. {li.get_text(strip=True)}")
        elif el.name == 'figure':
            caption = el.find('figcaption')
            if caption:
                parts.append(f"[Image: {caption.get_text(strip=True)}]")
        elif el.name == 'div':
            text = el.get_text(strip=True)
            if text and len(text) > 20:
                parts.append(text)
        else:
            text = el.get_text(strip=True)
            if text:
                parts.append(text)

    return '\n\n'.join(parts)


# ---------------------------------------------------------------------------
# Comment extraction (WordPress comment plugins: wpDiscuz, native, etc.)
# ---------------------------------------------------------------------------

SITE_TEAM_MARKERS = ['wpd-blog-administrator', 'bypostauthor', 'wpd-blog-post_author']


def extract_comments(soup):
    """Extract comments with threading structure.

    Returns list of {author, date, text, depth, is_site_team, reply_to}.
    """
    comments = []

    # Try wpDiscuz format
    comment_divs = soup.select('div[id^="wpd-comm-"]')
    if not comment_divs:
        # Try native WordPress comments
        comment_divs = soup.select('li.comment, div.comment')

    for div in comment_divs:
        classes = div.get('class', [])

        # Depth from class (depth-1, depth-2, etc.)
        depth = 1
        for cls in classes:
            m = re.match(r'depth-(\d+)', cls)
            if m:
                depth = int(m.group(1))
                break

        # Site team detection
        wrap = div.select_one('div.wpd-comment-wrap') or div
        wrap_classes = ' '.join(wrap.get('class', []))
        is_site_team = any(marker in wrap_classes for marker in SITE_TEAM_MARKERS)

        # Author
        author_el = (div.select_one('div.wpd-comment-author') or
                     div.select_one('.comment-author .fn') or
                     div.select_one('.vcard .fn'))
        author = author_el.get_text(strip=True) if author_el else 'Unknown'

        # Date
        date_el = div.select_one('div.wpd-comment-date') or div.select_one('time.comment-date')
        date = ''
        if date_el:
            date = date_el.get('title', '') or date_el.get('datetime', '') or date_el.get_text(strip=True)

        # Reply-to
        reply_el = div.select_one('div.wpd-reply-to a')
        reply_to = reply_el.get_text(strip=True) if reply_el else ''

        # Comment text
        text_el = (div.select_one('div.wpd-comment-text') or
                   div.select_one('.comment-content'))
        text = text_el.get_text(strip=True) if text_el else ''

        if text:
            comments.append({
                'author': author,
                'date': date,
                'text': text,
                'depth': depth,
                'is_site_team': is_site_team,
                'reply_to': reply_to,
            })

    return comments


def comments_to_markdown(comments):
    """Convert comment list to threaded markdown with indentation."""
    if not comments:
        return ''

    lines = ['## Comments\n']
    for c in comments:
        indent = '  ' * (c['depth'] - 1)
        if c['is_site_team']:
            author_label = f"**[Site Team] {c['author']}**"
        else:
            author_label = f"**{c['author']}**"

        reply_info = ''
        if c['reply_to'] and c['reply_to'] != c['author']:
            reply_info = f" (reply to {c['reply_to']})"

        date_str = f" | {c['date']}" if c['date'] else ''

        lines.append(f"{indent}---")
        lines.append(f"{indent}{author_label}{reply_info}{date_str}")
        lines.append(f"{indent}")
        for text_line in c['text'].split('\n'):
            lines.append(f"{indent}{text_line}")
        lines.append('')

    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# Media discovery
# ---------------------------------------------------------------------------

PDF_RE = re.compile(r'\.pdf(\?|$)', re.IGNORECASE)
AUDIO_RE = re.compile(r'\.(mp3|wav|ogg)(\?|$)', re.IGNORECASE)
VIDEO_RE = re.compile(r'\.(mp4|webm)(\?|$)', re.IGNORECASE)


def discover_media(soup, page_url):
    """Extract referenced PDF, audio, video links from the page."""
    pdfs, audio, video = set(), set(), set()
    for tag in soup.find_all(['a', 'source', 'video', 'audio', 'embed', 'iframe']):
        href = tag.get('href') or tag.get('src') or ''
        if not href or not href.startswith(('http', '/')):
            continue
        if href.startswith('/'):
            from urllib.parse import urlparse
            parsed = urlparse(page_url)
            href = f"{parsed.scheme}://{parsed.netloc}{href}"
        if PDF_RE.search(href):
            pdfs.add(href.split('?')[0])
        elif AUDIO_RE.search(href):
            audio.add(href.split('?')[0])
        elif VIDEO_RE.search(href):
            video.add(href.split('?')[0])
    return list(pdfs), list(audio), list(video)


# ---------------------------------------------------------------------------
# Full conversion
# ---------------------------------------------------------------------------

def convert_single_file(html_path, output_dir=None):
    """Convert one HTML file to RAG-ready markdown. Returns (md_content, word_count) or (None, reason)."""
    html_text = Path(html_path).read_text(encoding='utf-8', errors='replace')
    soup = BeautifulSoup(html_text, 'html.parser')

    title = extract_title(soup)
    author = extract_author(soup)
    date = extract_date(soup)
    tags = extract_tags(soup)
    source_url = extract_source_url(soup)
    body = extract_body(soup)
    comments = extract_comments(soup)

    if not body and not comments:
        return None, 'empty'

    pdfs, audio, video = discover_media(soup, source_url)

    # Build YAML frontmatter
    tags_yaml = ', '.join(tags) if tags else ''
    parts = [
        '---',
        f'title: "{title}"',
        f'author: "{author}"',
        f'date: "{date}"',
        f'tags: [{tags_yaml}]',
        f'source: {source_url or "unknown"}',
    ]
    if pdfs:
        pdf_names = [unquote(u.split('/')[-1]) for u in pdfs]
        parts.append(f'referenced_pdfs: {json.dumps(pdf_names, ensure_ascii=False)}')
    if audio:
        parts.append(f'referenced_audio: {json.dumps(audio, ensure_ascii=False)}')
    if video:
        parts.append(f'referenced_video: {json.dumps(video, ensure_ascii=False)}')
    parts.extend(['---', '', f'# {title}', '', body])

    if comments:
        parts.extend(['', comments_to_markdown(comments)])

    md_content = '\n'.join(parts) + '\n'
    word_count = len(md_content.split())
    return md_content, word_count


def _worker_convert(args):
    """Multiprocessing worker wrapper."""
    html_path, output_dir, resume = args
    fname = os.path.basename(html_path)
    md_name = fname.replace('.html', '.md')
    md_path = os.path.join(output_dir, md_name)

    if resume and os.path.exists(md_path):
        return {'status': 'skipped', 'file': fname}

    try:
        md_content, result = convert_single_file(html_path)
    except Exception as e:
        return {'status': 'error', 'file': fname, 'error': str(e)}

    if md_content is None:
        return {'status': 'empty', 'file': fname}

    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(md_content)

    return {
        'status': 'ok', 'file': fname, 'md': md_name,
        'words': result, 'has_comments': '## Comments' in md_content,
    }


def process_batch(files, output_dir, resume=False, workers=None):
    """Convert HTML files to MD using multiprocessing."""
    os.makedirs(output_dir, exist_ok=True)
    workers = workers or min(8, cpu_count())

    tasks = [(f, output_dir, resume) for f in files]
    stats = {'converted': 0, 'skipped': 0, 'empty': 0, 'errors': 0,
             'total_words': 0, 'with_comments': 0}

    print(f"Converting {len(files)} files with {workers} workers...")

    with Pool(processes=workers) as pool:
        for i, result in enumerate(pool.imap_unordered(_worker_convert, tasks)):
            if result['status'] == 'ok':
                stats['converted'] += 1
                stats['total_words'] += result['words']
                if result.get('has_comments'):
                    stats['with_comments'] += 1
            elif result['status'] == 'skipped':
                stats['skipped'] += 1
            elif result['status'] == 'empty':
                stats['empty'] += 1
            else:
                stats['errors'] += 1

            if (i + 1) % 200 == 0:
                print(f"  [{i+1}/{len(files)}] "
                      f"OK:{stats['converted']} SKIP:{stats['skipped']} "
                      f"ERR:{stats['errors']}")

    return stats


def main():
    parser = argparse.ArgumentParser(description='HTML to RAG-ready Markdown converter')
    parser.add_argument('--input', required=True, help='Directory of HTML files')
    parser.add_argument('--output', required=True, help='Output directory for .md files')
    parser.add_argument('--all', action='store_true', help='Convert all files')
    parser.add_argument('--file', type=str, help='Convert one file')
    parser.add_argument('--sample', type=int, help='Convert N random samples')
    parser.add_argument('--resume', action='store_true', help='Skip already converted')
    parser.add_argument('--workers', type=int, default=None, help='Number of workers')
    args = parser.parse_args()

    print(f"=== HTML -> Markdown Converter ===")
    print(f"Date: {datetime.now().isoformat()}")

    if args.file:
        fpath = os.path.join(args.input, args.file)
        if not os.path.exists(fpath):
            print(f"[ERROR] {fpath} not found")
            sys.exit(1)
        files = [fpath]
    else:
        files = sorted([
            os.path.join(root, f)
            for root, dirs, fnames in os.walk(args.input)
            for f in fnames if f.endswith('.html')
        ])
        if args.sample:
            files = random.sample(files, min(args.sample, len(files)))

    if not args.all and not args.file and not args.sample:
        print(f"Found {len(files)} HTML files. Use --all, --file, or --sample N")
        return

    print(f"Processing {len(files)} files")
    stats = process_batch(files, args.output, resume=args.resume, workers=args.workers)

    print(f"\n{'='*50}")
    print(f"CONVERSION COMPLETE")
    print(f"{'='*50}")
    print(f"Converted:      {stats['converted']:,}")
    print(f"With comments:  {stats['with_comments']:,}")
    print(f"Skipped:        {stats['skipped']:,}")
    print(f"Empty:          {stats['empty']:,}")
    print(f"Errors:         {stats['errors']:,}")
    print(f"Total words:    {stats['total_words']:,}")


if __name__ == '__main__':
    main()
