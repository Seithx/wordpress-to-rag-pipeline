#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Content-Hash Duplicate Detector
================================
Detects duplicate HTML files by content hash (SHA-256 on normalized text).
Groups duplicates and produces a deletion manifest for review before action.

Strategy:
  - Parse HTML, extract text content only (strip tags, scripts, styles)
  - Normalize: collapse whitespace, lowercase, strip
  - SHA-256 hash the normalized text
  - Group files with identical hashes
  - Pick canonical file per group (shortest URL = most likely canonical)
  - Output review manifest (no deletion without explicit confirmation)

Usage:
  py detect_duplicates.py --input downloaded_html/
  py detect_duplicates.py --input downloaded_html/ --report duplicates_review.json
"""

import argparse
import hashlib
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime

from bs4 import BeautifulSoup

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')


def extract_text_content(html_text):
    """Extract visible text content from HTML, stripping all markup."""
    soup = BeautifulSoup(html_text, 'html.parser')
    # Remove script, style, and other non-content tags
    for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'noscript']):
        tag.decompose()
    text = soup.get_text(separator=' ')
    # Normalize whitespace and case
    text = re.sub(r'\s+', ' ', text).strip().lower()
    return text


def content_hash(html_text):
    """SHA-256 hash of normalized text content."""
    text = extract_text_content(html_text)
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def extract_url_from_metadata(html_text):
    """Extract original URL from the embedded metadata comment (if present)."""
    match = re.search(r'Original URL:\s*(https?://\S+)', html_text[:2000])
    return match.group(1) if match else None


def scan_directory(input_dir):
    """Scan all HTML files, compute content hashes, group duplicates."""
    hash_groups = defaultdict(list)
    file_count = 0

    for root, dirs, files in os.walk(input_dir):
        # Skip metadata directory
        if 'metadata' in root:
            continue
        for fname in files:
            if not fname.endswith('.html'):
                continue
            filepath = os.path.join(root, fname)
            file_count += 1

            try:
                with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                    html_text = f.read()

                h = content_hash(html_text)
                url = extract_url_from_metadata(html_text) or ''
                rel_path = os.path.relpath(filepath, input_dir)
                size = os.path.getsize(filepath)

                hash_groups[h].append({
                    'file': rel_path,
                    'url': url,
                    'size': size,
                })
            except Exception as e:
                print(f"  [ERROR] {filepath}: {e}")

            if file_count % 500 == 0:
                print(f"  Scanned {file_count} files...")

    print(f"Scanned {file_count} HTML files total")
    return hash_groups, file_count


def build_deletion_manifest(hash_groups):
    """Build a manifest marking which files to keep (canonical) and which to delete.

    Canonical selection: shortest URL (most likely the clean permalink).
    """
    duplicate_groups = []
    total_dupes = 0

    for h, files in hash_groups.items():
        if len(files) < 2:
            continue

        # Sort by URL length -- shortest is canonical
        sorted_files = sorted(files, key=lambda f: len(f.get('url', '')))
        canonical = sorted_files[0]
        duplicates = sorted_files[1:]
        total_dupes += len(duplicates)

        duplicate_groups.append({
            'hash': h,
            'count': len(files),
            'canonical': canonical,
            'duplicates': duplicates,
        })

    # Sort groups by size (largest groups first) for review prioritization
    duplicate_groups.sort(key=lambda g: g['count'], reverse=True)

    return duplicate_groups, total_dupes


def main():
    parser = argparse.ArgumentParser(description='Content-hash duplicate detector')
    parser.add_argument('--input', required=True, help='Directory of HTML files')
    parser.add_argument('--report', default='duplicates_review.json', help='Output report path')
    args = parser.parse_args()

    print(f"=== Content-Hash Duplicate Detector ===")
    print(f"Date: {datetime.now().isoformat()}")
    print(f"Scanning: {args.input}\n")

    hash_groups, file_count = scan_directory(args.input)
    unique_hashes = len(hash_groups)
    dup_groups_raw = sum(1 for g in hash_groups.values() if len(g) > 1)

    print(f"\nUnique content hashes: {unique_hashes}")
    print(f"Duplicate groups: {dup_groups_raw}")

    groups, total_dupes = build_deletion_manifest(hash_groups)

    report = {
        'generated': datetime.now().isoformat(),
        'input_dir': args.input,
        'total_files': file_count,
        'unique_hashes': unique_hashes,
        'duplicate_groups': len(groups),
        'total_duplicates': total_dupes,
        'space_reclaimable_bytes': sum(
            sum(d['size'] for d in g['duplicates']) for g in groups
        ),
        'groups': groups,
    }

    with open(args.report, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*50}")
    print(f"DUPLICATE DETECTION COMPLETE")
    print(f"{'='*50}")
    print(f"Files scanned:     {file_count:,}")
    print(f"Unique content:    {unique_hashes:,}")
    print(f"Duplicate groups:  {len(groups):,}")
    print(f"Files to remove:   {total_dupes:,}")
    print(f"Space reclaimable: {report['space_reclaimable_bytes'] / (1024*1024):.1f} MB")
    print(f"\nReview manifest:   {args.report}")
    print(f"Run delete_duplicates.py after reviewing the manifest.")


if __name__ == '__main__':
    main()
