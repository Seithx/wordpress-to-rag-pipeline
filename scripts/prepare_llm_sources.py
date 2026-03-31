#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LLM Source Bundler
==================
Concatenates converted .md files into mega-files for LLM consumption
(NotebookLM, custom RAG, etc.) respecting per-source word limits.

Each mega-file gets a clear document separator so boundaries are preserved
during chunking. Generates an upload manifest with word counts and
source-to-file mappings.

Usage:
  py prepare_llm_sources.py --input md_output/ --output bundled/ --max-words 450000
  py prepare_llm_sources.py --input md_output/ --output bundled/ --max-words 450000 --prefix corpus
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DOCUMENT_SEPARATOR = "\n\n" + "=" * 80 + "\n" + "=" * 80 + "\n\n"


def count_words(text):
    return len(text.split())


def collect_md_files(input_dir):
    """Collect all .md files sorted by name."""
    files = []
    for root, dirs, fnames in os.walk(input_dir):
        for f in sorted(fnames):
            if f.endswith('.md'):
                files.append(os.path.join(root, f))
    return files


def bundle_files(md_files, output_dir, max_words=450000, prefix='bundle'):
    """Bundle .md files into mega-files respecting word limit."""
    os.makedirs(output_dir, exist_ok=True)

    bundles = []
    current_parts = []
    current_words = 0
    bundle_idx = 1
    manifest_entries = []

    for md_path in md_files:
        with open(md_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()

        words = count_words(content)
        fname = os.path.basename(md_path)

        # If adding this file exceeds the limit, flush current bundle
        if current_words + words > max_words and current_parts:
            bundle_name = f"{prefix}_{bundle_idx:02d}.md"
            _write_bundle(output_dir, bundle_name, current_parts)
            bundles.append({'name': bundle_name, 'words': current_words,
                            'files': len(current_parts)})
            bundle_idx += 1
            current_parts = []
            current_words = 0

        current_parts.append({'filename': fname, 'content': content, 'words': words})
        current_words += words
        manifest_entries.append({'file': fname, 'words': words, 'bundle': f"{prefix}_{bundle_idx:02d}.md"})

    # Flush remaining
    if current_parts:
        bundle_name = f"{prefix}_{bundle_idx:02d}.md"
        _write_bundle(output_dir, bundle_name, current_parts)
        bundles.append({'name': bundle_name, 'words': current_words,
                        'files': len(current_parts)})

    return bundles, manifest_entries


def _write_bundle(output_dir, bundle_name, parts):
    """Write a single mega-file from parts with separators."""
    path = os.path.join(output_dir, bundle_name)
    with open(path, 'w', encoding='utf-8') as f:
        for i, part in enumerate(parts):
            if i > 0:
                f.write(DOCUMENT_SEPARATOR)
            f.write(part['content'])


def main():
    parser = argparse.ArgumentParser(description='Bundle .md files into mega-files for LLM')
    parser.add_argument('--input', required=True, help='Directory of .md files')
    parser.add_argument('--output', required=True, help='Output directory for bundles')
    parser.add_argument('--max-words', type=int, default=450000, help='Max words per bundle')
    parser.add_argument('--prefix', default='bundle', help='Bundle filename prefix')
    args = parser.parse_args()

    print(f"=== LLM Source Bundler ===")
    print(f"Date: {datetime.now().isoformat()}")

    md_files = collect_md_files(args.input)
    print(f"Found {len(md_files)} .md files")

    if not md_files:
        print("No files to bundle.")
        return

    bundles, manifest_entries = bundle_files(
        md_files, args.output, max_words=args.max_words, prefix=args.prefix
    )

    # Write manifest
    manifest = {
        'generated': datetime.now().isoformat(),
        'settings': {'max_words': args.max_words, 'prefix': args.prefix},
        'bundles': bundles,
        'total_files': len(manifest_entries),
        'total_words': sum(b['words'] for b in bundles),
        'total_bundles': len(bundles),
    }
    manifest_path = os.path.join(args.output, 'upload_manifest.json')
    with open(manifest_path, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*50}")
    print(f"BUNDLING COMPLETE")
    print(f"{'='*50}")
    print(f"Files:   {len(manifest_entries):,}")
    print(f"Bundles: {len(bundles)}")
    print(f"Words:   {manifest['total_words']:,}")
    print(f"\nBundles:")
    for b in bundles:
        print(f"  {b['name']}: {b['words']:,} words ({b['files']} files)")
    print(f"\nManifest: {manifest_path}")


if __name__ == '__main__':
    main()
