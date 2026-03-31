#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Execute Duplicate Deletion from Review Manifest
================================================
Reads the manifest produced by detect_duplicates.py and deletes
the marked duplicate files. Always produces a deletion log.

Safety:
  - Requires explicit --confirm flag to actually delete
  - Without --confirm, runs in dry-run mode (shows what would be deleted)
  - Logs every deletion with timestamp

Usage:
  py delete_duplicates.py --manifest duplicates_review.json              # dry run
  py delete_duplicates.py --manifest duplicates_review.json --confirm    # execute
"""

import argparse
import json
import os
import sys
from datetime import datetime

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')


def main():
    parser = argparse.ArgumentParser(description='Delete duplicates from review manifest')
    parser.add_argument('--manifest', required=True, help='Path to duplicates_review.json')
    parser.add_argument('--base-dir', default='downloaded_html', help='Base directory for relative paths')
    parser.add_argument('--confirm', action='store_true', help='Actually delete (otherwise dry run)')
    args = parser.parse_args()

    with open(args.manifest, 'r', encoding='utf-8') as f:
        report = json.load(f)

    groups = report.get('groups', [])
    total_dupes = sum(len(g['duplicates']) for g in groups)

    mode = "DELETING" if args.confirm else "DRY RUN"
    print(f"=== Duplicate Deletion ({mode}) ===")
    print(f"Groups: {len(groups)} | Files to remove: {total_dupes}")

    deleted = 0
    errors = 0
    deletion_log = []

    for group in groups:
        canonical = group['canonical']['file']
        for dup in group['duplicates']:
            filepath = os.path.join(args.base_dir, dup['file'])

            if args.confirm:
                try:
                    if os.path.exists(filepath):
                        os.remove(filepath)
                        deleted += 1
                        deletion_log.append({
                            'file': dup['file'],
                            'canonical': canonical,
                            'hash': group['hash'],
                            'timestamp': datetime.now().isoformat(),
                        })
                    else:
                        print(f"  [SKIP] Not found: {dup['file']}")
                except Exception as e:
                    print(f"  [ERROR] {dup['file']}: {e}")
                    errors += 1
            else:
                print(f"  Would delete: {dup['file']}")
                print(f"    Canonical:  {canonical}")
                deleted += 1

    if args.confirm and deletion_log:
        log_path = args.manifest.replace('.json', '_deletion_log.json')
        with open(log_path, 'w', encoding='utf-8') as f:
            json.dump(deletion_log, f, ensure_ascii=False, indent=2)
        print(f"\nDeletion log: {log_path}")

    print(f"\n{'='*50}")
    print(f"{'DELETED' if args.confirm else 'WOULD DELETE'}: {deleted:,} files")
    if errors:
        print(f"ERRORS: {errors}")
    if not args.confirm:
        print(f"\nRe-run with --confirm to execute.")


if __name__ == '__main__':
    main()
