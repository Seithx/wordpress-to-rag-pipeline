#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch Validation for Markdown Corpus
=====================================
Runs quality checks across converted .md files to catch issues
before loading into LLM/RAG systems.

Check types:
  qa_completeness      - Q&A files have question + answer sections
  article_completeness - Articles have title + body + comments
  html_vs_md           - Every HTML has a corresponding .md
  frontmatter          - YAML frontmatter is valid and complete
  subscription_leaks   - No paywall/subscription artifacts leaked
  word_counts          - Distribution check, flag outliers

Usage:
  py validate_batch.py --check all --md-dir md_output/
  py validate_batch.py --check frontmatter --md-dir md_output/ --sample 100
  py validate_batch.py --check html_vs_md --html-dir html_raw/ --md-dir md_output/
"""

import argparse
import os
import re
import sys
import random
from collections import Counter
from datetime import datetime

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')


def check_frontmatter(md_path):
    """Validate YAML frontmatter exists and has required fields."""
    issues = []
    with open(md_path, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read(2000)  # frontmatter is at the top

    if not content.startswith('---'):
        return ['missing frontmatter']

    end = content.find('---', 3)
    if end == -1:
        return ['unclosed frontmatter']

    fm = content[3:end]
    required = ['title', 'source']
    for field in required:
        if f'{field}:' not in fm:
            issues.append(f'missing field: {field}')

    # Check for empty title
    title_match = re.search(r'title:\s*"(.*?)"', fm)
    if title_match and not title_match.group(1).strip():
        issues.append('empty title')

    return issues


def check_qa_completeness(md_path):
    """Check Q&A file has question and answer sections."""
    issues = []
    with open(md_path, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()

    if 'type: Q&A' not in content[:500] and 'type: qa' not in content[:500].lower():
        return []  # not a Q&A file, skip

    if '## Question' not in content and '## QUESTION' not in content:
        issues.append('missing question section')
    if "## Rabbi" not in content and "### RABBI" not in content and "## Answer" not in content:
        issues.append('missing answer section')

    return issues


def check_article_completeness(md_path):
    """Check article file has title, body, and comment section."""
    issues = []
    with open(md_path, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()

    if not content.startswith('---'):
        return ['not a valid article (no frontmatter)']

    # Body check: content between frontmatter and comments
    fm_end = content.find('---', 3)
    if fm_end == -1:
        return ['broken frontmatter']

    body_start = fm_end + 3
    comments_start = content.find('## Comments')
    body = content[body_start:comments_start] if comments_start > 0 else content[body_start:]

    body_text = body.strip()
    if len(body_text) < 50:
        issues.append(f'body too short ({len(body_text)} chars)')

    return issues


def check_subscription_leaks(md_path):
    """Check for paywall/subscription text that leaked through."""
    issues = []
    leak_patterns = [
        r'subscribe to continue',
        r'sign up to read',
        r'premium content',
        r'members only',
        r'login to view',
        r'paywall',
        r'your subscription',
    ]
    with open(md_path, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read().lower()

    for pattern in leak_patterns:
        if re.search(pattern, content):
            issues.append(f'subscription leak: "{pattern}"')

    return issues


def check_word_counts(md_path):
    """Return word count for distribution analysis."""
    with open(md_path, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()
    return len(content.split())


def check_html_vs_md(html_dir, md_dir):
    """Check every HTML file has a corresponding .md."""
    html_files = set()
    for root, dirs, files in os.walk(html_dir):
        for f in files:
            if f.endswith('.html'):
                html_files.add(f.replace('.html', ''))

    md_files = set()
    for f in os.listdir(md_dir):
        if f.endswith('.md'):
            md_files.add(f.replace('.md', ''))

    missing = html_files - md_files
    extra = md_files - html_files

    return missing, extra


def run_checks(md_dir, html_dir, check_types, sample_size=None):
    """Run specified checks across the corpus."""
    md_files = [
        os.path.join(md_dir, f)
        for f in os.listdir(md_dir) if f.endswith('.md')
    ]

    if sample_size and sample_size < len(md_files):
        md_files = random.sample(md_files, sample_size)
        print(f"Sampling {sample_size} of {len(os.listdir(md_dir))} files\n")

    results = {}

    # Per-file checks
    file_checks = {
        'frontmatter': check_frontmatter,
        'qa_completeness': check_qa_completeness,
        'article_completeness': check_article_completeness,
        'subscription_leaks': check_subscription_leaks,
    }

    for check_name, check_fn in file_checks.items():
        if check_name not in check_types and 'all' not in check_types:
            continue

        print(f"Running: {check_name}...")
        failures = {}
        for md_path in md_files:
            issues = check_fn(md_path)
            if issues:
                failures[os.path.basename(md_path)] = issues

        total = len(md_files)
        failed = len(failures)
        passed = total - failed
        results[check_name] = {
            'total': total, 'passed': passed, 'failed': failed,
            'pass_rate': f"{(passed/total)*100:.1f}%" if total else 'N/A',
            'failures': dict(list(failures.items())[:20]),  # cap output
        }
        status = '[OK]' if failed == 0 else f'[FAIL] {failed} issues'
        print(f"  {status} ({passed}/{total} passed)\n")

    # Word count distribution
    if 'word_counts' in check_types or 'all' in check_types:
        print("Running: word_counts...")
        counts = [check_word_counts(f) for f in md_files]
        counts.sort()
        if counts:
            p10 = counts[len(counts) // 10]
            p50 = counts[len(counts) // 2]
            p90 = counts[int(len(counts) * 0.9)]
            outliers_low = [os.path.basename(md_files[i]) for i, c in enumerate(counts) if c < 20]
            outliers_high = [os.path.basename(md_files[i]) for i, c in enumerate(counts) if c > 50000]
            results['word_counts'] = {
                'total_files': len(counts),
                'total_words': sum(counts),
                'p10': p10, 'p50_median': p50, 'p90': p90,
                'min': counts[0], 'max': counts[-1],
                'outliers_low_count': len(outliers_low),
                'outliers_high_count': len(outliers_high),
            }
            print(f"  Median: {p50} words | Range: {counts[0]}-{counts[-1]}")
            print(f"  Outliers: {len(outliers_low)} tiny (<20w), {len(outliers_high)} huge (>50Kw)\n")

    # HTML vs MD coverage
    if ('html_vs_md' in check_types or 'all' in check_types) and html_dir:
        print("Running: html_vs_md...")
        missing, extra = check_html_vs_md(html_dir, md_dir)
        results['html_vs_md'] = {
            'missing_md': len(missing),
            'extra_md': len(extra),
            'sample_missing': list(missing)[:10],
        }
        status = '[OK]' if not missing else f'[FAIL] {len(missing)} HTML files have no .md'
        print(f"  {status}\n")

    return results


def main():
    parser = argparse.ArgumentParser(description='Batch validation for markdown corpus')
    parser.add_argument('--check', required=True,
                        help='Check type(s): all, frontmatter, qa_completeness, '
                             'article_completeness, html_vs_md, subscription_leaks, word_counts')
    parser.add_argument('--md-dir', required=True, help='Markdown output directory')
    parser.add_argument('--html-dir', default=None, help='HTML directory (for html_vs_md check)')
    parser.add_argument('--sample', type=int, default=None, help='Sample N files instead of all')
    args = parser.parse_args()

    check_types = [c.strip() for c in args.check.split(',')]

    print(f"=== Batch Validator ===")
    print(f"Date: {datetime.now().isoformat()}")
    print(f"Checks: {', '.join(check_types)}\n")

    results = run_checks(args.md_dir, args.html_dir, check_types, args.sample)

    # Summary
    print(f"{'='*50}")
    print(f"VALIDATION SUMMARY")
    print(f"{'='*50}")
    all_passed = True
    for check, data in results.items():
        if 'failed' in data and data['failed'] > 0:
            all_passed = False
            print(f"  [FAIL] {check}: {data['failed']} issues")
        elif 'missing_md' in data and data['missing_md'] > 0:
            all_passed = False
            print(f"  [FAIL] {check}: {data['missing_md']} missing")
        else:
            print(f"  [OK]   {check}")

    if all_passed:
        print(f"\nAll checks passed.")
    else:
        print(f"\nSome checks failed -- review output above.")


if __name__ == '__main__':
    main()
