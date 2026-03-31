#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RAG Readiness Assessment
========================
Scores converted .md files for RAG suitability:
  - Structure density (headings per 1K words)
  - Paragraph length distribution (too short = fragments, too long = walls)
  - Garbled character detection (OCR artifacts, encoding issues)
  - Chunk-friendliness (can the file be cleanly split at headings?)
  - Frontmatter completeness

Output: per-file scores + aggregate report.

Usage:
  py assess_rag_readiness.py --input md_output/
  py assess_rag_readiness.py --input md_output/ --sample 50 --report readiness.json
"""

import argparse
import json
import os
import re
import sys
import random
from datetime import datetime

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Garbled character patterns (common OCR / encoding artifacts)
GARBLED_PATTERNS = [
    re.compile(r'[\ufffd\ufffe\uffff]'),           # Unicode replacement chars
    re.compile(r'[^\x00-\x7f\u0590-\u05ff\u0600-\u06ff\u00c0-\u024f\s\d\p{P}]{3,}'),  # long non-text runs
    re.compile(r'(?:[A-Z]{2,}\d{2,}){2,}'),         # encoded garbage like AB12CD34
]


def assess_file(md_path):
    """Score a single .md file for RAG readiness. Returns dict of metrics."""
    with open(md_path, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()

    words = content.split()
    word_count = len(words)

    if word_count < 10:
        return {'file': os.path.basename(md_path), 'score': 0, 'reason': 'too_short',
                'word_count': word_count}

    # --- Frontmatter check ---
    has_frontmatter = content.startswith('---') and content.find('---', 3) > 0
    fm_score = 1.0 if has_frontmatter else 0.0

    # --- Structure density (headings per 1K words) ---
    headings = re.findall(r'^#{1,6}\s+.+', content, re.MULTILINE)
    heading_density = (len(headings) / word_count) * 1000 if word_count else 0
    # Ideal: 2-10 headings per 1K words
    if 2 <= heading_density <= 10:
        structure_score = 1.0
    elif heading_density > 0:
        structure_score = 0.5
    else:
        structure_score = 0.2

    # --- Paragraph quality ---
    paragraphs = [p.strip() for p in content.split('\n\n') if p.strip() and not p.strip().startswith('#')]
    para_lengths = [len(p.split()) for p in paragraphs]

    if para_lengths:
        avg_para = sum(para_lengths) / len(para_lengths)
        # Ideal: 30-200 words per paragraph
        tiny_paras = sum(1 for l in para_lengths if l < 5) / len(para_lengths)
        wall_paras = sum(1 for l in para_lengths if l > 500) / len(para_lengths)
        para_score = max(0, 1.0 - tiny_paras - wall_paras)
    else:
        para_score = 0.0
        avg_para = 0

    # --- Garbled character detection ---
    garbled_count = 0
    for pattern in GARBLED_PATTERNS:
        garbled_count += len(pattern.findall(content))
    garbled_ratio = garbled_count / word_count if word_count else 0
    garbled_score = max(0, 1.0 - garbled_ratio * 100)  # penalize heavily

    # --- Chunk-friendliness ---
    # Can we split at headings into chunks of 200-2000 words?
    if len(headings) >= 2:
        chunk_score = 1.0
    elif word_count < 2000:
        chunk_score = 0.8  # small file, one chunk is fine
    else:
        chunk_score = 0.3  # large file with no headings = bad for chunking

    # --- Composite score ---
    composite = (
        fm_score * 0.15 +
        structure_score * 0.25 +
        para_score * 0.20 +
        garbled_score * 0.20 +
        chunk_score * 0.20
    )

    return {
        'file': os.path.basename(md_path),
        'score': round(composite, 3),
        'word_count': word_count,
        'headings': len(headings),
        'heading_density_per_1k': round(heading_density, 2),
        'paragraphs': len(para_lengths),
        'avg_para_words': round(avg_para, 1),
        'garbled_chars': garbled_count,
        'has_frontmatter': has_frontmatter,
        'scores': {
            'frontmatter': round(fm_score, 2),
            'structure': round(structure_score, 2),
            'paragraphs': round(para_score, 2),
            'encoding': round(garbled_score, 2),
            'chunkability': round(chunk_score, 2),
        },
    }


def main():
    parser = argparse.ArgumentParser(description='Assess RAG readiness of .md files')
    parser.add_argument('--input', required=True, help='Directory of .md files')
    parser.add_argument('--sample', type=int, default=None, help='Sample N files')
    parser.add_argument('--report', default='rag_readiness_report.json', help='Output report')
    parser.add_argument('--threshold', type=float, default=0.6, help='Min score to pass')
    args = parser.parse_args()

    print(f"=== RAG Readiness Assessment ===")
    print(f"Date: {datetime.now().isoformat()}")

    md_files = [
        os.path.join(args.input, f)
        for f in os.listdir(args.input) if f.endswith('.md')
    ]

    if args.sample and args.sample < len(md_files):
        md_files = random.sample(md_files, args.sample)
        print(f"Sampling {args.sample} files")

    print(f"Assessing {len(md_files)} files...\n")

    results = [assess_file(f) for f in md_files]
    results.sort(key=lambda r: r['score'])

    # Aggregate
    scores = [r['score'] for r in results]
    passing = [r for r in results if r['score'] >= args.threshold]
    failing = [r for r in results if r['score'] < args.threshold]

    report = {
        'generated': datetime.now().isoformat(),
        'total_files': len(results),
        'threshold': args.threshold,
        'passing': len(passing),
        'failing': len(failing),
        'pass_rate': f"{len(passing)/len(results)*100:.1f}%" if results else 'N/A',
        'score_distribution': {
            'min': round(min(scores), 3) if scores else 0,
            'p25': round(scores[len(scores)//4], 3) if scores else 0,
            'median': round(scores[len(scores)//2], 3) if scores else 0,
            'p75': round(scores[int(len(scores)*0.75)], 3) if scores else 0,
            'max': round(max(scores), 3) if scores else 0,
        },
        'worst_files': failing[:20],
        'best_files': results[-5:] if results else [],
    }

    with open(args.report, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"{'='*50}")
    print(f"RAG READINESS REPORT")
    print(f"{'='*50}")
    print(f"Files assessed: {len(results)}")
    print(f"Passing (>={args.threshold}): {len(passing)} ({report['pass_rate']})")
    print(f"Failing: {len(failing)}")
    print(f"Score range: {report['score_distribution']['min']} - {report['score_distribution']['max']}")
    print(f"Median score: {report['score_distribution']['median']}")

    if failing:
        print(f"\nWorst files:")
        for r in failing[:5]:
            print(f"  {r['score']:.3f}  {r['file']}")

    print(f"\nFull report: {args.report}")


if __name__ == '__main__':
    main()
