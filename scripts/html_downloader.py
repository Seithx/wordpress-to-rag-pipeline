#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Threaded HTML Downloader with Resume, Robots.txt, and Manifest Tracking
========================================================================
Downloads HTML pages from URL lists with:
  - Concurrent downloads (configurable workers, default 10)
  - robots.txt compliance
  - Retry with exponential backoff (429, 5xx)
  - Content-type validation (skip non-HTML)
  - Size limits (configurable, default 10MB)
  - Resume from checkpoint
  - Per-category directory organization
  - Comprehensive error categorization and reporting
  - Metadata manifest (URL->file mapping, timestamps, ETags)

Usage:
  py html_downloader.py --urls urls.json --output html_raw/
  py html_downloader.py --urls urls.json --output html_raw/ --workers 5 --delay 1.0
"""

import argparse
import hashlib
import json
import os
import sys
import time
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib import robotparser
from urllib.parse import urlparse, unquote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')


class HTMLDownloader:
    """Thread-safe bulk HTML downloader with manifest tracking and resume."""

    def __init__(self, output_dir='downloaded_html', workers=10, delay=0.5,
                 respect_robots=True, max_size_mb=10, timeout=30):
        self.output_dir = output_dir
        self.workers = workers
        self.delay = delay
        self.respect_robots = respect_robots
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.timeout = timeout

        # Thread-safe counters
        self.lock = threading.Lock()
        self.successful = 0
        self.failed = 0
        self.skipped = 0

        # Metadata tracking
        self.manifest = {}
        self.url_to_file = {}
        self.errors = []
        self.checkpoint_data = {}

        # Error categorization
        self.error_categories = {
            'network_error': [],
            'timeout': [],
            'http_error': [],
            'too_large': [],
            'non_html': [],
            'robots_blocked': [],
            'encoding_error': [],
            'parse_error': [],
            'other': []
        }

        # Per-thread session storage
        self._tls = threading.local()

        # Robots.txt cache
        self.robot_cache = {}

        os.makedirs(os.path.join(self.output_dir, 'metadata'), exist_ok=True)
        self.load_manifest()
        self.load_checkpoint()

    def _make_session(self):
        """Create a session with retry logic for each thread."""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'ContentArchiver/3.0 (Educational archive project)'
        })
        retry_strategy = Retry(
            total=5,
            backoff_factor=0.5,  # 0.5, 1, 2, 4, 8 seconds
            status_forcelist=[429, 500, 502, 503, 504],
            respect_retry_after_header=True,
            allowed_methods=["GET", "HEAD"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_maxsize=100)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _get_session(self):
        """Get or create thread-local session."""
        if not hasattr(self._tls, "session"):
            self._tls.session = self._make_session()
        return self._tls.session

    def _robots_allowed(self, url):
        """Check if URL is allowed by robots.txt."""
        if not self.respect_robots:
            return True
        try:
            parsed = urlparse(url)
            host = parsed.netloc
            if host not in self.robot_cache:
                rp = robotparser.RobotFileParser()
                rp.set_url(f"{parsed.scheme}://{host}/robots.txt")
                try:
                    rp.read()
                    self.robot_cache[host] = rp
                except Exception:
                    return True
            return self.robot_cache[host].can_fetch("*", url)
        except Exception:
            return True

    def load_manifest(self):
        """Load previous manifest for resume support."""
        manifest_file = os.path.join(self.output_dir, 'metadata', 'download_manifest.json')
        if os.path.exists(manifest_file):
            try:
                with open(manifest_file, 'r', encoding='utf-8') as f:
                    self.manifest = json.load(f)
                    for filename, meta in self.manifest.items():
                        self.url_to_file[meta['url']] = filename
                print(f"Loaded previous manifest: {len(self.manifest)} files")
            except Exception as e:
                print(f"Could not load manifest: {e}")

    def load_checkpoint(self):
        """Load previous progress for resume."""
        checkpoint_file = os.path.join(self.output_dir, 'metadata', 'checkpoint.json')
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, 'r', encoding='utf-8') as f:
                    self.checkpoint_data = json.load(f)
                print(f"Loaded checkpoint: {len(self.checkpoint_data.get('completed', []))} URLs done")
            except Exception:
                self.checkpoint_data = {}

    def save_checkpoint(self):
        """Save current progress."""
        checkpoint_file = os.path.join(self.output_dir, 'metadata', 'checkpoint.json')
        checkpoint = {
            'last_updated': datetime.now().isoformat(),
            'completed': list(self.url_to_file.keys()),
            'successful': self.successful,
            'failed': self.failed,
            'skipped': self.skipped,
        }
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)

    def log_error(self, url, category, error_type, error_message, http_status=None):
        """Centralized error logging with categorization."""
        error_info = {
            'url': url,
            'category': category,
            'error_type': error_type,
            'error_message': error_message,
            'timestamp': datetime.now().isoformat(),
        }
        if http_status:
            error_info['http_status'] = http_status
        with self.lock:
            self.errors.append(error_info)
            bucket = self.error_categories.get(error_type, self.error_categories['other'])
            bucket.append(error_info)

    def download_single_url(self, url_data):
        """Download a single URL and save with metadata."""
        url = url_data['url']
        category = url_data.get('category', 'default')
        index = url_data['index']

        # Skip if already done
        if url in self.checkpoint_data.get('completed', []):
            with self.lock:
                self.skipped += 1
            return {'status': 'skipped', 'url': url, 'reason': 'already_completed'}

        # Category directory
        cat_dir = os.path.join(self.output_dir, category)
        os.makedirs(cat_dir, exist_ok=True)
        filename = f"{category}/{index:05d}.html"
        filepath = os.path.join(self.output_dir, filename)

        if os.path.exists(filepath):
            with self.lock:
                self.skipped += 1
            return {'status': 'skipped', 'url': url, 'reason': 'file_exists'}

        # robots.txt check
        if not self._robots_allowed(url):
            self.log_error(url, category, 'robots_blocked', 'Blocked by robots.txt')
            with self.lock:
                self.failed += 1
            return {'status': 'failed', 'url': url, 'error': 'robots_blocked'}

        try:
            session = self._get_session()
            response = session.get(url, timeout=self.timeout, stream=True)
            response.raise_for_status()

            # Validate content type
            content_type = response.headers.get('Content-Type', '').lower()
            if 'text/html' not in content_type:
                self.log_error(url, category, 'non_html',
                               f'Content-Type: {content_type}', response.status_code)
                with self.lock:
                    self.skipped += 1
                return {'status': 'skipped', 'url': url, 'reason': f'non_html ({content_type})'}

            # Stream with size limit
            data = b""
            for chunk in response.iter_content(chunk_size=65536):
                data += chunk
                if len(data) > self.max_size_bytes:
                    self.log_error(url, category, 'too_large',
                                   f'Exceeds {self.max_size_bytes // (1024*1024)}MB',
                                   response.status_code)
                    with self.lock:
                        self.failed += 1
                    return {'status': 'failed', 'url': url, 'error': 'too_large'}

            # Decode
            try:
                encoding = response.encoding or response.apparent_encoding or 'utf-8'
                html_content = data.decode(encoding, errors='replace')
            except Exception as e:
                self.log_error(url, category, 'encoding_error', str(e), response.status_code)
                with self.lock:
                    self.failed += 1
                return {'status': 'failed', 'url': url, 'error': f'encoding: {e}'}

            # Extract title for manifest
            from bs4 import BeautifulSoup
            try:
                soup = BeautifulSoup(html_content, 'html.parser')
                title_tag = soup.find('title')
                title = title_tag.get_text().strip() if title_tag else ''
            except Exception:
                title = ''

            etag = response.headers.get('ETag', '')
            last_modified = response.headers.get('Last-Modified', '')

            # Embed download metadata as HTML comment
            metadata_comment = (
                f"<!--\nOriginal URL: {url}\nCategory: {category}\n"
                f"Downloaded: {datetime.now().isoformat()}\n"
                f"Status Code: {response.status_code}\n"
                f"Content-Length: {len(html_content)}\nEncoding: {encoding}\n"
                f"ETag: {etag}\nLast-Modified: {last_modified}\nTitle: {title}\n-->\n"
            )

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(metadata_comment + html_content)

            # Update manifest
            metadata = {
                'url': url, 'category': category, 'index': index,
                'timestamp': datetime.now().isoformat(),
                'http_status': response.status_code,
                'content_length': len(html_content), 'encoding': encoding,
                'etag': etag, 'last_modified': last_modified,
                'title': title, 'filename': filename,
            }
            with self.lock:
                self.manifest[filename] = metadata
                self.url_to_file[url] = filename
                self.successful += 1
                if self.successful % 100 == 0:
                    self.save_checkpoint()
                    self.save_manifest()

            return {'status': 'success', 'url': url, 'filename': filename}

        except requests.exceptions.Timeout as e:
            self.log_error(url, category, 'timeout', str(e))
            with self.lock:
                self.failed += 1
            return {'status': 'failed', 'url': url, 'error': f'timeout: {e}'}
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if hasattr(e, 'response') else 'unknown'
            self.log_error(url, category, 'http_error', str(e), code)
            with self.lock:
                self.failed += 1
            return {'status': 'failed', 'url': url, 'error': f'http_{code}'}
        except requests.exceptions.RequestException as e:
            self.log_error(url, category, 'network_error', str(e))
            with self.lock:
                self.failed += 1
            return {'status': 'failed', 'url': url, 'error': f'network: {e}'}
        except Exception as e:
            self.log_error(url, category, 'other', str(e))
            with self.lock:
                self.failed += 1
            return {'status': 'failed', 'url': url, 'error': f'unexpected: {e}'}

    def save_manifest(self):
        """Save manifest, URL mapping, and error reports."""
        meta_dir = os.path.join(self.output_dir, 'metadata')

        with open(os.path.join(meta_dir, 'download_manifest.json'), 'w', encoding='utf-8') as f:
            json.dump(self.manifest, f, ensure_ascii=False, indent=2)

        with open(os.path.join(meta_dir, 'url_to_file.json'), 'w', encoding='utf-8') as f:
            json.dump(self.url_to_file, f, ensure_ascii=False, indent=2)

        if self.errors:
            with open(os.path.join(meta_dir, 'errors.json'), 'w', encoding='utf-8') as f:
                json.dump(self.errors, f, ensure_ascii=False, indent=2)

        error_summary = {
            'total_errors': len(self.errors),
            'by_type': {k: len(v) for k, v in self.error_categories.items() if v},
        }
        with open(os.path.join(meta_dir, 'error_summary.json'), 'w', encoding='utf-8') as f:
            json.dump(error_summary, f, ensure_ascii=False, indent=2)

    def download_all(self, url_list_by_category):
        """Download all URLs using thread pool."""
        url_tasks = []
        for category, urls in url_list_by_category.items():
            for idx, url in enumerate(urls):
                url_tasks.append({'url': url, 'category': category, 'index': idx})

        total = len(url_tasks)
        print(f"\nDownloading {total:,} URLs | {self.workers} workers | "
              f"{self.delay}s delay | robots.txt: {self.respect_robots}")

        start_time = time.time()
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {executor.submit(self.download_single_url, t): t for t in url_tasks}
            completed = 0
            for future in as_completed(futures):
                future.result()
                completed += 1
                if completed % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    eta = (total - completed) / rate if rate > 0 else 0
                    print(f"  [{completed:,}/{total:,}] "
                          f"OK:{self.successful} FAIL:{self.failed} SKIP:{self.skipped} "
                          f"| {rate:.1f}/s | ETA:{eta/60:.0f}m")
                time.sleep(self.delay)

        self.save_manifest()
        self.save_checkpoint()

        elapsed = time.time() - start_time
        print(f"\n{'='*50}")
        print(f"DOWNLOAD COMPLETE ({elapsed/60:.1f} min)")
        print(f"{'='*50}")
        print(f"OK: {self.successful:,} | FAIL: {self.failed:,} | SKIP: {self.skipped:,}")
        if self.errors:
            print(f"\nError breakdown:")
            for etype, elist in self.error_categories.items():
                if elist:
                    print(f"  {etype}: {len(elist)}")

        return {
            'total': total, 'successful': self.successful,
            'failed': self.failed, 'skipped': self.skipped,
            'elapsed_minutes': elapsed / 60,
        }


def main():
    parser = argparse.ArgumentParser(description='Threaded HTML downloader with manifest')
    parser.add_argument('--urls', required=True, help='JSON file: {"category": ["url1", ...]}')
    parser.add_argument('--output', default='downloaded_html', help='Output directory')
    parser.add_argument('--workers', type=int, default=10, help='Concurrent workers')
    parser.add_argument('--delay', type=float, default=0.5, help='Delay between requests (seconds)')
    parser.add_argument('--timeout', type=int, default=30, help='Request timeout (seconds)')
    parser.add_argument('--max-size', type=int, default=10, help='Max file size (MB)')
    parser.add_argument('--no-robots', action='store_true', help='Skip robots.txt check')
    args = parser.parse_args()

    with open(args.urls, 'r', encoding='utf-8') as f:
        url_by_category = json.load(f)

    total = sum(len(v) for v in url_by_category.values())
    print(f"Loaded {total:,} URLs across {len(url_by_category)} categories")

    downloader = HTMLDownloader(
        output_dir=args.output,
        workers=args.workers,
        delay=args.delay,
        respect_robots=not args.no_robots,
        max_size_mb=args.max_size,
        timeout=args.timeout,
    )
    downloader.download_all(url_by_category)


if __name__ == "__main__":
    main()
