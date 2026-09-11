#!/usr/bin/env python3
"""
download_opentargets.py

Bulk-download every Open Targets Platform dataset for a release, in parallel,
over the EBI HTTPS mirror. Cross-platform (works natively on Windows).

Each dataset lands in its own subfolder:
    <dest>/<dataset>/part-*.parquet

Resumable: re-run and files already fully downloaded are skipped.

Requires:  requests   (pip install requests  /  conda install requests)

Examples
--------
    # all 56 datasets from release 26.03 into ./opentargets-26.03
    python download_opentargets.py

    # a subset
    python download_opentargets.py --datasets target disease association_by_overall_direct

    # different release / destination / more parallelism
    python download_opentargets.py --release 25.03 --dest D:/data/ot --workers 12
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin

import requests

HREF_RE = re.compile(r'href="([^"]+)"')


def list_dir(session: requests.Session, url: str, retries: int) -> list[str]:
    """Return the child entries (files and subdirs) of an Apache directory listing."""
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=60)
            resp.raise_for_status()
            break
        except requests.RequestException:
            if attempt == retries:
                raise
            time.sleep(5 * attempt)

    entries = []
    for href in HREF_RE.findall(resp.text):
        if href.startswith("?"):            # column sort links
            continue
        if href.startswith("/"):            # parent (absolute) link
            continue
        if href.startswith("http://") or href.startswith("https://"):
            continue                        # external link
        if href == "../":
            continue
        entries.append(href)
    return entries


def collect_files(session: requests.Session, base_url: str, retries: int) -> list[str]:
    """Recursively walk base_url and return the full URLs of every file under it."""
    files: list[str] = []
    stack = [base_url]
    while stack:
        current = stack.pop()
        for entry in list_dir(session, current, retries):
            child = urljoin(current, entry)
            if entry.endswith("/"):
                stack.append(child)
            else:
                files.append(child)
    return files


def local_path_for(file_url: str, base_url: str, dest: Path) -> Path:
    """Map a remote file URL to its local path, preserving the folder structure."""
    rel = file_url[len(base_url):] if file_url.startswith(base_url) else file_url
    return dest / rel


def download_one(session: requests.Session, url: str, out: Path, retries: int) -> tuple[str, str]:
    """Download a single file. Returns (status, url). status in {ok, skip, fail}."""
    if out.exists():
        return ("skip", url)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(out.suffix + ".part")
    for attempt in range(1, retries + 1):
        try:
            with session.get(url, stream=True, timeout=(30, 300)) as resp:
                resp.raise_for_status()
                with open(tmp, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1 << 20):  # 1 MiB
                        if chunk:
                            fh.write(chunk)
            tmp.replace(out)
            return ("ok", url)
        except (requests.RequestException, OSError):
            if tmp.exists():
                tmp.unlink(missing_ok=True)
            if attempt == retries:
                return ("fail", url)
            time.sleep(5 * attempt)
    return ("fail", url)


def main() -> int:
    p = argparse.ArgumentParser(description="Bulk-download Open Targets Platform datasets.")
    p.add_argument("--release", default="26.03", help="Release, e.g. 26.03 (default) or 25.03")
    p.add_argument("--dest", default=None, help="Download directory (default ./opentargets-<release>)")
    p.add_argument("--datasets", nargs="*", default=None,
                   help="Restrict to these dataset folder names (default: all)")
    p.add_argument("--workers", type=int, default=8, help="Parallel downloads (default 8)")
    p.add_argument("--retries", type=int, default=3, help="Retries per request (default 3)")
    args = p.parse_args()

    dest = Path(args.dest) if args.dest else Path(f"./opentargets-{args.release}")
    base_url = f"https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/{args.release}/output/"

    session = requests.Session()
    session.headers.update({"User-Agent": "opentargets-bulk-download/1.0"})

    print("=" * 58)
    print(" Open Targets bulk download")
    print(f"   release : {args.release}")
    print(f"   source  : {base_url}")
    print(f"   dest    : {dest}")
    print(f"   workers : {args.workers}")
    print("=" * 58)

    # Build the list of directories to crawl.
    if args.datasets:
        roots = [urljoin(base_url, ds.rstrip("/") + "/") for ds in args.datasets]
    else:
        roots = [base_url]

    print("Indexing remote files (this can take a moment)...")
    files: list[str] = []
    for root in roots:
        files.extend(collect_files(session, root, args.retries))
    total = len(files)
    print(f"Found {total} files to check.")
    if total == 0:
        print("Nothing to download - check the release number and dataset names.")
        return 1

    start = time.time()
    done = ok = skipped = failed = 0
    failures: list[str] = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(download_one, session, url,
                        local_path_for(url, base_url, dest), args.retries): url
            for url in files
        }
        for fut in as_completed(futures):
            status, url = fut.result()
            done += 1
            if status == "ok":
                ok += 1
            elif status == "skip":
                skipped += 1
            else:
                failed += 1
                failures.append(url)
            if done % 25 == 0 or done == total:
                print(f"  [{done}/{total}] ok={ok} skipped={skipped} failed={failed}")

    elapsed = time.time() - start
    n_folders = sum(1 for c in dest.iterdir() if c.is_dir()) if dest.exists() else 0

    print("=" * 58)
    print(f" Done in {int(elapsed // 60)}m {int(elapsed % 60)}s")
    print(f" files: {ok} downloaded, {skipped} already present, {failed} failed")
    print(f" dataset folders: {n_folders}")
    print(f" location: {dest.resolve()}")
    print("=" * 58)

    if failures:
        print("Failed URLs (re-run the script to retry just these):")
        for u in failures:
            print(f"  {u}")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
