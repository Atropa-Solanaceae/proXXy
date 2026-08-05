import asyncio
import json
import logging
import os
import re
from functools import lru_cache
from typing import Dict, Iterable, List, Set, Tuple

import aiohttp
from tqdm import tqdm

# Each octet is bounded to 0-255 and the port to 1-65535, so junk like
# 999.999.999.999:99999 is no longer accepted as a valid proxy.
_OCTET = r"(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)"
_PORT = r"(?:6553[0-5]|655[0-2]\d|65[0-4]\d{2}|6[0-4]\d{3}|[1-5]\d{4}|[1-9]\d{0,3})"
PROXY_PATTERN = re.compile(rf"\b{_OCTET}(?:\.{_OCTET}){{3}}:{_PORT}\b")


def load_proxy_sources(file_path: str | None = None) -> Dict[str, List[str]]:
    """Load proxy source URLs grouped by protocol from disk.

    Defaults to the bundled proxy_sources.json next to this file, not the
    caller's current working directory -- running proXXy.py from outside the
    repo used to fail with FileNotFoundError.
    """
    if file_path is None:
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "proxy_sources.json")
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)


@lru_cache(maxsize=1)
def proxy_sources() -> Dict[str, List[str]]:
    """Cached read of proxy sources; they rarely change during one run."""
    return load_proxy_sources()


def parse_proxies(blob: str) -> List[str]:
    """Extract host:port pairs from raw text."""
    return PROXY_PATTERN.findall(blob)


def dedupe_preserve_order(items: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def read_proxy_file(path: str) -> List[str]:
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8") as file:
        return [line.strip() for line in file if line.strip()]


def write_proxy_file(path: str, proxies: Iterable[str]):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as file:
        for proxy in proxies:
            file.write(f"{proxy}\n")


async def _fetch_source(session: aiohttp.ClientSession, url: str, timeout: int, retries: int, backoff: float) -> str:
    """Fetch a proxy source with retries; returns raw body or empty string."""
    for attempt in range(retries + 1):
        try:
            async with session.get(url, timeout=timeout) as resp:
                resp.raise_for_status()
                return await resp.text(errors="ignore")
        except Exception as exc:  # noqa: BLE001 (broad to log and continue)
            logging.warning("Source fetch failed (%s/%s): %s", attempt + 1, retries + 1, url)
            if attempt == retries:
                logging.error("Giving up on %s: %s", url, exc)
                return ""
            await asyncio.sleep(backoff * (attempt + 1))
    return ""


async def scrape_sources_async(
    sources: Dict[str, List[str]],
    *,
    concurrency: int = 100,
    timeout: int = 8,
    retries: int = 2,
    backoff: float = 0.5,
) -> Dict[str, List[str]]:
    """Fetch all sources concurrently and return proxies grouped by protocol."""
    connector = aiohttp.TCPConnector(limit=concurrency)
    timeout_cfg = aiohttp.ClientTimeout(total=timeout)
    sem = asyncio.Semaphore(concurrency)
    results: Dict[str, List[str]] = {key: [] for key in sources.keys()}

    async with aiohttp.ClientSession(connector=connector, timeout=timeout_cfg) as session:
        tasks = []
        for protocol, urls in sources.items():
            for url in urls:
                async def task(proto=protocol, src=url):
                    async with sem:
                        body = await _fetch_source(session, src, timeout, retries, backoff)
                        if not body:
                            return proto, []
                        return proto, parse_proxies(body)
                tasks.append(task())

        for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Scraping sources", unit="src", ascii=True):
            protocol, proxies = await coro
            results.setdefault(protocol, []).extend(proxies)

    for protocol, proxies in results.items():
        results[protocol] = dedupe_preserve_order(proxies)
    return results


async def _check_proxies_async(
    proxies: List[str],
    *,
    test_url: str,
    proxy_scheme: str,
    label: str,
    concurrency: int = 200,
    timeout: int = 5,
    limit: int | None = None,
) -> List[str]:
    """Test proxies with a fixed pool of workers pulling from a shared index.

    The previous implementation created one asyncio.Task per proxy up front,
    which meant materialising ~500,000 Task objects at once for a full
    HTTP.txt run -- enough to exhaust memory on modest machines. A bounded
    pool keeps memory flat regardless of list size.
    """
    connector = aiohttp.TCPConnector(limit=0)
    timeout_cfg = aiohttp.ClientTimeout(total=timeout)
    valid: List[str] = []
    total = len(proxies)
    next_index = 0
    lock = asyncio.Lock()
    stop = asyncio.Event()

    async with aiohttp.ClientSession(connector=connector, timeout=timeout_cfg) as session:
        async def check(proxy: str) -> bool:
            proxy_url = f"{proxy_scheme}://{proxy}"
            try:
                async with session.get(test_url, proxy=proxy_url) as resp:
                    return 100 <= resp.status < 400
            except Exception:
                return False

        progress = tqdm(total=total, desc=f"Validating {label}", unit="prox", ascii=True)

        async def worker():
            nonlocal next_index
            while not stop.is_set():
                async with lock:
                    if next_index >= total:
                        return
                    proxy = proxies[next_index]
                    next_index += 1

                ok = await check(proxy)

                async with lock:
                    if ok:
                        valid.append(proxy)
                    progress.update(1)
                    if limit and len(valid) >= limit:
                        stop.set()

        workers = [asyncio.create_task(worker()) for _ in range(min(concurrency, total or 1))]
        await asyncio.gather(*workers, return_exceptions=True)
        progress.close()

    return valid[:limit] if limit else valid


def validate_proxy_file(
    file_path: str,
    *,
    test_url: str,
    proxy_scheme: str,
    label: str,
    output_path: str | None = None,
    concurrency: int = 200,
    timeout: int = 5,
    limit: int | None = None,
):
    """Test proxies from ``file_path`` and write the working ones elsewhere.

    ``output_path`` defaults to a sibling file under ``validated/`` rather
    than overwriting ``file_path`` in place: the old behaviour meant a small
    --val-limit permanently discarded the rest of a freshly scraped list,
    with no way to get it back short of re-scraping.
    """
    proxies = read_proxy_file(file_path)
    if not proxies:
        print(f"No proxies found in {file_path}")
        return

    if output_path is None:
        output_path = os.path.join("validated", os.path.basename(file_path))

    import time
    start = time.perf_counter()
    valid = asyncio.run(
        _check_proxies_async(
            proxies,
            test_url=test_url,
            proxy_scheme=proxy_scheme,
            label=label,
            concurrency=concurrency,
            timeout=timeout,
            limit=limit,
        )
    )
    elapsed = time.perf_counter() - start

    print(f"[*] Valid {label} proxies: {len(valid):,} of {len(proxies):,} tested")
    print(f"[*] Validation time: {elapsed:.2f}s")

    write_proxy_file(output_path, dedupe_preserve_order(valid))


def http_check(file_path: str, *, concurrency: int = 400, timeout: int = 3, limit: int | None = None):
    validate_proxy_file(
        file_path,
        test_url="http://httpbin.org/ip",
        proxy_scheme="http",
        label="HTTP",
        concurrency=concurrency,
        timeout=timeout,
        limit=limit,
    )


def https_check(file_path: str, *, concurrency: int = 400, timeout: int = 3, limit: int | None = None):
    # proxy_scheme stays "http": that is the scheme used to *reach* the proxy
    # itself, independent of the https:// target it is asked to relay. The
    # previous code reused this value as the printed label too, so a run
    # against HTTPS.txt announced "Valid HTTP proxies" -- label is now separate.
    validate_proxy_file(
        file_path,
        test_url="https://api.myip.com/",
        proxy_scheme="http",
        label="HTTPS",
        concurrency=concurrency,
        timeout=timeout,
        limit=limit,
    )
