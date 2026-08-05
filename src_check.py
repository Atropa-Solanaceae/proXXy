import aiohttp
import asyncio
import json
import os
from rich.console import Console
from rich.table import Table
from tqdm import tqdm
import time

async def fetch_url(session, url):
    try:
        async with session.get(url, timeout=5) as response:
            response.raise_for_status()
            content = await response.read()
            text = content.decode('utf-8', errors='ignore')
            proxies = text.splitlines()
            return url, len(proxies)
    except Exception as e:
        print(f"[-] Error accessing {url}: {e}")
        return url, 0

async def verify_proxy_sources(sources):
    results = []
    total_urls = sum(len(urls) for urls in sources.values())

    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        tasks = []
        for protocol, urls in sources.items():
            tasks.extend(fetch_url(session, url) for url in urls)
        with tqdm(total=total_urls, ascii=" #", desc="Checking Proxy Counts") as pbar:
            for future in asyncio.as_completed(tasks):
                url, count = await future
                results.append((url, count))
                pbar.update(1)

    end_time = time.time()
    execution_time_ms = (end_time - start_time) * 1000

    results.sort(key=lambda x: x[1], reverse=True)

    console = Console()
    table = Table(title="# of Returned Proxies")

    table.add_column("URL", justify="left", style="cyan", no_wrap=True)
    table.add_column("Proxy Count", justify="right", style="magenta", width=10)

    for url, count in results:
        table.add_row(url, str(count))

    console.print(table)
    console.print(f"Execution Time: {execution_time_ms:.2f} ms")

def main():
    # Resolve relative to this file, not the caller's working directory --
    # `python /path/to/proXXy.py --src_check` from elsewhere used to raise
    # FileNotFoundError instead of finding the bundled source list.
    sources_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'proxy_sources.json')
    with open(sources_path, 'r') as file:
        proxy_sources = json.load(file)
    asyncio.run(verify_proxy_sources(proxy_sources))

if __name__ == "__main__":
    main()