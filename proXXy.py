#!/usr/bin/python3
# Built by Solanaceae -- https://solanaceae.xyz/
import argparse
import asyncio
import json
import logging
import os
import platform
import re
import shutil
import subprocess
import time
from typing import Dict, List

import pystyle
import utils
from urllib import error as urllib_error
from urllib import request as urllib_request
from yaspin import yaspin

import src_check

SCRIPT_VERSION = 'v2.7'

def banner(script_version):
    banner = r"""
                     _  ___  __     
    ____  _________ | |/ / |/ /_  __
   / __ \/ ___/ __ \|   /|   / / / /
  / /_/ / /  / /_/ /   |/   / /_/ / 
 / .___/_/   \____/_/|_/_/|_\__, /  
/_/                        /____/   """

    clr_cmd = "cls" if os.name == "nt" else "clear"
    if os.name == "nt":
        os.system(f"title proXXy -- by Solanaceae && {clr_cmd}")
    elif 'TERM' in os.environ:
        os.system(clr_cmd)
    text = f"{script_version}\nby Solanaceae\nhttps://solanaceae.xyz/"
    print(pystyle.Add.Add(banner, text, 4))
    print()
    print(vanity_line())

def vanity_line():
    try:
        terminal_width = shutil.get_terminal_size().columns
    except OSError:
        terminal_width = 80
    dash = "—"
    dashes = dash * (terminal_width - 2)
    return f"<{dashes}>"

def init_logging():
    log_dir = 'output'
    log_file = os.path.join(log_dir, 'error.log')
    os.makedirs(log_dir, exist_ok=True)

    try:
        logging.basicConfig(filename=log_file, level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
        # error.log is warnings/errors only (see the level above), so a
        # logging.info() call here could never actually be written -- it was
        # dead code that misleadingly suggested a startup line gets logged.
    except Exception as e:
        print(f"Failed to initialize logging: {e}")

def init_spinner(script_version: str = SCRIPT_VERSION):
    banner(script_version)
    with yaspin().bouncingBar as sp:
        sp.text = "Initializing..."
        time.sleep(1.5)
        sp.ok("[_OK_]")
        time.sleep(0.5)


def run_update_script(script_version):
    current_os = platform.system()
    update_folder = os.path.join(os.path.dirname(__file__), 'update')
    env = os.environ.copy()
    env['SCRIPT_VERSION'] = script_version

    try:
        if current_os in ['Linux', 'Darwin']:
            update_script = os.path.join(update_folder, 'update.sh')
            subprocess.run(['chmod', '+x', update_script], check=True)
            subprocess.run([update_script], env=env, check=True)
        elif current_os == 'Windows':
            update_script = os.path.join(update_folder, 'update.ps1')
            command = ['powershell', '-ExecutionPolicy', 'Bypass', '-File', update_script]
            subprocess.run(command, env=env, check=True)
        else:
            logging.error(f"Unsupported OS: {current_os}")
            print('[-] Unsupported operating system.')
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to run update script: {e}")
        print("[-] Update failed. Check logs for details.")

def check_for_update(script_version):
    banner(script_version)
    api_url = "https://api.github.com/repos/0xSolanaceae/proXXy/releases/latest"

    try:
        with urllib_request.urlopen(api_url, timeout=5) as resp:
            if resp.status == 200:
                release_info = json.loads(resp.read().decode('utf-8'))
                return update_notif(release_info, script_version)
            print(f"[-] Failed to check for updates. HTTP Status Code: {resp.status}")
            return False
    except (urllib_error.URLError, urllib_error.HTTPError) as e:
        print(f"[-] Error checking for updates: {e}")
        return False


def _version_tuple(version: str):
    """'v2.10.1' -> (2, 10, 1), so releases compare numerically, not lexically."""
    return tuple(int(part) for part in re.findall(r'\d+', version or '')) or (0,)


def update_notif(release_info, script_version):
    latest_version = release_info.get('tag_name', script_version)

    # A plain string comparison sorted "v2.7" above "v2.10" (since "7" > "1"
    # lexically), which meant this notice silently stopped firing the moment
    # the project reached a two-digit minor version.
    if _version_tuple(latest_version) <= _version_tuple(script_version):
        return False
    print(f"[+] A new version is available: {latest_version}. You are running {script_version}.")
    print("[+] Please update your script to the latest version by using the -u flag.")
    return True

def scrape_and_write(
    sources: Dict[str, List[str]],
    *,
    output_dir: str = "output",
    concurrency: int = 120,
    timeout: int = 8,
    retries: int = 2,
    backoff: float = 0.5,
) -> Dict[str, List[str]]:
    os.makedirs(output_dir, exist_ok=True)
    proxies_by_protocol = asyncio.run(
        utils.scrape_sources_async(
            sources,
            concurrency=concurrency,
            timeout=timeout,
            retries=retries,
            backoff=backoff,
        )
    )

    for protocol, proxies in proxies_by_protocol.items():
        filepath = os.path.join(output_dir, f"{protocol}.txt")
        utils.write_proxy_file(filepath, proxies)

    return proxies_by_protocol


def summarize(proxies_by_protocol: Dict[str, List[str]]):
    total = sum(len(v) for v in proxies_by_protocol.values())
    for proto, proxies in proxies_by_protocol.items():
        print(f"[+] {proto}: {len(proxies):,} proxies")
    print(f"\n[+] {total:,} total proxies scraped.")
    print(vanity_line())


def main():
    script_version = SCRIPT_VERSION
    parser = argparse.ArgumentParser(description='Fast async proxy scraper with optional validation')
    parser.add_argument('--validate', '-V', action='store_true', help='Validate HTTP/HTTPS proxies after scraping')
    parser.add_argument('--update', '-u', action='store_true', help='Run the update script and exit')
    parser.add_argument('--version', '-v', action='version', version=f'%(prog)s {script_version}', help='Print the script version and exit')
    parser.add_argument('--src_check', '-s', action='store_true', help='Verify sources only and exit')
    parser.add_argument('--concurrency', '-c', type=int, default=120, help='Concurrent source fetches (default: 120)')
    parser.add_argument('--timeout', '-t', type=int, default=8, help='Per-source timeout seconds (default: 8)')
    parser.add_argument('--val-concurrency', type=int, default=400, help='Validation concurrency (default: 400)')
    parser.add_argument('--val-timeout', type=int, default=3, help='Validation timeout seconds (default: 3)')
    parser.add_argument('--val-limit', type=int, default=0, help='Stop validation after N valid proxies (0 = no limit)')
    args = parser.parse_args()

    if args.update:
        run_update_script(script_version)
        return

    if check_for_update(script_version):
        time.sleep(1.5)

    if args.src_check:
        src_check.main()
        return

    init_logging()
    init_spinner(script_version)
    banner(script_version)

    sources = utils.proxy_sources()

    proxies_by_protocol = scrape_and_write(
        sources,
        concurrency=max(10, args.concurrency),
        timeout=max(3, args.timeout),
    )

    summarize(proxies_by_protocol)

    if args.validate:
        limit = args.val_limit if args.val_limit > 0 else None
        utils.http_check(
            os.path.join('output', 'HTTP.txt'),
            concurrency=max(10, args.val_concurrency),
            timeout=max(1, args.val_timeout),
            limit=limit,
        )
        utils.https_check(
            os.path.join('output', 'HTTPS.txt'),
            concurrency=max(10, args.val_concurrency),
            timeout=max(1, args.val_timeout),
            limit=limit,
        )
        print(vanity_line())


if __name__ == "__main__":
    main()
