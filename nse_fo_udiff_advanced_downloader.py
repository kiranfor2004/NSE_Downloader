#!/usr/bin/env python3
"""
🧠 Advanced NSE F&O UDiFF Downloader (Research / Experimental)

Goal:
  Attempt to fetch F&O UDiFF Common Bhavcopy (Final) files (ZIP/CSV) for a given date range
  using enhanced techniques: session priming, realistic headers, multiple URL patterns,
  and heuristic analysis of NSE behavior.

Current Status:
  Public direct archive URLs for 2025 F&O appear to return 404 for historical patterns.
  This script is a best-effort automated attempt. If it fails, use the fallback strategy
  (Selenium manual capture) described at bottom.
"""

import os
import time
import json
import math
from datetime import datetime, date, timedelta
import requests
from typing import List, Dict
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

HEADERS_BASE = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
}

# Known legacy & hypothesized patterns for F&O UDIFF / bhavcopy derivatives data.
URL_PATTERNS = [
    # 1. Classic derivatives daily bhavcopy zipped CSV
    "https://archives.nseindia.com/content/historical/DERIVATIVES/{YYYY}/{MON_ABBR}/fo{DD}{MON_ABBR}{YYYY}bhav.csv.zip",
    # 2. Alternative uppercase prefix (some years used FO_)
    "https://archives.nseindia.com/content/historical/DERIVATIVES/{YYYY}/{MON_ABBR}/FO_{DD}{MON_ABBR}{YYYY}.zip",
    # 3. UDIFF guessed patterns
    "https://archives.nseindia.com/content/historical/DERIVATIVES/{YYYY}/{MON_ABBR}/udiff{DD}{MON_ABBR}{YYYY}.zip",
    # 4. Product section (equities-like) hypothetical placements
    "https://archives.nseindia.com/products/content/derivatives/equities/udiff_{DD}{MM}{YYYY}.zip",
    "https://archives.nseindia.com/products/content/derivatives/equities/udiff{DD}{MON_ABBR}{YYYY}.zip",
    # 5. Lowercase fo (observed in some docs)
    "https://archives.nseindia.com/content/historical/DERIVATIVES/{YYYY}/{MON_ABBR}/Fo{DD}{MON_ABBR}{YYYY}.zip",
    # 6. Direct fo* zipped guess inside products
    "https://archives.nseindia.com/products/content/derivatives/equities/fo{DD}{MM}{YYYY}bhav.csv.zip",
    # 7. Potential moved path (speculative)
    "https://archives.nseindia.com/content/derivatives/bhav/fo{DD}{MM}{YYYY}.zip",
]

# Small content sizes (~3.2–3.6 KB) indicate NSE error/placeholder page.
ERROR_SIZE_THRESHOLD = 5000

class Downloader:
    def __init__(self, output_dir: str = "NSE_FO_UDIFF_Advanced_Feb_2025"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update(HEADERS_BASE)
        self.log = []

    def prime_session(self):
        """Hit a few NSE landing pages to obtain cookies (esp. nseappid, ak_bmsc, etc.)."""
        priming_urls = [
            "https://www.nseindia.com/",
            "https://www.nseindia.com/all-reports-derivatives",
            "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY",
        ]
        for url in priming_urls:
            try:
                r = self.session.get(url, timeout=12, verify=False)
                self._record(f"PRIME {url}", r.status_code, len(r.content))
                time.sleep(1.2)
            except Exception as e:
                self._record(f"PRIME {url}", error=str(e))

    def build_date_list(self, year: int, month: int) -> List[date]:
        first = date(year, month, 1)
        next_month = (first.replace(day=28) + timedelta(days=4)).replace(day=1)
        cur = first
        dates = []
        while cur < next_month:
            if cur.weekday() < 5:  # Monday-Friday
                dates.append(cur)
            cur += timedelta(days=1)
        return dates

    def attempt_download(self, target_date: date):
        DD = f"{target_date.day:02d}"
        MM = f"{target_date.month:02d}"
        YYYY = str(target_date.year)
        MON_ABBR = target_date.strftime("%b").upper()

        for idx, pattern in enumerate(URL_PATTERNS, 1):
            url = pattern.format(DD=DD, MM=MM, YYYY=YYYY, MON_ABBR=MON_ABBR)
            try:
                r = self.session.get(url, timeout=15, verify=False, headers={
                    **HEADERS_BASE,
                    "Referer": "https://www.nseindia.com/all-reports-derivatives",
                })
                size = len(r.content)
                ok = r.status_code == 200 and size > ERROR_SIZE_THRESHOLD
                status_note = "OK" if ok else "NO-DATA"
                self._record(f"{target_date} TRY {idx}", r.status_code, size, url=url, note=status_note)

                if ok:
                    fname = os.path.join(self.output_dir, os.path.basename(url))
                    with open(fname, "wb") as f:
                        f.write(r.content)
                    self._record(f"SAVED {fname}")
                    return True
            except Exception as e:
                self._record(f"{target_date} TRY {idx}", error=str(e), url=url)
                continue
            time.sleep(0.8)
        return False

    def _record(self, label: str, status: int = None, size: int = None, url: str = None, note: str = None, error: str = None):
        entry = {
            "time": datetime.now().strftime("%H:%M:%S"),
            "label": label,
        }
        if status is not None: entry["status"] = status
        if size is not None: entry["size"] = size
        if url: entry["url"] = url
        if note: entry["note"] = note
        if error: entry["error"] = error
        self.log.append(entry)
        # Console output concise
        if error:
            print(f"❌ {label} :: {error}")
        elif status is not None:
            print(f"{label}: {status} size={size} {note or ''}")
        else:
            print(label)

    def save_log(self):
        path = os.path.join(self.output_dir, "attempt_log.json")
        with open(path, "w") as f:
            json.dump(self.log, f, indent=2)
        print(f"📝 Log saved: {path}")


def main():
    year = 2025
    month = 2
    d = Downloader()
    print("🧠 Priming session...")
    d.prime_session()
    targets = d.build_date_list(year, month)
    print(f"📅 Trading days to attempt: {len(targets)}\n")

    success_count = 0
    for dt in targets:
        print(f"\n===== {dt} =====")
        if d.attempt_download(dt):
            success_count += 1
        else:
            print(f"   ⚠️ No file found for {dt}")

    d.save_log()

    print("\n==================================================")
    print("SUMMARY")
    print("==================================================")
    print(f"Successful files: {success_count}")
    print(f"Failed dates   : {len(targets) - success_count}")
    print("\nIf all failed:")
    print("  1. NSE likely moved F&O UDiFF behind JS/API or authenticated flow.")
    print("  2. Next step: Use a headless browser (Playwright/Selenium) to capture network calls while manually downloading.")
    print("  3. Extract the real API or dynamic URL from DevTools > Network.")
    print("  4. Recreate the request (cookies + headers + possible tokens).")

    print("\nManual Fallback (Selenium outline):")
    print("  - Open https://www.nseindia.com/all-reports-derivatives")
    print("  - Accept cookies, open DevTools Network tab")
    print("  - Filter by 'udiff' while triggering the download")
    print("  - Copy as cURL (right-click) and adapt into Python requests")

if __name__ == "__main__":
    main()
