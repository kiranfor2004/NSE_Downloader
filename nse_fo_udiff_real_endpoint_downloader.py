#!/usr/bin/env python3
"""
NSE F&O UDiFF Downloader using discovered merged-daily-reports API

Discovery:
  Network capture revealed endpoint:
    https://www.nseindia.com/api/merged-daily-reports?key=favDerivatives
  Which returns JSON objects including:
    {"name": "F&O - UDiFF Common Bhavcopy Final (zip)", "link": "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip"}

Approach:
  1. Use session priming (GET / and a simple API) to obtain cookies.
  2. Pull favDerivatives JSON for today and previous trading date(s) to validate pattern.
  3. For a target historical date (February 2025), construct expected URL directly:
       https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip
     (Assumption validated by current date structure.)
  4. Attempt download; if 404, record failure (may indicate archives retention rules or different naming for historical months).

Output:
  Directory: fo_udiff_downloads/
    - JSON log
    - Each successful ZIP.

Note:
  If February 2025 returns 404, corroborate by testing contiguous months to confirm availability window or permission constraints.
"""

import os
import json
import time
from datetime import date, timedelta, datetime
import requests
from typing import List
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_PATTERN = "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{YYYY}{MM}{DD}_F_0000.csv.zip"
FAV_ENDPOINT = "https://www.nseindia.com/api/merged-daily-reports?key=favDerivatives"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "Referer": "https://www.nseindia.com/all-reports-derivatives",
}

OUT_DIR = "fo_udiff_downloads"
os.makedirs(OUT_DIR, exist_ok=True)

log = []

def log_entry(**kw):
    kw["ts"] = datetime.now().strftime("%H:%M:%S")
    log.append(kw)
    print(kw)

def prime(sess: requests.Session):
    seeds = [
        "https://www.nseindia.com/",
        "https://www.nseindia.com/api/allMarketStatus",
    ]
    for u in seeds:
        try:
            r = sess.get(u, timeout=12, verify=False)
            log_entry(step="prime", url=u, status=r.status_code, size=len(r.content))
            time.sleep(1.2)
        except Exception as e:
            log_entry(step="prime", url=u, error=str(e))

def fetch_fav(sess: requests.Session):
    try:
        r = sess.get(FAV_ENDPOINT, timeout=15, verify=False)
        log_entry(step="fav", status=r.status_code, size=len(r.content))
        if r.status_code == 200:
            try:
                data = r.json()
                for item in data:
                    if "UDiFF" in item.get("name", "") and "FO" in item.get("name", ""):
                        log_entry(step="fav_udiff_found", link=item.get("link"))
                with open(os.path.join(OUT_DIR, "favDerivatives_raw.json"), "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
            except Exception as je:
                log_entry(step="fav_parse_error", error=str(je))
    except Exception as e:
        log_entry(step="fav_error", error=str(e))

def trading_days_for_month(year: int, month: int) -> List[date]:
    from calendar import monthrange
    days = monthrange(year, month)[1]
    d0 = date(year, month, 1)
    days_list = []
    for i in range(days):
        d = d0 + timedelta(days=i)
        if d.weekday() < 5:  # Mon-Fri
            days_list.append(d)
    return days_list

def attempt_download(sess: requests.Session, d: date):
    url = BASE_PATTERN.format(YYYY=d.year, MM=f"{d.month:02d}", DD=f"{d.day:02d}")
    try:
        r = sess.get(url, timeout=20, verify=False, headers={**HEADERS})
        size = len(r.content)
        ok = r.status_code == 200 and size > 6000  # rudimentary size check
        log_entry(step="download", date=str(d), url=url, status=r.status_code, size=size, ok=ok)
        if ok:
            fname = os.path.join(OUT_DIR, os.path.basename(url))
            with open(fname, "wb") as f:
                f.write(r.content)
            log_entry(step="saved", file=fname)
            return True
    except Exception as e:
        log_entry(step="error", date=str(d), error=str(e))
    return False

def main():
    target_year = 2025
    target_month = 2  # February 2025
    sess = requests.Session()
    sess.headers.update(HEADERS)

    log_entry(step="start", info="Priming session")
    prime(sess)
    fetch_fav(sess)

    # Validate pattern on yesterday (if within retention) using constructed URL
    today = date.today()
    yesterday = today - timedelta(days=1)
    log_entry(step="validate_current", info=str(yesterday))
    attempt_download(sess, yesterday)

    # Download February 2025 trading days
    dates = trading_days_for_month(target_year, target_month)
    success = 0
    for d in dates:
        time.sleep(0.9)
        if attempt_download(sess, d):
            success += 1

    log_entry(step="summary", feb_trading_days=len(dates), feb_success=success)
    with open(os.path.join(OUT_DIR, "run_log.json"), "w", encoding="utf-8") as f:
        json.dump(log, f, indent=2)
    print("Summary: Feb 2025 successes:", success)
    if success == 0:
        print("No February 2025 UDiFF files retrieved. Possible reasons:\n"
              " - Historical retention or path variation for older months\n"
              " - Additional auth or dynamic token for historical queries\n"
              " - Different naming before certain cutoff. Next step: test January & March, inspect site for historical date selection, or alternative API parameterization.")

if __name__ == "__main__":
    main()
