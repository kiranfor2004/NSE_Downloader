#!/usr/bin/env python3
"""
Playwright Network Capture for NSE F&O UDiFF Discovery

Purpose:
  Launches Chromium, navigates to derivatives reports page, listens to network traffic,
  captures URLs & response bodies that contain any of the target keywords. Stores logs
  for offline analysis so we can identify the true endpoint serving F&O / UDiFF data.

Usage:
  1. Install: pip install -r requirements_playwright.txt
  2. Install browsers: python -m playwright install chromium
  3. Run: python capture_nse_fo_udiff_endpoints.py
  4. (Optional) Interact manually: toggle headless=False to view browser.

Output:
  Directory: network_capture_logs/
    - requests_meta.jsonl  (one JSON per line with timing + status + url)
    - keyword_hits.jsonl   (subset where URL or body matched target keywords)
    - last_summary.txt     (quick stats)

Next Step:
  Inspect keyword_hits.jsonl for any candidate URL that looks like an API returning
  file references or direct archives. Once found, replicate via requests.Session.
"""

import os, json, asyncio
from datetime import datetime
from typing import Set
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

TARGET_PAGE = "https://www.nseindia.com/all-reports-derivatives"
ALT_TARGET = "https://www.nseindia.com/"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
OUTPUT_DIR = "network_capture_logs"
KEYWORDS = ["udiff","fo","bhav","deriv",".zip","bhavcopy","derivatives","report"]
MAX_REQUESTS = 1200
HEADLESS = False
MAX_NAV_RETRIES = 4

EXTRA_HEADERS = {"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Accept-Language":"en-US,en;q=0.8"}

os.makedirs(OUTPUT_DIR, exist_ok=True)
META_LOG = os.path.join(OUTPUT_DIR, "requests_meta.jsonl")
HITS_LOG = os.path.join(OUTPUT_DIR, "keyword_hits.jsonl")
SUMMARY = os.path.join(OUTPUT_DIR, "last_summary.txt")
seen_urls: Set[str] = set()
keyword_count = 0


def log_line(path: str, obj: dict):
    with open(path,'a',encoding='utf-8') as f: f.write(json.dumps(obj)+'\n')


def keyword_match(url: str, body_snippet: str = "") -> bool:
    blob = (url + " " + body_snippet).lower()
    return any(k in blob for k in KEYWORDS)

async def robust_goto(page, url: str):
    last_error=None
    for attempt in range(1, MAX_NAV_RETRIES+1):
        try:
            print(f"Attempt {attempt} -> {url}")
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            await page.wait_for_timeout(1500)
            return True
        except PlaywrightTimeoutError as e:
            last_error=f"Timeout {e}"
        except Exception as e:
            last_error=str(e)
        await page.wait_for_timeout(1800*attempt)
    print(f"Navigation failed: {last_error}")
    return False

async def run_capture():
    global keyword_count
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS, args=["--no-sandbox","--disable-http2"])
        context = await browser.new_context(user_agent=USER_AGENT, extra_http_headers=EXTRA_HEADERS, viewport={"width":1400,"height":900})
        page = await context.new_page()

        async def handle_response(response):
            global keyword_count
            if len(seen_urls) >= MAX_REQUESTS: return
            url = response.url
            if url in seen_urls: return
            seen_urls.add(url)
            ct = response.headers.get('content-type','')
            status = response.status
            entry = {'time':datetime.utcnow().isoformat(),'url':url,'status':status,'content_type':ct}
            body_snippet=""
            try:
                if any(x in ct for x in ['json','text']) or any(x in url for x in ['api','json','reports','download','data']):
                    txt = await response.text()
                    body_snippet = txt[:1500]
                    if keyword_match(url, body_snippet):
                        entry['sample'] = body_snippet
            except Exception as e:
                entry['error']=str(e)
            log_line(META_LOG, entry)
            if keyword_match(url, body_snippet):
                log_line(HITS_LOG, entry)
                keyword_count += 1
        page.on('response', handle_response)

        if not await robust_goto(page, TARGET_PAGE):
            await robust_goto(page, ALT_TARGET)
            await page.wait_for_timeout(4000)
            try:
                await page.evaluate(f"window.location.href='{TARGET_PAGE}'")
                await page.wait_for_timeout(8000)
            except Exception as e:
                print(f"JS redirect failed: {e}")

        # Save initial HTML
        try:
            html = await page.content()
            with open(os.path.join(OUTPUT_DIR, 'initial_page.html'),'w',encoding='utf-8') as f:
                f.write(html)
        except Exception as e:
            print(f"HTML save failed: {e}")

        for _ in range(5):
            await page.mouse.wheel(0, 1600)
            await page.wait_for_timeout(1600)
        await page.wait_for_timeout(6000)
        await browser.close()

    summary={'total_requests':len(seen_urls),'keyword_hits':keyword_count,'keywords':KEYWORDS}
    with open(SUMMARY,'w') as f: f.write(json.dumps(summary,indent=2))
    print("Summary:")
    print(json.dumps(summary,indent=2))

if __name__=='__main__':
    asyncio.run(run_capture())
