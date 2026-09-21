"""
Probe NSE's index website (niftyindices.com) from GitHub's servers.
Downloads no real data. It only records what works, so the real index
download job can be built on proven facts.

Output: data/indices/_probe_result.txt
"""
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

OUT = Path(__file__).resolve().parents[1] / "data" / "indices" / "_probe_result.txt"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": UA,
    "Content-Type": "application/json; charset=UTF-8",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://www.niftyindices.com/reports/historical-data",
    "Origin": "https://www.niftyindices.com",
}
ENDPOINTS = [
    "https://www.niftyindices.com/BackPage/getTotalReturnIndexString",
    "https://www.niftyindices.com/Backpage.aspx/getTotalReturnIndexString",
    "https://niftyindices.com/Backpage.aspx/getTotalReturnIndexString",
]
PRICE_ENDPOINTS = [
    "https://www.niftyindices.com/BackPage/getHistoricaldatatabletoString",
    "https://www.niftyindices.com/Backpage.aspx/getHistoricaldatatabletoString",
]
NAMES = [
    "NIFTY 50", "NIFTY 500", "NIFTY MIDCAP 150", "NIFTY SMALLCAP 250",
    "NIFTY MIDCAP150 MOMENTUM 50", "NIFTY MIDCAP150 QUALITY 50",
    "NIFTY500 VALUE 50", "NIFTY MIDCAP150 VALUE 50",
    "NIFTY MIDSMALLCAP400 MOMENTUM QUALITY 100", "NIFTY200 VALUE 30",
    "NIFTY LOW VOLATILITY 50",
]
lines = []


def log(msg=""):
    print(msg, flush=True)
    lines.append(msg)


def post(session, url, name, start, end, timeout=45):
    cinfo = f"{{'name':'{name}','startDate':'{start}','endDate':'{end}','indexName':'{name}'}}"
    t = time.time()
    try:
        r = session.post(url, headers=HEADERS, data=json.dumps({"cinfo": cinfo}), timeout=timeout)
    except Exception as e:
        return {"ok": False, "why": f"{type(e).__name__}: {e}"[:200], "secs": round(time.time() - t)}
    res = {"status": r.status_code, "secs": round(time.time() - t), "ctype": r.headers.get("content-type", ""),
           "head": r.text[:200].replace("\n", " ")}
    try:
        rows = json.loads(r.json()["d"])
        res.update(ok=True, rows=len(rows), first=rows[0] if rows else None, last=rows[-1] if rows else None)
    except Exception as e:
        res.update(ok=False, why=f"not the expected data format ({type(e).__name__})")
    return res


def main():
    log(f"Probe run {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}")
    s = requests.Session()
    try:
        r = s.get("https://www.niftyindices.com/reports/historical-data", headers={"User-Agent": UA}, timeout=45)
        log(f"Home page visit: HTTP {r.status_code}, cookies: {list(s.cookies.keys())}")
    except Exception as e:
        log(f"Home page visit FAILED: {e}")

    log("\n== Step 1: which TRI address works (NIFTY 50, Jan 2024) ==")
    working = None
    for url in ENDPOINTS:
        res = post(s, url, "NIFTY 50", "01-Jan-2024", "31-Jan-2024")
        log(f"{url}\n   -> {res}")
        if res.get("ok") and res.get("rows"):
            working = working or url
    log(f"\nWORKING TRI ADDRESS: {working}")

    log("\n== Step 1b: price-index address (backup) ==")
    for url in PRICE_ENDPOINTS:
        res = post(s, url, "NIFTY 50", "01-Jan-2024", "31-Jan-2024")
        log(f"{url}\n   -> ok={res.get('ok')} rows={res.get('rows')} status={res.get('status')} why={res.get('why','')}")

    if not working:
        log("\nNo working TRI address. Stopping.")
        return

    log("\n== Step 2: which index names return data (Jan 2024) ==")
    for name in NAMES:
        res = post(s, working, name, "01-Jan-2024", "31-Jan-2024")
        log(f"{name:45s} rows={res.get('rows')} ok={res.get('ok')} {res.get('why','')}")
        time.sleep(2)

    log("\n== Step 3: can one request return 20 years? (NIFTY MIDCAP 150) ==")
    res = post(s, working, "NIFTY MIDCAP 150", "01-Apr-2005", "31-Dec-2025", timeout=120)
    log(f"rows={res.get('rows')} secs={res.get('secs')} ok={res.get('ok')} {res.get('why','')}")
    log(f"first row: {res.get('first')}\nlast row:  {res.get('last')}")


if __name__ == "__main__":
    try:
        main()
    finally:
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text("\n".join(lines) + "\n")
