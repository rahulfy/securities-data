"""
NSE index Total Return history downloader for the securities-data repository.

Reads the list of indices from config/indices.csv (editable in the browser).
For each index it downloads the full daily Total Return Index history from
niftyindices.com in one request and saves:

  data/indices/tri_daily.parquet   one row per index per day:
                                   key, index_name, date, tri, ntr
  data/indices/_status.csv         rows, first and last date per index

NSE returns an empty answer (no error) when a name is misspelt, so each row
in the list may give alternative spellings separated by "|"; the first one
that returns data is used. Any index with no data makes the run fail (red),
after everything else has been saved.
"""
import json
import re
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
LIST_FILE = ROOT / "config" / "indices.csv"
OUT_DIR = ROOT / "data" / "indices"
DATA_FILE = OUT_DIR / "tri_daily.parquet"
STATUS_FILE = OUT_DIR / "_status.csv"

TRI_URL = "https://www.niftyindices.com/BackPage/getTotalReturnIndexString"  # proven by probe, Sep 2026
START = "01-Jan-1995"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": UA,  # a non-browser identity makes the site hang silently
    "Content-Type": "application/json; charset=UTF-8",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://www.niftyindices.com/reports/historical-data",
    "Origin": "https://www.niftyindices.com",
}


def spellings(listed: str) -> list:
    """NSE's menu spellings don't always match what its data service accepts
    (menu 'NIFTY SMALL CAP 250' vs data 'NIFTY SMALLCAP 250'). Try the names
    given in the list first, then common variations."""
    out = []
    for base in [n.strip() for n in listed.split("|") if n.strip()]:
        b = base.upper().replace("\u2013", "-").replace("\u2014", "-")
        b = re.sub(r"\s+", " ", b)
        cands = [base, b]
        for c in list(cands):
            cands.append(c.replace("SMALL CAP", "SMALLCAP"))
            cands.append(c.replace("MID CAP", "MIDCAP"))
            cands.append(re.sub(r"^NIFTY (\d+) ", r"NIFTY\1 ", c))   # NIFTY 100 X -> NIFTY100 X
            cands.append(re.sub(r"^NIFTY(\d+) ", r"NIFTY \1 ", c))   # NIFTY100 X -> NIFTY 100 X
            cands.append(re.sub(r" INDEX$", "", c))
        out += cands
    return list(dict.fromkeys(out))  # unique, keep order


def num(v):
    try:
        return float(str(v).replace(",", ""))
    except ValueError:
        return float("nan")  # '-' means not published


def fetch(session, name, retries=3):
    end = date.today().strftime("%d-%b-%Y")
    cinfo = f"{{'name':'{name}','startDate':'{START}','endDate':'{end}','indexName':'{name}'}}"
    last = None
    for attempt in range(1, retries + 1):
        try:
            r = session.post(TRI_URL, headers=HEADERS, data=json.dumps({"cinfo": cinfo}), timeout=90)
            r.raise_for_status()
            body = r.json()
            return body if isinstance(body, list) else json.loads(body["d"])
        except Exception as e:
            last = e
            print(f"    attempt {attempt} failed: {type(e).__name__}: {str(e)[:150]}", flush=True)
            time.sleep(15 * attempt)
    raise RuntimeError(f"{type(last).__name__}: {str(last)[:150]}")


def to_frame(rows, key):
    df = pd.DataFrame(rows)
    out = pd.DataFrame({
        "key": key,
        "index_name": df["Index Name"],
        "date": pd.to_datetime(df["Date"], format="%d %b %Y"),
        "tri": df["TotalReturnsIndex"].map(num),
        "ntr": df["NTR_Value"].map(num) if "NTR_Value" in df else float("nan"),
    })
    out = out.dropna(subset=["tri"])
    out = out[out.tri > 0]
    return out.drop_duplicates(["key", "date"]).sort_values("date")


def main():
    wanted = pd.read_csv(LIST_FILE, dtype=str).fillna("")
    s = requests.Session()
    s.get("https://www.niftyindices.com/reports/historical-data", headers={"User-Agent": UA}, timeout=60)
    frames, status, failed = [], [], 0
    run_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    for _, row in wanted.iterrows():
        key = row["key"].strip()
        got, used, err = None, "", ""
        for name in spellings(row["nse_names"]):
            print(f"{key}: trying '{name}'", flush=True)
            try:
                rows = fetch(s, name)
            except Exception as e:
                err = str(e)
                continue
            if rows:
                got, used = to_frame(rows, key), name
                break
            err = "NSE returned no data under any spelling tried"
            time.sleep(1)
        if got is not None and len(got):
            frames.append(got)
            status.append((key, used, len(got), got.date.min().date(), got.date.max().date(), "ok", run_at))
            print(f"    {len(got):,} days, {got.date.min().date()} to {got.date.max().date()}", flush=True)
        else:
            failed += 1
            status.append((key, row["nse_names"], 0, None, None, f"FAILED: {err}"[:200], run_at))
            print(f"    FAILED: {err}", flush=True)
        time.sleep(2)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(status, columns=["key", "nse_name_used", "days", "first_date", "last_date",
                                  "status", "run_at_utc"]).to_csv(STATUS_FILE, index=False)
    if frames:
        new = pd.concat(frames, ignore_index=True)
        if DATA_FILE.exists():
            # keep earlier data for any index that failed this week
            old = pd.read_parquet(DATA_FILE)
            new = pd.concat([old[~old.key.isin(new.key.unique())], new], ignore_index=True)
        new["key"] = new["key"].astype("category")
        new["index_name"] = new["index_name"].astype("category")
        new.sort_values(["key", "date"]).to_parquet(DATA_FILE, index=False, compression="zstd")
        print(f"Saved {DATA_FILE.relative_to(ROOT)}: {len(new):,} rows, {new.key.nunique()} indices")
    if failed:
        sys.exit(f"ERROR: {failed} index(es) returned no data; see data/indices/_status.csv")


if __name__ == "__main__":
    main()
