"""
AMFI NAV history downloader for the securities-data repository.

What it does
  fetch  --year YYYY : downloads every scheme's daily NAV for that year from
                       AMFI (one month at a time) and saves
                       data/mutual-funds/nav/nav_YYYY.parquet
  master             : rebuilds data/mutual-funds/scheme_master.csv from all
                       yearly files (one row per scheme, incl. closed ones)

AMFI's history lists every scheme that existed on each date, so schemes that
were later merged or wound up are kept. That avoids survivorship bias.
"""

import argparse
import calendar
import datetime as dt
import io
import re
import sys
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

AMFI_URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"
ROOT = Path(__file__).resolve().parents[1]
NAV_DIR = ROOT / "data" / "mutual-funds" / "nav"
LOG_FILE = NAV_DIR / "_fetch_log.csv"
MASTER_FILE = ROOT / "data" / "mutual-funds" / "scheme_master.csv"
FIRST_DATE = dt.date(2006, 4, 1)  # AMFI history starts here
HEADERS = {"User-Agent": "Mozilla/5.0 (securities-data research downloader)"}
CATEGORY_RE = re.compile(r"^\s*(open|close|closed|interval)\s*ended\s*schemes", re.I)


# ---------------------------------------------------------------- download
def download(frm: dt.date, to: dt.date, retries: int = 5) -> str:
    params = {"frmdt": frm.strftime("%d-%b-%Y"), "todt": to.strftime("%d-%b-%Y")}
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(AMFI_URL, params=params, headers=HEADERS, timeout=300)
            r.raise_for_status()
            text = r.content.decode("utf-8", errors="replace")
            if "Scheme Code" not in text[:2000]:
                raise ValueError("AMFI did not return a NAV file (got a web page instead)")
            return text
        except Exception as e:  # network hiccups, AMFI timeouts
            last_err = e
            wait = 20 * attempt
            print(f"    attempt {attempt} failed ({e}); retrying in {wait}s", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"gave up on {params}: {last_err}")


# ---------------------------------------------------------------- parse
def parse(text: str) -> pd.DataFrame:
    """Turn AMFI's semicolon text into a table.

    Layout: header line, then blocks of
      'Open Ended Schemes ( Equity Scheme - Large Cap Fund )'   <- category
      'HDFC Mutual Fund'                                         <- fund house
      '119018;HDFC Large Cap Fund - Growth;INF179...;;812.3;...;01-Apr-2024'
    """
    rows = []
    category, amc = None, None
    for raw in io.StringIO(text):
        line = raw.strip()
        if not line or line.startswith("Scheme Code"):
            continue
        if ";" not in line:
            if CATEGORY_RE.match(line):
                category = re.sub(r"\s+", " ", line)
            else:
                amc = line
            continue
        parts = [p.strip() for p in line.split(";")]
        if len(parts) < 8:
            continue
        code, name, isin1, isin2, nav, _, _, date = parts[:8]
        try:
            code = int(code)
            nav = float(nav.replace(",", ""))
            date = dt.datetime.strptime(date, "%d-%b-%Y").date()
        except ValueError:
            continue  # 'N.A.', blanks, malformed lines
        if nav <= 0:
            continue
        rows.append((code, date, nav, name, category, amc,
                     isin1 if isin1 not in ("", "-") else None,
                     isin2 if isin2 not in ("", "-") else None))
    cols = ["scheme_code", "date", "nav", "scheme_name", "category", "amc",
            "isin_growth_or_payout", "isin_reinvest"]
    return pd.DataFrame(rows, columns=cols)


# ---------------------------------------------------------------- save
def save_year(df: pd.DataFrame, year: int) -> Path:
    NAV_DIR.mkdir(parents=True, exist_ok=True)
    for c in ["scheme_name", "category", "amc", "isin_growth_or_payout", "isin_reinvest"]:
        df[c] = df[c].astype("object").astype("category")
    df = (df.drop_duplicates(["scheme_code", "date"], keep="last")
            .sort_values(["scheme_code", "date"]).reset_index(drop=True))
    df["scheme_code"] = df["scheme_code"].astype("int32")
    df["date"] = pd.to_datetime(df["date"])
    for c in ["scheme_name", "category", "amc", "isin_growth_or_payout", "isin_reinvest"]:
        df[c] = df[c].astype("category")
    table = pa.Table.from_pandas(df, preserve_index=False)
    path = NAV_DIR / f"nav_{year}.parquet"
    pq.write_table(table, path, compression="zstd", compression_level=19,
                   use_dictionary=["scheme_name", "category", "amc",
                                   "isin_growth_or_payout", "isin_reinvest"],
                   column_encoding={"nav": "BYTE_STREAM_SPLIT",
                                    "scheme_code": "DELTA_BINARY_PACKED"},
                   row_group_size=1_000_000)
    return path


def write_log(entries: list) -> None:
    NAV_DIR.mkdir(parents=True, exist_ok=True)
    new = pd.DataFrame(entries, columns=["year", "month", "rows", "status", "run_at_utc"])
    if LOG_FILE.exists():
        old = pd.read_csv(LOG_FILE)
        old = old[~old.set_index(["year", "month"]).index.isin(new.set_index(["year", "month"]).index)]
        new = pd.concat([old, new])
    new.sort_values(["year", "month"]).to_csv(LOG_FILE, index=False)


def fetch_year(year: int) -> None:
    today = dt.date.today()
    frames, log = [], []
    run_at = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    for month in range(1, 13):
        start = max(dt.date(year, month, 1), FIRST_DATE)
        end = dt.date(year, month, calendar.monthrange(year, month)[1])
        if start > min(end, today) or end < FIRST_DATE:
            continue
        end = min(end, today)
        print(f"  {year}-{month:02d}: downloading {start} to {end}", flush=True)
        try:
            part = parse(download(start, end))
            for c in ["scheme_name", "category", "amc", "isin_growth_or_payout", "isin_reinvest"]:
                part[c] = part[c].astype("category")  # keeps memory low
            frames.append(part)
            log.append((year, month, len(part), "ok", run_at))
            print(f"    {len(part):,} NAV rows", flush=True)
        except Exception as e:
            log.append((year, month, 0, f"FAILED: {e}"[:200], run_at))
            print(f"    FAILED: {e}", flush=True)
        time.sleep(3)  # be polite to AMFI
    write_log(log)
    if not frames:
        print(f"No data for {year}; nothing saved.")
        return
    path = save_year(pd.concat(frames, ignore_index=True), year)
    print(f"Saved {path.relative_to(ROOT)} ({path.stat().st_size / 1e6:.1f} MB)")
    if any(r[3] != "ok" for r in log):
        print("WARNING: some months failed; see data/mutual-funds/nav/_fetch_log.csv")


# ---------------------------------------------------------------- master
def build_master() -> None:
    files = sorted(NAV_DIR.glob("nav_*.parquet"))
    if not files:
        print("No NAV files yet.")
        return
    parts = []
    for f in files:
        d = pd.read_parquet(f)
        for c in d.select_dtypes("category"):
            d[c] = d[c].astype("object")
        d = d.sort_values(["scheme_code", "date"])
        g = d.groupby("scheme_code")
        parts.append(pd.DataFrame({
            "first_nav_date": g["date"].min(),
            "last_nav_date": g["date"].max(),
            "nav_count": g.size(),
            "scheme_name": g["scheme_name"].last(),
            "first_category": g["category"].first(),
            "category": g["category"].last(),
            "amc": g["amc"].last(),
            "isin_growth_or_payout": g["isin_growth_or_payout"].last(),
            "isin_reinvest": g["isin_reinvest"].last(),
        }))
    allp = pd.concat(parts).reset_index().sort_values(["scheme_code", "last_nav_date"])
    g = allp.groupby("scheme_code")
    m = pd.DataFrame({
        "scheme_name": g["scheme_name"].last(),
        "amc": g["amc"].last(),
        "category_latest": g["category"].last(),
        "category_first": g["first_category"].first(),
        "isin_growth_or_payout": g["isin_growth_or_payout"].last(),
        "isin_reinvest": g["isin_reinvest"].last(),
        "first_nav_date": g["first_nav_date"].min().dt.date,
        "last_nav_date": g["last_nav_date"].max().dt.date,
        "nav_count": g["nav_count"].sum(),
    }).reset_index()
    latest = pd.to_datetime(m["last_nav_date"]).max()
    m["status"] = (pd.to_datetime(m["last_nav_date"]) >= latest - pd.Timedelta(days=15)).map(
        {True: "active", False: "closed_or_merged"})
    MASTER_FILE.parent.mkdir(parents=True, exist_ok=True)
    m.to_csv(MASTER_FILE, index=False)
    print(f"Saved {MASTER_FILE.relative_to(ROOT)}: {len(m):,} schemes "
          f"({(m.status == 'active').sum():,} active)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch")
    f.add_argument("--year", type=int, required=True)
    sub.add_parser("master")
    a = ap.parse_args()
    if a.cmd == "fetch":
        fetch_year(a.year)
    else:
        build_master()
    sys.exit(0)
