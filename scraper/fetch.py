#!/usr/bin/env python3
"""
Galveston County Motivated Seller Lead Scraper  v2
Clerk Portal : https://ava.fidlar.com/TXGalveston/AvaWeb/
CAD Data     : https://galvestoncad.org/current-and-historical-supplement-roll-export-data/
Look-back    : 90 days

FIXES in v2
  - CAD ZIP: handles pipe-delimited .txt (GCAD standard), CSV, and DBF
  - Logs every file inside the ZIP so column names are visible in CI output
  - Clerk: multi-strategy Playwright flow (doc-type tabs + broad date search)
  - Screenshots saved to /tmp/ava_*.png for CI debugging
  - Deduplication by doc number
  - Never crashes on bad records
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
import unicodedata
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

# ─── logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("galveston_scraper")

# ─── constants ─────────────────────────────────────────────────────────────────
LOOK_BACK_DAYS   = 90
RETRY_ATTEMPTS   = 3
RETRY_DELAY      = 6

CLERK_AVA_URL    = "https://ava.fidlar.com/TXGalveston/AvaWeb/"
GCAD_EXPORT_PAGE = "https://galvestoncad.org/current-and-historical-supplement-roll-export-data/"

LEAD_TYPES: dict[str, tuple[str, list[str]]] = {
    "LP":       ("Lis Pendens",            ["LP", "LISPEND", "LISPENDENS", "LIS PENDENS"]),
    "NOFC":     ("Notice of Foreclosure",  ["NOFC", "NOTFORECLOSURE", "FORECLOSURE", "NOF",
                                            "NOTICE OF FORECLOSURE", "NTCFORECLOSURE"]),
    "TAXDEED":  ("Tax Deed",               ["TAXDEED", "TAX DEED", "TAXD", "TD"]),
    "JUD":      ("Judgment",               ["JUD", "JUDGMENT", "JUDG"]),
    "CCJ":      ("Certified Judgment",     ["CCJ", "CERTJUD", "CERTIFIEDJUDGMENT"]),
    "DRJUD":    ("Domestic Judgment",      ["DRJUD", "DOMJUD", "DOMESTICJUD"]),
    "LNCORPTX": ("Corp Tax Lien",          ["LNCORPTX", "CORPTAX", "CORPTAXLIEN"]),
    "LNIRS":    ("IRS Lien",               ["LNIRS", "IRS", "IRSLIEN", "FEDERALTAXLIEN", "FTL"]),
    "LNFED":    ("Federal Lien",           ["LNFED", "FEDLIEN", "FEDERALLIEN", "FEDERAL LIEN"]),
    "LN":       ("Lien",                   ["LN", "LIEN", "GEN LIEN", "GENLIEN"]),
    "LNMECH":   ("Mechanic Lien",          ["LNMECH", "MECHLIEN", "MECHANICLIEN",
                                            "MECHANIC LIEN", "CONTRACTORLIEN", "MATMANLIEN"]),
    "LNHOA":    ("HOA Lien",               ["LNHOA", "HOALIEN", "HOA LIEN"]),
    "MEDLN":    ("Medicaid Lien",          ["MEDLN", "MEDICAID", "MEDICAIDLIEN"]),
    "PRO":      ("Probate",                ["PRO", "PROBATE", "LETTEST", "LETADM",
                                            "LETTERS TESTAMENTARY", "MUNIMENT"]),
    "NOC":      ("Notice of Commencement", ["NOC", "NOTCOMMENCE", "NOTICE OF COMMENCEMENT"]),
    "RELLP":    ("Release Lis Pendens",    ["RELLP", "RELLISPENDENS", "RELEASELP",
                                            "RELEASE LIS PENDENS"]),
}

AVA_SEARCH_CODES = list(LEAD_TYPES.keys())

# ─── utilities ─────────────────────────────────────────────────────────────────

def retry_call(fn, *args, attempts=RETRY_ATTEMPTS, delay=RETRY_DELAY, label="", **kwargs):
    for i in range(attempts):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            log.warning(f"[{label}] attempt {i+1}/{attempts} failed: {exc}")
            if i < attempts - 1:
                time.sleep(delay)
    log.error(f"[{label}] all {attempts} attempts failed")
    return None


def normalize(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", text).strip().lower()


def parse_amount(text: str) -> float | None:
    if not text:
        return None
    cleaned = re.sub(r"[^\d.]", "", str(text).replace(",", ""))
    try:
        v = float(cleaned)
        return v if v > 0 else None
    except ValueError:
        return None


def map_doc_type(raw: str) -> tuple[str, str] | None:
    upper = re.sub(r"[\s\-_/]", "", raw.upper())
    if not upper:
        return None
    best: tuple[str, str] | None = None
    best_pri, best_len = 999, 0
    for cat, (label, variants) in LEAD_TYPES.items():
        for v in variants:
            vc = re.sub(r"[\s\-_/]", "", v.upper())
            if not vc:
                continue
            if vc == upper:
                pri = 1
            elif upper.startswith(vc) or upper.endswith(vc):
                pri = 2
            elif vc in upper:
                pri = 3
            elif upper in vc:
                pri = 4
            else:
                continue
            if pri < best_pri or (pri == best_pri and len(vc) > best_len):
                best_pri, best_len, best = pri, len(vc), (cat, label)
    return best


def normalize_date(raw: str) -> str:
    if not raw:
        return ""
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%Y%m%d", "%m-%d-%Y", "%d-%b-%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    m = re.search(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})", raw)
    if m:
        try:
            mo, dy, yr = m.groups()
            yr = int(yr)
            if yr < 100:
                yr += 2000
            return f"{yr:04d}-{int(mo):02d}-{int(dy):02d}"
        except Exception:
            pass
    return raw.strip()


def is_new_this_week(filed: str) -> bool:
    try:
        return (datetime.now() - datetime.strptime(filed, "%Y-%m-%d")).days <= 7
    except Exception:
        return False


def score_record(rec: dict) -> tuple[int, list[str]]:
    flags: list[str] = []
    score = 30
    cat    = rec.get("cat", "")
    amount = rec.get("amount")
    filed  = rec.get("filed", "")
    owner  = rec.get("owner", "")

    if cat == "LP":            flags.append("Lis pendens")
    if cat in ("NOFC", "LP"): flags.append("Pre-foreclosure")
    if cat in ("JUD", "CCJ", "DRJUD"): flags.append("Judgment lien")
    if cat in ("LNCORPTX", "LNIRS", "LNFED", "TAXDEED"): flags.append("Tax lien")
    if cat == "LNMECH":        flags.append("Mechanic lien")
    if cat == "PRO":           flags.append("Probate / estate")
    if re.search(r"\b(LLC|INC|CORP|LTD|LLP|TRUST|TRUSTEE)\b", owner.upper()):
        flags.append("LLC / corp owner")
    if is_new_this_week(filed): flags.append("New this week")

    score += 10 * len(flags)
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20
    if amount:
        if amount > 100_000:  score += 15
        elif amount > 50_000: score += 10
    if "New this week" in flags: score += 5
    if rec.get("prop_address") or rec.get("mail_address"): score += 5
    return min(score, 100), flags


# ─── GCAD Parcel Index ─────────────────────────────────────────────────────────

class ParcelIndex:

    def __init__(self):
        self._by_name: dict[str, dict] = {}
        self._loaded = False

    @staticmethod
    def _find_latest_export_url() -> str | None:
        log.info("[CAD] scanning export page…")
        resp = requests.get(GCAD_EXPORT_PAGE, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        candidates: list[tuple[int, str]] = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            text = a.get_text(strip=True)
            if not re.search(r"\.(zip|txt|csv|dbf)$", href, re.I):
                if not re.search(r"(export|appr_exp|supp|owner)", href, re.I):
                    continue
            ym = re.search(r"(202\d)", href + " " + text)
            year = int(ym.group(1)) if ym else 0
            full = href if href.startswith("http") else "https://galvestoncad.org" + href
            candidates.append((year, full))
        if not candidates:
            return None
        candidates.sort(key=lambda x: x[0], reverse=True)
        url = candidates[0][1]
        log.info(f"[CAD] selected: {url}")
        return url

    @staticmethod
    def _download(url: str) -> bytes | None:
        log.info(f"[CAD] downloading {url}")
        resp = requests.get(url, timeout=180)
        resp.raise_for_status()
        log.info(f"[CAD] {len(resp.content):,} bytes received")
        return resp.content

    def _parse_pipe_txt(self, data: bytes, name: str) -> list[dict]:
        rows = []
        try:
            text = data.decode("latin-1", errors="replace")
            lines = text.splitlines()
            # Find first line containing pipes
            hi = next((i for i, l in enumerate(lines[:20]) if "|" in l), 0)
            headers = [h.strip().upper() for h in lines[hi].split("|")]
            log.info(f"[CAD] pipe columns ({name}): {headers[:15]}")
            for line in lines[hi + 1:]:
                if not line.strip():
                    continue
                cols = line.split("|")
                while len(cols) < len(headers):
                    cols.append("")
                rows.append({headers[i]: cols[i].strip() for i in range(len(headers))})
        except Exception as exc:
            log.warning(f"[CAD] pipe parse error ({name}): {exc}")
        log.info(f"[CAD] pipe-TXT: {len(rows):,} rows from {name}")
        return rows

    def _parse_csv(self, data: bytes, name: str) -> list[dict]:
        rows = []
        try:
            text = data.decode("utf-8", errors="replace")
            for row in csv.DictReader(io.StringIO(text)):
                rows.append({k.strip().upper(): str(v).strip() for k, v in row.items()})
        except Exception as exc:
            log.warning(f"[CAD] CSV parse error ({name}): {exc}")
        log.info(f"[CAD] CSV: {len(rows):,} rows from {name}")
        return rows

    def _parse_dbf(self, data: bytes, name: str) -> list[dict]:
        rows = []
        try:
            import tempfile
            from dbfread import DBF
            with tempfile.NamedTemporaryFile(suffix=".dbf", delete=False) as tf:
                tf.write(data); tmp = tf.name
            for rec in DBF(tmp, load=True, encoding="latin-1"):
                rows.append({k.upper(): str(v).strip() for k, v in dict(rec).items()})
            os.unlink(tmp)
        except ImportError:
            log.warning("[CAD] dbfread not installed")
        except Exception as exc:
            log.warning(f"[CAD] DBF error ({name}): {exc}")
        log.info(f"[CAD] DBF: {len(rows):,} rows from {name}")
        return rows

    def _parse_fixed_width(self, data: bytes, name: str,
                           cols: list[tuple[str, int, int]]) -> list[dict]:
        """
        Parse a fixed-width text file given a column layout.
        cols: list of (field_name, 0-based_start, length)
        """
        rows = []
        try:
            text = data.decode("latin-1", errors="replace")
            for line in text.splitlines():
                if not line.strip():
                    continue
                row = {}
                for fname, start, length in cols:
                    row[fname] = line[start:start + length].strip()
                rows.append(row)
        except Exception as exc:
            log.warning(f"[CAD] fixed-width parse error ({name}): {exc}")
        log.info(f"[CAD] fixed-width: {len(rows):,} rows from {name}")
        return rows

    def _parse_header_txt(self, data: bytes, name: str) -> list[tuple[str, int, int]] | None:
        """
        Parse a GCAD *_HEADER.TXT file to extract column layout.
        GCAD header files are pipe-delimited: FIELD_NAME|START_POS|LENGTH
        (1-indexed positions). Returns list of (name, 0-based-start, length).
        """
        try:
            text = data.decode("latin-1", errors="replace").strip()
            cols = []
            for line in text.splitlines():
                parts = [p.strip() for p in line.split("|")]
                if len(parts) >= 3:
                    fname = parts[0].upper()
                    start = int(parts[1]) - 1   # convert 1-indexed → 0-indexed
                    length = int(parts[2])
                    cols.append((fname, start, length))
            if cols:
                log.info(f"[CAD] header parsed {len(cols)} columns from {name}")
                return cols
        except Exception as exc:
            log.warning(f"[CAD] header parse error ({name}): {exc}")
        return None

    def _auto_parse(self, data: bytes, name: str,
                    header_cols: list[tuple[str, int, int]] | None = None) -> list[dict]:
        """Auto-detect format: use provided header layout, pipe-TXT, CSV, DBF, or fixed-width."""
        ext = Path(name).suffix.lower()

        # If caller supplied column layout, use fixed-width directly
        if header_cols:
            return self._parse_fixed_width(data, name, header_cols)

        # Detect pipe-delimited by content
        sample = data[:4000]
        if b"|" in sample and sample.count(b"|") > 5:
            return self._parse_pipe_txt(data, name)

        if ext == ".csv":
            rows = self._parse_csv(data, name)
            if rows and len(list(rows[0].keys())) > 3:
                return rows
            # CSV with single giant key = fixed-width without delimiter
            log.warning(f"[CAD] {name} looks fixed-width (CSV got {len(rows)} rows "
                        f"with {len(rows[0].keys()) if rows else 0} cols) – need header")
            return []

        if ext == ".dbf":
            return self._parse_dbf(data, name)

        # .txt with no pipes and no extension hint = fixed-width
        # Return empty and let the load() method handle it with header
        log.warning(f"[CAD] {name} appears fixed-width but no header supplied")
        return []

    @staticmethod
    def _get(row: dict, *keys: str) -> str:
        for k in keys:
            v = row.get(k, "")
            if v and str(v).strip() not in ("", "None", "NULL", "N/A"):
                return str(v).strip()
        return ""

    def _index_row(self, row: dict):
        owner = self._get(row,
            "OWNER_NAME", "OWNERNAME", "OWNER", "OWN1", "OWNER1", "PROP_OWNER")
        if not owner:
            return

        site_num   = self._get(row, "SITUS_NUM",    "SITE_NUM",   "STR_NUM")
        site_str   = self._get(row, "SITUS_STREET", "STR_NAME",   "STREET_NAME", "SITUS_STR")
        site_full  = self._get(row, "SITUS",        "PROP_ADDR",  "SITE_ADDR",
                                    "SITEADDR",      "ADDRESS",    "PROP_ADDRESS",
                                    "SITE_ADDRESS",  "SITUS_ADDRESS")
        if not site_full and (site_num or site_str):
            site_full = (site_num + " " + site_str).strip()

        site_city  = self._get(row, "SITUS_CITY",  "SITE_CITY",  "SITECITY",
                                    "PROP_CITY",    "CITY_NAME",  "CITY")
        site_state = self._get(row, "SITUS_STATE", "SITE_STATE", "STATE") or "TX"
        site_zip   = self._get(row, "SITUS_ZIP",   "SITE_ZIP",   "SITEZIP",
                                    "PROP_ZIP",     "ZIP5",       "ZIP")

        mail_addr  = self._get(row, "MAIL_ADDR1",  "MAILADR1",   "ADDR_1",
                                    "MAIL_ADDRESS", "MAILING_ADDRESS", "MAIL_STR",
                                    "MAIL_ADDR",    "ADDR1",   "ADDRESS1")  # AGENT_OWNER uses ADDR1
        mail_city  = self._get(row, "MAIL_CITY",   "MAILCITY",   "CITY",   "MAIL_CITY2")
        mail_state = self._get(row, "MAIL_STATE",  "MAILSTATE",  "STATE")  or "TX"
        mail_zip   = self._get(row, "MAIL_ZIP",    "MAILZIP",    "ZIP",    "ZIP5")

        entry = dict(
            prop_address=site_full, prop_city=site_city,
            prop_state=site_state,  prop_zip=site_zip,
            mail_address=mail_addr, mail_city=mail_city,
            mail_state=mail_state,  mail_zip=mail_zip,
        )

        parts = owner.split(",", 1)
        if len(parts) == 2:
            last, first = parts[0].strip(), parts[1].strip()
            keys = [
                normalize(f"{first} {last}"),
                normalize(f"{last} {first}"),
                normalize(f"{last}, {first}"),
                normalize(owner),
            ]
        else:
            sp = owner.strip().split()
            keys = [normalize(owner)]
            if len(sp) >= 2:
                keys.append(normalize(f"{sp[-1]} {' '.join(sp[:-1])}"))
                keys.append(normalize(f"{' '.join(sp[:-1])}, {sp[-1]}"))
        for k in keys:
            if k:
                self._by_name[k] = entry

    def load(self):
        if self._loaded:
            return
        self._loaded = True

        url = retry_call(self._find_latest_export_url, label="CAD-url")
        if not url:
            log.warning("[CAD] no export URL – address enrichment skipped")
            return

        raw = retry_call(self._download, url, label="CAD-dl")
        if not raw:
            return

        rows: list[dict] = []
        try:
            zf = zipfile.ZipFile(io.BytesIO(raw))
            log.info(f"[CAD] ZIP contains {len(zf.namelist())} files:")
            for info in zf.infolist():
                log.info(f"  {info.filename}  ({info.file_size:,} bytes)")

            names = zf.namelist()

            # ── Step A: Find and parse header files for column layouts ───────
            header_map: dict[str, list[tuple[str, int, int]]] = {}
            for n in names:
                if "HEADER" in n.upper() and Path(n).suffix.upper() == ".TXT":
                    hdata = zf.read(n)
                    # Always log raw header content so we can see real column names
                    log.info(f"[CAD] header raw ({n}, {len(hdata)}b): "
                             f"{hdata.decode('latin-1','replace').strip()[:200]!r}")
                    cols = self._parse_header_txt(hdata, n)
                    if cols:
                        # Map header to its data file by stripping "HEADER"
                        base = n.upper().replace("_HEADER", "").replace("HEADER_", "")
                        header_map[base] = cols
                        header_map[Path(n).stem.upper()] = cols
                        # Extra: also map with OWNER_AGENT ↔ AGENT_OWNER swapped
                        swapped = base.replace("OWNER_AGENT", "AGENT_OWNER")                                      .replace("AGENT_OWNER", "OWNER_AGENT")
                        header_map[swapped] = cols
                        log.info(f"[CAD] header {n} → {len(cols)} columns: "
                                 f"{[c[0] for c in cols[:8]]}")
                    else:
                        # Header didn't parse as positional – might be field-name-only
                        # Treat as pipe-delimited field list (positional columns)
                        field_names = [p.strip().upper()
                                       for p in hdata.decode('latin-1','replace').split('|')
                                       if p.strip()]
                        if len(field_names) >= 3:
                            log.info(f"[CAD] header fields only ({n}): {field_names}")
                            # Store as field-name list for positional matching
                            header_map[Path(n).stem.upper() + "_FIELDS"] = field_names

            # ── Step B: Target files in priority order ───────────────────────
            # Priority 1: AGENT_OWNER (owner name + mailing address)
            owner_files = sorted(
                [n for n in names
                 if ("AGENT_OWNER" in n.upper() or "OWNER_AGENT" in n.upper()
                     or "OWNER" in n.upper())
                 if Path(n).suffix.upper() in (".TXT", ".CSV", ".DBF")
                 if zf.getinfo(n).file_size > 100_000
                 if "HEADER" not in n.upper()],
                key=lambda n: zf.getinfo(n).file_size, reverse=True
            )
            # Priority 2: APPRAISAL_INFO
            info_files = [n for n in names
                          if "APPRAISAL_INFO" in n.upper()
                          if Path(n).suffix.upper() in (".TXT", ".CSV", ".DBF")
                          if zf.getinfo(n).file_size > 1_000_000
                          if "HEADER" not in n.upper()]
            # Priority 3: other large files
            big_files = sorted(
                [n for n in names
                 if Path(n).suffix.upper() in (".TXT", ".CSV", ".DBF")
                 if zf.getinfo(n).file_size > 1_000_000
                 if "HEADER" not in n.upper()
                 if "DETAIL" not in n.upper()
                 if "IMPROVEMENT" not in n.upper()],
                key=lambda n: zf.getinfo(n).file_size, reverse=True
            )

            for candidate_list in (owner_files, info_files, big_files):
                for name in candidate_list:
                    sz = zf.getinfo(name).file_size
                    log.info(f"[CAD] trying: {name} ({sz:,} bytes)")
                    data_bytes = zf.read(name)

                    # Find matching header layout
                    cols = None
                    name_upper = name.upper()
                    name_stem  = Path(name).stem.upper()
                    for hkey, hcols in header_map.items():
                        if not isinstance(hcols, list) or not hcols:
                            continue
                        if not isinstance(hcols[0], tuple):
                            continue  # field-name list, skip
                        hkey_clean = hkey.replace("_HEADER","").replace("HEADER_","")
                        # Flexible match: handle OWNER_AGENT <-> AGENT_OWNER swaps
                        swapped_name = name_upper.replace("AGENT_OWNER","OWNER_AGENT")
                        swapped_key  = hkey_clean.replace("OWNER_AGENT","AGENT_OWNER")
                        if (hkey_clean in name_upper or name_upper in hkey_clean
                                or name_stem in hkey or hkey in name_stem
                                or swapped_name in hkey_clean
                                or hkey_clean in swapped_name
                                or swapped_key in name_upper):
                            cols = hcols
                            log.info(f"[CAD] matched header '{hkey}' to {name}: "
                                     f"{[c[0] for c in cols[:8]]}")
                            break

                    # If no header found but file looks fixed-width, use known GCAD layouts
                    sample = data_bytes[:4000]
                    is_pipe = b"|" in sample and sample.count(b"|") > 5
                    if not cols and not is_pipe:
                        cols = self._guess_fixed_width_layout(name, data_bytes)
                    elif cols:
                        # Validate: if header-derived cols don't look right, fall back to guess
                        test_rows = self._parse_fixed_width(data_bytes[:4000], name, cols)
                        if test_rows:
                            test_owner = self._get(test_rows[0],
                                "OWNER_NAME","OWNERNAME","OWNER","OWN1","NAME")
                            # Owner should look like a name, not a numeric ID
                            if test_owner and re.match(r'^[\d\s]+$', test_owner.strip()):
                                log.warning(f"[CAD] header layout gave numeric owner "
                                            f"{test_owner!r} – falling back to guess")
                                cols = self._guess_fixed_width_layout(name, data_bytes)

                    rows = self._auto_parse(data_bytes, name, cols)
                    if rows:
                        log.info(f"[CAD] sample keys: {list(rows[0].keys())[:16]}")
                        # Validate we got useful data
                        owner_col = self._get(rows[0],
                            "OWNER_NAME","OWNERNAME","OWNER","OWN1","NAME")
                        if owner_col or len(rows) > 100:
                            log.info(f"[CAD] sample owner: {owner_col!r}")
                            break
                        else:
                            log.warning(f"[CAD] {name}: no owner column found, trying next")
                            rows = []
                if rows:
                    break

        except zipfile.BadZipFile:
            log.warning("[CAD] not a ZIP – trying raw parse")
            name_str = url.split("/")[-1]
            rows = self._auto_parse(raw, name_str)

        log.info(f"[CAD] indexing {len(rows):,} rows…")
        for row in rows:
            try:
                self._index_row(row)
            except Exception:
                pass
        log.info(f"[CAD] index built: {len(self._by_name):,} name keys")

    def _guess_fixed_width_layout(self, name: str,
                                   data: bytes) -> list[tuple[str, int, int]] | None:
        """
        Guess fixed-width column layout.
        1. Scan data bytes to find where owner names actually appear.
        2. Fall back to known layouts if scan fails.
        """
        import re as _re
        name_upper = name.upper()
        lines_raw  = data.split(b"\n")
        data_lines = [l.rstrip(b"\r") for l in lines_raw if len(l.rstrip(b"\r")) > 200]
        first_line = data_lines[0] if data_lines else lines_raw[0].rstrip(b"\r")
        rec_len    = len(first_line)
        log.info(f"[CAD] {name}: rec_len={rec_len}, scanning for name offset")

        is_owner = "AGENT_OWNER" in name_upper or "OWNER_AGENT" in name_upper
        is_appr  = "APPRAISAL_INFO" in name_upper

        if is_owner or is_appr:
            # ── Step 1: dynamic scan ────────────────────────────────────────
            name_off = self._scan_name_offset(data_lines[:50], rec_len)
            if name_off:
                addr_off = self._scan_addr_offset(data_lines[:50], name_off, rec_len)
                city_off = self._scan_city_offset(data_lines[:50],
                                                  (addr_off or name_off+75), rec_len)
                log.info(f"[CAD] scan: name_off={name_off} addr_off={addr_off} "
                         f"city_off={city_off}")
                if name_off and addr_off and city_off:
                    cols = [
                        ("PROP_ID",    0,          min(19, name_off)),
                        ("OWNER_NAME", name_off,   addr_off - name_off),
                        ("ADDR1",      addr_off,   min(75, city_off - addr_off)),
                        ("CITY",       city_off,   50),
                        ("STATE",      city_off+50, 2),
                        ("ZIP",        city_off+52, 10),
                    ]
                    log.info(f"[CAD] dynamic layout: {[(c[0],c[1]) for c in cols]}")
                    return cols

            # ── Step 2: try candidate static offsets ─────────────────────
            candidates = []
            if is_owner:
                # (name_off, addr_off, city_off, state_off, zip_off)
                candidates = [
                    (49, 169, 319, 369, 371),   # 1299-byte modern (OWNER_ID at 30)
                    (30, 105, 255, 305, 307),   # ~320-byte standard
                    (30,  75, 175, 225, 227),   # compact
                    (40, 115, 230, 280, 282),   # variant
                    (50, 170, 320, 370, 372),   # off-by-one modern
                ]
            else:  # APPRAISAL_INFO
                candidates = [
                    (547, 622, 697, 747, 749),
                    (300, 375, 450, 500, 502),
                ]

            for n_off, a_off, c_off, s_off, z_off in candidates:
                if n_off + 10 > rec_len:
                    continue
                sample = first_line[n_off:n_off+40].decode("latin-1","replace")
                if _re.match(r"[A-Za-z]{2}", sample.strip()):
                    log.info(f"[CAD] static candidate: name_off={n_off} "
                             f"sample={sample.strip()[:25]!r}")
                    w = min(75, a_off - n_off) if a_off > n_off else 75
                    aw = min(75, c_off - a_off) if a_off + 5 < rec_len else 75
                    return [
                        ("PROP_ID",    0,     min(19, n_off)),
                        ("OWNER_NAME", n_off, w),
                        ("ADDR1",      a_off, aw),
                        ("CITY",       c_off, 50),
                        ("STATE",      s_off,  2),
                        ("ZIP",        z_off, 10),
                    ]

            # ── Step 3: exhaustive single-byte scan ───────────────────────
            log.warning("[CAD] all static layouts failed, exhaustive scan")
            for off in range(19, min(200, rec_len - 40)):
                sample = first_line[off:off+40].decode("latin-1","replace")
                if _re.match(r"^[A-Z][A-Z ,.\-']+$", sample.strip()) \
                        and len(sample.strip()) >= 8:
                    log.info(f"[CAD] exhaustive hit at {off}: {sample.strip()[:25]!r}")
                    return [
                        ("PROP_ID",    0,      min(19, off)),
                        ("OWNER_NAME", off,    75),
                        ("ADDR1",      off+75, 75),
                        ("CITY",       off+150,50),
                        ("STATE",      off+200, 2),
                        ("ZIP",        off+202,10),
                    ]

        return None

    @staticmethod
    def _scan_name_offset(lines: list[bytes], rec_len: int) -> int | None:
        """
        Find where owner names appear.
        Owner names look like: "SMITH, JOHN" or "ABC PROPERTIES LLC" or "JONES MARY E"
        They have: commas OR mixed short/long words OR "LLC/INC/TRUST/LTD"
        Cities look like: "GALVESTON    TX" or "FRIENDSWOOD    TX" (city + state abbrev)
        We reject patterns that look like city+state.
        """
        import re as _re
        name_pat  = _re.compile(rb"[A-Z][A-Z ,.\-']{9,}[A-Z]")
        # City pattern: city name followed by 2-char state abbreviation with spaces
        city_pat  = _re.compile(rb"[A-Z]{4,}\s{2,}[A-Z]{2}\s*$")
        # Name quality indicators: comma (LAST, FIRST) or LLC/INC/CORP/TRUST
        name_qual = _re.compile(rb"(,\s*[A-Z]|LLC|INC|CORP|TRUST|LTD|L\.P\.|ESTATE)")

        votes: dict[int, int] = {}
        for line in lines:
            if len(line) < 60:
                continue
            for m in name_pat.finditer(line[19:]):
                off = m.start() + 19
                val = m.group()
                val_str = val.decode("latin-1","replace")

                # Reject if too few distinct chars
                if len(set(val_str.replace(" ","").replace(",",""))) < 3:
                    continue
                # Reject if it looks like a city+state pattern
                if city_pat.search(val):
                    continue
                # Boost score if it has name-quality indicators
                score = 1
                if name_qual.search(val):
                    score = 3
                elif b"," in val:
                    score = 2
                votes[off] = votes.get(off, 0) + score

        if not votes:
            return None
        best = max(votes, key=votes.get)
        return best if votes[best] >= 2 else None

    @staticmethod
    def _scan_addr_offset(lines: list[bytes], name_off: int, rec_len: int) -> int | None:
        """Find mailing address: digits followed by uppercase street name."""
        import re as _re
        pat = _re.compile(rb"\d{1,6} +[A-Z][A-Z ]{5,}")
        for line in lines:
            search_from = name_off + 20
            if len(line) < search_from + 20:
                continue
            for m in pat.finditer(line[search_from:]):
                off = m.start() + search_from
                if off > name_off + 5:
                    return off
        return None

    @staticmethod
    def _scan_city_offset(lines: list[bytes], after_off: int, rec_len: int) -> int | None:
        """Find city: uppercase word(s) after address block."""
        import re as _re
        pat = _re.compile(rb"[A-Z][A-Z ]{4,}")
        search_from = after_off + 30
        for line in lines:
            if len(line) < search_from + 10:
                continue
            window = line[search_from: min(search_from+200, len(line))]
            for m in pat.finditer(window):
                off = m.start() + search_from
                val = m.group().decode("latin-1","replace").strip()
                if len(val) >= 5 and not val.replace(" ","").isdigit():
                    return off
        return None


    def lookup(self, owner: str) -> dict:
        if not self._loaded:
            self.load()
        parts = owner.split(",", 1)
        if len(parts) == 2:
            last, first = parts[0].strip(), parts[1].strip()
            keys = [normalize(f"{first} {last}"), normalize(f"{last} {first}"),
                    normalize(f"{last}, {first}"), normalize(owner)]
        else:
            sp = owner.strip().split()
            keys = [normalize(owner)]
            if len(sp) >= 2:
                keys.append(normalize(f"{sp[-1]} {' '.join(sp[:-1])}"))
        for k in keys:
            if k in self._by_name:
                return self._by_name[k]
        return {}


# ─── Clerk AVA Scraper ─────────────────────────────────────────────────────────

class ClerkScraper:

    def __init__(self):
        self.records: list[dict] = []
        self._seen: set[str] = set()
        now = datetime.now()
        self.date_from = (now - timedelta(days=LOOK_BACK_DAYS)).strftime("%m/%d/%Y")
        self.date_to   = now.strftime("%m/%d/%Y")

    async def scrape(self) -> list[dict]:
        if not HAS_PLAYWRIGHT:
            log.error("[Clerk] Playwright not installed")
            return []
        log.info(f"[Clerk] range: {self.date_from} → {self.date_to}")
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage",
                      "--disable-blink-features=AutomationControlled"],
            )
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 900},
            )
            page = await ctx.new_page()
            page.set_default_timeout(45_000)
            try:
                await self._run(page)
            except Exception as exc:
                log.error(f"[Clerk] fatal: {exc}", exc_info=True)
                await self._ss(page, "fatal")
            finally:
                await browser.close()
        log.info(f"[Clerk] total records collected: {len(self.records)}")
        return self.records

    async def _run(self, page):
        await page.goto(CLERK_AVA_URL, wait_until="domcontentloaded", timeout=60_000)
        await self._dismiss(page)
        await asyncio.sleep(3)
        await self._ss(page, "01_loaded")
        log.info(f"[Clerk] page title: {await page.title()}")
        log.info(f"[Clerk] URL: {page.url}")

        # Run per-doc-type searches
        for attempt in range(RETRY_ATTEMPTS):
            try:
                await self._search_doc_types(page)
                break
            except Exception as exc:
                log.warning(f"[Clerk] doc-type search attempt {attempt+1}: {exc}")
                await asyncio.sleep(RETRY_DELAY)
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=60_000)
                    await self._dismiss(page)
                    await asyncio.sleep(3)
                except Exception:
                    pass

        # Also run a broad date-range search
        for attempt in range(RETRY_ATTEMPTS):
            try:
                await self._search_broad(page)
                break
            except Exception as exc:
                log.warning(f"[Clerk] broad search attempt {attempt+1}: {exc}")
                await asyncio.sleep(RETRY_DELAY)
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=60_000)
                    await self._dismiss(page)
                    await asyncio.sleep(3)
                except Exception:
                    pass

    async def _dismiss(self, page):
        for text in ("Accept", "OK", "Agree", "Continue", "I Agree", "Close", "Got it"):
            try:
                btn = page.get_by_role("button", name=re.compile(f"^{text}$", re.I))
                if await btn.count():
                    await btn.first.click(timeout=3000)
                    await asyncio.sleep(0.5)
            except Exception:
                pass
        for sel in ("[data-dismiss='modal']", ".modal-close", "button.close",
                    '[aria-label="Close"]', ".close-btn"):
            try:
                el = page.locator(sel)
                if await el.count():
                    await el.first.click(timeout=2000)
                    await asyncio.sleep(0.5)
            except Exception:
                pass

    async def _find_and_click_tab(self, page, labels: list[str]) -> bool:
        for label in labels:
            for finder in (
                lambda l: page.get_by_role("tab", name=re.compile(l, re.I)),
                lambda l: page.get_by_role("link", name=re.compile(l, re.I)),
                lambda l: page.get_by_text(re.compile(f"^{l}$", re.I)),
            ):
                try:
                    el = finder(label)
                    if await el.count():
                        await el.first.click()
                        await asyncio.sleep(1.5)
                        log.info(f"[Clerk] clicked tab: {label}")
                        return True
                except Exception:
                    pass
        return False

    async def _search_doc_types(self, page):
        log.info("[Clerk] starting per-doc-type search…")
        tab_found = await self._find_and_click_tab(page, [
            "Document Type", "Doc Type", "Instrument Type",
            "Type Search", "Advanced Search", "Advanced",
        ])
        if not tab_found:
            log.info("[Clerk] no doc-type tab – skipping targeted search")
            return
        await self._ss(page, "02_doctype_tab")

        for code in AVA_SEARCH_CODES:
            try:
                await self._clear_form(page)
                await self._fill_form(page, doc_type=code,
                                      date_from=self.date_from, date_to=self.date_to)
                await self._harvest(page, label=code)
            except Exception as exc:
                log.warning(f"[Clerk] {code} search failed: {exc}")
            await asyncio.sleep(0.8)

    async def _search_broad(self, page):
        log.info("[Clerk] starting broad date-range search…")
        await self._find_and_click_tab(page, [
            "Quick Search", "Date Search", "Search", "Filing Date", "General",
        ])
        await self._ss(page, "03_broad_tab")
        try:
            await self._fill_form(page, doc_type=None,
                                  date_from=self.date_from, date_to=self.date_to)
            await self._harvest(page, label="BROAD")
        except Exception as exc:
            log.warning(f"[Clerk] broad search error: {exc}")

    async def _clear_form(self, page):
        """Clear all form inputs before a new search."""
        try:
            # Clear all visible text inputs
            els = page.locator('input[type="text"]:visible, input:not([type]):visible')
            n = await els.count()
            for i in range(n):
                try:
                    el = els.nth(i)
                    await el.triple_click()
                    await el.press("Delete")
                    await asyncio.sleep(0.05)
                except Exception:
                    pass
        except Exception:
            pass

    async def _fill_form(self, page, doc_type, date_from, date_to):
        """
        Fill AVA Angular Material search form.
        
        Known DOM structure (confirmed from CI log 2026-04-28):
          mat-input-0  placeholder='MM/DD/YYYY'      → Recorded Date From
          mat-input-1  placeholder='MM/DD/YYYY'      → Recorded Date To
          mat-input-2  placeholder='Document Type'   → Document Type (aria-label='Number')
        
        Angular Material (mat-input) requires click() to focus, then
        type() character-by-character to trigger ngModel binding.
        fill() and JS nativeInputValueSetter do NOT trigger Angular's
        change detection reliably.
        """
        # Wait for Angular Material inputs to render
        for wait_sel in ('#mat-input-0', 'input[placeholder="MM/DD/YYYY"]',
                         'input[type="text"]', 'input'):
            try:
                await page.wait_for_selector(wait_sel, state="visible", timeout=10_000)
                log.info(f"[Clerk] form ready (matched {wait_sel!r})")
                break
            except Exception:
                pass
        await asyncio.sleep(1.0)

        # ── Log every input in the DOM so we can see real attribute names ──────
        await self._log_inputs(page)

        # ── Get ordered list of all visible text inputs ───────────────────────
        visible = await self._visible_text_inputs(page)
        log.info(f"[Clerk] {len(visible)} visible text inputs found")

        # ── Doc type ──────────────────────────────────────────────────────────
        if doc_type:
            filled = False
            # Known selector from DOM inspection: placeholder='Document Type'
            for sel in [
                'input[placeholder="Document Type"]',      # confirmed from logs
                '#mat-input-2',                            # confirmed mat-input id
                'input[placeholder*="Document"]',
                'input[placeholder*="Type"]',
                'input[id*="docType"]', 'input[id*="type"]',
                'input[aria-label*="Type"]', 'input[aria-label*="Document"]',
            ]:
                try:
                    el = page.locator(sel).first
                    if await el.is_visible(timeout=2000):
                        await self._angular_fill(page, el, doc_type)
                        log.info(f"[Clerk] doc_type via {sel!r}: {doc_type}")
                        filled = True
                        break
                except Exception:
                    pass
            if not filled and visible:
                try:
                    await self._angular_fill(page, visible[0], doc_type)
                    log.info(f"[Clerk] doc_type via pos[0] fallback: {doc_type}")
                except Exception:
                    pass

        # ── Date-from ─────────────────────────────────────────────────────────
        # Confirmed from CI logs: mat-input-0 with placeholder='MM/DD/YYYY' is date-from
        filled_from = False
        for sel in [
            '#mat-input-0',                          # confirmed Angular mat-input id
            'input[placeholder="MM/DD/YYYY"]:nth-of-type(1)',
            'input[id*="dateFrom"]', 'input[id*="DateFrom"]',
            'input[id*="beginDate"]', 'input[id*="RecordedDateFrom"]',
            'input[aria-label*="From"]', 'input[aria-label*="Start"]',
            'input[name*="from"]', 'input[name*="begin"]',
        ]:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=2000):
                    await self._angular_fill(page, el, date_from)
                    log.info(f"[Clerk] date_from via {sel!r}: {date_from}")
                    filled_from = True
                    break
            except Exception:
                pass

        # Positional: mat-input-0 = index 2 in DOM order (after Last Name, First Name)
        if not filled_from:
            date_inputs = page.locator('input[placeholder="MM/DD/YYYY"]')
            try:
                n = await date_inputs.count()
                if n >= 1:
                    await self._angular_fill(page, date_inputs.nth(0), date_from)
                    log.info(f"[Clerk] date_from via MM/DD/YYYY[0]: {date_from}")
                    filled_from = True
            except Exception:
                pass

        if not filled_from and len(visible) >= 3:
            try:
                await self._angular_fill(page, visible[2], date_from)
                log.info(f"[Clerk] date_from via pos[2] fallback: {date_from}")
                filled_from = True
            except Exception:
                pass

        # ── Date-to ───────────────────────────────────────────────────────────
        # Confirmed: mat-input-1 with placeholder='MM/DD/YYYY' is date-to
        filled_to = False
        for sel in [
            '#mat-input-1',                          # confirmed Angular mat-input id
            'input[id*="dateTo"]', 'input[id*="DateTo"]',
            'input[id*="endDate"]', 'input[id*="RecordedDateTo"]',
            'input[aria-label*="To"]', 'input[aria-label*="End"]',
            'input[name*="to"]', 'input[name*="end"]',
        ]:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=2000):
                    await self._angular_fill(page, el, date_to)
                    log.info(f"[Clerk] date_to via {sel!r}: {date_to}")
                    filled_to = True
                    break
            except Exception:
                pass

        # Positional: second MM/DD/YYYY input
        if not filled_to:
            date_inputs = page.locator('input[placeholder="MM/DD/YYYY"]')
            try:
                n = await date_inputs.count()
                if n >= 2:
                    await self._angular_fill(page, date_inputs.nth(1), date_to)
                    log.info(f"[Clerk] date_to via MM/DD/YYYY[1]: {date_to}")
                    filled_to = True
                elif n == 1:
                    # Only one date field; already filled as date_from - skip
                    pass
            except Exception:
                pass

        if not filled_to and visible:
            try:
                await self._angular_fill(page, visible[-1], date_to)
                log.info(f"[Clerk] date_to via pos[-1] fallback: {date_to}")
                filled_to = True
            except Exception:
                pass

        await self._ss(page, f"04_form_{doc_type or 'broad'}")

        # ── Submit ────────────────────────────────────────────────────────────
        submitted = False
        for label in ("Search", "Submit", "Find Records", "Go", "Run Search", "Find"):
            try:
                btn = page.get_by_role("button", name=re.compile(f"^{label}$", re.I))
                if await btn.count():
                    await btn.first.click()
                    await asyncio.sleep(5)
                    submitted = True
                    log.info(f"[Clerk] submitted via button: {label!r}")
                    break
            except Exception:
                pass
        if not submitted:
            try:
                await page.keyboard.press("Enter")
                await asyncio.sleep(5)
                log.info("[Clerk] submitted via Enter key")
            except Exception:
                pass

        try:
            await page.wait_for_load_state("networkidle", timeout=25_000)
        except Exception:
            await asyncio.sleep(4)

        await self._ss(page, f"05_results_{doc_type or 'broad'}")
        # Log page state for debugging
        try:
            title = await page.title()
            url   = page.url
            text  = await page.evaluate("() => document.body.innerText")
            snippet = text.replace("\n"," ")[:600]
            log.info(f"[Clerk] results page: title={title!r} url={url}")
            log.info(f"[Clerk] results text: {snippet!r}")
            # Also log count of mat-row elements
            mat_rows = await page.locator("mat-row, tr[mat-row], [class*=mat-row]").count()
            plain_rows = await page.locator("table tr").count()
            log.info(f"[Clerk] mat-rows={mat_rows}, table-rows={plain_rows}")
        except Exception as e:
            log.warning(f"[Clerk] page state log failed: {e}")

    async def _visible_text_inputs(self, page):
        """Return ordered list of visible text/date input locators."""
        inputs = []
        seen_bboxes = set()
        for sel in [
            'input[type="text"]', 'input[type="date"]',
            'input:not([type])', 'input[type="search"]',
            'input[type=""]',
        ]:
            try:
                els = page.locator(sel)
                n = await els.count()
                for i in range(n):
                    el = els.nth(i)
                    try:
                        if await el.is_visible(timeout=500):
                            bb = await el.bounding_box()
                            key = (round(bb["x"]), round(bb["y"])) if bb else None
                            if key and key not in seen_bboxes:
                                seen_bboxes.add(key)
                                inputs.append(el)
                    except Exception:
                        pass
            except Exception:
                pass
        return inputs

    async def _log_inputs(self, page):
        """Dump all input elements to CI log for selector debugging."""
        try:
            info = await page.evaluate("""() =>
                Array.from(document.querySelectorAll("input")).map(el => ({
                    t: el.type,
                    id: el.id,
                    nm: el.name,
                    ph: el.placeholder,
                    al: el.getAttribute("aria-label"),
                    cl: el.className.slice(0, 50),
                    vis: el.offsetParent !== null,
                }))
            """)
            log.info(f"[Clerk] DOM inputs ({len(info)}):")
            for inp in info:
                log.info(
                    f"  type={inp['t']!r:8} id={inp['id']!r:30} "
                    f"placeholder={inp['ph']!r:25} aria-label={inp['al']!r:25} "
                    f"visible={inp['vis']}"
                )
        except Exception as exc:
            log.warning(f"[Clerk] input enumeration failed: {exc}")

    @staticmethod
    async def _fill_input(el, value: str):
        """Triple-click to select all, fill value, then Tab to trigger change events."""
        await el.triple_click()
        await el.fill(value)
        await el.press("Tab")
        await asyncio.sleep(0.25)

    @staticmethod
    async def _angular_fill(page, el, value: str):
        """
        Fill an Angular Material (mat-input) field correctly.
        Angular requires the actual keyboard events to trigger ngModel updates.
        Strategy: click → Ctrl+A → Delete → type char-by-char → Tab
        """
        try:
            await el.click()
            await asyncio.sleep(0.15)
            await el.press("Control+a")
            await el.press("Delete")
            await el.type(value, delay=40)   # type() sends real key events
            await el.press("Tab")
            await asyncio.sleep(0.3)
        except Exception:
            # Fallback: fill() + dispatch input event
            try:
                await el.triple_click()
                await el.fill(value)
                await page.evaluate(
                    "(el) => el.dispatchEvent(new Event('input', {bubbles:true}))",
                    await el.element_handle()
                )
                await el.press("Tab")
                await asyncio.sleep(0.3)
            except Exception:
                pass

    async def _js_fill_nth_input(self, page, idx: int, value: str, label: str):
        """
        Use JavaScript nativeInputValueSetter to force-fill a React-controlled input.
        Works even when Playwright fill() doesn't trigger React's onChange.
        idx: 0-based index, negative counts from end.
        """
        try:
            result = await page.evaluate(f"""(val) => {{
                const inputs = Array.from(document.querySelectorAll('input'))
                    .filter(el => el.offsetParent !== null);
                const idx = {idx};
                const el = idx < 0 ? inputs[inputs.length + idx] : inputs[idx];
                if (!el) return false;
                const nativeInputValueSetter = Object.getOwnPropertyDescriptor(
                    window.HTMLInputElement.prototype, 'value').set;
                nativeInputValueSetter.call(el, val);
                el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                return el.value;
            }}""", value)
            if result:
                log.info(f"[Clerk] {label} via JS nativeSetter: {value}")
        except Exception as exc:
            log.warning(f"[Clerk] JS fill failed for {label}: {exc}")

    async def _js_extract_results(self, page) -> list[dict]:
        """
        Extract search results directly from AVA's Angular DOM via JavaScript.
        AVA renders results in a virtual scroll container as repeated div rows.
        We extract every visible text row and parse doc number / type / date / party.
        """
        try:
            raw_rows = await page.evaluate("""() => {
                const results = [];

                // Strategy 1: find repeating result rows by common AVA class patterns
                const rowSelectors = [
                    '.search-result-row',
                    '.result-item',
                    '.document-row',
                    '[class*="result-row"]',
                    '[class*="search-result"]',
                    'cdk-virtual-scroll-viewport > div > div',
                    '.cdk-virtual-scroll-content-wrapper > *',
                    '.mat-list-item',
                    'mat-list-item',
                ];
                for (const sel of rowSelectors) {
                    const els = document.querySelectorAll(sel);
                    if (els.length > 0) {
                        els.forEach(el => {
                            const txt = el.innerText || el.textContent || '';
                            if (txt.trim().length > 10) results.push(txt.trim());
                        });
                        if (results.length > 0) break;
                    }
                }

                // Strategy 2: find ALL text nodes in the results container
                if (results.length === 0) {
                    const containers = document.querySelectorAll(
                        '[class*="result"], [class*="search"], [id*="result"], ' +
                        'cdk-virtual-scroll-viewport, .mat-table, mat-table'
                    );
                    for (const c of containers) {
                        const txt = c.innerText || c.textContent || '';
                        if (txt.length > 50) {
                            results.push(txt.trim());
                            break;
                        }
                    }
                }

                // Strategy 3: get all text from body, let Python parse it
                if (results.length === 0) {
                    results.push(document.body.innerText || '');
                }

                return results;
            }""")

            # Now parse the extracted text
            records = []
            full_text = " ".join(raw_rows)

            # Parse document records from the AVA results text
            # Pattern from observed output:
            # "2026019373 CERTIFIED COPY 4/28/2026 4:32:41 PM UNOFFICIAL"
            # "2026019372 RELEASE 4/28/2026 4:26:13 PM UNOFFICIAL"
            records = self._parse_ava_text(full_text, page.url)
            if records:
                log.info(f"[Clerk] JS extracted {len(records)} records from page text")
            return records

        except Exception as exc:
            log.warning(f"[Clerk] JS extraction failed: {exc}")
            return []

    def _parse_ava_text(self, text: str, url: str) -> list[dict]:
        """
        Parse AVA results text.
        Format (confirmed from CI log 2026-04-29):
          Results: N
          Document No  Document Type  Recorded Date  Party1  Party2  Legals
          2026019373  CERTIFIED COPY  4/28/2026 4:32:41 PM  UNOFFICIAL
          [detail block with same doc num]
          2026019372  RELEASE  4/28/2026 4:26:13 PM  UNOFFICIAL
          ...
        """
        records = []
        seen_in_text: set[str] = set()

        # Match doc number: 10-13 digit number at start of a token group
        # Followed by doc type words, then a date
        # Pattern: DOCNUM  DOC_TYPE_WORDS  MM/DD/YYYY  HH:MM:SS AM/PM  [PARTY]
        pattern = re.compile(
            r"(\d{7,13})"                       # doc number
            r"\s+"
            r"([A-Z][A-Z /&\-,]{2,50}?)"        # doc type (uppercase words)
            r"\s+"
            r"(\d{1,2}/\d{1,2}/\d{4})"          # date MM/DD/YYYY
            r"(?:\s+\d{1,2}:\d{2}:\d{2}\s+[AP]M)?"  # optional time
            r"(?:\s+([A-Z][A-Z .,&'-]{2,60}))?",  # optional party1
            re.MULTILINE
        )

        for m in pattern.finditer(text):
            doc_num  = m.group(1).strip()
            doc_type = m.group(2).strip().rstrip(",").strip()
            filed    = normalize_date(m.group(3))
            party1   = (m.group(4) or "").strip().rstrip("Page").strip()

            # Skip duplicate doc numbers (detail block appears twice in text)
            if doc_num in seen_in_text:
                continue
            seen_in_text.add(doc_num)

            # Skip obvious non-document tokens
            if len(doc_num) < 7:
                continue
            if doc_type.upper() in ("DOCUMENT NO", "DOC", "NUMBER", "RESULTS"):
                continue

            # Build direct URL to this document
            direct_url = f"https://ava.fidlar.com/TXGalveston/AvaWeb/#/searchresults/{doc_num}"

            records.append({
                "doc_num":   doc_num,
                "raw_type":  doc_type,
                "filed":     filed,
                "owner":     party1,
                "grantee":   "",
                "legal":     "",
                "amount":    None,
                "clerk_url": direct_url,
            })

        log.info(f"[Clerk] _parse_ava_text: {len(records)} records from "
                 f"{len(text)} chars of text")
        return records

    async def _harvest(self, page, label: str = ""):
        p_num = 0
        empty_streak = 0
        while p_num < 500:
            p_num += 1
            await asyncio.sleep(1)
            try:
                # Save first page HTML for debugging
                if p_num == 1:
                    try:
                        html = await page.content()
                        with open(f"/tmp/ava_results_{label}_p1.html", "w",
                                  encoding="utf-8", errors="replace") as f:
                            f.write(html)
                        log.info(f"[Clerk] saved results HTML: "
                                 f"/tmp/ava_results_{label}_p1.html")
                    except Exception:
                        pass

                # Primary: JS extraction (works with Angular virtual scroll)
                js_recs = await self._js_extract_results(page)
                if js_recs:
                    recs = [r for r in js_recs if self._accept(r)]
                    if recs:
                        self.records.extend(recs)
                        for r in recs:
                            self._seen.add(r.get("doc_num", str(r)[:80]))
                        empty_streak = 0
                        log.info(f"[Clerk][{label}] p{p_num}: +{len(recs)} JS "
                                 f"(total={len(self.records)})")
                    else:
                        empty_streak += 1
                    if not await self._next_page(page):
                        break
                    continue

                # Fallback: HTML parsing
                html = await page.content()
                recs = [r for r in self._parse_html(html, page.url)
                        if self._accept(r)]
                if recs:
                    self.records.extend(recs)
                    for r in recs:
                        self._seen.add(r.get("doc_num", str(r)))
                    empty_streak = 0
                    log.info(f"[Clerk][{label}] p{p_num}: +{len(recs)} "
                             f"(total={len(self.records)})")
                else:
                    empty_streak += 1
                    if empty_streak >= 2:
                        break
            except Exception as exc:
                log.warning(f"[Clerk] parse error p{p_num}: {exc}")
            if not await self._next_page(page):
                break

    def _accept(self, rec: dict) -> bool:
        key = rec.get("doc_num", "") or str(rec)[:80]
        if key in self._seen:
            return False
        return True

    async def _next_page(self, page) -> bool:
        for sel in (
            "button[aria-label='Next page']", "button[aria-label='Next']",
            "a[aria-label='Next page']", "button.next-page", ".pagination-next",
        ):
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=1500):
                    if await el.get_attribute("disabled") is None:
                        await el.click()
                        await asyncio.sleep(2)
                        return True
            except Exception:
                pass
        for text in ("Next", ">", "›", "»", "Next Page"):
            try:
                btn = page.get_by_role("button", name=re.compile(f"^{re.escape(text)}$"))
                if not await btn.count():
                    btn = page.get_by_role("link", name=re.compile(f"^{re.escape(text)}$"))
                if await btn.count():
                    if await btn.first.get_attribute("disabled") is None:
                        if await btn.first.get_attribute("aria-disabled") != "true":
                            await btn.first.click()
                            await asyncio.sleep(2)
                            return True
            except Exception:
                pass
        return False

    def _parse_html(self, html: str, url: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        records: list[dict] = []

        # ── Strategy 0: Angular Material mat-table (AVA uses this) ─────────
        # AVA renders results as <mat-table> with <mat-header-row> and <mat-row>
        # or <table mat-table> with <tr mat-header-row> and <tr mat-row>
        for mat_table in soup.find_all(["mat-table", "table"], attrs={"class": True}):
            classes = " ".join(mat_table.get("class", []))
            if "mat-table" not in classes and mat_table.name != "mat-table":
                if "cdk-table" not in classes:
                    continue
            # Get headers from mat-header-cell or th
            header_cells = mat_table.find_all(["mat-header-cell", "th"])
            headers = [h.get_text(" ", strip=True).lower() for h in header_cells]
            if not headers:
                continue
            log.info(f"[Clerk] mat-table headers: {headers}")
            # Get data rows from mat-row or tr[mat-row]
            for row_el in mat_table.find_all(["mat-row", "tr"]):
                if row_el.get("class") and "mat-header-row" in " ".join(row_el.get("class",[])):
                    continue
                cells = row_el.find_all(["mat-cell", "td"])
                if len(cells) < 2:
                    continue
                rec = self._from_tds(cells, headers, url)
                if rec:
                    records.append(rec)

        # ── Strategy 1: Plain HTML tables ─────────────────────────────────────
        if not records:
            for table in soup.find_all("table"):
                ths = table.find_all("th")
                headers = [th.get_text(" ", strip=True).lower() for th in ths]
                if not any(kw in " ".join(headers) for kw in
                           ["doc", "type", "instrument", "filed", "grantor",
                            "recorded", "book", "page", "number"]):
                    continue
                for tr in table.find_all("tr")[1:]:
                    tds = tr.find_all("td")
                    if len(tds) < 2:
                        continue
                    rec = self._from_tds(tds, headers, url)
                    if rec:
                        records.append(rec)

        # ── Strategy 2: Any repeated div/li rows ──────────────────────────────
        if not records:
            for el in soup.select(
                "mat-row, [class*='mat-row'], "
                "div.result-row, div.record-item, div[class*='document'], "
                "div[class*='result'], li.document-result, "
                "tr[class*='result'], tr[class*='document']"
            ):
                rec = self._from_card(el, url)
                if rec:
                    records.append(rec)

        # ── Strategy 3: Log what WAS found so we can debug ────────────────────
        if not records:
            # Log structural hints from the page
            all_text = soup.get_text(" ", strip=True)
            if re.search(r"\d{4,}", all_text):
                # Page has numeric content - might be results
                # Check for any element containing doc-number-like patterns
                for el in soup.find_all(string=re.compile(
                        r"\d{4,14}.*(LP|JUD|LIEN|LIS|PENDENS|FORECLOSURE)",
                        re.I)):
                    parent = el.parent
                    while parent and parent.name not in ("div","tr","li","section"):
                        parent = parent.parent
                    if parent:
                        rec = self._from_card(parent, url)
                        if rec:
                            records.append(rec)

            # Always log a snippet of the page for debugging
            snippet = all_text[:500].replace("\n", " ")
            log.info(f"[Clerk] page text snippet: {snippet!r}")

        # JSON in scripts
        if not records:
            for script in soup.find_all("script"):
                src = script.string or ""
                for m in re.finditer(
                    r'"(?:documents?|results?|records?|items?)":\s*(\[.*?\])',
                    src, re.DOTALL
                ):
                    try:
                        for item in json.loads(m.group(1)):
                            rec = self._from_json(item, url)
                            if rec:
                                records.append(rec)
                    except Exception:
                        pass

        # Filter to target doc types only
        out = []
        for rec in records:
            mapped = map_doc_type(rec.get("raw_type", ""))
            if mapped:
                rec["cat"], rec["cat_label"] = mapped
                out.append(rec)
        return out

    def _from_tds(self, tds, headers: list[str], url: str) -> dict | None:
        try:
            cells = [td.get_text(" ", strip=True) for td in tds]
            while len(cells) < len(headers):
                cells.append("")
            data = {headers[i]: cells[i] for i in range(len(headers))}

            def g(*keys):
                for k in keys:
                    for h in headers:
                        if k in h:
                            return data.get(h, "").strip()
                return ""

            doc_num  = g("instrument", "document number", "doc no", "doc#", "number", "id")
            doc_type = g("instrument type", "doc type", "type", "document type")
            filed    = g("recorded", "filed", "record date", "file date")
            grantor  = g("grantor", "party 1", "owner", "seller")
            grantee  = g("grantee", "party 2", "buyer")
            legal    = g("legal", "description")
            amount   = g("amount", "consideration", "value")

            direct_url = url
            for a in (td.find("a") for td in tds):
                if a and a.get("href"):
                    h = a["href"]
                    direct_url = h if h.startswith("http") else "https://ava.fidlar.com" + h
                    break

            if not (doc_num or doc_type):
                return None
            return {"doc_num": doc_num, "raw_type": doc_type,
                    "filed": normalize_date(filed), "owner": grantor,
                    "grantee": grantee, "legal": legal,
                    "amount": parse_amount(amount), "clerk_url": direct_url}
        except Exception:
            return None

    def _from_card(self, el, url: str) -> dict | None:
        try:
            text = el.get_text(" | ", strip=True)
            def rx(pat):
                m = re.search(pat, text, re.I)
                return m.group(1).strip() if m else ""
            doc_num  = rx(r"(?:Instrument|Document\s*(?:No|#|Number))[:\s#]+([A-Z0-9\-\.]+)")
            doc_type = rx(r"(?:Type|Doc\s*Type|Instrument\s*Type)[:\s]+([^\|]{3,40}?)(?:\||$)")
            filed    = rx(r"(?:Recorded|Filed|Date)[:\s]+(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})")
            grantor  = rx(r"(?:Grantor|Owner|Party\s*1)[:\s]+([^\|]{2,60}?)(?:\||$)")
            grantee  = rx(r"(?:Grantee|Party\s*2)[:\s]+([^\|]{2,60}?)(?:\||$)")
            amount_m = re.search(r"\$[\d,]+(?:\.\d{2})?", text)
            amount   = parse_amount(amount_m.group(0)) if amount_m else None
            link = el.find("a")
            direct_url = url
            if link and link.get("href"):
                h = link["href"]
                direct_url = h if h.startswith("http") else "https://ava.fidlar.com" + h
            if not (doc_num or doc_type):
                return None
            return {"doc_num": doc_num, "raw_type": doc_type.strip(),
                    "filed": normalize_date(filed), "owner": grantor,
                    "grantee": grantee, "legal": "",
                    "amount": amount, "clerk_url": direct_url}
        except Exception:
            return None

    def _from_json(self, item: dict, url: str) -> dict | None:
        try:
            def jg(*keys):
                for k in keys:
                    for ik in item:
                        if ik.lower().replace("_","") == k.lower().replace("_",""):
                            v = item[ik]
                            if v is not None and str(v).strip():
                                return str(v).strip()
                return ""
            doc_num  = jg("instrumentnumber","documentnumber","docnum","docno","id")
            doc_type = jg("instrumenttype","documenttype","doctype","type")
            filed    = jg("recordeddate","fileddate","recorddate","date")
            grantor  = jg("grantor","owner","party1")
            grantee  = jg("grantee","party2")
            legal    = jg("legaldescription","legal","description")
            amount   = parse_amount(jg("amount","consideration","value"))
            link     = jg("url","link","href","documenturl")
            direct_url = link if link.startswith("http") else url
            if not (doc_num or doc_type):
                return None
            return {"doc_num": doc_num, "raw_type": doc_type,
                    "filed": normalize_date(filed), "owner": grantor,
                    "grantee": grantee, "legal": legal,
                    "amount": amount, "clerk_url": direct_url}
        except Exception:
            return None

    @staticmethod
    async def _ss(page, name: str):
        try:
            await page.screenshot(path=f"/tmp/ava_{name}.png", full_page=False)
            log.info(f"[Clerk] screenshot /tmp/ava_{name}.png")
        except Exception:
            pass


# ─── enrichment ────────────────────────────────────────────────────────────────

def enrich_records(raw_records: list[dict], parcel_index: ParcelIndex) -> list[dict]:
    enriched = []
    for raw in raw_records:
        try:
            owner  = raw.get("owner", "")
            parcel = parcel_index.lookup(owner) if owner else {}
            rec: dict[str, Any] = {
                "doc_num":      raw.get("doc_num", ""),
                "doc_type":     raw.get("raw_type", ""),
                "filed":        raw.get("filed", ""),
                "cat":          raw.get("cat", ""),
                "cat_label":    raw.get("cat_label", ""),
                "owner":        owner,
                "grantee":      raw.get("grantee", ""),
                "amount":       raw.get("amount"),
                "legal":        raw.get("legal", ""),
                "prop_address": parcel.get("prop_address", ""),
                "prop_city":    parcel.get("prop_city", ""),
                "prop_state":   parcel.get("prop_state", "TX"),
                "prop_zip":     parcel.get("prop_zip", ""),
                "mail_address": parcel.get("mail_address", ""),
                "mail_city":    parcel.get("mail_city", ""),
                "mail_state":   parcel.get("mail_state", "TX"),
                "mail_zip":     parcel.get("mail_zip", ""),
                "clerk_url":    raw.get("clerk_url", ""),
            }
            rec["score"], rec["flags"] = score_record(rec)
            enriched.append(rec)
        except Exception as exc:
            log.warning(f"[Enrich] skipping: {exc}")
    enriched.sort(key=lambda r: r["score"], reverse=True)
    return enriched


def build_output(records: list[dict], date_from: str, date_to: str) -> dict:
    return {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Galveston County Clerk (AVA) + GCAD Parcel Data",
        "date_range":   {"from": date_from, "to": date_to},
        "total":        len(records),
        "with_address": sum(1 for r in records
                            if r.get("prop_address") or r.get("mail_address")),
        "records":      records,
    }


def save_json(data: dict, *paths: str):
    for p in paths:
        path = Path(p)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
        log.info(f"[Output] {data['total']} records → {path}")


def save_ghl_csv(records: list[dict], path_str: str):
    path = Path(path_str)
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "First Name","Last Name",
        "Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number",
        "Amount/Debt Owed","Seller Score","Motivated Seller Flags",
        "Source","Public Records URL",
    ]
    def split_name(full):
        if not full: return "", ""
        if "," in full:
            la, fi = full.split(",", 1)
            return fi.strip().title(), la.strip().title()
        sp = full.strip().split()
        return sp[0].title(), " ".join(sp[1:]).title() if len(sp)>1 else ""
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for rec in records:
            fi, la = split_name(rec.get("owner",""))
            amt = f"${rec['amount']:,.2f}" if rec.get("amount") else ""
            w.writerow({
                "First Name": fi, "Last Name": la,
                "Mailing Address":  rec.get("mail_address",""),
                "Mailing City":     rec.get("mail_city",""),
                "Mailing State":    rec.get("mail_state",""),
                "Mailing Zip":      rec.get("mail_zip",""),
                "Property Address": rec.get("prop_address",""),
                "Property City":    rec.get("prop_city",""),
                "Property State":   rec.get("prop_state",""),
                "Property Zip":     rec.get("prop_zip",""),
                "Lead Type":              rec.get("cat_label",""),
                "Document Type":          rec.get("doc_type",""),
                "Date Filed":             rec.get("filed",""),
                "Document Number":        rec.get("doc_num",""),
                "Amount/Debt Owed":       amt,
                "Seller Score":           rec.get("score",0),
                "Motivated Seller Flags": "; ".join(rec.get("flags",[])),
                "Source":                 "Galveston County Clerk",
                "Public Records URL":     rec.get("clerk_url",""),
            })
    log.info(f"[Output] GHL CSV → {path} ({len(records)} rows)")


# ─── main ──────────────────────────────────────────────────────────────────────

async def main():
    t0 = datetime.now()
    log.info("="*60)
    log.info("Galveston County Motivated Seller Scraper  v2")
    log.info(f"Look-back: {LOOK_BACK_DAYS} days")
    log.info("="*60)

    date_to   = datetime.now().strftime("%Y-%m-%d")
    date_from = (datetime.now()-timedelta(days=LOOK_BACK_DAYS)).strftime("%Y-%m-%d")

    log.info("[Step 1/3] Loading GCAD parcel data…")
    parcel_index = ParcelIndex()
    parcel_index.load()

    log.info("[Step 2/3] Scraping Galveston County Clerk portal…")
    scraper = ClerkScraper()
    raw_records = await scraper.scrape()

    if not raw_records:
        log.warning("[Step 2/3] No records – writing empty output.")

    log.info("[Step 3/3] Enriching and scoring…")
    records = enrich_records(raw_records, parcel_index)
    output  = build_output(records, date_from, date_to)

    save_json(output, "dashboard/records.json", "data/records.json")
    save_ghl_csv(records, f"data/ghl_export_{datetime.now().strftime('%Y%m%d')}.csv")

    elapsed = (datetime.now()-t0).seconds
    log.info(f"\n✅  Done in {elapsed}s — {len(records)} leads | "
             f"{output['with_address']} with address")
    if records:
        top = records[0]
        log.info(f"   Top: {top['owner']} | {top['cat_label']} | "
                 f"score={top['score']} | {top.get('prop_address') or 'no address'}")


if __name__ == "__main__":
    asyncio.run(main())
