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
            "PY_OWNER_NAME",                            # APPRAISAL_INFO confirmed
            "APPR_OWNER_NAME",                          # APPRAISAL_INFO alt
            "OWNER_NAME", "OWNERNAME", "OWNER", "OWN1", "OWNER1", "PROP_OWNER")
        if not owner:
            return

        site_num   = self._get(row, "SITUS_NUM",    "SITE_NUM",   "STR_NUM")
        site_str   = self._get(row, "SITUS_STREET", "STR_NAME",   "STREET_NAME", "SITUS_STR")
        # Build situs/property address from components (APPRAISAL_INFO format)
        situs_prefx  = self._get(row, "SITUS_PREFX")
        situs_street = self._get(row, "SITUS_STREET")
        situs_suffix = self._get(row, "SITUS_SUFFIX")
        situs_num2   = self._get(row, "SITUS_NUM", "SITUS_NUM2")
        site_full    = self._get(row, "SITUS", "PROP_ADDR", "SITE_ADDR",
                                      "SITEADDR", "ADDRESS", "PROP_ADDRESS",
                                      "SITE_ADDRESS", "SITUS_ADDRESS")
        if not site_full:
            # Assemble from parts
            parts = [p for p in [site_num, situs_prefx, situs_street, situs_suffix] if p]
            if not parts:
                parts = [p for p in [situs_num2, situs_prefx, situs_street] if p]
            site_full = " ".join(parts).strip()

        site_city  = self._get(row, "SITUS_CITY",  "SITE_CITY", "SITECITY",
                                    "PROP_CITY",   "CITY_NAME", "CITY")
        site_state = self._get(row, "SITUS_STATE", "SITE_STATE","STATE") or "TX"
        site_zip   = self._get(row, "SITUS_ZIP",   "SITE_ZIP",  "SITEZIP",
                                    "PROP_ZIP",    "ZIP5",      "ZIP")

        mail_addr  = self._get(row,
            "PY_ADDR_LINE1",                            # APPRAISAL_INFO confirmed
            "MAIL_ADDR1", "MAILADR1", "ADDR_1",
            "MAIL_ADDRESS", "MAILING_ADDRESS", "MAIL_STR", "MAIL_ADDR",
            "ADDR1", "ADDRESS1")
        # Append line 2 if present
        mail_addr2 = self._get(row, "PY_ADDR_LINE2", "MAIL_ADDR2", "ADDR_2", "ADDR2")
        if mail_addr2 and mail_addr2 not in mail_addr:
            mail_addr = (mail_addr + " " + mail_addr2).strip()
        mail_city  = self._get(row, "PY_ADDR_CITY",  "MAIL_CITY", "MAILCITY", "CITY")
        mail_state = self._get(row, "PY_ADDR_STATE", "MAIL_STATE","MAILSTATE","STATE") or "TX"
        mail_zip   = self._get(row, "PY_ADDR_ZIP",   "MAIL_ZIP",  "MAILZIP",  "ZIP", "ZIP5")

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

    # ── confirmed column layouts from Appraisal_Export_Layout_-_8_0_30.xlsx ──
    # All positions are 0-indexed (layout doc uses 1-indexed, we subtract 1)

    # APPRAISAL_INFO.TXT - confirmed exact offsets
    APPRAISAL_INFO_COLS = [
        ("PROP_ID",        0,   12),
        ("PY_OWNER_NAME",  608,  70),   # Property Year Owner Name  ← primary owner
        ("PY_ADDR_LINE1",  693,  60),   # Mailing Address Line 1
        ("PY_ADDR_LINE2",  753,  60),   # Mailing Address Line 2
        ("PY_ADDR_CITY",   873,  50),   # Mailing City
        ("PY_ADDR_STATE",  923,  50),   # Mailing State
        ("PY_ADDR_ZIP",    978,   5),   # Mailing Zip (5-digit)
        ("SITUS_PREFX",   1039,  10),   # Situs Street Prefix
        ("SITUS_STREET",  1049,  50),   # Situs Street Name
        ("SITUS_SUFFIX",  1099,  10),   # Situs Suffix
        ("SITUS_CITY",    1109,  30),   # Situs City
        ("SITUS_ZIP",     1139,  10),   # Situs Zip
    ]

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

            # ── Priority 1: APPRAISAL_INFO.TXT (confirmed layout) ────────────
            appr_files = [n for n in names
                          if "APPRAISAL_INFO" in n.upper()
                          if Path(n).suffix.upper() == ".TXT"
                          if "HEADER" not in n.upper()
                          if zf.getinfo(n).file_size > 1_000_000]

            # ── Priority 2: AGENT_OWNER / OWNER_AGENT (dynamic scan) ─────────
            owner_files = sorted(
                [n for n in names
                 if ("AGENT_OWNER" in n.upper() or "OWNER_AGENT" in n.upper())
                 if Path(n).suffix.upper() in (".TXT", ".CSV", ".DBF")
                 if "HEADER" not in n.upper()
                 if zf.getinfo(n).file_size > 100_000],
                key=lambda n: zf.getinfo(n).file_size, reverse=True
            )

            for name in (appr_files + owner_files):
                sz = zf.getinfo(name).file_size
                log.info(f"[CAD] trying: {name} ({sz:,} bytes)")
                data_bytes = zf.read(name)

                if "APPRAISAL_INFO" in name.upper():
                    # Use confirmed layout, stream parse
                    rows = self._parse_appraisal_info(data_bytes)
                else:
                    # Dynamic scan for AGENT_OWNER
                    cols = self._guess_fixed_width_layout(name, data_bytes)
                    rows = self._auto_parse(data_bytes, name, cols)

                if rows:
                    owner_sample = self._get(rows[0],
                        "PY_OWNER_NAME","OWNER_NAME","OWNER","OWN1")
                    log.info(f"[CAD] sample keys: {list(rows[0].keys())[:10]}")
                    log.info(f"[CAD] sample owner: {owner_sample!r}")
                    # Validate owner looks like a name
                    if owner_sample and not re.match(r"^[\d\s]+$", owner_sample.strip()):
                        break
                    else:
                        log.warning(f"[CAD] {name}: owner looks numeric, trying next")
                        rows = []

        except zipfile.BadZipFile:
            log.warning("[CAD] not a ZIP – trying raw parse")
            rows = self._parse_appraisal_info(raw) or                    self._auto_parse(raw, url.split("/")[-1], None)

        log.info(f"[CAD] indexing {len(rows):,} rows…")
        for row in rows:
            try:
                self._index_row(row)
            except Exception:
                pass
        log.info(f"[CAD] index built: {len(self._by_name):,} name keys")

    def _parse_appraisal_info(self, data: bytes) -> list[dict]:
        """
        Stream-parse APPRAISAL_INFO.TXT using confirmed column offsets.
        Reads line-by-line to handle the 2GB file without OOM.
        Minimum record length needed: 1149 bytes (through SITUS_ZIP).
        """
        rows = []
        cols = self.APPRAISAL_INFO_COLS
        min_len = max(start + length for _, start, length in cols)
        count = 0
        blank_owner = 0

        lines = data.split(b"\n")
        for raw_line in lines:
            line = raw_line.rstrip(b"\r")
            if len(line) < min_len:
                continue
            try:
                row = {}
                for fname, start, length in cols:
                    row[fname] = line[start:start + length].decode(
                        "latin-1", "replace").strip()
                # Skip records with no owner name
                if not row["PY_OWNER_NAME"]:
                    blank_owner += 1
                    continue
                rows.append(row)
                count += 1
                if count == 1:
                    log.info(f"[CAD] APPRAISAL_INFO sample: "
                             f"owner={row['PY_OWNER_NAME']!r} "
                             f"addr={row['PY_ADDR_LINE1']!r} "
                             f"city={row['PY_ADDR_CITY']!r} "
                             f"situs={row['SITUS_PREFX'].strip()+row['SITUS_STREET']!r}")
            except Exception:
                pass

        log.info(f"[CAD] APPRAISAL_INFO: {len(rows):,} records "
                 f"(skipped {blank_owner:,} with blank owner)")
        return rows


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

        # Step 1: Broad date-range search (PROVEN to work — dates only, no doc type)
        # Returns up to 300 records across all doc types.
        for attempt in range(RETRY_ATTEMPTS):
            try:
                await self._search_broad_with_dates(page)
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

        # Step 2: Per-doc-type searches (dates + doc type) to capture
        # records beyond the 300-result cap and ensure full coverage.
        for attempt in range(RETRY_ATTEMPTS):
            try:
                await self._search_each_doc_type_with_dates(page)
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

    async def _search_broad_with_dates(self, page):
        """
        Broad date-range search — PROVEN WORKING in CI run 2026-04-30 12:19.
        Fills mat-input-0 (date_from) and mat-input-1 (date_to) via _angular_fill,
        leaves Document Type empty. Returns up to 300 mixed records.
        """
        log.info("[Clerk] running broad date-range search (proven approach)…")

        await self._find_and_click_tab(page, ["Search", "Quick Search"])
        await asyncio.sleep(1)
        await self._clear_form(page)

        # Wait for mat-input-0 to be present (confirmed in working run)
        try:
            await page.wait_for_selector('#mat-input-0', state='visible', timeout=10_000)
        except Exception:
            pass
        await asyncio.sleep(0.5)

        # Fill date_from (mat-input-0) — PROVEN WORKING
        filled_from = False
        el = page.locator('#mat-input-0').first
        if await el.is_visible(timeout=3000):
            await self._angular_fill(page, el, self.date_from)
            log.info(f"[Clerk] broad: date_from → {self.date_from}")
            filled_from = True

        if not filled_from:
            date_inputs = page.locator('input[placeholder="MM/DD/YYYY"]')
            if await date_inputs.count() >= 1:
                await self._angular_fill(page, date_inputs.nth(0), self.date_from)
                log.info(f"[Clerk] broad: date_from via placeholder → {self.date_from}")

        # Fill date_to (mat-input-1) — PROVEN WORKING
        filled_to = False
        el = page.locator('#mat-input-1').first
        if await el.is_visible(timeout=3000):
            await self._angular_fill(page, el, self.date_to)
            log.info(f"[Clerk] broad: date_to → {self.date_to}")
            filled_to = True

        if not filled_to:
            date_inputs = page.locator('input[placeholder="MM/DD/YYYY"]')
            if await date_inputs.count() >= 2:
                await self._angular_fill(page, date_inputs.nth(1), self.date_to)
                log.info(f"[Clerk] broad: date_to via placeholder → {self.date_to}")

        await self._ss(page, "broad_form_filled")

        # Submit
        for btn_label in ("Search", "Submit", "Find"):
            try:
                btn = page.get_by_role("button",
                      name=re.compile(f"^{btn_label}$", re.I))
                if await btn.count():
                    await btn.first.click()
                    log.info(f"[Clerk] broad: submitted via '{btn_label}'")
                    break
            except Exception:
                pass

        await asyncio.sleep(4)
        try:
            await page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            await asyncio.sleep(2)

        await self._log_result_state(page, "BROAD")
        await self._harvest(page, label="BROAD")

    async def _search_each_doc_type_with_dates(self, page):
        """
        Per-doc-type searches with dates filled — for full coverage beyond 300-cap.
        Fills all three fields: date_from + date_to + doc_type.
        """
        log.info("[Clerk] running per-doc-type searches with dates…")

        for code in AVA_SEARCH_CODES:
            try:
                await self._find_and_click_tab(page, ["Search","Quick Search"])
                await asyncio.sleep(1)
                await self._clear_form(page)
                await asyncio.sleep(0.3)

                # Wait for form inputs
                try:
                    await page.wait_for_selector('#mat-input-0',
                                                  state='visible', timeout=8_000)
                except Exception:
                    pass

                # Fill date_from
                el = page.locator('#mat-input-0').first
                if await el.is_visible(timeout=3000):
                    await self._angular_fill(page, el, self.date_from)

                # Fill date_to
                el = page.locator('#mat-input-1').first
                if await el.is_visible(timeout=3000):
                    await self._angular_fill(page, el, self.date_to)

                # Fill Document Type (mat-input-2, plain text — works fine)
                el = page.locator('#mat-input-2').first
                if await el.is_visible(timeout=3000):
                    await self._angular_fill(page, el, code)
                    log.info(f"[Clerk] {code}: dates + doc type filled")

                # Submit
                for btn_label in ("Search", "Submit", "Find"):
                    try:
                        btn = page.get_by_role("button",
                              name=re.compile(f"^{btn_label}$", re.I))
                        if await btn.count():
                            await btn.first.click()
                            break
                    except Exception:
                        pass

                await asyncio.sleep(4)
                try:
                    await page.wait_for_load_state("networkidle", timeout=15_000)
                except Exception:
                    await asyncio.sleep(2)

                await self._log_result_state(page, code)
                await self._harvest(page, label=code)

            except Exception as exc:
                log.warning(f"[Clerk] {code} search failed: {exc}")
            await asyncio.sleep(0.5)

    async def _log_result_state(self, page, label: str):
        """Log page title, URL, text snippet and mat-row count after a search."""
        try:
            title = await page.title()
            url   = page.url
            txt   = await page.evaluate("()=>document.body.innerText")
            snippet = txt.replace("\n"," ")[:400]
            log.info(f"[Clerk][{label}] url={url}")
            log.info(f"[Clerk][{label}] text={snippet!r}")
            rows = await page.locator(
                "mat-row, tr[mat-row], [class*=mat-row]").count()
            log.info(f"[Clerk][{label}] mat-rows={rows}")
        except Exception as e:
            log.warning(f"[Clerk][{label}] log_result_state failed: {e}")

    async def _search_by_doc_type_field(self, page):
        """
        Search AVA by entering each lead doc type into the Document Type field.
        Uses mat-input-2 (placeholder='Document Type') which is a plain text field
        that works reliably with keyboard input, unlike the date pickers.
        After each search, harvests all result pages then filters by date in Python.
        """
        log.info("[Clerk] searching by Document Type field…")

        # Navigate to Search tab
        await self._find_and_click_tab(page, [
            "Search", "Quick Search", "Basic Search",
        ])
        await asyncio.sleep(1.5)

        for code in AVA_SEARCH_CODES:
            try:
                log.info(f"[Clerk] searching doc type: {code}")
                await self._clear_form(page)

                # Fill Document Type field — mat-input-2, placeholder='Document Type'
                filled = False
                for sel in [
                    '#mat-input-2',
                    'input[placeholder="Document Type"]',
                    'input[id*="mat-input-2"]',
                ]:
                    try:
                        el = page.locator(sel).first
                        if await el.is_visible(timeout=3000):
                            await self._angular_fill(page, el, code)
                            log.info(f"[Clerk] doc type '{code}' → {sel}")
                            filled = True
                            break
                    except Exception:
                        pass

                if not filled:
                    log.warning(f"[Clerk] could not fill doc type for {code}")
                    continue

                await self._ss(page, f"form_{code}")

                # Submit
                submitted = False
                for btn_label in ("Search", "Submit", "Find", "Go"):
                    try:
                        btn = page.get_by_role("button",
                              name=re.compile(f"^{btn_label}$", re.I))
                        if await btn.count():
                            await btn.first.click()
                            submitted = True
                            log.info(f"[Clerk] submitted for {code}")
                            break
                    except Exception:
                        pass
                if not submitted:
                    await page.keyboard.press("Enter")

                await asyncio.sleep(4)
                try:
                    await page.wait_for_load_state("networkidle", timeout=15_000)
                except Exception:
                    await asyncio.sleep(2)

                # Log result state
                try:
                    title = await page.title()
                    url   = page.url
                    txt   = await page.evaluate("()=>document.body.innerText")
                    snippet = txt.replace("\n"," ")[:400]
                    log.info(f"[Clerk][{code}] result: url={url} text={snippet!r}")
                    rows_cnt = await page.locator(
                        "mat-row, tr[mat-row], [class*=mat-row]").count()
                    log.info(f"[Clerk][{code}] mat-rows={rows_cnt}")
                except Exception:
                    pass

                await self._harvest(page, label=code)

                # Go back to search form
                try:
                    back = page.locator("button:has-text('BACK'), a:has-text('BACK'), "
                                        "[aria-label='Back']").first
                    if await back.is_visible(timeout=2000):
                        await back.click()
                        await asyncio.sleep(1.5)
                    else:
                        await page.goto(CLERK_AVA_URL,
                                        wait_until="domcontentloaded", timeout=30_000)
                        await asyncio.sleep(2)
                        await self._find_and_click_tab(page, ["Search","Quick Search"])
                        await asyncio.sleep(1)
                except Exception:
                    try:
                        await page.goto(CLERK_AVA_URL,
                                        wait_until="domcontentloaded", timeout=30_000)
                        await asyncio.sleep(2)
                        await self._find_and_click_tab(page, ["Search","Quick Search"])
                        await asyncio.sleep(1)
                    except Exception:
                        pass

            except Exception as exc:
                log.warning(f"[Clerk] search for {code} failed: {exc}")
                try:
                    await page.goto(CLERK_AVA_URL,
                                    wait_until="domcontentloaded", timeout=30_000)
                    await asyncio.sleep(2)
                    await self._find_and_click_tab(page, ["Search","Quick Search"])
                    await asyncio.sleep(1)
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
        log.info("[Clerk] _search_doc_types: replaced by _search_by_doc_type_field")
        return  # no-op
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

    # Known multi-word Texas cities for address parsing
    _TX_CITIES = [
        "TEXAS CITY", "LEAGUE CITY", "SANTA FE", "LA MARQUE", "CLEAR LAKE",
        "CRYSTAL BEACH", "PORT BOLIVAR", "JAMAICA BEACH", "BOLIVAR PENINSULA",
        "BAYOU VISTA", "TIKI ISLAND", "WEST GALVESTON",
    ]

    @classmethod
    def _split_tx_addr(cls, full: str) -> tuple[str, str, str, str]:
        """
        Split 'STREET CITY TX ZIP' into (street, city, state, zip).
        Handles multi-word TX cities like TEXAS CITY, LEAGUE CITY.
        """
        import re as _re
        m = _re.search(r"(.+?)\s+TX\s+(\d{5}(?:-\d{4})?)", full, _re.I)
        if not m:
            return full.strip(), "", "TX", ""
        street_city = m.group(1).strip()
        zipcode     = m.group(2)

        city = ""
        street = street_city
        for mc in cls._TX_CITIES:
            if street_city.upper().endswith(mc):
                city   = mc.title()
                street = street_city[:-len(mc)].strip()
                break
        if not city:
            parts = street_city.rsplit(None, 1)
            if len(parts) == 2:
                street, city = parts[0], parts[1].title()

        return street.strip(), city.strip(), "TX", zipcode

    def _parse_ava_text(self, text: str, url: str) -> list[dict]:
        """
        Parse AVA results text.  Uses match-position windowing (not split)
        to avoid splitting 10-digit doc numbers.

        Confirmed AVA format (2026-04-30 CI log):
          DOC_NUM  DOC_TYPE  DATE TIME  PARTY1  [PARTY2]
          DOC_NUM  DATE TIME  DOC_TYPE  Ref No:...  Page Count:N
          Parties  Party 1: NAME  Party 2: NAME
          Legals   STREET CITY TX ZIP  SUBDIVISION  L: LOT
        """
        records = []
        seen: set[str] = set()

        # Primary: doc_num (not preceded by digit) + doc_type + date
        primary = re.compile(
            r"(?<!\d)(\d{7,13})"
            r"\s+([A-Z][A-Z /&\-,]{2,60}?)"
            r"\s+(\d{1,2}/\d{1,2}/\d{4})",
            re.MULTILINE
        )
        # TX address: "12531 WEST VENTURA DRIVE GALVESTON TX 77554"
        addr_pat = re.compile(
            r"(\d{1,6}(?:\s+\S+){1,8})\s+TX\s+(\d{5}(?:-\d{4})?)",
            re.IGNORECASE
        )
        party1_pat = re.compile(
            r"Party\s*1:\s*([A-Z][A-Z .,&'\-]{2,60}?)(?=\s+Party|\s+Legal|\s*$)",
            re.MULTILINE
        )
        party2_pat = re.compile(
            r"Party\s*2:\s*([A-Z][A-Z .,&'\-]{2,60}?)(?=\s+Legal|\s*$)",
            re.MULTILINE
        )

        all_matches = list(primary.finditer(text))
        for i, m in enumerate(all_matches):
            doc_num  = m.group(1)
            doc_type = m.group(2).strip().rstrip(",")
            filed    = normalize_date(m.group(3))

            if len(doc_num) < 7:
                continue
            if doc_type.upper() in ("DOCUMENT NO","DOC","NUMBER","RESULTS","BACK"):
                continue
            if doc_num in seen:
                continue
            seen.add(doc_num)

            # Context window: from this match to the next unique doc number
            ctx_end = all_matches[i+1].start() if i+1 < len(all_matches) else len(text)
            chunk   = text[m.start():ctx_end]

            # Parties
            p1 = party1_pat.search(chunk)
            p2 = party2_pat.search(chunk)
            owner   = p1.group(1).strip() if p1 else ""
            grantee = p2.group(1).strip() if p2 else ""

            # Fallback owner from text immediately after date
            if not owner:
                after = chunk[m.end():m.end() + 80].strip()
                nm = re.match(r"([A-Z][A-Z .,&'\-]{4,50})", after)
                if nm:
                    cand = nm.group(1).strip()
                    if cand.upper() not in ("UNOFFICIAL", "PAGE", "PARTIES",
                                            "LEGALS", "REF NO", "ASSOCIATED"):
                        owner = cand

            # Property address from Legals section
            prop_addr = prop_city = prop_zip = ""
            am = addr_pat.search(chunk)
            if am:
                street, city, _, zipcode = self._split_tx_addr(
                    am.group(1) + " TX " + am.group(2)
                )
                prop_addr = street
                prop_city = city
                prop_zip  = zipcode

            direct_url = (
                f"https://ava.fidlar.com/TXGalveston/AvaWeb/"
                f"#/searchresults/{doc_num}"
            )

            records.append({
                "doc_num":     doc_num,
                "raw_type":    doc_type,
                "filed":       filed,
                "owner":       owner,
                "grantee":     grantee,
                "legal":       "",
                "amount":      None,
                "clerk_url":   direct_url,
                "_prop_addr":  prop_addr,
                "_prop_city":  prop_city,
                "_prop_state": "TX" if prop_addr else "",
                "_prop_zip":   prop_zip,
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
        # Filter by look-back period (since AVA now returns all dates)
        filed = rec.get("filed", "")
        if filed:
            try:
                from datetime import datetime, timedelta
                filed_dt = datetime.strptime(filed, "%Y-%m-%d")
                cutoff   = datetime.now() - timedelta(days=LOOK_BACK_DAYS)
                if filed_dt < cutoff:
                    return False
            except Exception:
                pass  # if date parse fails, include the record
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
            # Property address: prefer AVA-extracted situs, fall back to CAD
            ava_prop_addr  = raw.get("_prop_addr", "")
            ava_prop_city  = raw.get("_prop_city", "")
            ava_prop_state = raw.get("_prop_state", "")
            ava_prop_zip   = raw.get("_prop_zip", "")

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
                "prop_address": ava_prop_addr  or parcel.get("prop_address", ""),
                "prop_city":    ava_prop_city  or parcel.get("prop_city", ""),
                "prop_state":   ava_prop_state or parcel.get("prop_state", "TX"),
                "prop_zip":     ava_prop_zip   or parcel.get("prop_zip", ""),
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
