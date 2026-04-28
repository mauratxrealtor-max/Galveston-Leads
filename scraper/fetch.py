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

    def _auto_parse(self, data: bytes, name: str) -> list[dict]:
        ext = Path(name).suffix.lower()
        # Detect pipe-delimited by content, regardless of extension
        if b"|" in data[:3000]:
            return self._parse_pipe_txt(data, name)
        if ext == ".csv":
            return self._parse_csv(data, name)
        if ext == ".dbf":
            return self._parse_dbf(data, name)
        # Last resort: try CSV
        rows = self._parse_csv(data, name)
        return rows

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
                                    "MAIL_ADDR")
        mail_city  = self._get(row, "MAIL_CITY",   "MAILCITY",   "CITY")
        mail_state = self._get(row, "MAIL_STATE",  "MAILSTATE",  "STATE") or "TX"
        mail_zip   = self._get(row, "MAIL_ZIP",    "MAILZIP",    "ZIP")

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

            # Priority: .txt → .csv → .dbf
            for ext in (".txt", ".csv", ".dbf"):
                for name in zf.namelist():
                    if Path(name).suffix.lower() == ext:
                        rows = self._auto_parse(zf.read(name), name)
                        if rows:
                            log.info(f"[CAD] sample keys: {list(rows[0].keys())[:16]}")
                            break
                if rows:
                    break

            # If no match by extension, try every file
            if not rows:
                for name in zf.namelist():
                    rows = self._auto_parse(zf.read(name), name)
                    if rows:
                        break

        except zipfile.BadZipFile:
            log.warning("[CAD] not a ZIP – trying raw parse")
            rows = self._auto_parse(raw, url.split("/")[-1])

        log.info(f"[CAD] indexing {len(rows):,} rows…")
        for row in rows:
            try:
                self._index_row(row)
            except Exception:
                pass
        log.info(f"[CAD] index built: {len(self._by_name):,} name keys")

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

    async def _fill_form(self, page, doc_type: str | None, date_from: str, date_to: str):
        await asyncio.sleep(0.5)

        if doc_type:
            for sel in [
                'input[placeholder*="Doc"]', 'input[placeholder*="Type"]',
                'input[placeholder*="Instrument"]',
                'input[id*="docType"]', 'input[id*="type"]',
                'input[name*="type"]', 'input[id*="instrument"]',
                'input[aria-label*="Type"]',
            ]:
                try:
                    el = page.locator(sel).first
                    if await el.is_visible(timeout=2000):
                        await el.triple_click()
                        await el.fill(doc_type)
                        await asyncio.sleep(0.3)
                        await el.press("Enter")
                        await asyncio.sleep(0.4)
                        # Accept autocomplete dropdown
                        drop = page.locator(
                            "li[role='option'], .dropdown-item, .autocomplete-item"
                        ).first
                        if await drop.is_visible(timeout=1200):
                            await drop.click()
                            await asyncio.sleep(0.3)
                        log.info(f"[Clerk] doc type set: {doc_type}")
                        break
                except Exception:
                    pass

        from_sels = [
            'input[placeholder*="From Date"]', 'input[placeholder*="Start Date"]',
            'input[placeholder*="Begin"]',
            'input[id*="dateFrom"]', 'input[id*="beginDate"]', 'input[id*="startDate"]',
            'input[name*="from"]', 'input[name*="begin"]',
            'input[aria-label*="From"]', 'input[aria-label*="Start"]',
        ]
        for sel in from_sels:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=2000):
                    await el.triple_click()
                    await el.fill(date_from)
                    await el.press("Tab")
                    log.info(f"[Clerk] date_from: {date_from}")
                    break
            except Exception:
                pass

        to_sels = [
            'input[placeholder*="To Date"]', 'input[placeholder*="End Date"]',
            'input[placeholder*="Thru"]',
            'input[id*="dateTo"]', 'input[id*="endDate"]', 'input[id*="toDate"]',
            'input[name*="to"]', 'input[name*="end"]',
            'input[aria-label*="To"]', 'input[aria-label*="End"]',
        ]
        for sel in to_sels:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=2000):
                    await el.triple_click()
                    await el.fill(date_to)
                    await el.press("Tab")
                    log.info(f"[Clerk] date_to: {date_to}")
                    break
            except Exception:
                pass

        await self._ss(page, f"04_form_{doc_type or 'broad'}")

        # Submit
        submitted = False
        for label in ("Search", "Submit", "Find Records", "Go", "Run Search"):
            try:
                btn = page.get_by_role("button", name=re.compile(f"^{label}$", re.I))
                if await btn.count():
                    await btn.first.click()
                    await asyncio.sleep(4)
                    submitted = True
                    log.info(f"[Clerk] clicked: {label}")
                    break
            except Exception:
                pass
        if not submitted:
            try:
                await page.keyboard.press("Enter")
                await asyncio.sleep(4)
            except Exception:
                pass

        try:
            await page.wait_for_load_state("networkidle", timeout=20_000)
        except Exception:
            await asyncio.sleep(3)

    async def _harvest(self, page, label: str = ""):
        p_num = 0
        empty_streak = 0
        while p_num < 500:
            p_num += 1
            await asyncio.sleep(1)
            try:
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

        # Table-based results
        for table in soup.find_all("table"):
            ths = table.find_all("th")
            headers = [th.get_text(" ", strip=True).lower() for th in ths]
            if not any(kw in " ".join(headers) for kw in
                       ["doc", "type", "instrument", "filed", "grantor",
                        "recorded", "book", "page"]):
                continue
            for tr in table.find_all("tr")[1:]:
                tds = tr.find_all("td")
                if len(tds) < 2:
                    continue
                rec = self._from_tds(tds, headers, url)
                if rec:
                    records.append(rec)

        # Card / row divs
        if not records:
            for el in soup.select(
                "div.result-row, div.record-item, div[class*='document'], "
                "div[class*='result'], li.document-result"
            ):
                rec = self._from_card(el, url)
                if rec:
                    records.append(rec)

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
