#!/usr/bin/env python3
"""
Galveston County Motivated Seller Lead Scraper
Clerk Portal: https://www.galvestoncountytx.gov/our-county/county-clerk/records-search
CAD Data:     https://galvestoncad.org/current-and-historical-supplement-roll-export-data/
Look-back:    90 days
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

# ── Playwright is optional at import-time; loaded only when clerk scraping runs
try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

# ─────────────────────────────── logging ──────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("galveston_scraper")

# ─────────────────────────────── constants ────────────────────────────────────
LOOK_BACK_DAYS = 90
RETRY_ATTEMPTS = 3
RETRY_DELAY    = 5   # seconds between retries

CLERK_BASE_URL = "https://www.galvestoncountytx.gov"
CLERK_SEARCH   = "https://ava.fidlar.com/TXGalveston/AvaWeb/"

# GCAD supplement roll export page – contains direct ZIP download links
GCAD_EXPORT_PAGE = "https://galvestoncad.org/current-and-historical-supplement-roll-export-data/"

# Document-type categories we want to collect
LEAD_TYPES = {
    "LP":       ("Lis Pendens",              ["LP", "LISPEND", "LISPENDENS"]),
    "NOFC":     ("Notice of Foreclosure",    ["NOFC", "NOTFORECLOSURE", "FORECLOSURE", "NOF"]),
    "TAXDEED":  ("Tax Deed",                 ["TAXDEED", "TAX DEED", "TAXD"]),
    "JUD":      ("Judgment",                 ["JUD", "JUDGMENT"]),
    "CCJ":      ("Certified Judgment",       ["CCJ", "CERTJUD"]),
    "DRJUD":    ("Domestic Judgment",        ["DRJUD", "DOMJUD", "DOMESTICJUD"]),
    "LNCORPTX": ("Corp Tax Lien",            ["LNCORPTX", "CORPTAX", "CORPTAXLIEN"]),
    "LNIRS":    ("IRS Lien",                 ["LNIRS", "IRS", "IRSLIEN", "IRSLIEN"]),
    "LNFED":    ("Federal Lien",             ["LNFED", "FEDLIEN", "FEDERALLIEN"]),
    "LN":       ("Lien",                     ["LN", "LIEN"]),
    "LNMECH":   ("Mechanic Lien",            ["LNMECH", "MECHLIEN", "MECHANICLIEN"]),
    "LNHOA":    ("HOA Lien",                 ["LNHOA", "HOALIEN"]),
    "MEDLN":    ("Medicaid Lien",            ["MEDLN", "MEDICAID", "MEDICAIDLIEN"]),
    "PRO":      ("Probate",                  ["PRO", "PROBATE"]),
    "NOC":      ("Notice of Commencement",   ["NOC", "NOTCOMMENCE"]),
    "RELLP":    ("Release Lis Pendens",      ["RELLP", "RELLISPENDENS", "RELEASELP"]),
}

# ──────────────────────────── utility helpers ─────────────────────────────────

def retry(fn, *args, attempts=RETRY_ATTEMPTS, delay=RETRY_DELAY, label="", **kwargs):
    """Call fn(*args, **kwargs) up to `attempts` times, sleeping `delay` between."""
    for i in range(attempts):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            log.warning(f"[{label}] attempt {i+1}/{attempts} failed: {exc}")
            if i < attempts - 1:
                time.sleep(delay)
    log.error(f"[{label}] all {attempts} attempts failed – skipping")
    return None


def normalize(text: str) -> str:
    """Strip accents, lowercase, collapse whitespace."""
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", text).strip().lower()


def parse_amount(text: str) -> float | None:
    """Extract a dollar amount from a string."""
    if not text:
        return None
    cleaned = re.sub(r"[^\d.]", "", text.replace(",", ""))
    try:
        val = float(cleaned)
        return val if val > 0 else None
    except ValueError:
        return None


def map_doc_type(raw: str) -> tuple[str, str] | None:
    """
    Return (cat_code, cat_label) if the raw document-type string matches
    any of our lead types; else None.

    Priority: exact > starts/ends-with > interior-substring > input-in-variant.
    Within a priority tier, prefer the longest matching variant.
    """
    upper = re.sub(r"[\s\-_/]", "", raw.upper())
    best: tuple[str, str] | None = None
    best_priority = 999
    best_len = 0

    for cat, (label, variants) in LEAD_TYPES.items():
        for v in variants:
            v_clean = re.sub(r"[\s\-_/]", "", v.upper())
            if not v_clean:
                continue

            if v_clean == upper:
                priority = 1
            elif upper.startswith(v_clean) or upper.endswith(v_clean):
                priority = 2
            elif v_clean in upper:
                priority = 3
            elif upper in v_clean:
                priority = 4
            else:
                continue

            if priority < best_priority or (
                    priority == best_priority and len(v_clean) > best_len):
                best_priority = priority
                best_len = len(v_clean)
                best = (cat, label)

    return best


def is_new_this_week(filed_str: str) -> bool:
    try:
        filed = datetime.strptime(filed_str, "%Y-%m-%d")
        return (datetime.now() - filed).days <= 7
    except Exception:
        return False


def score_record(rec: dict) -> tuple[int, list[str]]:
    """Compute seller score (0-100) and flags list."""
    flags = []
    score = 30  # base

    cat = rec.get("cat", "")
    amount = rec.get("amount")
    filed = rec.get("filed", "")
    owner = rec.get("owner", "")

    # Category-based flags
    if cat == "LP":
        flags.append("Lis pendens")
    if cat in ("NOFC", "LP"):
        flags.append("Pre-foreclosure")
    if cat in ("JUD", "CCJ", "DRJUD"):
        flags.append("Judgment lien")
    if cat in ("LNCORPTX", "LNIRS", "LNFED", "TAXDEED"):
        flags.append("Tax lien")
    if cat == "LNMECH":
        flags.append("Mechanic lien")
    if cat == "PRO":
        flags.append("Probate / estate")
    if re.search(r"\b(LLC|INC|CORP|LTD|LP|LLP|TRUST|TRUSTEE)\b", owner.upper()):
        flags.append("LLC / corp owner")
    if is_new_this_week(filed):
        flags.append("New this week")

    # Score additions (+10 per flag)
    score += 10 * len(flags)

    # LP + foreclosure combo bonus
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20

    # Amount bonuses
    if amount:
        if amount > 100_000:
            score += 15
        elif amount > 50_000:
            score += 10

    # New this week bonus
    if "New this week" in flags:
        score += 5

    # Has address bonus
    if rec.get("prop_address") or rec.get("mail_address"):
        score += 5

    return min(score, 100), flags


# ────────────────────────── GCAD parcel data loader ──────────────────────────

class ParcelIndex:
    """
    Downloads the latest GCAD supplement-roll export ZIP, extracts the CSV/DBF,
    and builds a name → {site_addr, mail_addr, …} lookup with three name variants.
    """

    def __init__(self):
        self._by_name: dict[str, dict] = {}
        self._loaded = False

    # ── download helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _find_latest_export_url() -> str | None:
        """Scrape the GCAD export page to find the most recent ZIP download link."""
        try:
            resp = requests.get(GCAD_EXPORT_PAGE, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
            # Look for links to ZIP files (supplement roll exports)
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if re.search(r"\.(zip|ZIP)$", href) or "export" in href.lower():
                    if href.startswith("http"):
                        return href
                    return "https://galvestoncad.org" + href
            # Fallback: any link with 'supp' or 'export' in text
            for a in soup.find_all("a", href=True):
                text = a.get_text(strip=True).lower()
                if any(k in text for k in ("2025", "2026", "supp", "export")):
                    href = a["href"]
                    if href.startswith("http"):
                        return href
                    return "https://galvestoncad.org" + href
        except Exception as exc:
            log.warning(f"[CAD] could not scrape export page: {exc}")
        return None

    @staticmethod
    def _download_zip(url: str) -> bytes | None:
        log.info(f"[CAD] downloading parcel ZIP: {url}")
        try:
            resp = requests.get(url, timeout=120, stream=True)
            resp.raise_for_status()
            return resp.content
        except Exception as exc:
            log.warning(f"[CAD] ZIP download failed: {exc}")
        return None

    # ── CSV / DBF parsing ─────────────────────────────────────────────────────

    def _parse_csv_bytes(self, data: bytes, filename: str) -> list[dict]:
        """Parse a CSV file from bytes."""
        rows = []
        try:
            text = data.decode("utf-8", errors="replace")
            reader = csv.DictReader(io.StringIO(text))
            for row in reader:
                rows.append(dict(row))
        except Exception as exc:
            log.warning(f"[CAD] CSV parse error ({filename}): {exc}")
        return rows

    def _parse_dbf_bytes(self, data: bytes, filename: str) -> list[dict]:
        """Parse a DBF file using dbfread (reads from a temp file)."""
        rows = []
        try:
            import tempfile
            from dbfread import DBF
            with tempfile.NamedTemporaryFile(suffix=".dbf", delete=False) as tf:
                tf.write(data)
                tmp_path = tf.name
            table = DBF(tmp_path, load=True, encoding="latin-1")
            for record in table:
                rows.append(dict(record))
            os.unlink(tmp_path)
        except ImportError:
            log.warning("[CAD] dbfread not installed – skipping DBF parsing")
        except Exception as exc:
            log.warning(f"[CAD] DBF parse error ({filename}): {exc}")
        return rows

    # ── name normalisation & index building ──────────────────────────────────

    @staticmethod
    def _get_col(row: dict, *keys: str) -> str:
        """Return the first non-empty value matching any of the given column names."""
        for k in keys:
            for rk in row:
                if rk.strip().upper() == k.upper():
                    v = row[rk]
                    if v and str(v).strip():
                        return str(v).strip()
        return ""

    def _index_row(self, row: dict):
        owner = self._get_col(row, "OWNER", "OWN1", "OWNER1", "OWNERNAME")
        if not owner:
            return

        site_addr  = self._get_col(row, "SITE_ADDR", "SITEADDR", "SITUS", "PROP_ADDR", "ADDRESS")
        site_city  = self._get_col(row, "SITE_CITY", "SITECITY", "PROP_CITY", "CITY")
        site_state = self._get_col(row, "SITE_STATE", "PROP_STATE", "STATE") or "TX"
        site_zip   = self._get_col(row, "SITE_ZIP",  "SITEZIP",  "PROP_ZIP", "ZIP")
        mail_addr  = self._get_col(row, "ADDR_1", "MAILADR1", "MAIL_ADDR", "MAILING_ADDRESS")
        mail_city  = self._get_col(row, "CITY",   "MAILCITY", "MAIL_CITY")
        mail_state = self._get_col(row, "STATE",  "MAILSTATE","MAIL_STATE") or "TX"
        mail_zip   = self._get_col(row, "ZIP",    "MAILZIP",  "MAIL_ZIP")

        entry = dict(
            prop_address=site_addr,  prop_city=site_city,
            prop_state=site_state,   prop_zip=site_zip,
            mail_address=mail_addr,  mail_city=mail_city,
            mail_state=mail_state,   mail_zip=mail_zip,
        )

        # Build three name-variant keys:  "FIRST LAST", "LAST FIRST", "LAST, FIRST"
        parts = owner.split(",")
        if len(parts) >= 2:
            last  = parts[0].strip()
            first = parts[1].strip()
            variants = [
                normalize(f"{first} {last}"),   # FIRST LAST
                normalize(f"{last} {first}"),   # LAST FIRST
                normalize(f"{last}, {first}"),  # LAST, FIRST
                normalize(owner),               # raw
            ]
        else:
            variants = [normalize(owner)]

        for v in variants:
            if v:
                self._by_name[v] = entry

    def load(self):
        """Download and index parcel data. Silently continues on any failure."""
        if self._loaded:
            return
        url = retry(self._find_latest_export_url, label="CAD-find-url")
        if not url:
            log.warning("[CAD] no export URL found – address enrichment will be skipped")
            self._loaded = True
            return

        raw = retry(self._download_zip, url, label="CAD-download")
        if not raw:
            self._loaded = True
            return

        rows = []
        try:
            zf = zipfile.ZipFile(io.BytesIO(raw))
            for name in zf.namelist():
                ext = Path(name).suffix.lower()
                data = zf.read(name)
                if ext == ".csv":
                    log.info(f"[CAD] parsing CSV: {name}")
                    rows.extend(self._parse_csv_bytes(data, name))
                elif ext == ".dbf":
                    log.info(f"[CAD] parsing DBF: {name}")
                    rows.extend(self._parse_dbf_bytes(data, name))
                if rows:
                    break  # use first data file found
        except zipfile.BadZipFile:
            # Maybe the URL points directly to a CSV/DBF
            log.warning("[CAD] downloaded file is not a ZIP – trying raw CSV/DBF")
            if raw[:3] == b"PKZ" or raw[:2] == b"PK":
                pass
            else:
                rows = self._parse_csv_bytes(raw, url)

        log.info(f"[CAD] indexing {len(rows):,} parcel records")
        for row in rows:
            try:
                self._index_row(row)
            except Exception:
                pass

        log.info(f"[CAD] name index built ({len(self._by_name):,} keys)")
        self._loaded = True

    def lookup(self, owner: str) -> dict:
        """Return address dict for owner name, trying all three variants."""
        if not self._loaded:
            self.load()
        parts = owner.split(",")
        if len(parts) >= 2:
            last  = parts[0].strip()
            first = parts[1].strip()
            keys = [
                normalize(f"{first} {last}"),
                normalize(f"{last} {first}"),
                normalize(f"{last}, {first}"),
                normalize(owner),
            ]
        else:
            # Try reversed first/last split on space
            sp = owner.strip().split()
            keys = [normalize(owner)]
            if len(sp) >= 2:
                keys.append(normalize(f"{sp[-1]} {' '.join(sp[:-1])}"))

        for k in keys:
            if k in self._by_name:
                return self._by_name[k]
        return {}


# ─────────────────────────── Clerk Portal Scraper ─────────────────────────────

class ClerkScraper:
    """
    Uses Playwright to interact with the Galveston County Clerk AVA portal
    (https://ava.fidlar.com/TXGalveston/AvaWeb/) and extract documents
    filed in the last LOOK_BACK_DAYS days.
    """

    BASE = "https://ava.fidlar.com/TXGalveston/AvaWeb/"

    def __init__(self):
        self.records: list[dict] = []
        self.date_from = (datetime.now() - timedelta(days=LOOK_BACK_DAYS)).strftime("%m/%d/%Y")
        self.date_to   = datetime.now().strftime("%m/%d/%Y")

    # ── helpers ───────────────────────────────────────────────────────────────

    async def _wait_and_fill(self, page, selector: str, value: str, timeout=15000):
        await page.wait_for_selector(selector, timeout=timeout)
        await page.fill(selector, value)

    async def _safe_text(self, element) -> str:
        try:
            return (await element.inner_text()).strip()
        except Exception:
            return ""

    # ── main search loop ──────────────────────────────────────────────────────

    async def scrape(self) -> list[dict]:
        if not HAS_PLAYWRIGHT:
            log.error("[Clerk] Playwright not installed – cannot scrape clerk portal")
            return []

        log.info(f"[Clerk] scraping AVA portal {self.date_from} → {self.date_to}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            page.set_default_timeout(30_000)

            try:
                await self._run_searches(page)
            except Exception as exc:
                log.error(f"[Clerk] fatal error during scrape: {exc}")
            finally:
                await browser.close()

        log.info(f"[Clerk] collected {len(self.records)} raw records")
        return self.records

    async def _run_searches(self, page):
        """Navigate to AVA, run date-range search for each document type."""
        await page.goto(self.BASE, wait_until="networkidle", timeout=60_000)
        await asyncio.sleep(2)

        # Accept any cookie/terms dialogs
        for btn_text in ("Accept", "OK", "Agree", "Continue", "I Agree"):
            try:
                btn = page.get_by_role("button", name=re.compile(btn_text, re.I))
                if await btn.count():
                    await btn.first.click()
                    await asyncio.sleep(1)
                    break
            except Exception:
                pass

        # Try to find search form
        # AVA uses a React SPA; look for the "Search" or "Advanced Search" tab
        for attempt in range(RETRY_ATTEMPTS):
            try:
                await self._do_date_range_search(page)
                return
            except Exception as exc:
                log.warning(f"[Clerk] search attempt {attempt+1} failed: {exc}")
                await asyncio.sleep(RETRY_DELAY)
                await page.reload(wait_until="networkidle", timeout=60_000)
                await asyncio.sleep(2)

    async def _do_date_range_search(self, page):
        """
        Perform a date-range search on the AVA portal.
        AVA's SPA exposes a search form that accepts date ranges and doc types.
        """
        # Click on "Search" tab if present
        for label in ("Search", "Advanced Search", "Quick Search"):
            try:
                el = page.get_by_text(re.compile(f"^{label}$", re.I))
                if await el.count():
                    await el.first.click()
                    await asyncio.sleep(1)
                    break
            except Exception:
                pass

        # Fill date-from field (various possible selectors in AVA)
        date_from_selectors = [
            'input[placeholder*="From"]',
            'input[placeholder*="Start"]',
            'input[id*="dateFrom"]',
            'input[id*="from"]',
            'input[name*="from"]',
            'input[type="date"]',
        ]
        filled_from = False
        for sel in date_from_selectors:
            try:
                els = await page.query_selector_all(sel)
                if els:
                    await els[0].fill(self.date_from)
                    filled_from = True
                    break
            except Exception:
                pass

        # Fill date-to field
        date_to_selectors = [
            'input[placeholder*="To"]',
            'input[placeholder*="End"]',
            'input[id*="dateTo"]',
            'input[id*="to"]',
            'input[name*="to"]',
        ]
        for sel in date_to_selectors:
            try:
                els = await page.query_selector_all(sel)
                if els:
                    idx = 1 if filled_from else 0
                    if len(els) > idx:
                        await els[idx].fill(self.date_to)
                    break
            except Exception:
                pass

        # Click Search / Submit
        for btn_text in ("Search", "Submit", "Find", "Go"):
            try:
                btn = page.get_by_role("button", name=re.compile(f"^{btn_text}$", re.I))
                if await btn.count():
                    await btn.first.click()
                    await asyncio.sleep(3)
                    break
            except Exception:
                pass

        # Wait for results and parse
        await self._parse_results_pages(page)

    async def _parse_results_pages(self, page):
        """Iterate through all result pages and extract records."""
        page_num = 0
        max_pages = 500  # safety limit

        while page_num < max_pages:
            page_num += 1
            await asyncio.sleep(1)

            try:
                html = await page.content()
                new_records = self._parse_results_html(html, page.url)
                if new_records:
                    self.records.extend(new_records)
                    log.info(f"[Clerk] page {page_num}: +{len(new_records)} records "
                             f"(total={len(self.records)})")
            except Exception as exc:
                log.warning(f"[Clerk] parse error on page {page_num}: {exc}")

            # Try to click "Next" pagination button
            next_found = False
            for next_text in ("Next", ">", "»", "Next Page"):
                try:
                    btn = page.get_by_role("button", name=re.compile(f"^{next_text}$", re.I))
                    if not await btn.count():
                        btn = page.get_by_role("link", name=re.compile(f"^{next_text}$", re.I))
                    if await btn.count():
                        is_disabled = await btn.first.get_attribute("disabled")
                        aria_disabled = await btn.first.get_attribute("aria-disabled")
                        if is_disabled is None and aria_disabled != "true":
                            await btn.first.click()
                            await asyncio.sleep(2)
                            next_found = True
                            break
                except Exception:
                    pass
            if not next_found:
                break

    def _parse_results_html(self, html: str, current_url: str) -> list[dict]:
        """
        Parse the results HTML from AVA portal.
        Returns list of raw record dicts.
        """
        soup = BeautifulSoup(html, "lxml")
        records = []

        # AVA renders results in a table or card layout
        # Try table rows first
        tables = soup.find_all("table")
        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if not any(h in str(headers) for h in ["doc", "type", "instrument", "filed", "grantor"]):
                continue
            for tr in table.find_all("tr")[1:]:  # skip header row
                tds = tr.find_all("td")
                if len(tds) < 3:
                    continue
                rec = self._extract_table_row(tds, headers, current_url)
                if rec:
                    records.append(rec)

        # Try card/list layout
        if not records:
            cards = soup.find_all(class_=re.compile(r"result|record|item|row", re.I))
            for card in cards:
                rec = self._extract_card(card, current_url)
                if rec:
                    records.append(rec)

        # Filter to only relevant doc types
        filtered = []
        for rec in records:
            mapped = map_doc_type(rec.get("raw_type", ""))
            if mapped:
                rec["cat"], rec["cat_label"] = mapped
                filtered.append(rec)

        return filtered

    def _extract_table_row(self, tds, headers: list[str], url: str) -> dict | None:
        """Extract a record from table cells."""
        try:
            row_text = [td.get_text(strip=True) for td in tds]
            row_links = [td.find("a") for td in tds]

            # Build a dict by trying to match header names
            data: dict[str, Any] = {}
            for i, h in enumerate(headers):
                if i < len(row_text):
                    data[h] = row_text[i]

            def get(*keys):
                for k in keys:
                    for h in headers:
                        if k in h:
                            return data.get(h, "")
                return ""

            doc_num  = get("instrument", "doc", "number", "id")
            doc_type = get("type", "doctype", "instrument type")
            filed    = get("date", "filed", "record")
            grantor  = get("grantor", "owner", "party 1", "seller")
            grantee  = get("grantee", "party 2", "buyer")
            legal    = get("legal", "description")
            amount   = get("amount", "consideration")

            # Find direct URL from a link in the row
            direct_url = url
            for a in (td.find("a") for td in tds):
                if a and a.get("href"):
                    href = a["href"]
                    if href.startswith("http"):
                        direct_url = href
                    elif href.startswith("/"):
                        direct_url = "https://ava.fidlar.com" + href

            if not doc_num and not doc_type:
                return None

            return {
                "doc_num":   doc_num,
                "raw_type":  doc_type,
                "filed":     self._normalize_date(filed),
                "owner":     grantor,
                "grantee":   grantee,
                "legal":     legal,
                "amount":    parse_amount(amount),
                "clerk_url": direct_url,
            }
        except Exception:
            return None

    def _extract_card(self, card, url: str) -> dict | None:
        """Extract a record from a card/div element."""
        try:
            text = card.get_text(" ", strip=True)
            # Look for doc number patterns
            doc_num_m = re.search(r"(?:Instrument|Doc(?:ument)?|Number)[:\s#]+([A-Z0-9\-]+)", text, re.I)
            type_m    = re.search(r"(?:Type|DocType)[:\s]+([A-Z\s/]+?)(?:\s{2,}|$)", text, re.I)
            date_m    = re.search(r"(?:Filed|Date)[:\s]+(\d{1,2}/\d{1,2}/\d{2,4})", text, re.I)
            grantor_m = re.search(r"(?:Grantor|Owner)[:\s]+([^:]+?)(?:\s{2,}|$)", text, re.I)
            grantee_m = re.search(r"(?:Grantee)[:\s]+([^:]+?)(?:\s{2,}|$)", text, re.I)
            amount_m  = re.search(r"\$[\d,]+(?:\.\d{2})?", text)

            doc_num   = doc_num_m.group(1).strip()  if doc_num_m  else ""
            doc_type  = type_m.group(1).strip()     if type_m     else ""
            filed     = date_m.group(1).strip()     if date_m     else ""
            grantor   = grantor_m.group(1).strip()  if grantor_m  else ""
            grantee   = grantee_m.group(1).strip()  if grantee_m  else ""
            amount    = parse_amount(amount_m.group(0)) if amount_m else None

            link = card.find("a")
            direct_url = url
            if link and link.get("href"):
                href = link["href"]
                direct_url = href if href.startswith("http") else "https://ava.fidlar.com" + href

            if not (doc_num or doc_type):
                return None

            return {
                "doc_num":   doc_num,
                "raw_type":  doc_type,
                "filed":     self._normalize_date(filed),
                "owner":     grantor,
                "grantee":   grantee,
                "legal":     "",
                "amount":    amount,
                "clerk_url": direct_url,
            }
        except Exception:
            return None

    @staticmethod
    def _normalize_date(raw: str) -> str:
        """Convert various date formats to YYYY-MM-DD."""
        if not raw:
            return ""
        for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%Y%m%d", "%m-%d-%Y"):
            try:
                return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                pass
        return raw.strip()


# ──────────────────────── record enrichment & scoring ─────────────────────────

def enrich_records(raw_records: list[dict], parcel_index: ParcelIndex) -> list[dict]:
    """
    Merge parcel address data, compute flags & scores, and build final record dicts.
    """
    enriched = []
    for raw in raw_records:
        try:
            owner = raw.get("owner", "")
            parcel = parcel_index.lookup(owner) if owner else {}

            rec: dict = {
                "doc_num":     raw.get("doc_num", ""),
                "doc_type":    raw.get("raw_type", ""),
                "filed":       raw.get("filed", ""),
                "cat":         raw.get("cat", ""),
                "cat_label":   raw.get("cat_label", ""),
                "owner":       owner,
                "grantee":     raw.get("grantee", ""),
                "amount":      raw.get("amount"),
                "legal":       raw.get("legal", ""),
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

            score, flags = score_record(rec)
            rec["score"] = score
            rec["flags"] = flags

            enriched.append(rec)
        except Exception as exc:
            log.warning(f"[Enrich] skipping bad record: {exc}")

    # Sort by score descending
    enriched.sort(key=lambda r: r["score"], reverse=True)
    return enriched


# ─────────────────────────────── output writers ───────────────────────────────

def build_output(records: list[dict], date_from: str, date_to: str) -> dict:
    with_address = sum(1 for r in records if r.get("prop_address") or r.get("mail_address"))
    return {
        "fetched_at":  datetime.utcnow().isoformat() + "Z",
        "source":      "Galveston County Clerk (AVA) + GCAD Parcel Data",
        "date_range":  {"from": date_from, "to": date_to},
        "total":       len(records),
        "with_address": with_address,
        "records":     records,
    }


def save_json(data: dict, *paths: str):
    for path_str in paths:
        path = Path(path_str)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
        log.info(f"[Output] saved {data['total']} records → {path}")


def save_ghl_csv(records: list[dict], path_str: str):
    """Export to GoHighLevel-compatible CSV."""
    path = Path(path_str)
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]

    def split_name(full: str) -> tuple[str, str]:
        """Split 'LAST, FIRST' or 'FIRST LAST' into (first, last)."""
        if "," in full:
            parts = full.split(",", 1)
            return parts[1].strip().title(), parts[0].strip().title()
        parts = full.strip().split()
        if len(parts) >= 2:
            return parts[0].title(), " ".join(parts[1:]).title()
        return full.title(), ""

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            first, last = split_name(rec.get("owner", ""))
            writer.writerow({
                "First Name":             first,
                "Last Name":              last,
                "Mailing Address":        rec.get("mail_address", ""),
                "Mailing City":           rec.get("mail_city", ""),
                "Mailing State":          rec.get("mail_state", ""),
                "Mailing Zip":            rec.get("mail_zip", ""),
                "Property Address":       rec.get("prop_address", ""),
                "Property City":          rec.get("prop_city", ""),
                "Property State":         rec.get("prop_state", ""),
                "Property Zip":           rec.get("prop_zip", ""),
                "Lead Type":              rec.get("cat_label", ""),
                "Document Type":          rec.get("doc_type", ""),
                "Date Filed":             rec.get("filed", ""),
                "Document Number":        rec.get("doc_num", ""),
                "Amount/Debt Owed":       (f"${rec['amount']:,.2f}"
                                           if rec.get("amount") else ""),
                "Seller Score":           rec.get("score", 0),
                "Motivated Seller Flags": "; ".join(rec.get("flags", [])),
                "Source":                 "Galveston County Clerk",
                "Public Records URL":     rec.get("clerk_url", ""),
            })

    log.info(f"[Output] GHL CSV saved → {path} ({len(records)} rows)")


# ─────────────────────────────────── main ────────────────────────────────────

async def main():
    start = datetime.now()
    log.info("=" * 60)
    log.info("Galveston County Motivated Seller Scraper")
    log.info(f"Look-back: {LOOK_BACK_DAYS} days")
    log.info("=" * 60)

    date_to   = datetime.now().strftime("%Y-%m-%d")
    date_from = (datetime.now() - timedelta(days=LOOK_BACK_DAYS)).strftime("%Y-%m-%d")

    # ── Step 1: Load parcel index ──────────────────────────────────────────────
    log.info("[Step 1/3] Loading GCAD parcel data…")
    parcel_index = ParcelIndex()
    parcel_index.load()

    # ── Step 2: Scrape clerk portal ────────────────────────────────────────────
    log.info("[Step 2/3] Scraping Galveston County Clerk portal…")
    scraper = ClerkScraper()
    raw_records = await scraper.scrape()

    if not raw_records:
        log.warning("[Step 2/3] No records retrieved from clerk portal.")
        log.info("           Writing empty output files so the pipeline does not fail.")

    # ── Step 3: Enrich, score, and save ───────────────────────────────────────
    log.info("[Step 3/3] Enriching records and computing scores…")
    records = enrich_records(raw_records, parcel_index)

    output = build_output(records, date_from, date_to)

    # Save JSON to both locations
    save_json(output, "dashboard/records.json", "data/records.json")

    # Save GHL CSV
    ts = datetime.now().strftime("%Y%m%d")
    save_ghl_csv(records, f"data/ghl_export_{ts}.csv")

    elapsed = (datetime.now() - start).seconds
    log.info(f"\n✅  Done in {elapsed}s — {len(records)} motivated seller leads collected.")
    log.info(f"   With address: {output['with_address']}")
    if records:
        top = records[0]
        log.info(f"   Top lead: {top['owner']} | {top['cat_label']} | "
                 f"score={top['score']} | {top.get('prop_address','no address')}")


if __name__ == "__main__":
    asyncio.run(main())
