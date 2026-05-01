#!/usr/bin/env python3
"""
Galveston County Motivated Seller — 365-Day Backfill Scraper
=============================================================
Reuses all logic from fetch.py. Outputs to dashboard/backfill.json and
dashboard/backfill.html — never overwrites the daily records.json.

Usage:
    python scraper/backfill.py
    python scraper/backfill.py --days-back 180
    python scraper/backfill.py --days-back 365 --chunk-days 3
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from fetch import (
    ClerkScraper, ParcelIndex, enrich_records, build_output,
    save_json, save_ghl_csv, embed_data_in_dashboard, log,
    CLERK_AVA_URL,
)

def _build_backfill_html() -> str:
    """
    Build backfill dashboard by adapting the working daily index.html.
    This guarantees the rendering JS is identical to the proven daily dashboard.
    """
    from pathlib import Path as _Path
    # Try to read the daily dashboard
    for candidate in ["dashboard/index.html", "../dashboard/index.html"]:
        p = _Path(candidate)
        if p.exists():
            html = p.read_text(encoding="utf-8")
            # Adapt for backfill
            html = html.replace(
                "<title>Galveston County \u2014 Motivated Seller Leads</title>",
                "<title>Galveston County \u2014 Backfill Leads (365 Days)</title>"
            )
            html = html.replace(
                "🏠 <span>Galveston</span> Motivated Seller Leads",
                "📚 <span>Galveston</span> Backfill Leads (365 Days)"
            )
            # Replace ALL records.json with backfill.json
            html = html.replace("'records.json'", "'backfill.json'")
            html = html.replace('"records.json"', '"backfill.json"')
            html = html.replace("'./records.json'", "'./backfill.json'")
            html = html.replace('"./records.json"', '"./backfill.json"')
            html = html.replace("records.json", "backfill.json")
            # Add backfill notice
            html = html.replace(
                '<div class="fetched-at" id="fetched-at"></div>',
                '<div class="fetched-at" id="fetched-at"></div>\n'
                '<div style="background:#2a2010;border:1px solid #f7c04f44;'
                'border-radius:8px;margin:8px 24px 0;padding:10px 16px;'
                'font-size:12px;color:#f7c04f;">📚 365-day historical backfill. '
                '<a href="index.html" style="color:var(--accent2)">→ Daily dashboard</a></div>'
            )
            return html
    # Fallback: return minimal working HTML
    return """<!DOCTYPE html><html><head><meta charset="UTF-8"/>
<title>Galveston Backfill</title></head><body>
<h2>Backfill data loading...</h2>
<script>
fetch('backfill.json?t='+Date.now(),{cache:'no-store'})
  .then(r=>r.json()).then(d=>{
    document.body.innerHTML='<h2>'+d.total+' backfill leads loaded</h2>'
    +'<p>Open the full dashboard to view them.</p>';
  }).catch(e=>{document.body.innerHTML='<h2>Error: '+e+'</h2>';});
</script></body></html>"""


BACKFILL_HTML = _build_backfill_html()


async def run_backfill(days_back: int = 365, chunk_days: int = 5):
    t0 = datetime.now()
    log.info("=" * 60)
    log.info(f"Galveston County Motivated Seller — {days_back}-Day Backfill")
    log.info(f"Window size: {chunk_days} days")
    log.info("=" * 60)

    # Step 1: Load CAD parcel data
    log.info("[Step 1/3] Loading GCAD parcel data…")
    parcel_index = ParcelIndex()
    parcel_index.load()

    # Step 2: Scrape all windows
    log.info("[Step 2/3] Scraping Galveston County Clerk portal…")
    now = datetime.now()
    chunks: list[tuple[str, str]] = []
    cursor = now - timedelta(days=days_back)
    while cursor < now:
        end = min(cursor + timedelta(days=chunk_days), now)
        chunks.append((cursor.strftime("%m/%d/%Y"), end.strftime("%m/%d/%Y")))
        cursor = end + timedelta(days=1)

    log.info(f"[Backfill] {len(chunks)} windows × ~{chunk_days} days each")

    from playwright.async_api import async_playwright

    scraper = ClerkScraper()
    # Extend the _accept() date filter to cover the full backfill period
    import fetch as _fetch
    _fetch.LOOK_BACK_DAYS = days_back

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        page = await ctx.new_page()

        for i, (d_from, d_to) in enumerate(chunks):
            log.info(f"[Backfill] window {i+1}/{len(chunks)}: {d_from} → {d_to}")
            for attempt in range(3):
                try:
                    await scraper._search_window(page, d_from, d_to,
                                                  label=f"BF{i+1}")
                    break
                except Exception as exc:
                    log.warning(f"[Backfill] w{i+1} attempt {attempt+1}: {exc}")
                    try:
                        await page.goto(CLERK_AVA_URL,
                                        wait_until="domcontentloaded",
                                        timeout=60_000)
                        await scraper._dismiss(page)
                        await asyncio.sleep(3)
                    except Exception:
                        pass

        await browser.close()

    log.info(f"[Backfill] raw records: {len(scraper.records)}")

    # Step 3: Enrich, deduplicate, save
    log.info("[Step 3/3] Enriching and scoring…")
    enriched = enrich_records(scraper.records, parcel_index)

    seen: set[str] = set()
    deduped: list[dict] = []
    for r in enriched:
        key = r.get("doc_num") or f"{r.get('owner')}|{r.get('filed')}"
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    removed = len(enriched) - len(deduped)
    log.info(f"[Backfill] {len(deduped)} unique leads "
             f"(removed {removed} cross-window duplicates)")

    dates = sorted(r["filed"] for r in deduped if r.get("filed"))
    date_from = dates[0] if dates else ""
    date_to   = dates[-1] if dates else ""
    output = build_output(deduped, date_from, date_to)

    # Save — backfill-specific paths only
    save_json(output, "dashboard/backfill.json", "data/backfill_latest.json")
    ts = datetime.now().strftime("%Y%m%d")
    save_ghl_csv(deduped, f"data/backfill_{ts}.csv")

    # Write and populate backfill dashboard
    Path("dashboard/backfill.html").write_text(BACKFILL_HTML, encoding="utf-8")
    embed_data_in_dashboard(output, "dashboard/backfill.html")

    elapsed = (datetime.now() - t0).seconds
    addr_ct = sum(1 for r in deduped
                  if r.get("prop_address") or r.get("mail_address"))
    log.info(f"\n✅  Backfill done in {elapsed}s — "
             f"{len(deduped)} leads | {addr_ct} with address")
    if deduped:
        top = deduped[0]
        log.info(f"   Top: {top['owner']} | {top['cat_label']} | "
                 f"score={top['score']} | {top.get('prop_address','no address')}")


def main():
    parser = argparse.ArgumentParser(
        description="Galveston County 365-Day Backfill Scraper"
    )
    parser.add_argument("--days-back",  type=int, default=365)
    parser.add_argument("--chunk-days", type=int, default=5)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()],
    )
    asyncio.run(run_backfill(args.days_back, args.chunk_days))


if __name__ == "__main__":
    main()
