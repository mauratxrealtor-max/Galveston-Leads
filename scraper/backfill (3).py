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

BACKFILL_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>Galveston County — Backfill Leads (365 Days)</title>
<style>
:root{--bg:#0f1117;--surface:#1a1d27;--card:#21253a;--border:#2d3151;
--accent:#f7c04f;--accent2:#4f8ef7;--green:#3ecf8e;--red:#f06565;
--text:#e4e8f0;--muted:#7a82a0;--radius:8px;}
*{box-sizing:border-box;margin:0;padding:0;}
body{background:var(--bg);color:var(--text);font-family:system-ui,sans-serif;font-size:14px;}
header{background:var(--surface);border-bottom:1px solid var(--border);padding:14px 24px;
display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;}
.logo{font-size:18px;font-weight:700;color:var(--accent);}
.logo span{color:var(--accent2);}
.toolbar{padding:12px 24px;display:flex;flex-wrap:wrap;gap:10px;
background:var(--surface);border-bottom:1px solid var(--border);}
input,select{background:var(--card);color:var(--text);border:1px solid var(--border);
border-radius:var(--radius);padding:7px 12px;font-size:13px;outline:none;}
input{width:220px;}select{cursor:pointer;}
.btn{background:var(--accent);color:#000;border:none;border-radius:var(--radius);
padding:7px 16px;font-size:13px;font-weight:700;cursor:pointer;}
.stats{display:flex;gap:14px;padding:14px 24px;flex-wrap:wrap;}
.stat{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);
padding:10px 18px;min-width:110px;}
.sv{font-size:22px;font-weight:700;color:var(--accent);}
.sl{font-size:11px;color:var(--muted);margin-top:2px;text-transform:uppercase;letter-spacing:.5px;}
.notice{background:#2a2010;border:1px solid #f7c04f44;border-radius:var(--radius);
margin:10px 24px 0;padding:10px 16px;font-size:12px;color:var(--accent);}
.content{padding:0 24px 40px;}
.wrap{overflow-x:auto;border-radius:var(--radius);border:1px solid var(--border);margin-top:14px;}
table{width:100%;border-collapse:collapse;}
thead th{background:var(--surface);color:var(--muted);font-size:11px;
text-transform:uppercase;padding:10px 12px;text-align:left;
border-bottom:1px solid var(--border);cursor:pointer;white-space:nowrap;}
tbody tr{border-bottom:1px solid var(--border);cursor:pointer;}
tbody tr:hover{background:var(--card);}
td{padding:9px 12px;vertical-align:middle;}
.sb{display:inline-block;border-radius:20px;padding:2px 9px;font-weight:700;font-size:12px;}
.sh{background:#1f3a2a;color:#3ecf8e;}.sm{background:#3a2d1a;color:#f7c04f;}
.sl2{background:#2d1a1a;color:#f06565;}
.cp{display:inline-block;border-radius:4px;padding:1px 7px;font-size:11px;font-weight:600;}
.cat-LP,.cat-NOFC{background:#2a1f3d;color:#b48ffc;}
.cat-JUD,.cat-CCJ,.cat-DRJUD{background:#1f3040;color:#5bc0fa;}
.cat-LNIRS,.cat-LNFED,.cat-LNCORPTX,.cat-TAXDEED{background:#3a1f1f;color:#f06565;}
.cat-LN,.cat-LNMECH,.cat-LNHOA,.cat-MEDLN{background:#1f2f1f;color:#3ecf8e;}
.cat-PRO{background:#2d2a1a;color:#f0c96f;}
.cat-NOC{background:#1f2438;color:#7ab5fa;}
.cat-RELLP{background:#1e2828;color:#4db89c;}
.cat-{background:var(--card);color:var(--muted);}
.fc{display:inline-block;background:var(--card);border:1px solid var(--border);
border-radius:4px;padding:1px 6px;font-size:10px;color:var(--muted);margin:1px;}
.fc.hot{border-color:#f06565;color:#f06565;}
.fc.def{border-color:#f7c04f;color:#f7c04f;background:#2d2a1a;}
.pg{display:flex;gap:6px;align-items:center;justify-content:flex-end;padding:12px 0;}
.pg button{background:var(--card);border:1px solid var(--border);color:var(--text);
border-radius:6px;padding:5px 12px;cursor:pointer;font-size:13px;}
.pg button.a{background:var(--accent);border-color:var(--accent);color:#000;}
.pg button:disabled{opacity:.4;cursor:default;}
.pi{color:var(--muted);font-size:12px;}
.empty{text-align:center;padding:60px 20px;color:var(--muted);}
.det{display:none;background:#191c2c;padding:14px 20px 14px 44px;
display:none;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:10px;}
.det.open{display:grid;}
.dg{display:flex;flex-direction:column;gap:2px;}
.dl{font-size:10px;text-transform:uppercase;letter-spacing:.5px;color:var(--muted);}
.dv{font-size:13px;color:var(--text);}
.dv a{color:var(--accent2);text-decoration:none;}
</style>
</head>
<body>
<header>
  <div class="logo">📚 <span>Galveston</span> Backfill Leads</div>
  <div id="meta" style="font-size:12px;color:var(--muted)">Loading…</div>
</header>
<div class="toolbar">
  <input id="q" placeholder="🔍 Search owner, address, doc #..." oninput="go()"/>
  <select id="cat" onchange="go()">
    <option value="">All Types</option>
    <option value="LP">Lis Pendens</option><option value="NOFC">Notice of Foreclosure</option>
    <option value="TAXDEED">Tax Deed</option><option value="JUD">Judgment</option>
    <option value="CCJ">Certified Judgment</option><option value="DRJUD">Dom. Judgment</option>
    <option value="LNIRS">IRS Lien</option><option value="LNFED">Federal Lien</option>
    <option value="LNCORPTX">Corp Tax Lien</option><option value="LN">Lien</option>
    <option value="LNMECH">Mechanic Lien</option><option value="LNHOA">HOA Lien</option>
    <option value="MEDLN">Medicaid Lien</option><option value="PRO">Probate</option>
    <option value="NOC">Notice of Commencement</option><option value="RELLP">Release LP</option>
  </select>
  <select id="sc" onchange="go()">
    <option value="">All Scores</option>
    <option value="70">≥ 70 Hot</option><option value="50">≥ 50</option>
    <option value="30">≥ 30</option>
  </select>
  <select id="yr" onchange="go()">
    <option value="">All Years</option>
    <option value="2026">2026</option><option value="2025">2025</option>
    <option value="2024">2024</option>
  </select>
  <select id="addr" onchange="go()">
    <option value="">All Records</option>
    <option value="1">Has Address</option>
  </select>
  <button class="btn" onclick="csv()">⬇ Export CSV</button>
</div>
<div class="stats">
  <div class="stat"><div class="sv" id="st">—</div><div class="sl">Total Leads</div></div>
  <div class="stat"><div class="sv" id="sh">—</div><div class="sl">Hot (≥70)</div></div>
  <div class="stat"><div class="sv" id="sa">—</div><div class="sl">With Address</div></div>
  <div class="stat"><div class="sv" id="sr">—</div><div class="sl">Date Range</div></div>
</div>
<div class="notice">📚 365-day historical backfill.
  <a href="index.html" style="color:var(--accent2)">→ Daily dashboard</a></div>
<div id="upd" style="font-size:11px;color:var(--muted);padding:5px 24px 0"></div>
<div class="content">
  <div class="wrap"><table>
    <thead><tr>
      <th></th>
      <th onclick="sort('score')">Score</th>
      <th onclick="sort('filed')">Filed</th>
      <th onclick="sort('cat')">Type</th>
      <th onclick="sort('doc_num')">Doc #</th>
      <th onclick="sort('owner')">Owner</th>
      <th>Property Address</th>
      <th onclick="sort('amount')">Amount</th>
      <th>Flags</th>
    </tr></thead>
    <tbody id="tb"></tbody>
  </table></div>
  <div class="pg" id="pg"></div>
</div>
<script>
const PS=100,HOT=new Set(["Lis pendens","Pre-foreclosure","Judgment lien","Tax lien"]),
      DEF=new Set(["Tax deferral"]);
let all=[],fil=[],pg=1,sc="score",asc=false;

async function init(){
  let d=null;
  try{const el=document.getElementById("inline-records-data");
    if(el&&el.textContent.trim()){const p=JSON.parse(el.textContent);if(p&&p.total>0)d=p;}
  }catch(e){}
  if(!d){try{const r=await fetch("backfill.json?t="+Date.now());if(r.ok)d=await r.json();}catch(e){}}
  if(!d||!d.total){
    document.getElementById("meta").textContent="No backfill data — run the workflow";
    document.getElementById("tb").innerHTML=
      "<tr><td colspan='9' class='empty'>Run the backfill workflow from GitHub Actions.</td></tr>";
    return;
  }
  all=d.records||[];
  const dr=d.date_range;
  document.getElementById("meta").textContent=
    dr&&dr.from?dr.from+" → "+dr.to+" · "+d.total.toLocaleString()+" leads":d.total.toLocaleString()+" leads";
  if(d.fetched_at)document.getElementById("upd").textContent="Last backfill: "+new Date(d.fetched_at).toLocaleString();
  go();
}

function stats(r){
  document.getElementById("st").textContent=r.length.toLocaleString();
  document.getElementById("sh").textContent=r.filter(x=>(x.score||0)>=70).length.toLocaleString();
  document.getElementById("sa").textContent=r.filter(x=>x.prop_address||x.mail_address).length.toLocaleString();
  const ds=r.map(x=>x.filed).filter(Boolean).sort();
  document.getElementById("sr").textContent=ds.length?ds[0].slice(0,7)+" – "+ds[ds.length-1].slice(0,7):"—";
}

function filt(){
  const q=document.getElementById("q").value.toLowerCase(),
        cat=document.getElementById("cat").value,
        sc2=parseInt(document.getElementById("sc").value)||0,
        yr=document.getElementById("yr").value,
        ad=document.getElementById("addr").value;
  return all.filter(r=>{
    if(cat&&r.cat!==cat)return false;
    if(sc2&&(r.score||0)<sc2)return false;
    if(yr&&r.filed&&!r.filed.startsWith(yr))return false;
    if(ad&&!r.prop_address&&!r.mail_address)return false;
    if(q&&![r.owner,r.doc_num,r.prop_address,r.cat_label].join(" ").toLowerCase().includes(q))return false;
    return true;
  });
}

function sort(c){sc===c?asc=!asc:(sc=c,asc=c!=="score");go();}

function go(){
  fil=filt();
  fil.sort((a,b)=>{
    let va=a[sc]??"",vb=b[sc]??"";
    if(sc==="score"||sc==="amount"){va=parseFloat(va)||0;vb=parseFloat(vb)||0;}
    else{va=String(va).toLowerCase();vb=String(vb).toLowerCase();}
    return va<vb?(asc?-1:1):va>vb?(asc?1:-1):0;
  });
  pg=1;draw();pag();stats(fil);
}

function sc2(s){return s>=70?"sh":s>=50?"sm":"sl2";}
function e(s){return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");}

function draw(){
  const sl=fil.slice((pg-1)*PS,pg*PS),tb=document.getElementById("tb");
  if(!sl.length){tb.innerHTML="<tr><td colspan='9' class='empty'>No records match filters.</td></tr>";return;}
  tb.innerHTML=sl.map((r,i)=>{
    const idx=(pg-1)*PS+i,s=r.score||0,
      pa=[r.prop_address,r.prop_city,r.prop_state,r.prop_zip].filter(Boolean).join(", ")||"—",
      ma=[r.mail_address,r.mail_city,r.mail_state,r.mail_zip].filter(Boolean).join(", ")||"—",
      fl=(r.flags||[]).map(f=>`<span class="fc ${HOT.has(f)?"hot":DEF.has(f)?"def":""}">${f}</span>`).join(""),
      amt=r.amount?"$"+Number(r.amount).toLocaleString("en-US",{minimumFractionDigits:2}):"—",
      dl=r.clerk_url?`<a href="${r.clerk_url}" target="_blank" style="color:var(--accent2);font-size:11px">${r.doc_num||"—"} ↗</a>`
                    :`<span style="font-size:11px;color:var(--muted)">${r.doc_num||"—"}</span>`;
    return `<tr onclick="tog(${idx})">
      <td><span id="ex${idx}" style="color:var(--muted);font-size:10px">▶</span></td>
      <td><span class="sb ${sc2(s)}">${s}</span></td>
      <td style="white-space:nowrap">${r.filed||"—"}</td>
      <td><span class="cp cat-${r.cat||""}">${r.cat_label||r.doc_type||"—"}</span></td>
      <td>${dl}</td><td>${e(r.owner||"—")}</td>
      <td style="font-size:12px">${e(pa)}</td>
      <td style="white-space:nowrap">${amt}</td>
      <td>${fl||"—"}</td>
    </tr>
    <tr id="dt${idx}"><td colspan="9"><div class="det" id="dp${idx}">
      ${[["Doc Number",dl],["Type",e(r.doc_type||r.cat_label||"—")],
         ["Category",`<span class="cp cat-${r.cat||""}">${r.cat_label||"—"}</span>`],
         ["Filed",r.filed||"—"],["Owner",e(r.owner||"—")],["Grantee",e(r.grantee||"—")],
         ["Amount",amt],["Property",e(pa)],["Mailing",e(ma)],
         ["Score",`<span class="sb ${sc2(s)}">${s}</span>`],
         ["Flags",(r.flags||[]).join(" · ")||"—"]
        ].map(([l,v])=>`<div class="dg"><span class="dl">${l}</span><span class="dv">${v}</span></div>`).join("")}
    </div></td></tr>`;
  }).join("");
}

function tog(i){
  const dp=document.getElementById("dp"+i),ex=document.getElementById("ex"+i);
  if(!dp)return;
  const open=dp.classList.toggle("open");
  if(ex)ex.textContent=open?"▼":"▶";
}

function pag(){
  const tot=Math.ceil(fil.length/PS),el=document.getElementById("pg");
  if(tot<=1){el.innerHTML="";return;}
  let h=`<span class="pi">${fil.length.toLocaleString()} records</span>`;
  h+=`<button ${pg===1?"disabled":""} onclick="gp(${pg-1})">‹</button>`;
  for(let i=Math.max(1,pg-2);i<=Math.min(tot,pg+2);i++)
    h+=`<button class="${i===pg?"a":""}" onclick="gp(${i})">${i}</button>`;
  h+=`<button ${pg===tot?"disabled":""} onclick="gp(${pg+1})">›</button>`;
  el.innerHTML=h;
}

function gp(p){pg=p;draw();pag();scrollTo(0,0);}

function csv(){
  const cols=["First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
    "Property Address","Property City","Property State","Property Zip","Lead Type","Document Type",
    "Date Filed","Document Number","Amount","Seller Score","Flags","Source","URL"];
  const rows=[cols.join(",")];
  for(const r of fil){
    let fi="",la="";
    if(r.owner){if(r.owner.includes(","))
      {const[l,f]=r.owner.split(",",2);fi=f.trim();la=l.trim();}
      else{const p=r.owner.split(" ");fi=p[0];la=p.slice(1).join(" ");}}
    rows.push([fi,la,r.mail_address||"",r.mail_city||"",r.mail_state||"",r.mail_zip||"",
      r.prop_address||"",r.prop_city||"",r.prop_state||"",r.prop_zip||"",
      r.cat_label||"",r.doc_type||"",r.filed||"",r.doc_num||"",
      r.amount?"$"+Number(r.amount).toFixed(2):"",r.score||0,
      (r.flags||[]).join("; "),"Galveston County Clerk (Backfill)",r.clerk_url||""]
      .map(v=>'"'+String(v).replace(/"/g,'""')+'"').join(","));
  }
  const a=document.createElement("a");
  a.href=URL.createObjectURL(new Blob([rows.join("\n")],{type:"text/csv"}));
  a.download="galveston_backfill_"+new Date().toISOString().slice(0,10)+".csv";
  a.click();
}

init();
</script>
</body>
</html>'''


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
