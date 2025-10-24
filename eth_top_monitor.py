#!/usr/bin/env python3
"""
eth_top_monitor.py
Монитор топ-100 держателей ETH -> Telegram

Стратегия:
- пытаемся получить топ-100 с https://etherscan.io/accounts (парсинг HTML)
- если парсинг неудачен и есть BITQUERY_API_KEY, пробуем Bitquery (GraphQL) как запасной вариант
- сравниваем с локальным снимком (JSON)
- если есть изменения (вход, выход, сдвиги ранга), шлём детальное сообщение в Telegram
- по расписанию (SEND_FULL_EVERY) отправляем полный список

Настройки через .env или переменные окружения.
"""

import os
import time
import json
import logging
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler

load_dotenv()

# config
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "600"))
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "").strip()
BITQUERY_API_KEY = os.getenv("BITQUERY_API_KEY", "").strip()
SNAPSHOT_FILE = os.getenv("SNAPSHOT_FILE", "top100_snapshot.json")
SEND_FULL_EVERY = int(os.getenv("SEND_FULL_EVERY", "144"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise SystemExit("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in environment or .env")

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("eth_top_monitor")

@dataclass
class Holder:
    rank: int
    address: str
    balance_eth: float
    balance_readable: str  # like "1,234.5678 ETH"
    percent_of_total: Optional[float] = None
    label: Optional[str] = None  # e.g., 'Exchange: Binance 14'

def send_telegram(text: str, parse_mode: str = "HTML"):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(url, json={
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }, timeout=20)
    try:
        resp.raise_for_status()
        logger.debug("Telegram sent")
    except Exception:
        logger.error("Telegram send failed: %s %s", resp.status_code, resp.text if resp is not None else "no response")

def load_snapshot() -> Dict[str, dict]:
    if not os.path.exists(SNAPSHOT_FILE):
        return {}
    try:
        with open(SNAPSHOT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.exception("Failed to read snapshot file: %s", e)
        return {}

def save_snapshot(snapshot: Dict[str, dict]):
    try:
        with open(SNAPSHOT_FILE, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.exception("Failed to save snapshot file: %s", e)

def parse_etherscan_accounts_page(html: str) -> List[Holder]:
    soup = BeautifulSoup(html, "html.parser")
    # Find table that contains "Rank" or header "Address"
    table = None
    for t in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in t.find_all("th")]
        if any("rank" in h for h in headers) and any("address" in h for h in headers):
            table = t
            break
    if not table:
        raise ValueError("Top accounts table not found on etherscan page")
    rows = []
    for tr in table.find_all("tr"):
        cols = tr.find_all(["td","th"])
        if len(cols) < 3:
            continue
        try:
            rank_text = cols[0].get_text(strip=True)
            rank_digits = ''.join(ch for ch in rank_text if ch.isdigit())
            if not rank_digits:
                continue
            rank = int(rank_digits)
            addr_el = cols[1].find("a")
            address = addr_el.get_text(strip=True) if addr_el else cols[1].get_text(strip=True)
            bal_text = cols[2].get_text(strip=True)
            bal_num_str = bal_text.replace("ETH","").replace(",","").strip().split()[0]
            bal_num = float(bal_num_str)
            percent = None
            if len(cols) >= 4:
                pct_text = cols[3].get_text(strip=True).replace("%","").replace(",","")
                try:
                    percent = float(pct_text)
                except Exception:
                    percent = None
            rows.append(Holder(rank=rank, address=address, balance_eth=bal_num, balance_readable=bal_text, percent_of_total=percent))
        except Exception:
            continue
    rows_sorted = sorted(rows, key=lambda h: h.rank)[:100]
    return rows_sorted

def fetch_top_from_etherscan(pages_needed: int = 4) -> List[Holder]:
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; eth-top-monitor/1.0)"
    }
    holders: List[Holder] = []
    per_page = 25
    page = 1
    while len(holders) < 100 and page <= pages_needed:
        try_urls = [
            f"https://etherscan.io/accounts/{ (page-1)*per_page }",
            f"https://etherscan.io/accounts?page={page}",
            f"https://etherscan.io/accounts/c?page={page}",
        ]
        page_html = None
        for u in try_urls:
            logger.debug("Requesting %s", u)
            try:
                r = session.get(u, headers=headers, timeout=20)
                if r.status_code == 200 and ("Top Accounts" in r.text or "accounts list" in r.text.lower()):
                    page_html = r.text
                    break
            except Exception:
                continue
        if not page_html:
            r = session.get("https://etherscan.io/accounts", headers=headers, timeout=20)
            page_html = r.text
        parsed = parse_etherscan_accounts_page(page_html)
        for h in parsed:
            if len(holders) >= 100:
                break
            if not any(other.address == h.address for other in holders):
                holders.append(h)
        page += 1
        time.sleep(1.0)
    return holders[:100]

def fetch_top_with_bitquery(limit: int = 100) -> List[Holder]:
    if not BITQUERY_API_KEY:
        raise ValueError("No BITQUERY_API_KEY configured")
    url = "https://graphql.bitquery.io"
    query = """
    query ($network: String!, $limit: Int!) {
      ethereum(network: $network) {
        balances(options: {desc: "balance", limit: $limit}) {
          address { address }
          currency { symbol }
          balance
        }
      }
    }
    """
    variables = {"network": "ethereum", "limit": limit}
    headers = {"X-API-KEY": BITQUERY_API_KEY, "Content-Type": "application/json"}
    r = requests.post(url, json={"query": query, "variables": variables}, headers=headers, timeout=30)
    r.raise_for_status()
    data = r.json()
    out = []
    for i, item in enumerate(data.get("data", {}).get("ethereum", {}).get("balances", []), start=1):
        addr = item.get("address", {}).get("address")
        bal = float(item.get("balance", 0))
        out.append(Holder(rank=i, address=addr, balance_eth=bal, balance_readable=str(bal) + " ETH"))
    return out

def holders_to_map(holders: List[Holder]) -> Dict[str, dict]:
    return {h.address.lower(): {"rank": h.rank, "balance_eth": h.balance_eth, "readable": h.balance_readable} for h in holders}

def compare_snapshots(old: Dict[str, dict], new: Dict[str, dict]):
    rank_changes = []
    new_entries = []
    removed = []
    old_addrs = set(old.keys())
    new_addrs = set(new.keys())
    for addr in new_addrs & old_addrs:
        old_rank = old[addr]["rank"]
        new_rank = new[addr]["rank"]
        if old_rank != new_rank:
            rank_changes.append((addr, old_rank, new_rank))
    for addr in new_addrs - old_addrs:
        new_entries.append((addr, new[addr]["rank"]))
    for addr in old_addrs - new_addrs:
        removed.append((addr, old[addr]["rank"]))
    return rank_changes, new_entries, removed

def format_holder_line(rank: int, address: str, readable: str, label: Optional[str]=None) -> str:
    lbl = f" — {label}" if label else ""
    return f"<b>{rank:3d}.</b> <code>{address}</code> — {readable}{lbl}"

def job(state: dict):
    try:
        logger.info("Fetching top holders from Etherscan...")
        try:
            holders = fetch_top_from_etherscan()
            if not holders or len(holders) < 10:
                raise ValueError("Etherscan returned suspiciously few holders")
        except Exception as e:
            logger.warning("Etherscan fetch failed: %s", e)
            if BITQUERY_API_KEY:
                logger.info("Trying Bitquery as backup...")
                holders = fetch_top_with_bitquery(100)
            else:
                raise

        new_map = holders_to_map(holders)
        old_snapshot = load_snapshot()
        old_map = old_snapshot.get("holders_map", {})

        rank_changes, new_entries, removed = compare_snapshots(old_map, new_map)

        messages = []
        if rank_changes or new_entries or removed:
            header = f"🔔 <b>Top-100 ETH list changed</b>\n"
            messages.append(header)
            if new_entries:
                messages.append("<b>➕ New in top-100:</b>")
                for addr, rnk in sorted(new_entries, key=lambda x: x[1]):
                    messages.append(f"{rnk:3d}. <code>{addr}</code>")
            if removed:
                messages.append("\n<b>➖ Removed from top-100:</b>")
                for addr, orank in sorted(removed, key=lambda x: x[1]):
                    messages.append(f"{orank:3d}. <code>{addr}</code>")
            if rank_changes:
                messages.append("\n<b>🔀 Rank changes:</b>")
                for addr, orank, nrank in sorted(rank_changes, key=lambda x: (x[1]-x[2])):
                    delta = nrank - orank
                    arrow = "↑" if delta < 0 else "↓"
                    messages.append(f"{nrank:3d}. <code>{addr}</code> {arrow} ({orank} → {nrank})")

            full_text = "\n".join(messages)
            if len(full_text) > 3800:
                chunks = [full_text[i:i+3500] for i in range(0, len(full_text), 3500)]
                for ch in chunks:
                    send_telegram(ch)
            else:
                send_telegram(full_text)
        else:
            logger.info("No changes in top-100")

        state["polls_done"] = state.get("polls_done", 0) + 1
        if SEND_FULL_EVERY > 0 and state["polls_done"] % SEND_FULL_EVERY == 0:
            header = "<b>📋 Top 100 ETH holders (snapshot)</b>\n"
            lines = [header]
            for h in holders:
                lines.append(format_holder_line(h.rank, h.address, h.balance_readable, h.label))
            text = "\n".join(lines)
            for i in range(0, len(text), 3800):
                send_telegram(text[i:i+3800])

        snapshot = {"timestamp": int(time.time()), "holders_map": new_map}
        save_snapshot(snapshot)

    except Exception as e:
        logger.exception("Job failed: %s", e)
        send_telegram(f"⚠️ <b>eth_top_monitor error</b>\n{str(e)}")

def main():
    state = {"polls_done": 0}
    scheduler = BlockingScheduler()
    job(state)
    scheduler.add_job(lambda: job(state), 'interval', seconds=POLL_INTERVAL, max_instances=1)
    try:
        logger.info("Scheduler start, polling every %s seconds", POLL_INTERVAL)
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Stopping...")

if __name__ == "__main__":
    main()
