#!/usr/bin/env python3
"""
AlgoTrade 2026 — History Bot (Python)

Connects to one or more exchanges and records all market data to files
for offline analysis and backtesting.

Saved files (per exchange):
    <output_dir>/<exchange>_orderbooks.csv   — top-of-book snapshots every tick
    <output_dir>/<exchange>_trades.csv       — all trade events
    <output_dir>/<exchange>_candles.csv      — completed OHLCV candles
    <output_dir>/<exchange>_events.csv       — all events (trades + cancels)

Usage:
    python history_bot.py

    Environment variables:
        EXCHANGES       — comma-separated list of exchange names to connect to,
                          e.g. "NYSE,NASDAQ,LSE". Defaults to all 10 exchanges.
                          Authentication on the production network is by team IP,
                          so no team secret is required.
        OUTPUT_DIR      — output directory (default: ./market_data)
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Optional

from websockets.asyncio.client import connect as ws_connect


# ──────────────────────────────────────────────────────────────────────
#  Configuration
# ──────────────────────────────────────────────────────────────────────

# Production exchange network — each exchange has a fixed IP, all on port 9001.
EXCHANGE_PORT = 9001
EXCHANGE_HOSTS: dict[str, str] = {
    "NYSE":     "10.0.201.2",
    "NASDAQ":   "10.0.202.2",
    "SSE":      "10.0.203.2",
    "JPX":      "10.0.204.2",
    "Euronext": "10.0.205.2",
    "LSE":      "10.0.206.2",
    "HKEX":     "10.0.207.2",
    "NSE":      "10.0.208.2",
    "TMX":      "10.0.209.2",
    "ZSE":      "10.0.210.2",
}

DEFAULT_EXCHANGES = ",".join(EXCHANGE_HOSTS.keys())
EXCHANGES_STR = os.environ.get("EXCHANGES", DEFAULT_EXCHANGES)
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "./market_data")

MAX_RECONNECT_ATTEMPTS = 10
RECONNECT_DELAY_SECONDS = 2


# ──────────────────────────────────────────────────────────────────────
#  CSV Writers
# ──────────────────────────────────────────────────────────────────────

class CSVRecorder:
    """
    Manages CSV files for one exchange.
    Opens files lazily on first write and flushes periodically.
    """

    def __init__(self, exchange: str, output_dir: str) -> None:
        self.exchange = exchange
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self._files: dict[str, IO] = {}
        self._writers: dict[str, csv.writer] = {}
        self._row_counts: dict[str, int] = {}
        self._flush_interval = 100  # flush every N rows

    def _get_writer(self, name: str, headers: list[str]) -> csv.writer:
        """Get or create a CSV writer for the given file."""
        if name not in self._writers:
            path = self.output_dir / f"{self.exchange}_{name}.csv"
            f = open(path, "w", newline="", buffering=1)
            writer = csv.writer(f)
            writer.writerow(headers)
            self._files[name] = f
            self._writers[name] = writer
            self._row_counts[name] = 0
            print(f"[Recorder:{self.exchange}] Writing {name} to {path}")
        return self._writers[name]

    def _maybe_flush(self, name: str) -> None:
        """Flush the file if enough rows have been written."""
        self._row_counts[name] = self._row_counts.get(name, 0) + 1
        if self._row_counts[name] % self._flush_interval == 0:
            self._files[name].flush()

    # ── Recording methods ──

    def record_orderbook(self, server_time: int, instrument: str,
                         bids: dict, asks: dict) -> None:
        """
        Record a top-of-book snapshot.

        Columns: time, instrument, bid1_price, bid1_qty, bid2_price, bid2_qty,
                 bid3_price, bid3_qty, ask1_price, ask1_qty, ask2_price, ask2_qty,
                 ask3_price, ask3_qty
        """
        headers = [
            "time", "instrument",
            "bid1_price", "bid1_qty", "bid2_price", "bid2_qty",
            "bid3_price", "bid3_qty",
            "ask1_price", "ask1_qty", "ask2_price", "ask2_qty",
            "ask3_price", "ask3_qty",
        ]
        writer = self._get_writer("orderbooks", headers)

        # Sort bids descending, asks ascending
        sorted_bids = sorted(
            ((int(p), q) for p, q in bids.items()),
            key=lambda x: x[0], reverse=True
        )
        sorted_asks = sorted(
            ((int(p), q) for p, q in asks.items()),
            key=lambda x: x[0]
        )

        # Pad to 3 levels
        while len(sorted_bids) < 3:
            sorted_bids.append(("", ""))
        while len(sorted_asks) < 3:
            sorted_asks.append(("", ""))

        row = [server_time, instrument]
        for price, qty in sorted_bids[:3]:
            row.extend([price, qty])
        for price, qty in sorted_asks[:3]:
            row.extend([price, qty])

        writer.writerow(row)
        self._maybe_flush("orderbooks")

    def record_trade(self, trade_data: dict) -> None:
        """
        Record a trade event.

        Columns: time, instrument, price, quantity, passive_order_id, active_order_id
        """
        headers = [
            "time", "instrument", "price", "quantity",
            "passive_order_id", "active_order_id",
        ]
        writer = self._get_writer("trades", headers)
        writer.writerow([
            trade_data.get("time", ""),
            trade_data.get("instrumentID", ""),
            trade_data.get("price", ""),
            trade_data.get("quantity", ""),
            trade_data.get("passiveOrderID", ""),
            trade_data.get("activeOrderID", ""),
        ])
        self._maybe_flush("trades")

    def record_candle(self, instrument: str, candle: dict) -> None:
        """
        Record a completed candle.

        Columns: index, instrument, open, high, low, close, volume
        """
        headers = ["index", "instrument", "open", "high", "low", "close", "volume"]
        writer = self._get_writer("candles", headers)
        writer.writerow([
            candle.get("index", ""),
            instrument,
            candle.get("open", ""),
            candle.get("high", ""),
            candle.get("low", ""),
            candle.get("close", ""),
            candle.get("volume", ""),
        ])
        self._maybe_flush("candles")

    def record_cancel(self, cancel_data: dict) -> None:
        """
        Record a cancel event.

        Columns: time, instrument, order_id, expired
        """
        headers = ["time", "instrument", "order_id", "expired"]
        writer = self._get_writer("events", headers)
        writer.writerow([
            cancel_data.get("time", ""),
            cancel_data.get("instrumentID", ""),
            cancel_data.get("orderID", ""),
            cancel_data.get("expired", ""),
        ])
        self._maybe_flush("events")

    def close(self) -> None:
        """Flush and close all open files."""
        for name, f in self._files.items():
            try:
                f.flush()
                f.close()
            except Exception:
                pass
        total = sum(self._row_counts.values())
        print(f"[Recorder:{self.exchange}] Closed — {total} total rows written")


# ──────────────────────────────────────────────────────────────────────
#  Exchange Handler
# ──────────────────────────────────────────────────────────────────────

async def handle_exchange(
    name: str,
    host: str,
    port: int,
    output_dir: str,
) -> None:
    """
    Connect to one exchange, receive market data, and record everything.
    Handles reconnection on failure.
    """
    recorder = CSVRecorder(name, output_dir)
    # Production network authenticates by team IP — no team_secret needed.
    url = f"ws://{host}:{port}/trade"
    tick_count = 0

    try:
        for attempt in range(1, MAX_RECONNECT_ATTEMPTS + 1):
            try:
                async with ws_connect(url) as ws:
                    # Read welcome
                    welcome_raw = await ws.recv()
                    welcome = json.loads(welcome_raw)
                    if welcome.get("type") == "welcome":
                        print(f"[{name}] Connected: {welcome.get('message', '')}")
                    else:
                        print(f"[{name}] Unexpected welcome: {welcome}")

                    # Main receive loop
                    async for raw_msg in ws:
                        msg = json.loads(raw_msg)
                        msg_type = msg.get("type", "")

                        if msg_type == "market_data_update":
                            tick_count += 1
                            server_time = msg.get("time", 0)

                            # ── Record order books ──
                            for inst_id, book in msg.get("orderbook_depths", {}).items():
                                recorder.record_orderbook(
                                    server_time, inst_id,
                                    book.get("bids", {}),
                                    book.get("asks", {}),
                                )

                            # ── Record candles ──
                            for inst_id, candle_list in msg.get("candles", {}).get("tradeable", {}).items():
                                for candle in candle_list:
                                    recorder.record_candle(inst_id, candle)

                            # ── Record events (trades + cancels) ──
                            for event in msg.get("events", []):
                                event_type = event.get("event_type", "")
                                data = event.get("data", {})

                                if event_type == "trade":
                                    recorder.record_trade(data)
                                elif event_type == "cancel":
                                    recorder.record_cancel(data)

                            # Progress log
                            if tick_count % 100 == 0:
                                print(
                                    f"[{name}] {tick_count} ticks recorded "
                                    f"(server_time={server_time})"
                                )

                        elif msg_type == "end_of_round":
                            print(f"[{name}] Round ended — {tick_count} ticks total")
                            return  # clean exit

                        elif msg_type == "error":
                            print(f"[{name}] Error: {msg.get('message', '')}")

            except Exception as e:
                print(f"[{name}] Connection error (attempt {attempt}): {e}")
                if attempt < MAX_RECONNECT_ATTEMPTS:
                    await asyncio.sleep(RECONNECT_DELAY_SECONDS)

        print(f"[{name}] Failed to connect after {MAX_RECONNECT_ATTEMPTS} attempts")

    finally:
        recorder.close()


# ──────────────────────────────────────────────────────────────────────
#  Parsing & Entry Point
# ──────────────────────────────────────────────────────────────────────

def parse_exchanges(exchanges_str: str) -> list[tuple[str, str, int]]:
    """Parse 'NYSE,NASDAQ,...' into [(name, host, port), ...]."""
    result = []
    for entry in exchanges_str.split(","):
        name = entry.strip()
        if not name:
            continue
        host = EXCHANGE_HOSTS.get(name)
        if host is None:
            print(f"[HistoryBot] Unknown exchange: {name!r}. "
                  f"Known: {', '.join(EXCHANGE_HOSTS)}")
            continue
        result.append((name, host, EXCHANGE_PORT))
    return result


async def run() -> None:
    """Main entry point — connect to all exchanges and record data."""
    exchanges = parse_exchanges(EXCHANGES_STR)
    if not exchanges:
        print("[HistoryBot] No exchanges configured. Set EXCHANGES env var.")
        print(f"[HistoryBot] Format: EXCHANGES='NYSE,NASDAQ,LSE' "
              f"(known: {', '.join(EXCHANGE_HOSTS)})")
        return

    output_path = Path(OUTPUT_DIR).resolve()
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"[HistoryBot] Recording {len(exchanges)} exchange(s) to {output_path}")
    print(f"[HistoryBot] Exchanges: {', '.join(n for n, _, _ in exchanges)}")

    # Run all exchange handlers concurrently
    tasks = [
        asyncio.create_task(
            handle_exchange(name, host, port, str(output_path))
        )
        for name, host, port in exchanges
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("\n[HistoryBot] Interrupted — saving files and exiting")
    finally:
        print(f"[HistoryBot] Done. Data saved to {output_path}/")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
