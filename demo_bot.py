#!/usr/bin/env python3
"""
AlgoTrade 2026 — Demo Trading Bot (Python)

A reference trading bot that demonstrates the full exchange API.
The bot is structured around a pluggable Strategy interface — replace
the example strategy with your own logic.

Architecture:
    ExchangeConnection  — manages one WebSocket connection to one exchange
    MarketState         — parsed, queryable snapshot of the latest market data
    Strategy            — abstract base class; implement on_market_data() and on_fill()
    SimpleStrategy      — minimal example that prints the top-of-book
    Bot                 — orchestrates connections and dispatches events to the strategy

Usage:
    python demo_bot.py

    Environment variables (or edit the constants below):
        EXCHANGES       — comma-separated list of exchange names to connect to,
                          e.g. "NYSE,NASDAQ,LSE". Defaults to all 10 exchanges.
                          Authentication on the production network is by team IP,
                          so no team secret is required.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

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

# Comma-separated list of exchange names. Defaults to all 10.
DEFAULT_EXCHANGES = ",".join(EXCHANGE_HOSTS.keys())
EXCHANGES_STR = os.environ.get("EXCHANGES", DEFAULT_EXCHANGES)

# Reconnection settings
MAX_RECONNECT_ATTEMPTS = 10
RECONNECT_DELAY_SECONDS = 2

# Order defaults
DEFAULT_ORDER_EXPIRY_MS = 10_000  # 10 seconds


# ──────────────────────────────────────────────────────────────────────
#  Data Structures
# ──────────────────────────────────────────────────────────────────────

@dataclass
class OrderBookLevel:
    """A single price level in the order book."""
    price: int       # cents
    quantity: int    # shares


@dataclass
class OrderBook:
    """Top-of-book snapshot for one instrument."""
    bids: list[OrderBookLevel] = field(default_factory=list)  # sorted descending by price
    asks: list[OrderBookLevel] = field(default_factory=list)  # sorted ascending by price

    @property
    def best_bid(self) -> Optional[int]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[int]:
        return self.asks[0].price if self.asks else None

    @property
    def mid_price(self) -> Optional[float]:
        if self.best_bid is not None and self.best_ask is not None:
            return (self.best_bid + self.best_ask) / 2.0
        return None

    @property
    def spread(self) -> Optional[int]:
        if self.best_bid is not None and self.best_ask is not None:
            return self.best_ask - self.best_bid
        return None


@dataclass
class Candle:
    """One completed OHLCV candle (1 second period)."""
    open: Optional[int]
    close: Optional[int]
    high: Optional[int]
    low: Optional[int]
    volume: Optional[int]
    index: int


@dataclass
class Trade:
    """A trade event from the exchange."""
    instrument_id: str
    passive_order_id: int
    active_order_id: int
    quantity: int
    price: int
    time: int


@dataclass
class Fill:
    """A fill on one of our orders."""
    exchange: str
    instrument_id: str
    order_id: int
    side: str
    price: int
    quantity: int
    inventory_change: Optional[int]
    balance_change: Optional[int]


@dataclass
class MarketState:
    """
    Aggregated market state across all exchanges.
    Updated every time a market_data_update is received.
    """
    # exchange_name -> instrument_id -> OrderBook
    order_books: dict[str, dict[str, OrderBook]] = field(default_factory=dict)

    # exchange_name -> instrument_id -> list of recent candles
    candles: dict[str, dict[str, list[Candle]]] = field(default_factory=dict)

    # exchange_name -> instrument_id -> list of recent trades
    recent_trades: dict[str, dict[str, list[Trade]]] = field(default_factory=dict)

    # exchange_name -> server time (ms)
    server_times: dict[str, int] = field(default_factory=dict)

    # ── Helpers ──

    def get_book(self, exchange: str, instrument: str) -> Optional[OrderBook]:
        """Get the order book for an instrument on a specific exchange."""
        return self.order_books.get(exchange, {}).get(instrument)

    def get_best_bid(self, exchange: str, instrument: str) -> Optional[int]:
        book = self.get_book(exchange, instrument)
        return book.best_bid if book else None

    def get_best_ask(self, exchange: str, instrument: str) -> Optional[int]:
        book = self.get_book(exchange, instrument)
        return book.best_ask if book else None

    def get_mid(self, exchange: str, instrument: str) -> Optional[float]:
        book = self.get_book(exchange, instrument)
        return book.mid_price if book else None

    def instruments_on(self, exchange: str) -> list[str]:
        """List all instruments seen on an exchange."""
        return list(self.order_books.get(exchange, {}).keys())


# ──────────────────────────────────────────────────────────────────────
#  Strategy Interface
# ──────────────────────────────────────────────────────────────────────

class Strategy(ABC):
    """
    Abstract strategy interface.

    Subclass this and implement the callback methods.
    Every callback receives the Bot instance explicitly so you can
    place orders, cancel orders, and query inventory directly:

        async def on_market_data(self, bot, exchange, state):
            await bot.place_order(exchange, "NYSE-CARD", "bid", 10000, 10)

    Lifecycle:
        1. on_connected(bot, exchange)           — WebSocket connected
        2. on_market_data(bot, exchange, state)   — every 100ms per exchange
        3. on_fill(bot, fill)                     — your order was filled
        4. on_round_end(bot, exchange)            — round ended
    """

    @abstractmethod
    async def on_market_data(self, bot: Bot, exchange: str, state: MarketState) -> None:
        """
        Called every time a market_data_update is received from an exchange.

        Args:
            bot:      The Bot instance — use it to place/cancel orders
            exchange: The exchange name (e.g. "NYSE")
            state:    The full aggregated MarketState across all exchanges
        """
        ...

    async def on_connected(self, bot: Bot, exchange: str) -> None:
        """Called when a WebSocket connection to an exchange is established."""
        pass

    async def on_fill(self, bot: Bot, fill: Fill) -> None:
        """Called when one of your orders is filled (partially or fully)."""
        pass

    async def on_round_end(self, bot: Bot, exchange: str) -> None:
        """Called when the round ends on an exchange."""
        pass


# ──────────────────────────────────────────────────────────────────────
#  Example Strategy — SimpleStrategy
# ──────────────────────────────────────────────────────────────────────

class SimpleStrategy(Strategy):
    """
    A minimal example strategy that logs top-of-book data.

    This does NOT trade — it just prints market data to the console.
    Replace this with your own strategy logic.
    """

    def __init__(self) -> None:
        self._tick_count: dict[str, int] = {}

    async def on_connected(self, bot: Bot, exchange: str) -> None:
        print(f"[SimpleStrategy] Connected to {exchange}")
        # Example: request inventory on connect
        await bot.get_inventory(exchange)

    async def on_market_data(self, bot: Bot, exchange: str, state: MarketState) -> None:
        # Print top-of-book once every 10 ticks (1 second) to avoid spam
        self._tick_count[exchange] = self._tick_count.get(exchange, 0) + 1
        if self._tick_count[exchange] % 10 != 0:
            return

        instruments = state.instruments_on(exchange)
        if not instruments:
            return

        print(f"\n[SimpleStrategy] {exchange} — tick {self._tick_count[exchange]}")
        for inst in sorted(instruments)[:5]:  # show first 5
            book = state.get_book(exchange, inst)
            if book and book.best_bid and book.best_ask:
                print(
                    f"  {inst:20s}  "
                    f"bid={book.best_bid:>8d}  "
                    f"ask={book.best_ask:>8d}  "
                    f"spread={book.spread:>6d}  "
                    f"mid={book.mid_price:>10.1f}"
                )

        # Example: this is where you'd call bot.place_order() to trade
        # await bot.place_order(exchange, "NYSE-CARD", "bid", 9900, 10)

    async def on_fill(self, bot: Bot, fill: Fill) -> None:
        print(
            f"[SimpleStrategy] FILL on {fill.exchange}: "
            f"{fill.instrument_id} {fill.side} "
            f"{fill.quantity}@{fill.price}"
        )

    async def on_round_end(self, bot: Bot, exchange: str) -> None:
        print(f"[SimpleStrategy] Round ended on {exchange}")


# ──────────────────────────────────────────────────────────────────────
#  Exchange Connection
# ──────────────────────────────────────────────────────────────────────

class ExchangeConnection:
    """
    Manages a single WebSocket connection to one exchange.
    Handles connecting, reconnecting, parsing messages, and sending orders.
    """

    def __init__(self, name: str, host: str, port: int) -> None:
        self.name = name
        self.host = host
        self.port = port
        self.ws = None
        self._request_counter = 0
        self._connected = False

    @property
    def url(self) -> str:
        # Production network authenticates by team IP — no team_secret needed.
        return f"ws://{self.host}:{self.port}/trade"

    @property
    def connected(self) -> bool:
        return self._connected and self.ws is not None

    def _next_request_id(self) -> str:
        self._request_counter += 1
        return f"{self.name}-{self._request_counter}"

    # ── Connection ──

    async def connect(self) -> bool:
        """Establish the WebSocket connection. Returns True on success."""
        for attempt in range(1, MAX_RECONNECT_ATTEMPTS + 1):
            try:
                self.ws = await ws_connect(self.url)
                welcome = json.loads(await self.ws.recv())
                if welcome.get("type") == "welcome":
                    self._connected = True
                    print(f"[{self.name}] Connected: {welcome.get('message', '')}")
                    return True
            except Exception as e:
                print(f"[{self.name}] Connection attempt {attempt} failed: {e}")
                if attempt < MAX_RECONNECT_ATTEMPTS:
                    await asyncio.sleep(RECONNECT_DELAY_SECONDS)
        print(f"[{self.name}] Failed to connect after {MAX_RECONNECT_ATTEMPTS} attempts")
        return False

    async def disconnect(self) -> None:
        """Close the WebSocket connection."""
        self._connected = False
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass
            self.ws = None

    async def recv(self) -> Optional[dict]:
        """Receive and parse one JSON message. Returns None on error."""
        if not self.ws:
            return None
        try:
            raw = await self.ws.recv()
            return json.loads(raw)
        except Exception:
            self._connected = False
            return None

    # ── Order Management ──

    async def place_order(
        self,
        instrument_id: str,
        side: str,
        price: int,
        quantity: int,
        expiry_ms: Optional[int] = None,
    ) -> Optional[dict]:
        """
        Place a limit order on this exchange.

        Args:
            instrument_id: e.g. "NYSE-CARD"
            side:          "bid" (buy) or "ask" (sell)
            price:         price in cents
            quantity:      number of shares
            expiry_ms:     order lifetime in ms from now (default: 10s)

        Returns:
            The server response dict, or None on failure.
        """
        if not self.connected:
            return None

        if expiry_ms is None:
            expiry_ms = DEFAULT_ORDER_EXPIRY_MS

        request_id = self._next_request_id()
        msg = {
            "type": "add_order",
            "user_request_id": request_id,
            "instrument_id": instrument_id,
            "price": price,
            "expiry": int(time.time() * 1000) + expiry_ms,
            "side": side,
            "quantity": quantity,
        }
        try:
            await self.ws.send(json.dumps(msg))
            return {"request_id": request_id}
        except Exception as e:
            print(f"[{self.name}] Failed to send order: {e}")
            return None

    async def cancel_order(self, order_id: int, instrument_id: str) -> Optional[dict]:
        """
        Cancel a resting order.

        Args:
            order_id:       the order ID returned by the exchange
            instrument_id:  the instrument the order is on

        Returns:
            The server response dict, or None on failure.
        """
        if not self.connected:
            return None

        request_id = self._next_request_id()
        msg = {
            "type": "cancel_order",
            "user_request_id": request_id,
            "order_id": order_id,
            "instrument_id": instrument_id,
        }
        try:
            await self.ws.send(json.dumps(msg))
            return {"request_id": request_id}
        except Exception as e:
            print(f"[{self.name}] Failed to send cancel: {e}")
            return None

    async def get_inventory(self) -> Optional[dict]:
        """Request the current inventory snapshot."""
        if not self.connected:
            return None

        request_id = self._next_request_id()
        msg = {
            "type": "get_inventory",
            "user_request_id": request_id,
        }
        try:
            await self.ws.send(json.dumps(msg))
            return {"request_id": request_id}
        except Exception as e:
            print(f"[{self.name}] Failed to request inventory: {e}")
            return None

    async def get_pending_orders(self) -> Optional[dict]:
        """Request all pending orders."""
        if not self.connected:
            return None

        request_id = self._next_request_id()
        msg = {
            "type": "get_pending_orders",
            "user_request_id": request_id,
        }
        try:
            await self.ws.send(json.dumps(msg))
            return {"request_id": request_id}
        except Exception as e:
            print(f"[{self.name}] Failed to request pending orders: {e}")
            return None


# ──────────────────────────────────────────────────────────────────────
#  Market Data Parser
# ──────────────────────────────────────────────────────────────────────

def parse_order_book(raw: dict) -> OrderBook:
    """Parse a raw orderbook_depths entry into an OrderBook."""
    bids = []
    for price_str, qty in raw.get("bids", {}).items():
        bids.append(OrderBookLevel(price=int(price_str), quantity=qty))
    bids.sort(key=lambda x: x.price, reverse=True)

    asks = []
    for price_str, qty in raw.get("asks", {}).items():
        asks.append(OrderBookLevel(price=int(price_str), quantity=qty))
    asks.sort(key=lambda x: x.price)

    return OrderBook(bids=bids, asks=asks)


def parse_candle(raw: dict) -> Candle:
    """Parse a raw candle dict."""
    return Candle(
        open=raw.get("open"),
        close=raw.get("close"),
        high=raw.get("high"),
        low=raw.get("low"),
        volume=raw.get("volume"),
        index=raw.get("index", 0),
    )


def parse_trade(raw: dict) -> Trade:
    """Parse a raw trade event."""
    d = raw["data"]
    return Trade(
        instrument_id=d["instrumentID"],
        passive_order_id=d["passiveOrderID"],
        active_order_id=d["activeOrderID"],
        quantity=d["quantity"],
        price=d["price"],
        time=d["time"],
    )


def update_market_state(state: MarketState, exchange: str, data: dict) -> list[Trade]:
    """
    Update the MarketState from a market_data_update message.
    Returns a list of parsed Trade events.
    """
    state.server_times[exchange] = data.get("time", 0)

    # Order books
    if exchange not in state.order_books:
        state.order_books[exchange] = {}
    for inst_id, raw_book in data.get("orderbook_depths", {}).items():
        state.order_books[exchange][inst_id] = parse_order_book(raw_book)

    # Candles
    if exchange not in state.candles:
        state.candles[exchange] = {}
    for inst_id, raw_candles in data.get("candles", {}).get("tradeable", {}).items():
        if inst_id not in state.candles[exchange]:
            state.candles[exchange][inst_id] = []
        for rc in raw_candles:
            state.candles[exchange][inst_id].append(parse_candle(rc))

    # Events — extract trades
    trades = []
    for event in data.get("events", []):
        if event.get("event_type") == "trade":
            trades.append(parse_trade(event))

    return trades


# ──────────────────────────────────────────────────────────────────────
#  Bot — Main Orchestrator
# ──────────────────────────────────────────────────────────────────────

class Bot:
    """
    Main bot orchestrator.

    Manages connections to multiple exchanges, maintains the aggregated
    MarketState, and dispatches events to the plugged-in Strategy.

    Usage:
        strategy = SimpleStrategy()
        bot = Bot(strategy)
        asyncio.run(bot.run())
    """

    def __init__(self, strategy: Strategy) -> None:
        self.strategy = strategy
        self.state = MarketState()
        self.connections: dict[str, ExchangeConnection] = {}
        self._running = False

    # ── Public API for strategies ──

    async def place_order(
        self,
        exchange: str,
        instrument_id: str,
        side: str,
        price: int,
        quantity: int,
        expiry_ms: Optional[int] = None,
    ) -> Optional[dict]:
        """
        Place an order on a specific exchange.
        Convenience wrapper for use inside strategies.
        """
        conn = self.connections.get(exchange)
        if not conn:
            print(f"[Bot] No connection to {exchange}")
            return None
        return await conn.place_order(instrument_id, side, price, quantity, expiry_ms)

    async def cancel_order(
        self, exchange: str, order_id: int, instrument_id: str
    ) -> Optional[dict]:
        """Cancel an order on a specific exchange."""
        conn = self.connections.get(exchange)
        if not conn:
            return None
        return await conn.cancel_order(order_id, instrument_id)

    async def get_inventory(self, exchange: str) -> Optional[dict]:
        """Request inventory from a specific exchange."""
        conn = self.connections.get(exchange)
        if not conn:
            return None
        return await conn.get_inventory()

    # ── Internal ──

    def _parse_exchanges(self) -> list[tuple[str, str, int]]:
        """Parse the EXCHANGES env var into [(name, host, port), ...]."""
        result = []
        for entry in EXCHANGES_STR.split(","):
            name = entry.strip()
            if not name:
                continue
            host = EXCHANGE_HOSTS.get(name)
            if host is None:
                print(f"[Bot] Unknown exchange: {name!r}. "
                      f"Known: {', '.join(EXCHANGE_HOSTS)}")
                continue
            result.append((name, host, EXCHANGE_PORT))
        return result

    async def _handle_exchange(self, conn: ExchangeConnection) -> None:
        """Main loop for one exchange connection."""
        if not await conn.connect():
            return

        await self.strategy.on_connected(self, conn.name)

        while self._running and conn.connected:
            msg = await conn.recv()
            if msg is None:
                # Connection lost — try to reconnect
                print(f"[{conn.name}] Connection lost, attempting reconnect...")
                if not await conn.connect():
                    break
                continue

            msg_type = msg.get("type", "")

            if msg_type == "market_data_update":
                trades = update_market_state(self.state, conn.name, msg)
                await self.strategy.on_market_data(self, conn.name, self.state)

            elif msg_type == "add_order_response":
                if msg.get("success"):
                    data = msg.get("data", {})
                    inv_change = data.get("immediate_inventory_change")
                    bal_change = data.get("immediate_balance_change")
                    if inv_change is not None and inv_change != 0:
                        fill = Fill(
                            exchange=conn.name,
                            instrument_id="",  # not available in response
                            order_id=data.get("order_id", 0),
                            side="",
                            price=0,
                            quantity=abs(inv_change),
                            inventory_change=inv_change,
                            balance_change=bal_change,
                        )
                        await self.strategy.on_fill(self, fill)
                else:
                    err = msg.get("data", {}).get("message", "unknown")
                    req_id = msg.get("user_request_id", "?")
                    print(f"[{conn.name}] Order rejected ({req_id}): {err}")

            elif msg_type == "cancel_order_response":
                if not msg.get("success"):
                    err = msg.get("message", "unknown")
                    print(f"[{conn.name}] Cancel failed: {err}")

            elif msg_type == "end_of_round":
                print(f"[{conn.name}] Round ended")
                await self.strategy.on_round_end(self, conn.name)
                break

            elif msg_type == "error":
                print(f"[{conn.name}] Error: {msg.get('message', '')}")

        await conn.disconnect()

    async def run(self) -> None:
        """Start the bot — connect to all exchanges and run until the round ends."""
        exchanges = self._parse_exchanges()
        if not exchanges:
            print("[Bot] No exchanges configured. Set the EXCHANGES env var.")
            print(f"[Bot] Format: EXCHANGES='NYSE,NASDAQ,LSE' "
                  f"(known: {', '.join(EXCHANGE_HOSTS)})")
            return

        print(f"[Bot] Starting with {len(exchanges)} exchange(s): "
              f"{', '.join(name for name, _, _ in exchanges)}")
        print(f"[Bot] Strategy: {self.strategy.__class__.__name__}")

        # Create connections
        for name, host, port in exchanges:
            conn = ExchangeConnection(name, host, port)
            self.connections[name] = conn

        # Run all exchange handlers concurrently
        self._running = True
        tasks = [
            asyncio.create_task(self._handle_exchange(conn))
            for conn in self.connections.values()
        ]

        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            print("\n[Bot] Interrupted — shutting down")
        finally:
            self._running = False
            for conn in self.connections.values():
                await conn.disconnect()
            print("[Bot] Shutdown complete")


# ──────────────────────────────────────────────────────────────────────
#  Entry Point
# ──────────────────────────────────────────────────────────────────────

def main() -> None:
    """
    Run the demo bot with the SimpleStrategy.

    To use your own strategy:
        1. Subclass Strategy
        2. Implement on_market_data() (and optionally on_fill, etc.)
        3. Replace SimpleStrategy() below with your class
    """
    strategy = SimpleStrategy()
    bot = Bot(strategy)
    asyncio.run(bot.run())


if __name__ == "__main__":
    main()
