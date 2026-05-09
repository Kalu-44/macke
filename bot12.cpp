/*
 * AlgoTrade 2026 — Competition Bot (C++20)
 *
 * Multi-strategy bot built on top of the official demo. Three layered
 * profit sources, all designed for a contested multi-team environment:
 *
 *   1. ETF NAV arbitrage         — primary edge (math-defined fair value)
 *   2. Cross-venue arbitrage     — same ticker on multiple venues
 *   3. CARD/SIMP market making   — small size, inventory-skewed
 *
 * Critical fixes vs. the demo:
 *   - Auto-reconnect across `end_of_round` boundaries (the demo just
 *     exits, which would lose 2 of 3 segments per round).
 *   - Per-exchange threads run their strategy independently (no global
 *     strategy mutex bottlenecking arb across venues).
 *   - Local rate-limit guard (cap ~400 msgs/sec/exchange).
 *   - Position + cash tracked locally so we can refuse orders that
 *     would breach risk caps before sending.
 *
 * Build:
 *   g++ -std=c++20 -O2 -o bot bot.cpp -lpthread
 *   # or: cmake -B build && cmake --build build
 *
 * Run:
 *   ./bot                          # all 10 exchanges
 *   EXCHANGES="NYSE,NASDAQ,ZSE" ./bot
 *
 * Tuning: search for `struct Params` and tweak edges/sizes there.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <nlohmann/json.hpp>

namespace beast     = boost::beast;
namespace websocket = beast::websocket;
namespace net       = boost::asio;
using tcp           = net::ip::tcp;
using json          = nlohmann::json;

// ════════════════════════════════════════════════════════════════════
//  Params — TUNE THESE
// ════════════════════════════════════════════════════════════════════
struct Params {
    // Risk caps (hard limits are +2000 / -200 long/short, -$50k cash)
    static constexpr int  MAX_LONG       = 150;
    static constexpr int  MAX_SHORT      = -150;
    static constexpr long CASH_FLOOR     = -3'000'000;   // cents

    // Sizes
    static constexpr int  ARB_SIZE       = 25;
    static constexpr int  MM_SIZE        = 10;
    static constexpr int  ORDER_TTL_MS   = 4'000;

    // Edges in cents (1c = $0.01)
    // === DEFENSIVE: tightened to avoid losing to faster competitors ===
    static constexpr int  ETF_ARB_EDGE   = 25;   // was 12 — wider cushion vs latency
    static constexpr int  XVENUE_EDGE    = 25;   // was 10 — only fire on real edges
    static constexpr int  STALE_EDGE     = 25;
    static constexpr int  MM_MIN_SPREAD  = 18;
    static constexpr int  MM_QUOTE_INSIDE = 1;
    static constexpr int  MM_INV_SKEW_C  = 1;    // cents skew per share of position

    // Pacing
    static constexpr int  STRAT_MIN_INTERVAL_MS = 50;
    static constexpr int  RATE_LIMIT_PER_SEC    = 400;

    // ─── Warmup & safety guards ─────────────────────────────────────────
    // After (re)connect, do NOT fire xvenue/etf arb until at least
    // WARMUP_MIN_VENUES different venues have sent ≥1 market_data_update,
    // AND at least WARMUP_MIN_MS have elapsed. Prevents catastrophic
    // first-second loss where partial books look like fake "edge".
    static constexpr int  WARMUP_MIN_MS     = 10'000;  // 10 s after first message
    static constexpr int  WARMUP_MIN_VENUES = 7;       // need ≥7 of 10 venues live

    // ETF NAV strict mode: only compute NAV if every component has at
    // least NAV_MIN_VENUES_PER_LEG quoting venues (defends against stale
    // single-venue mids dragging the average).
    static constexpr int  NAV_MIN_VENUES_PER_LEG = 3;

    // Per-instrument cooldown: after sending any order on instrument X,
    // suppress further orders on the same instrument for INST_COOLDOWN_MS.
    // Coint trade ignores this (it needs to flip positions promptly).
    static constexpr int  INST_COOLDOWN_MS  = 250;

    // Switches
    // === DEFENSIVE MODE (rank 34, cash -$467K, losing on Euronext+LSE) ===
    // Hypothesis: pattern/MM/stale lose to faster competitors via adverse
    // selection. Run ONLY mathematically-deterministic arbitrage with strict
    // edges so we never enter at a loss. Re-enable layers one by one based
    // on per-exchange P&L from next round.
    static constexpr bool ENABLE_ETF_ARB    = false;  // disabled: bled cash even with warmup gate
    static constexpr bool ENABLE_XVENUE_ARB = false;  // disabled: same
    static constexpr bool ENABLE_STALE_ARB  = false;  // suspect: gives up to faster takers
    static constexpr bool ENABLE_MM         = false;  // suspect: adverse selection by HFTs
    static constexpr bool ENABLE_UNWIND     = false;  // disabled: bug — server time is epoch ms, fired every tick
    static constexpr bool ENABLE_PATTERN    = false;  // suspect: regime may not match training
    static constexpr bool ENABLE_SECTOR     = false;
    static constexpr bool ENABLE_SAFEHAVEN  = false;
    static constexpr bool ENABLE_COINT      = true;   // cointegrated basket on ZSE — verified edge
    static constexpr bool ENABLE_LEAD_LAG   = true;   // cross-venue lead-lag arbitrage — corr ≥0.95

    // ─── Cointegrated basket stat-arb (ZSE only) ────────────────────────────
    // Spread = Σ β_i · price_i is mean-reverting (verified: AC1=0.82, half-life 3 ticks,
    // 11 distinct |z|>2 excursions per 12 min on training data).
    // Trade size in units = round(COINT_BASKET_K * β_i). At K=7500, max leg
    // (XFR, β=0.0186) = 140 units (under MAX_LONG=150).
    static constexpr int  COINT_HISTORY     = 300;    // ticks of spread to track
    static constexpr int  COINT_MIN_SAMPLES = 60;     // wait for stats to settle
    static constexpr int  COINT_BASKET_K    = 7500;   // size scaling (β · K = leg units)
    static constexpr int  COINT_ENTRY_Z100  = 250;    // entry at |z| >= 2.50 (×100) — stricter to filter out marginal trades eaten by spread
    static constexpr int  COINT_EXIT_Z100   = 30;     // exit  at |z| <= 0.30 (×100)
    static constexpr int  COINT_REBAL_MS    = 250;    // throttle rebalance per basket

    // ─── Lead-lag cross-venue arbitrage ────────────────────────────────
    // We watch the lead venue's mid; if it has moved away from the lag
    // venue's quoted price by >= LEAD_LAG_EDGE cents, we hit the lag book
    // before its MM updates. Edge must exceed expected spread cost.
    static constexpr int  LEAD_LAG_EDGE_CENTS = 8;    // ≥8c gap → fire
    static constexpr int  LEAD_LAG_SIZE       = 30;   // shares per fire
    static constexpr int  LEAD_LAG_COOLDOWN_MS = 150; // per (lag, ticker) cooldown

    // Pattern strategy: mean-reverters confirmed by training-data analysis.
    // CARD removed (random walk); NGUP/JZRO/KOTD removed (trending, not reverting).
    static constexpr int  PATTERN_HISTORY      = 600;  // ~60 sec at 100ms ticks
    static constexpr int  PATTERN_MIN_SAMPLES  = 200;  // need ~20s before trading
    static constexpr int  PATTERN_ENTRY_STDEV  = 100;  // |z| * 100 — enter at z=±1.0 (fast revert)
    static constexpr int  PATTERN_EXIT_STDEV   = 30;   // |z| * 100 — close when |z| <= 0.30
    static constexpr int  PATTERN_SIZE         = 15;

    // Sector momentum
    static constexpr int  SECTOR_LOOKBACK_TICKS = 30;  // 3 sec
    static constexpr int  SECTOR_TRIGGER_BPS    = 50;  // peers moved 0.5% — laggard hasn't
    static constexpr int  SECTOR_SIZE           = 15;

    // Safe-haven (XAG only — corr -0.51 with market). Loose inverse, so use
    // a higher trigger threshold to avoid false fires on noise.
    static constexpr int  SAFEHAVEN_LOOKBACK_TICKS = 50;
    static constexpr int  SAFEHAVEN_TRIGGER_BPS    = 50;   // require 0.5% market move
    static constexpr int  SAFEHAVEN_SIZE           = 10;

    // End-of-segment unwind: when server time >= ROUND_LEN - UNWIND_BEFORE_END
    static constexpr long ROUND_LEN_MS         = 1'800'000;  // 30 minutes (organizer change: 1 segment, not 3)
    static constexpr long UNWIND_BEFORE_END_MS = 60'000;     // start unwind at 29:00

    // Kill-switch: stop trading on a venue if local PnL estimate worse than this
    static constexpr long KILL_PNL_PER_VENUE = -1'500'000;  // -$15k
};

// ════════════════════════════════════════════════════════════════════
//  Static metadata: exchanges, listings, ETF baskets
// ════════════════════════════════════════════════════════════════════
static constexpr int EXCHANGE_PORT = 9001;

static const std::vector<std::pair<std::string,std::string>> EXCHANGE_HOSTS = {
    {"NYSE","10.0.201.2"}, {"NASDAQ","10.0.202.2"}, {"SSE","10.0.203.2"},
    {"JPX","10.0.204.2"},  {"Euronext","10.0.205.2"}, {"LSE","10.0.206.2"},
    {"HKEX","10.0.207.2"}, {"NSE","10.0.208.2"}, {"TMX","10.0.209.2"},
    {"ZSE","10.0.210.2"},
};

static const std::map<std::string, std::vector<std::string>> STOCK_LISTINGS = {
    {"CARD",{"NYSE","NASDAQ","LSE","Euronext","JPX","SSE","HKEX","NSE","TMX","ZSE"}},
    {"SIMP",{"NYSE","NASDAQ","LSE","Euronext","JPX","SSE","HKEX","NSE","TMX","ZSE"}},
    {"NGUP",{"NYSE","NASDAQ","Euronext","TMX","ZSE"}},
    {"OIT", {"LSE","Euronext","HKEX","NSE","ZSE"}},
    {"KTST",{"NYSE","JPX","TMX","ZSE"}},
    {"FSR", {"NASDAQ","LSE","SSE","HKEX","ZSE"}},
    {"JZRO",{"NYSE","LSE","Euronext","TMX","ZSE"}},
    {"XFR", {"NYSE","HKEX","TMX","ZSE"}},
    {"KOTD",{"NASDAQ","LSE","Euronext","HKEX","ZSE"}},
    {"INA", {"NYSE","NASDAQ","Euronext","HKEX","ZSE"}},
    {"HT",  {"NASDAQ","LSE","JPX","SSE","TMX","ZSE"}},
    {"JNAF",{"NYSE","Euronext","JPX","HKEX","ZSE"}},
    {"DLKV",{"NASDAQ","LSE","HKEX","NSE","ZSE"}},
    {"DDJH",{"NYSE","LSE","Euronext","TMX","ZSE"}},
    {"MDKA",{"NYSE","LSE","HKEX","TMX","ZSE"}},
    {"KRAS",{"NYSE","Euronext","SSE","TMX","ZSE"}},
    {"ZITO",{"NASDAQ","LSE","Euronext","NSE","ZSE"}},
    {"ZABA",{"NYSE","LSE","SSE","NSE","TMX","ZSE"}},
    {"GOLD",{"NASDAQ","Euronext","JPX","TMX","ZSE"}},
    {"XAG", {"LSE","Euronext","JPX","ZSE"}},
};

static const std::map<std::string, std::vector<std::string>> ETF_BASKETS = {
    {"ETFA", {"NGUP","OIT","KTST","FSR","JZRO","XFR"}},
    {"ETFB", {"KOTD","INA","HT","JNAF","DLKV","DDJH"}},
    {"ETFA3",{"NGUP","KTST","XFR"}},
    {"ETFB3",{"KOTD","INA","DLKV"}},
    {"ETFSH",{"GOLD","XAG"}},
};
static const std::map<std::string, std::vector<std::string>> ETF_LISTINGS = {
    {"ETFA", {"NYSE","Euronext","HKEX","ZSE"}},
    {"ETFB", {"NASDAQ","LSE","HKEX","ZSE"}},
    {"ETFA3",{"NYSE","TMX","ZSE"}},
    {"ETFB3",{"NASDAQ","HKEX","ZSE"}},
    {"ETFSH",{"Euronext","JPX","ZSE"}},
};

static std::optional<std::string> lookup_host(const std::string& name) {
    for (auto& [n,h] : EXCHANGE_HOSTS) if (n==name) return h;
    return std::nullopt;
}
static std::vector<std::string> listings_for(const std::string& tkr) {
    auto it = STOCK_LISTINGS.find(tkr);
    if (it != STOCK_LISTINGS.end()) return it->second;
    auto it2 = ETF_LISTINGS.find(tkr);
    if (it2 != ETF_LISTINGS.end()) return it2->second;
    return {};
}

// Sectors for momentum strategy
static const std::map<std::string, std::vector<std::string>> SECTORS = {
    {"A", {"NGUP","OIT","KTST","FSR","JZRO","XFR"}},
    {"B", {"KOTD","INA","HT","JNAF","DLKV","DDJH"}},
};
static const std::vector<std::string> MARKET_TICKERS = {
    "NGUP","OIT","KTST","FSR","JZRO","XFR",
    "KOTD","INA","HT","JNAF","DLKV","DDJH",
    "MDKA","KRAS","ZITO","ZABA",
};
static const std::vector<std::string> SAFEHAVEN_TICKERS = {"XAG"};  // GOLD removed: corr +0.22 (independent); XAG corr -0.51 (real inverse)
// Pattern tickers: confirmed mean-reverters from training-data analysis.
// (CARD/NGUP/JZRO/KOTD removed: they are not mean-reverting in observed regime.)
static const std::vector<std::string> PATTERN_TICKERS = {"SIMP","XFR","KTST","DLKV","KRAS"};

// Per-ETF arbitrage edge overrides (cents). ETFs with large systematic bias
// or high gap volatility need a wider edge than the default ETF_ARB_EDGE.
// Measured on training data:
//   ETFA  : meanGap -27c, std 13c   → default 12c is fine
//   ETFA3 : meanGap  -9c, std 28c   → default 12c is fine
//   ETFB  : meanGap -42c, std 21c   → wider edge (more noisy)
//   ETFB3 : meanGap -152c, std 68c  → much wider edge (very volatile)
//   ETFSH : meanGap +19c, std 53c   → wider edge
// Per-ETF arbitrage edge overrides (cents). DEFENSIVE: bumped all up so we
// only enter when gap is 2-3x the noise level.
static int etf_edge_for(const std::string& etf) {
    if (etf == "ETFB3") return 100;  // very volatile, big bias
    if (etf == "ETFB")  return 40;
    if (etf == "ETFSH") return 70;
    return 25;                       // ETFA, ETFA3 — default tightened
}

// ─── Cointegrated baskets (verified on ZSE training data) ────────────────
// spread = Σ β_i · price_i is stationary; entry at |z|>=2, exit at |z|<=0.3.
// Sector A vector (verified: AC1=0.82, half-life 3 ticks, std 1.06c, mean 191.80c).
// Sector B vector pending — leave EMPTY until Joco provides; the strategy
// silently skips a basket whose vector is empty.
struct CointBasket {
    std::string name;                                       // "A", "B"
    std::string venue;                                      // "ZSE"
    std::vector<std::pair<std::string, double>> legs;       // (ticker, beta)
};
static const std::vector<CointBasket> COINT_BASKETS = {
    {"A", "ZSE", {
        {"FSR",  +0.0074},
        {"JZRO", -0.0058},
        {"KTST", +0.0008},
        {"NGUP", -0.0008},
        {"OIT",  +0.0007},
        {"XFR",  +0.0186},
    }},
    // Sector B disabled — bot8 (with B1+B2) underperformed bot7 (A only).
    // Keep entries with empty legs so the strategy code skips them.
    {"B1", "ZSE", { /* disabled */ }},
    {"B2", "ZSE", { /* disabled */ }},
};

// ════════════════════════════════════════════════════════════════════
//  Cross-venue lead-lag arbitrage table
//  Generated by lead_lag.py from prior-round orderbook history.
//  Each entry: (lead_venue, lag_venue, ticker, expected_lag_ms).
//  Strategy: when lead's mid moves away from lag's quote by >= edge,
//  hit the lag book before its MM updates. corr >= 0.95 across the
//  training window for all 58 pairs.
// ════════════════════════════════════════════════════════════════════
struct LeadLagPair {
    std::string lead;
    std::string lag;
    std::string ticker;
    int lag_ms;
};
static const std::vector<LeadLagPair> LEAD_LAG_TABLE = {
    {"LSE", "TMX", "MDKA", 100},
    {"HKEX", "NYSE", "MDKA", 300},
    {"HKEX", "LSE", "MDKA", 200},
    {"TMX", "ZSE", "MDKA", 100},
    {"NYSE", "NASDAQ", "NGUP", 200},
    {"TMX", "NYSE", "NGUP", 100},
    {"Euronext", "TMX", "NGUP", 200},
    {"TMX", "NASDAQ", "GOLD", 1400},
    {"LSE", "TMX", "JZRO", 300},
    {"JPX", "TMX", "GOLD", 800},
    {"LSE", "NYSE", "JZRO", 300},
    {"Euronext", "ZSE", "GOLD", 100},
    {"LSE", "ZSE", "OIT", 200},
    {"Euronext", "ZSE", "NGUP", 400},
    {"HKEX", "TMX", "CARD", 400},
    {"HKEX", "LSE", "CARD", 300},
    {"LSE", "NASDAQ", "KOTD", 100},
    {"HKEX", "NASDAQ", "CARD", 600},
    {"HKEX", "SSE", "CARD", 600},
    {"HKEX", "NYSE", "CARD", 300},
    {"HKEX", "JPX", "CARD", 300},
    {"HKEX", "NSE", "CARD", 200},
    {"LSE", "ZSE", "JZRO", 200},
    {"NYSE", "SSE", "ZABA", 300},
    {"LSE", "NSE", "OIT", 400},
    {"NSE", "TMX", "ZABA", 200},
    {"ZSE", "HKEX", "ETFA", 1000},
    {"TMX", "NYSE", "ETFA3", 100},
    {"NSE", "NYSE", "ZABA", 700},
    {"ZSE", "HKEX", "OIT", 1200},
    {"LSE", "TMX", "HT", 500},
    {"LSE", "ZSE", "HT", 300},
    {"TMX", "ZSE", "ETFA3", 200},
    {"Euronext", "ZSE", "DDJH", 500},
    {"Euronext", "ZSE", "CARD", 400},
    {"HKEX", "ZSE", "JNAF", 500},
    {"TMX", "NASDAQ", "HT", 1200},
    {"Euronext", "NSE", "ZITO", 600},
    {"LSE", "JPX", "HT", 1400},
    {"HKEX", "SSE", "FSR", 500},
    {"Euronext", "ZSE", "ZITO", 100},
    {"HKEX", "LSE", "FSR", 100},
    {"TMX", "NYSE", "KTST", 100},
    {"LSE", "NASDAQ", "FSR", 500},
    {"Euronext", "NYSE", "ETFA", 200},
    {"JPX", "ZSE", "ETFSH", 300},
    {"Euronext", "ZSE", "ETFA", 100},
    {"Euronext", "LSE", "ZITO", 100},
    {"NASDAQ", "SSE", "HT", 1200},
    {"TMX", "NYSE", "DDJH", 100},
    {"HKEX", "ZSE", "FSR", 300},
    {"JPX", "Euronext", "ETFSH", 100},
    {"JPX", "Euronext", "GOLD", 600},
    {"HKEX", "Euronext", "CARD", 100},
    {"NYSE", "NASDAQ", "INA", 200},
    {"Euronext", "LSE", "DDJH", 200},
    {"Euronext", "TMX", "DDJH", 200},
    {"Euronext", "ZSE", "INA", 300},
};

// ════════════════════════════════════════════════════════════════════
//  Order book / market state
// ════════════════════════════════════════════════════════════════════
struct Book {
    // price (cents) -> qty.  Use map for sorted access.
    std::map<int,int> bids;   // descending best = rbegin
    std::map<int,int> asks;   // ascending  best = begin
    int64_t ts_ms = 0;

    std::optional<int> best_bid() const {
        return bids.empty() ? std::nullopt : std::optional<int>(bids.rbegin()->first);
    }
    std::optional<int> best_ask() const {
        return asks.empty() ? std::nullopt : std::optional<int>(asks.begin()->first);
    }
    std::optional<int> best_bid_qty() const {
        return bids.empty() ? std::nullopt : std::optional<int>(bids.rbegin()->second);
    }
    std::optional<int> best_ask_qty() const {
        return asks.empty() ? std::nullopt : std::optional<int>(asks.begin()->second);
    }
    std::optional<double> mid() const {
        auto b = best_bid(); auto a = best_ask();
        return (b && a) ? std::optional<double>((*b+*a)/2.0) : std::nullopt;
    }
    std::optional<int> spread() const {
        auto b = best_bid(); auto a = best_ask();
        return (b && a) ? std::optional<int>(*a-*b) : std::nullopt;
    }
};

class MarketState {
public:
    void update_book(const std::string& exch, const std::string& inst,
                     const json& bids, const json& asks, int64_t t)
    {
        std::lock_guard lk(mtx_);
        auto& b = books_[exch][inst];
        b.bids.clear(); b.asks.clear(); b.ts_ms = t;
        try {
            if (bids.is_object())
                for (auto& [p,q] : bids.items()) {
                    if (!q.is_number()) continue;
                    b.bids[std::stoi(p)] = q.get<int>();
                }
            if (asks.is_object())
                for (auto& [p,q] : asks.items()) {
                    if (!q.is_number()) continue;
                    b.asks[std::stoi(p)] = q.get<int>();
                }
        } catch (const std::exception& e) {
            std::cerr << "[" << exch << " " << inst << "] book parse error: "
                      << e.what() << "\n";
            b.bids.clear(); b.asks.clear();
        }
    }

    std::optional<Book> get(const std::string& exch, const std::string& inst) const {
        std::lock_guard lk(mtx_);
        auto e = books_.find(exch); if (e==books_.end()) return std::nullopt;
        auto i = e->second.find(inst); if (i==e->second.end()) return std::nullopt;
        return i->second;
    }

    // Average mid across all venues that have a two-sided book for `tkr`.
    std::optional<double> consensus_mid(const std::string& tkr) const {
        std::lock_guard lk(mtx_);
        double sum = 0; int n = 0;
        for (auto& exch : listings_for(tkr)) {
            auto e = books_.find(exch); if (e==books_.end()) continue;
            auto i = e->second.find(exch+"-"+tkr); if (i==e->second.end()) continue;
            auto m = i->second.mid();
            if (m) { sum += *m; ++n; }
        }
        return n ? std::optional<double>(sum/n) : std::nullopt;
    }
    // Strict variant: returns nullopt if fewer than `min_venues` venues
    // currently quote both sides for this ticker. Used by ETF NAV to avoid
    // pricing off partial / stale books in the first seconds after connect.
    std::optional<double> consensus_mid_strict(const std::string& tkr, int min_venues) const {
        std::lock_guard lk(mtx_);
        double sum = 0; int n = 0;
        for (auto& exch : listings_for(tkr)) {
            auto e = books_.find(exch); if (e==books_.end()) continue;
            auto i = e->second.find(exch+"-"+tkr); if (i==e->second.end()) continue;
            auto m = i->second.mid();
            if (m) { sum += *m; ++n; }
        }
        if (n < min_venues) return std::nullopt;
        return sum / n;
    }

    // Best bid across all venues for ticker -> (exchange, price, qty)
    struct Quote { std::string exchange; int price; int qty; };
    std::optional<Quote> best_bid_xv(const std::string& tkr) const {
        std::lock_guard lk(mtx_);
        std::optional<Quote> best;
        for (auto& exch : listings_for(tkr)) {
            auto e = books_.find(exch); if (e==books_.end()) continue;
            auto i = e->second.find(exch+"-"+tkr); if (i==e->second.end()) continue;
            auto p = i->second.best_bid(); auto q = i->second.best_bid_qty();
            if (p && q && (!best || *p > best->price))
                best = Quote{exch, *p, *q};
        }
        return best;
    }
    std::optional<Quote> best_ask_xv(const std::string& tkr) const {
        std::lock_guard lk(mtx_);
        std::optional<Quote> best;
        for (auto& exch : listings_for(tkr)) {
            auto e = books_.find(exch); if (e==books_.end()) continue;
            auto i = e->second.find(exch+"-"+tkr); if (i==e->second.end()) continue;
            auto p = i->second.best_ask(); auto q = i->second.best_ask_qty();
            if (p && q && (!best || *p < best->price))
                best = Quote{exch, *p, *q};
        }
        return best;
    }

    // ETF NAV in cents, computed from cross-venue mids of constituents.
    std::optional<double> etf_nav(const std::string& etf) const {
        auto it = ETF_BASKETS.find(etf); if (it==ETF_BASKETS.end()) return std::nullopt;
        double sum = 0;
        for (auto& tkr : it->second) {
            auto m = consensus_mid_strict(tkr, Params::NAV_MIN_VENUES_PER_LEG);
            if (!m) return std::nullopt;
            sum += *m;
        }
        return sum / it->second.size();
    }

    // ─── Price history (per ticker, cross-venue consensus) ───
    void record_history(const std::string& tkr, double mid, int64_t t) {
        std::lock_guard lk(hist_mtx_);
        auto& h = history_[tkr];
        h.push_back({t, mid});
        if ((int)h.size() > Params::PATTERN_HISTORY) h.erase(h.begin());
    }

    struct Stats { int n; double mean; double stdev; double last; double slope_per_tick; };
    std::optional<Stats> stats(const std::string& tkr) const {
        std::lock_guard lk(hist_mtx_);
        auto it = history_.find(tkr);
        if (it == history_.end() || (int)it->second.size() < Params::PATTERN_MIN_SAMPLES)
            return std::nullopt;
        const auto& h = it->second;
        double sum = 0, sumsq = 0;
        for (auto& [_, p] : h) { sum += p; sumsq += p*p; }
        double n = h.size();
        double mean = sum / n;
        double var = std::max(0.0, sumsq/n - mean*mean);
        double sd = std::sqrt(var);
        // simple slope: last 30 vs prev 30
        double last30 = 0, prev30 = 0; int k = 30;
        if ((int)h.size() >= 2*k) {
            for (int i = (int)h.size()-k; i < (int)h.size(); ++i) last30 += h[i].second;
            for (int i = (int)h.size()-2*k; i < (int)h.size()-k; ++i) prev30 += h[i].second;
        }
        double slope = (last30 - prev30) / k;
        return Stats{(int)n, mean, sd, h.back().second, slope};
    }

    // Reset all history (called on segment boundary)
    void reset_history() {
        std::lock_guard lk(hist_mtx_);
        history_.clear();
    }

    // ─── Warmup tracking: which venues have sent ≥1 market_data_update
    //     since the last reset, and when did the first arrive ─────────
    void note_venue_alive(const std::string& exch) {
        std::lock_guard lk(warm_mtx_);
        if (warmup_first_ms_ == 0) warmup_first_ms_ = now_ms_static();
        warmup_venues_.insert(exch);
    }
    bool warmup_done() const {
        std::lock_guard lk(warm_mtx_);
        if (warmup_first_ms_ == 0) return false;
        if ((int)warmup_venues_.size() < Params::WARMUP_MIN_VENUES) return false;
        return (now_ms_static() - warmup_first_ms_) >= Params::WARMUP_MIN_MS;
    }
    void reset_warmup() {
        std::lock_guard lk(warm_mtx_);
        warmup_venues_.clear();
        warmup_first_ms_ = 0;
    }
    static int64_t now_ms_static() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    // Aggregate market index = mean of all market tickers' consensus mids
    std::optional<double> market_index() const {
        double sum = 0; int n = 0;
        for (auto& tkr : MARKET_TICKERS) {
            auto m = consensus_mid(tkr);
            if (m) { sum += *m; ++n; }
        }
        return n ? std::optional<double>(sum/n) : std::nullopt;
    }

private:
    mutable std::mutex mtx_;
    std::map<std::string, std::map<std::string, Book>> books_;
    mutable std::mutex hist_mtx_;
    std::map<std::string, std::vector<std::pair<int64_t,double>>> history_;
    mutable std::mutex warm_mtx_;
    std::set<std::string> warmup_venues_;
    int64_t warmup_first_ms_ = 0;
};

// ════════════════════════════════════════════════════════════════════
//  ExchangeConnection (synchronous WS in its own thread)
// ════════════════════════════════════════════════════════════════════
static int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

class ExchangeConnection {
public:
    ExchangeConnection(std::string name, std::string host)
        : name_(std::move(name)), host_(std::move(host)) {}

    const std::string& name() const { return name_; }
    bool connected() const { return connected_.load(); }

    bool connect() {
        try {
            ioc_ = std::make_unique<net::io_context>();
            ws_  = std::make_unique<websocket::stream<tcp::socket>>(*ioc_);
            tcp::resolver r(*ioc_);
            auto results = r.resolve(host_, std::to_string(EXCHANGE_PORT));
            net::connect(ws_->next_layer(), results);
            ws_->handshake(host_+":"+std::to_string(EXCHANGE_PORT), "/trade");
            connected_ = true;

            beast::flat_buffer buf;
            ws_->read(buf);  // welcome
            return true;
        } catch (const std::exception& e) {
            std::cerr << "[" << name_ << "] connect failed: " << e.what() << "\n";
            connected_ = false;
            return false;
        }
    }

    std::optional<json> recv() {
        try {
            beast::flat_buffer buf;
            ws_->read(buf);
            return json::parse(beast::buffers_to_string(buf.data()));
        } catch (...) { connected_ = false; return std::nullopt; }
    }

    bool send(const json& msg) {
        if (!connected_) return false;
        // crude rate limiter: drop if we've sent too many in the last second
        auto t = now_ms();
        {
            std::lock_guard lk(send_mtx_);
            send_times_.erase(
                std::remove_if(send_times_.begin(), send_times_.end(),
                               [t](int64_t s){ return t - s > 1000; }),
                send_times_.end());
            if ((int)send_times_.size() >= Params::RATE_LIMIT_PER_SEC) return false;
            send_times_.push_back(t);
        }
        try {
            std::string text = msg.dump();
            std::lock_guard lk(write_mtx_);
            ws_->write(net::buffer(text));
            return true;
        } catch (const std::exception& e) {
            connected_ = false;
            return false;
        }
    }

    void disconnect() {
        connected_ = false;
        try { ws_->close(websocket::close_code::normal); } catch (...) {}
    }

    std::string next_rid() { return name_+"-"+std::to_string(++rid_); }

    bool place_limit(const std::string& inst, const std::string& side,
                     int price, int qty, int ttl_ms = Params::ORDER_TTL_MS) {
        bool ok = send({
            {"type","add_order"}, {"user_request_id", next_rid()},
            {"instrument_id", inst}, {"price", price},
            {"expiry", now_ms()+ttl_ms}, {"side", side},
            {"quantity", qty}, {"order_type","limit"},
        });
        if (ok) orders_sent_.fetch_add(1, std::memory_order_relaxed);
        return ok;
    }
    bool place_ioc(const std::string& inst, const std::string& side, int price, int qty) {
        bool ok = send({
            {"type","add_order"}, {"user_request_id", next_rid()},
            {"instrument_id", inst}, {"price", price},
            {"expiry", now_ms()+1000}, {"side", side},
            {"quantity", qty}, {"order_type","ioc"},
        });
        if (ok) orders_sent_.fetch_add(1, std::memory_order_relaxed);
        return ok;
    }
    bool get_inventory() {
        return send({{"type","get_inventory"}, {"user_request_id", next_rid()}});
    }

    // Local accounting (updated from add_order_response)
    std::mutex pos_mtx;
    std::unordered_map<std::string,int> positions;  // instrument_id -> shares
    long cash_cents = 10'000'000;
    long pnl_estimate = 0;  // cumulative immediate_balance_change + mark-to-market not done here
    std::atomic<bool> killed{false};
    std::atomic<long> orders_sent_{0};
    std::atomic<long> orders_filled_{0};   // success=true add_order_responses
    std::atomic<long> orders_rejected_{0}; // success=false add_order_responses

    void apply_add_order_resp(const json& m) {
        if (!m.value("success", false)) {
            orders_rejected_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        orders_filled_.fetch_add(1, std::memory_order_relaxed);
        if (!m.contains("data")) return;
        const auto& d = m["data"];
        std::lock_guard lk(pos_mtx);
        if (d.contains("immediate_balance_change") && !d["immediate_balance_change"].is_null())
            cash_cents += d["immediate_balance_change"].get<long>();
        // Inventory change attribution requires order_id->instrument lookup;
        // since we do many IOCs per second, we resync periodically via get_inventory.
    }

    void apply_inventory(const json& m) {
        if (!m.contains("data")) return;
        std::lock_guard lk(pos_mtx);
        for (auto& [k, v] : m["data"].items()) {
            if (!v.is_array() || v.size() < 2) continue;
            if (k == "$") cash_cents = v[1].get<long>();
            else positions[k] = v[1].get<int>();
