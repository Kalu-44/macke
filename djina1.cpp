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
    static constexpr int  ETF_ARB_EDGE   = 12;   // ETF vs NAV
    static constexpr int  XVENUE_EDGE    = 10;   // venue A bid vs venue B ask
    static constexpr int  STALE_EDGE     = 25;   // venue mid vs cross-venue mid
    static constexpr int  MM_MIN_SPREAD  = 18;
    static constexpr int  MM_QUOTE_INSIDE = 1;
    static constexpr int  MM_INV_SKEW_C  = 1;    // cents skew per share of position

    // Pacing
    static constexpr int  STRAT_MIN_INTERVAL_MS = 50;
    static constexpr int  RATE_LIMIT_PER_SEC    = 400;

    // Switches
    // Proven (math-defined or structural edge): ON
    static constexpr bool ENABLE_ETF_ARB    = true;
    static constexpr bool ENABLE_XVENUE_ARB = true;
    static constexpr bool ENABLE_STALE_ARB  = true;
    static constexpr bool ENABLE_MM         = true;
    static constexpr bool ENABLE_UNWIND     = true;   // close positions near segment end
    // Hypotheses — turn ON only after the recorder + analyze.py confirm them:
    static constexpr bool ENABLE_PATTERN    = false;  // CARD/SIMP mean-revert
    static constexpr bool ENABLE_SECTOR     = false;  // sector momentum (groups A,B)
    static constexpr bool ENABLE_SAFEHAVEN  = false;  // GOLD/XAG anti-corr

    // Pattern strategy (for CARD, SIMP, NGUP, JZRO, KOTD)
    static constexpr int  PATTERN_HISTORY      = 600;  // ~60 sec at 100ms ticks
    static constexpr int  PATTERN_MIN_SAMPLES  = 200;  // need ~20s before trading
    static constexpr int  PATTERN_ENTRY_STDEV  = 150;  // |z| * 100 — enter at z=±1.5
    static constexpr int  PATTERN_SIZE         = 15;

    // Sector momentum
    static constexpr int  SECTOR_LOOKBACK_TICKS = 30;  // 3 sec
    static constexpr int  SECTOR_TRIGGER_BPS    = 50;  // peers moved 0.5% — laggard hasn't
    static constexpr int  SECTOR_SIZE           = 15;

    // Safe-haven
    static constexpr int  SAFEHAVEN_LOOKBACK_TICKS = 50;
    static constexpr int  SAFEHAVEN_TRIGGER_BPS    = 30;
    static constexpr int  SAFEHAVEN_SIZE           = 15;

    // End-of-segment unwind: when server time >= ROUND_LEN - UNWIND_BEFORE_END
    static constexpr long ROUND_LEN_MS         = 600'000;  // 10 minutes
    static constexpr long UNWIND_BEFORE_END_MS = 30'000;   // start unwind at 9:30

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
static const std::vector<std::string> SAFEHAVEN_TICKERS = {"GOLD","XAG"};
static const std::vector<std::string> PATTERN_TICKERS = {"CARD","SIMP","NGUP","JZRO","KOTD"};

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
        if (bids.is_object())
            for (auto& [p,q] : bids.items())
                b.bids[std::stoi(p)] = q.get<int>();
        if (asks.is_object())
            for (auto& [p,q] : asks.items())
                b.asks[std::stoi(p)] = q.get<int>();
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
            auto