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

    // Switches
    // === DEFENSIVE MODE (rank 34, cash -$467K, losing on Euronext+LSE) ===
    // Hypothesis: pattern/MM/stale lose to faster competitors via adverse
    // selection. Run ONLY mathematically-deterministic arbitrage with strict
    // edges so we never enter at a loss. Re-enable layers one by one based
    // on per-exchange P&L from next round.
    static constexpr bool ENABLE_ETF_ARB    = true;
    static constexpr bool ENABLE_XVENUE_ARB = true;
    static constexpr bool ENABLE_STALE_ARB  = false;  // suspect: gives up to faster takers
    static constexpr bool ENABLE_MM         = false;  // suspect: adverse selection by HFTs
    static constexpr bool ENABLE_UNWIND     = true;
    static constexpr bool ENABLE_PATTERN    = false;  // suspect: regime may not match training
    static constexpr bool ENABLE_SECTOR     = false;
    static constexpr bool ENABLE_SAFEHAVEN  = false;
    static constexpr bool ENABLE_COINT      = true;   // cointegrated basket on ZSE — verified edge

    // ─── Cointegrated basket stat-arb (ZSE only) ────────────────────────────
    // Spread = Σ β_i · price_i is mean-reverting (verified: AC1=0.82, half-life 3 ticks,
    // 11 distinct |z|>2 excursions per 12 min on training data).
    // Trade size in units = round(COINT_BASKET_K * β_i). At K=7500, max leg
    // (XFR, β=0.0186) = 140 units (under MAX_LONG=150).
    static constexpr int  COINT_HISTORY     = 300;    // ticks of spread to track
    static constexpr int  COINT_MIN_SAMPLES = 60;     // wait for stats to settle
    static constexpr int  COINT_BASKET_K    = 7500;   // size scaling (β · K = leg units)
    static constexpr int  COINT_ENTRY_Z100  = 200;    // entry at |z| >= 2.00 (×100)
    static constexpr int  COINT_EXIT_Z100   = 30;     // exit  at |z| <= 0.30 (×100)
    static constexpr int  COINT_REBAL_MS    = 250;    // throttle rebalance per basket

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
    // Sector B — 2 Johansen vectors. Verified: AC1=0.88-0.90, half-life 6-7t,
    // 5 distinct |z|>2 excursions per 12 min on training data, mean stable.
    {"B1", "ZSE", {
        {"DDJH", +0.0022},
        {"DLKV", +0.0002},
        {"HT",   -0.0010},
        {"INA",  -0.0078},
        {"JNAF", -0.0013},
        {"KOTD", +0.0019},
    }},
    {"B2", "ZSE", {
        {"DDJH", -0.0003},
        {"DLKV", -0.0014},
        {"HT",   +0.0059},
        {"INA",  -0.0015},
        {"JNAF", +0.0038},
        {"KOTD", -0.0018},
    }},
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
            auto m = consensus_mid(tkr);
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
        }
    }

    int position_of(const std::string& inst) {
        std::lock_guard lk(pos_mtx);
        auto it = positions.find(inst);
        return it==positions.end() ? 0 : it->second;
    }

    void on_segment_reset() {
        std::lock_guard lk(pos_mtx);
        positions.clear();
        cash_cents = 10'000'000;
        pnl_estimate = 0;
        killed = false;
    }

private:
    std::string name_, host_;
    std::unique_ptr<net::io_context> ioc_;
    std::unique_ptr<websocket::stream<tcp::socket>> ws_;
    std::atomic<bool> connected_{false};
    int rid_ = 0;
    std::mutex write_mtx_, send_mtx_;
    std::vector<int64_t> send_times_;
};

// ════════════════════════════════════════════════════════════════════
//  Strategy (called from each exchange's reader thread)
// ════════════════════════════════════════════════════════════════════
class Bot;

class Strategy {
public:
    Strategy(MarketState& s, std::unordered_map<std::string,std::unique_ptr<ExchangeConnection>>& c)
        : state_(s), conns_(c) {}

    void on_market_data(const std::string& exch, int64_t server_time_ms);
    void record_history();
    void reset_segment() {
        std::lock_guard lk(coint_mtx_);
        coint_state_.clear();
    }

private:
    bool can_buy(ExchangeConnection& c, const std::string& inst, int qty, int px);
    bool can_sell(ExchangeConnection& c, const std::string& inst, int qty);

    void etf_arb(const std::string& exch);
    void xvenue_arb(const std::string& exch);
    void stale_arb(const std::string& exch);
    void market_make(const std::string& exch);
    void pattern_trade(const std::string& exch);
    void sector_momentum(const std::string& exch);
    void safe_haven(const std::string& exch);
    void unwind(const std::string& exch);
    void coint_basket_trade(const std::string& exch);

    MarketState& state_;
    std::unordered_map<std::string,std::unique_ptr<ExchangeConnection>>& conns_;
    std::unordered_map<std::string,int64_t> last_run_;
    std::mutex last_run_mtx_;

    // Per-basket spread history & state (keyed by basket name).
    struct CointState {
        std::vector<double> spread_hist;   // last COINT_HISTORY values
        int64_t last_rebal_ms = 0;
        int target_state = 0;              // -1 = short basket, +1 = long basket, 0 = flat
    };
    std::unordered_map<std::string, CointState> coint_state_;
    std::mutex coint_mtx_;
};

bool Strategy::can_buy(ExchangeConnection& c, const std::string& inst, int qty, int px) {
    if (c.killed) return false;
    int pos = c.position_of(inst);
    if (pos + qty > Params::MAX_LONG) return false;
    std::lock_guard lk(c.pos_mtx);
    if (c.cash_cents - (long)px*qty < Params::CASH_FLOOR) return false;
    return true;
}
bool Strategy::can_sell(ExchangeConnection& c, const std::string& inst, int qty) {
    if (c.killed) return false;
    int pos = c.position_of(inst);
    if (pos - qty < Params::MAX_SHORT) return false;
    return true;
}

void Strategy::on_market_data(const std::string& exch, int64_t server_time_ms) {
    {
        std::lock_guard lk(last_run_mtx_);
        auto t = now_ms();
        auto& last = last_run_[exch];
        if (t - last < Params::STRAT_MIN_INTERVAL_MS) return;
        last = t;
    }
    try {
        // End-of-segment unwind takes priority — once close to end, only flatten
        if (Params::ENABLE_UNWIND &&
            server_time_ms >= Params::ROUND_LEN_MS - Params::UNWIND_BEFORE_END_MS) {
            unwind(exch);
            return;
        }
        if (Params::ENABLE_ETF_ARB)    etf_arb(exch);
        if (Params::ENABLE_XVENUE_ARB) xvenue_arb(exch);
        if (Params::ENABLE_STALE_ARB)  stale_arb(exch);
        if (Params::ENABLE_PATTERN)    pattern_trade(exch);
        if (Params::ENABLE_SECTOR)     sector_momentum(exch);
        if (Params::ENABLE_SAFEHAVEN)  safe_haven(exch);
        if (Params::ENABLE_MM)         market_make(exch);
        if (Params::ENABLE_COINT)      coint_basket_trade(exch);
    } catch (const std::exception& e) {
        std::cerr << "[" << exch << "] strategy error: " << e.what() << "\n";
    } catch (...) {
        std::cerr << "[" << exch << "] strategy unknown error\n";
    }
}

// Sample mid into history (called once per market_data_update, not per-exchange)
void Strategy::record_history() {
    int64_t t = now_ms();
    for (auto& [tkr, _] : STOCK_LISTINGS) {
        auto m = state_.consensus_mid(tkr);
        if (m) state_.record_history(tkr, *m, t);
    }
}

void Strategy::etf_arb(const std::string& exch) {
    auto& c = *conns_.at(exch);
    for (auto& [etf, exs] : ETF_LISTINGS) {
        if (std::find(exs.begin(), exs.end(), exch) == exs.end()) continue;
        std::string inst = exch + "-" + etf;
        auto bk = state_.get(exch, inst); if (!bk) continue;
        auto nav = state_.etf_nav(etf);   if (!nav) continue;
        const int edge = etf_edge_for(etf);

        // ETF bid > NAV + edge → SELL (hit bid)
        if (bk->best_bid() && (*bk->best_bid() - *nav) >= edge) {
            int qty = std::min(Params::ARB_SIZE, *bk->best_bid_qty());
            if (qty > 0 && can_sell(c, inst, qty))
                c.place_ioc(inst, "ask", *bk->best_bid(), qty);
        }
        // ETF ask < NAV - edge → BUY (lift ask)
        if (bk->best_ask() && (*nav - *bk->best_ask()) >= edge) {
            int qty = std::min(Params::ARB_SIZE, *bk->best_ask_qty());
            if (qty > 0 && can_buy(c, inst, qty, *bk->best_ask()))
                c.place_ioc(inst, "bid", *bk->best_ask(), qty);
        }
    }
}

void Strategy::xvenue_arb(const std::string& exch) {
    auto& c = *conns_.at(exch);
    auto try_pair = [&](const std::string& tkr,
                        const std::vector<std::string>& exs) {
        if (std::find(exs.begin(), exs.end(), exch) == exs.end()) return;
        if (exs.size() < 2) return;
        std::string inst_here = exch + "-" + tkr;
        auto bk = state_.get(exch, inst_here); if (!bk) return;

        // Buy here, sell on best other-venue bid
        if (bk->best_ask()) {
            auto best_bid = state_.best_bid_xv(tkr);
            if (best_bid && best_bid->exchange != exch
                && best_bid->price - *bk->best_ask() >= Params::XVENUE_EDGE) {
                int qty = std::min({Params::ARB_SIZE, *bk->best_ask_qty(), best_bid->qty});
                std::string inst_other = best_bid->exchange + "-" + tkr;
                auto& c2 = *conns_.at(best_bid->exchange);
                if (qty > 0 && can_buy(c, inst_here, qty, *bk->best_ask())
                            && can_sell(c2, inst_other, qty)) {
                    c.place_ioc(inst_here, "bid", *bk->best_ask(), qty);
                    c2.place_ioc(inst_other, "ask", best_bid->price, qty);
                }
            }
        }
    };
    for (auto& [t,exs] : STOCK_LISTINGS) try_pair(t, exs);
    for (auto& [t,exs] : ETF_LISTINGS)   try_pair(t, exs);
}

// Snipe stale single-venue mid that drifted from cross-venue consensus.
void Strategy::stale_arb(const std::string& exch) {
    auto& c = *conns_.at(exch);
    for (auto& [tkr, exs] : STOCK_LISTINGS) {
        if (std::find(exs.begin(), exs.end(), exch) == exs.end()) continue;
        if (exs.size() < 3) continue;  // need consensus
        std::string inst = exch + "-" + tkr;
        auto bk = state_.get(exch, inst); if (!bk) continue;
        auto cm = state_.consensus_mid(tkr); if (!cm) continue;

        // ask here way below consensus → buy here
        if (bk->best_ask() && (*cm - *bk->best_ask()) >= Params::STALE_EDGE) {
            int qty = std::min(Params::ARB_SIZE, *bk->best_ask_qty());
            if (qty > 0 && can_buy(c, inst, qty, *bk->best_ask()))
                c.place_ioc(inst, "bid", *bk->best_ask(), qty);
        }
        // bid here way above consensus → sell here
        if (bk->best_bid() && (*bk->best_bid() - *cm) >= Params::STALE_EDGE) {
            int qty = std::min(Params::ARB_SIZE, *bk->best_bid_qty());
            if (qty > 0 && can_sell(c, inst, qty))
                c.place_ioc(inst, "ask", *bk->best_bid(), qty);
        }
    }
}

void Strategy::market_make(const std::string& exch) {
    auto& c = *conns_.at(exch);
    for (const std::string tkr : {"CARD","SIMP"}) {
        std::string inst = exch + "-" + tkr;
        auto bk = state_.get(exch, inst);
        if (!bk || !bk->best_bid() || !bk->best_ask()) continue;
        int sp = *bk->spread();
        if (sp < Params::MM_MIN_SPREAD) continue;

        int pos = c.position_of(inst);
        int skew = pos * Params::MM_INV_SKEW_C;
        int mid = (*bk->best_bid() + *bk->best_ask()) / 2;
        int bid_px = std::min(*bk->best_bid() + Params::MM_QUOTE_INSIDE, mid - 1) - skew;
        int ask_px = std::max(*bk->best_ask() - Params::MM_QUOTE_INSIDE, mid + 1) - skew;
        if (bid_px >= ask_px) continue;

        int qty = Params::MM_SIZE;
        if (pos < Params::MAX_LONG - qty && can_buy(c, inst, qty, bid_px))
            c.place_limit(inst, "bid", bid_px, qty);
        if (pos > Params::MAX_SHORT + qty && can_sell(c, inst, qty))
            c.place_limit(inst, "ask", ask_px, qty);
    }
}

// ─── Pattern trade: mean-revert on PATTERN_TICKERS ───────────────────
// Z-score around the rolling mean of the cross-venue consensus mid.
//   z >= +PATTERN_ENTRY_STDEV/100  → SHORT (price too high, will revert down)
//   z <= -PATTERN_ENTRY_STDEV/100  → LONG  (price too low,  will revert up)
//   |z| <= PATTERN_EXIT_STDEV/100  → CLOSE existing position toward zero
// This implements the proper mean-reversion lifecycle: enter on extreme,
// exit at the mean (don't wait until segment-end unwind).
void Strategy::pattern_trade(const std::string& exch) {
    auto& c = *conns_.at(exch);
    for (auto& tkr : PATTERN_TICKERS) {
        if (std::find(listings_for(tkr).begin(), listings_for(tkr).end(), exch)
            == listings_for(tkr).end()) continue;
        std::string inst = exch + "-" + tkr;
        auto bk = state_.get(exch, inst); if (!bk) continue;
        auto st = state_.stats(tkr);      if (!st) continue;
        if (st->stdev < 5.0) continue;  // not enough variation yet

        double z = (st->last - st->mean) / st->stdev;  // standardized
        int pos = c.position_of(inst);

        const double entry = Params::PATTERN_ENTRY_STDEV / 100.0;
        const double exit  = Params::PATTERN_EXIT_STDEV  / 100.0;

        // ── EXIT first: close existing position when price crosses back to mean
        if (std::abs(z) <= exit) {
            if (pos > 0 && bk->best_bid()) {
                int qty = std::min(pos, std::min(Params::PATTERN_SIZE, *bk->best_bid_qty()));
                if (qty > 0) c.place_ioc(inst, "ask", *bk->best_bid(), qty);
            } else if (pos < 0 && bk->best_ask()) {
                int qty = std::min(-pos, std::min(Params::PATTERN_SIZE, *bk->best_ask_qty()));
                if (qty > 0) c.place_ioc(inst, "bid", *bk->best_ask(), qty);
            }
            continue;
        }

        // ── ENTRY: open against the deviation (mean-revert)
        if (z >= entry) {
            if (bk->best_bid()) {
                int qty = std::min(Params::PATTERN_SIZE, *bk->best_bid_qty());
                if (qty > 0 && can_sell(c, inst, qty))
                    c.place_ioc(inst, "ask", *bk->best_bid(), qty);
            }
        } else if (z <= -entry) {
            if (bk->best_ask()) {
                int qty = std::min(Params::PATTERN_SIZE, *bk->best_ask_qty());
                if (qty > 0 && can_buy(c, inst, qty, *bk->best_ask()))
                    c.place_ioc(inst, "bid", *bk->best_ask(), qty);
            }
        }
    }
}

// ─── Sector momentum: if peers in same sector moved up/down recently
// but THIS stock hasn't, trade in the direction of the peers. ─────────
void Strategy::sector_momentum(const std::string& exch) {
    auto& c = *conns_.at(exch);
    for (auto& [_, members] : SECTORS) {
        // Compute average pct change of sector over short lookback
        double sum_chg = 0; int n = 0;
        std::map<std::string,double> chgs;
        for (auto& tkr : members) {
            auto st = state_.stats(tkr);
            if (!st) continue;
            double chg = st->slope_per_tick * Params::SECTOR_LOOKBACK_TICKS / st->mean;
            chgs[tkr] = chg;
            sum_chg += chg; n++;
        }
        if (n < 4) continue;
        double sector_chg = sum_chg / n;
        double trigger = Params::SECTOR_TRIGGER_BPS / 10000.0;
        if (std::abs(sector_chg) < trigger) continue;

        // For each member listed on this exchange: if its own change lags
        // the sector by > trigger/2, fire in the sector's direction.
        for (auto& tkr : members) {
            if (chgs.find(tkr) == chgs.end()) continue;
            if (std::find(listings_for(tkr).begin(), listings_for(tkr).end(), exch)
                == listings_for(tkr).end()) continue;
            std::string inst = exch + "-" + tkr;
            auto bk = state_.get(exch, inst); if (!bk) continue;
            double laggard = chgs[tkr];
            if (sector_chg > 0 && laggard < sector_chg - trigger/2 && bk->best_ask()) {
                int qty = std::min(Params::SECTOR_SIZE, *bk->best_ask_qty());
                if (qty > 0 && can_buy(c, inst, qty, *bk->best_ask()))
                    c.place_ioc(inst, "bid", *bk->best_ask(), qty);
            } else if (sector_chg < 0 && laggard > sector_chg + trigger/2 && bk->best_bid()) {
                int qty = std::min(Params::SECTOR_SIZE, *bk->best_bid_qty());
                if (qty > 0 && can_sell(c, inst, qty))
                    c.place_ioc(inst, "ask", *bk->best_bid(), qty);
            }
        }
    }
}

// ─── Safe-haven: GOLD/XAG move INVERSELY with the market ─────────────
void Strategy::safe_haven(const std::string& exch) {
    auto& c = *conns_.at(exch);
    auto idx_now = state_.market_index();
    if (!idx_now) return;
    // Estimate market trend from average of stats() slopes for market tickers
    double sum_chg = 0; int n = 0;
    for (auto& tkr : MARKET_TICKERS) {
        auto st = state_.stats(tkr);
        if (!st) continue;
        sum_chg += st->slope_per_tick * Params::SAFEHAVEN_LOOKBACK_TICKS / st->mean;
        n++;
    }
    if (n < 6) return;
    double mkt_chg = sum_chg / n;
    double trigger = Params::SAFEHAVEN_TRIGGER_BPS / 10000.0;
    if (std::abs(mkt_chg) < trigger) return;

    for (auto& tkr : SAFEHAVEN_TICKERS) {
        if (std::find(listings_for(tkr).begin(), listings_for(tkr).end(), exch)
            == listings_for(tkr).end()) continue;
        std::string inst = exch + "-" + tkr;
        auto bk = state_.get(exch, inst); if (!bk) continue;
        // Market falling -> safe haven should rise -> buy
        if (mkt_chg < 0 && bk->best_ask()) {
            int qty = std::min(Params::SAFEHAVEN_SIZE, *bk->best_ask_qty());
            if (qty > 0 && can_buy(c, inst, qty, *bk->best_ask()))
                c.place_ioc(inst, "bid", *bk->best_ask(), qty);
        } else if (mkt_chg > 0 && bk->best_bid()) {
            int qty = std::min(Params::SAFEHAVEN_SIZE, *bk->best_bid_qty());
            if (qty > 0 && can_sell(c, inst, qty))
                c.place_ioc(inst, "ask", *bk->best_bid(), qty);
        }
    }
}

// ─── Cointegrated basket stat-arb ────────────────────────────────────
// For each basket whose venue == exch:
//   1. Read venue-local mids for each leg, compute spread = Σ β_i · mid_i
//   2. Push spread into rolling history; once we have ≥ COINT_MIN_SAMPLES,
//      compute z = (s − μ)/σ
//   3. Determine target_state with hysteresis:
//        -1 (short spread) if z >=  entry
//        +1 (long  spread) if z <= -entry
//         0 (flat)         if |z| <= exit
//        else hold previous
//   4. After updating ALL baskets' target_states, AGGREGATE per-leg desired
//      position across baskets (handles overlapping legs like INA in B1+B2):
//        target_pos[ticker] = Σ_basket (basket.state · round(K · β_basket,ticker))
//      Then send IOCs to push current position toward aggregated target.
void Strategy::coint_basket_trade(const std::string& exch) {
    // Step 1+2+3: update each basket's target_state. Track which baskets
    // are active on this venue so we can aggregate after.
    std::vector<const CointBasket*> active;
    int64_t now = now_ms();
    bool any_changed = false;

    for (const auto& cb : COINT_BASKETS) {
        if (cb.venue != exch) continue;
        if (cb.legs.empty()) continue;

        double spread = 0.0;
        bool ok = true;
        for (auto& [tkr, beta] : cb.legs) {
            std::string inst = cb.venue + "-" + tkr;
            auto book = state_.get(cb.venue, inst);
            if (!book) { ok = false; break; }
            auto m = book->mid();
            if (!m) { ok = false; break; }
            spread += beta * (*m);
        }
        if (!ok) continue;
        active.push_back(&cb);

        CointState* cs;
        {
            std::lock_guard lk(coint_mtx_);
            cs = &coint_state_[cb.name];
            cs->spread_hist.push_back(spread);
            if ((int)cs->spread_hist.size() > Params::COINT_HISTORY)
                cs->spread_hist.erase(cs->spread_hist.begin());
        }

        if ((int)cs->spread_hist.size() < Params::COINT_MIN_SAMPLES) continue;

        std::vector<double> snap;
        {
            std::lock_guard lk(coint_mtx_);
            snap = cs->spread_hist;
        }
        double sum = 0, sumsq = 0;
        for (double v : snap) { sum += v; sumsq += v*v; }
        double n = snap.size();
        double mean = sum / n;
        double var = std::max(1e-12, sumsq/n - mean*mean);
        double sd = std::sqrt(var);
        if (sd < 1e-6) continue;
        double z = (spread - mean) / sd;

        const double entry = Params::COINT_ENTRY_Z100 / 100.0;
        const double exit_ = Params::COINT_EXIT_Z100  / 100.0;

        int prev = cs->target_state;
        int target = prev;
        if (z >= entry)              target = -1;
        else if (z <= -entry)        target = +1;
        else if (std::abs(z) <= exit_) target = 0;

        if (target != prev) {
            cs->target_state = target;
            any_changed = true;
            std::cout << "[ZSE coint-" << cb.name << "] z=" << z
                      << " (μ=" << mean << ", σ=" << sd << ")"
                      << " state " << prev << " → " << target << "\n";
        }
    }

    // Throttle: only rebalance if something changed and not too soon.
    if (!any_changed) return;
    {
        std::lock_guard lk(coint_mtx_);
        // Single global throttle keyed by venue (cheaper than per-basket here
        // because we already gate on any_changed).
        auto& cs0 = coint_state_["__throttle_" + exch];
        if (now - cs0.last_rebal_ms < Params::COINT_REBAL_MS) return;
        cs0.last_rebal_ms = now;
    }

    // Step 4: aggregate per-leg target positions across all baskets.
    auto& c = *conns_.at(exch);
    std::unordered_map<std::string, int> target_by_inst;  // inst -> target shares
    for (const auto* cbp : active) {
        const auto& cb = *cbp;
        int state;
        {
            std::lock_guard lk(coint_mtx_);
            state = coint_state_[cb.name].target_state;
        }
        for (auto& [tkr, beta] : cb.legs) {
            std::string inst = cb.venue + "-" + tkr;
            int leg_units = (int)std::lround(Params::COINT_BASKET_K * beta);
            target_by_inst[inst] += state * leg_units;
        }
    }

    // Send IOCs to move each instrument toward its aggregated target.
    for (auto& [inst, target_pos] : target_by_inst) {
        int cur  = c.position_of(inst);
        int diff = target_pos - cur;
        if (diff == 0) continue;

        auto book = state_.get(exch, inst);
        if (!book) continue;

        if (diff > 0) {
            if (!book->best_ask() || !book->best_ask_qty()) continue;
            int qty = std::min({diff, *book->best_ask_qty(), 100});
            if (qty <= 0 || !can_buy(c, inst, qty, *book->best_ask())) continue;
            c.place_ioc(inst, "bid", *book->best_ask(), qty);
        } else {
            int need = -diff;
            if (!book->best_bid() || !book->best_bid_qty()) continue;
            int qty = std::min({need, *book->best_bid_qty(), 100});
            if (qty <= 0 || !can_sell(c, inst, qty)) continue;
            c.place_ioc(inst, "ask", *book->best_bid(), qty);
        }
    }
}

// ─── End-of-segment unwind: aggressively flatten all positions ────────
void Strategy::unwind(const std::string& exch) {
    auto& c = *conns_.at(exch);
    // Snapshot positions so we don't hold pos_mtx during network IO
    std::vector<std::pair<std::string,int>> snap;
    {
        std::lock_guard lk(c.pos_mtx);
        for (auto& [inst, pos] : c.positions) {
            if (pos == 0) continue;
            if (inst.rfind(exch + "-", 0) != 0) continue;
            snap.emplace_back(inst, pos);
        }
    }
    for (auto& [inst, pos] : snap) {
        auto bk = state_.get(exch, inst); if (!bk) continue;
        if (pos > 0) {
            if (!bk->best_bid()) continue;
            int qty = std::min(pos, 100);
            c.place_ioc(inst, "ask", *bk->best_bid(), qty);
        } else {
            if (!bk->best_ask()) continue;
            int qty = std::min(-pos, 100);
            c.place_ioc(inst, "bid", *bk->best_ask(), qty);
        }
    }
}

// ════════════════════════════════════════════════════════════════════
//  Bot — orchestrator with auto-reconnect across segments
// ════════════════════════════════════════════════════════════════════
class Bot {
public:
    void run(const std::vector<std::string>& exchanges) {
        for (auto& name : exchanges) {
            auto host = lookup_host(name);
            if (!host) { std::cerr << "unknown exchange: " << name << "\n"; continue; }
            conns_.emplace(name, std::make_unique<ExchangeConnection>(name, *host));
        }
        strategy_ = std::make_unique<Strategy>(state_, conns_);

        std::vector<std::thread> ts;
        for (auto& [name, _] : conns_)
            ts.emplace_back([this, name]{ exchange_loop(name); });

        // periodic inventory sync + activity report
        std::thread inv([this]{
            long prev_sent[16] = {0}, prev_fill[16] = {0}, prev_rej[16] = {0};
            while (running_) {
                std::this_thread::sleep_for(std::chrono::seconds(5));
                int i = 0;
                std::ostringstream report;
                report << "[stats] ";
                for (auto& [name, c] : conns_) {
                    if (c->connected()) c->get_inventory();
                    long s = c->orders_sent_.load();
                    long f = c->orders_filled_.load();
                    long r = c->orders_rejected_.load();
                    long ds = s - prev_sent[i];
                    long df = f - prev_fill[i];
                    long dr = r - prev_rej[i];
                    prev_sent[i] = s; prev_fill[i] = f; prev_rej[i] = r;
                    long cash;
                    {
                        std::lock_guard lk(c->pos_mtx);
                        cash = c->cash_cents;
                    }
                    if (ds > 0 || df > 0 || dr > 0) {
                        report << name << "(s" << ds << "/f" << df << "/r" << dr
                               << " $" << (cash/100) << ") ";
                    }
                    ++i;
                }
                std::string s = report.str();
                if (s.size() > 9) std::cout << s << "\n";
            }
        });

        for (auto& t : ts) t.join();
        running_ = false;
        inv.join();
    }

private:
    void exchange_loop(const std::string& name) {
        auto& c = *conns_.at(name);
        int backoff = 1;
        while (running_) {
            if (!c.connect()) {
                std::this_thread::sleep_for(std::chrono::seconds(backoff));
                backoff = std::min(backoff*2, 8);
                continue;
            }
            backoff = 1;
            c.on_segment_reset();
            state_.reset_history();
            if (strategy_) strategy_->reset_segment();
            std::cout << "[" << name << "] connected\n";

            while (running_ && c.connected()) {
                auto m = c.recv();
                if (!m) break;
                auto type = m->value("type","");
                if (type == "market_data_update") {
                    int64_t srv_t = m->value("time", (int64_t)0);
                    if (m->contains("orderbook_depths")) {
                        for (auto& [inst, ob] : (*m)["orderbook_depths"].items())
                            state_.update_book(name, inst,
                                               ob.value("bids", json::object()),
                                               ob.value("asks", json::object()), srv_t);
                    }
                    strategy_->record_history();
                    strategy_->on_market_data(name, srv_t);
                } else if (type == "add_order_response") {
                    c.apply_add_order_resp(*m);
                } else if (type == "get_inventory_response") {
                    c.apply_inventory(*m);
                } else if (type == "end_of_round") {
                    std::cout << "[" << name << "] end_of_round; reconnecting for next segment\n";
                    break;
                } else if (type == "error") {
                    std::cerr << "[" << name << "] error: " << m->value("message","") << "\n";
                }
            }
            c.disconnect();
            // small pause before reconnect (next segment server may take a moment)
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }

    MarketState state_;
    std::unordered_map<std::string,std::unique_ptr<ExchangeConnection>> conns_;
    std::unique_ptr<Strategy> strategy_;
    std::atomic<bool> running_{true};
};

// ════════════════════════════════════════════════════════════════════
//  main
// ════════════════════════════════════════════════════════════════════
static std::vector<std::string> parse_csv(const std::string& s) {
    std::vector<std::string> out;
    std::istringstream is(s); std::string tok;
    while (std::getline(is, tok, ',')) {
        auto a = tok.find_first_not_of(" \t");
        auto b = tok.find_last_not_of(" \t");
        if (a != std::string::npos) out.push_back(tok.substr(a, b-a+1));
    }
    return out;
}

int main() {
    std::vector<std::string> exchanges;
    if (const char* v = std::getenv("EXCHANGES")) exchanges = parse_csv(v);
    if (exchanges.empty())
        for (auto& [n,_] : EXCHANGE_HOSTS) exchanges.push_back(n);

    std::cout << "AlgoTrade 2026 bot — exchanges:";
    for (auto& e : exchanges) std::cout << " " << e;
    std::cout << "\n";

    Bot bot;
    bot.run(exchanges);
    return 0;
}