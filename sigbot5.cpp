/*
 * AlgoTrade 2026 — Competition Bot (C++20)
 *
 * Four strategies:
 *   1. ETF arbitrage (hedged: ETF mispriced vs mean of constituents)
 *   2. Safe-haven spread (ETFA/B − ETFSH mean-reverts around rolling μ)
 *   3. Sector pair z-arb (stock vs sector-peer-mean, online β=1 hedge)
 *   4. Cross-venue same-ticker arbitrage (global min-ask vs max-bid scan,
 *      all tickers, all 10 venues simultaneously)
 *
 * Build:
 *   g++ -std=c++20 -O2 -o bot bot.cpp -lpthread
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
//  Params
// ════════════════════════════════════════════════════════════════════
struct Params {
    // Risk caps
    static constexpr int  MAX_LONG    = 150;
    static constexpr int  MAX_SHORT   = -150;
    static constexpr long CASH_FLOOR  = -3'000'000;   // cents

    static constexpr int  ORDER_TTL_MS         = 4'000;
    static constexpr int  STRAT_MIN_INTERVAL_MS = 50;
    static constexpr int  RATE_LIMIT_PER_SEC    = 400;

    // Warmup gate (after each (re)connect)
    static constexpr int  WARMUP_MIN_MS     = 10'000;
    static constexpr int  WARMUP_MIN_VENUES = 7;

    // Per-instrument cooldown after sending an order
    static constexpr int  INST_COOLDOWN_MS = 250;

    // ─── ETF arbitrage (hedged: ETF mispriced vs constituents) ──────
    // Trade when |ETF_mid - mean(constituent_mids)| ≥ EDGE on a venue
    // where ALL constituents are listed (full local hedge → no drift risk).
    static constexpr int  ETF_EDGE_CENTS    = 20;    // entry threshold
    static constexpr int  ETF_EXIT_CENTS    = 5;     // unwind when mispricing collapses
    static constexpr int  ETF_HEDGE_UNIT    = 1;     // base hedge size
    static constexpr int  ETF_POS_CAP       = 60;    // max ETF qty per (etf, venue)
    static constexpr int  ETF_COOLDOWN_MS   = 250;   // per (etf, venue)

    // ─── Safe-haven spread (market index + ETFSH ≈ 200) ─────────────
    // Per guide: safe-haven (GOLD/XAG → ETFSH) inversely correlated with
    // sectors (ETFA/ETFB). Spread = ETFA + ETFSH should oscillate near 200.
    // Trade z-score of deviation from rolling mean.
    static constexpr int  SH_HISTORY           = 600;   // ~60s @ 100ms
    static constexpr int  SH_MIN_SAMPLES       = 100;
    static constexpr int  SH_ENTRY_Z100        = 250;   // |z| ≥ 2.5
    static constexpr int  SH_EXIT_Z100         = 50;
    static constexpr int  SH_STOP_Z100         = 600;
    static constexpr int  SH_STOP_COOLDOWN_MS  = 30'000;
    static constexpr int  SH_SIZE              = 10;    // qty per leg
    static constexpr int  SH_POS_CAP           = 40;
    static constexpr int  SH_REBAL_MS          = 250;

    // ─── Sector pair z-arb (per-venue, per-sector intra-correlation) ────
    // For each stock i in a sector with N≥2 peers on the venue:
    //   peer_mean_i = Σ_j w_j * price_j     where j ≠ i
    //   w_j = max(0, rolling_corr(returns_i, returns_j))  normalised
    //   residual_i = price_i − peer_mean_i
    // z-score residual against rolling μ/σ, mean-revert.
    // Hedge: when target=+1 (stock cheap), LONG (N-1) of stock_i and
    //        SHORT 1 of each of the (N-1) peers → dollar-neutral.
    // Correlations are recomputed every signal evaluation (rolling).
    static constexpr int  SECPAIR_HISTORY          = 600;   // ~60s @ 100ms (resid hist)
    static constexpr int  SECPAIR_MID_HIST         = 300;   // ~30s @ 100ms (mid hist for corr)
    static constexpr int  SECPAIR_CORR_MIN_SAMPLES = 100;   // ~10s minimum for valid corr
    static constexpr int  SECPAIR_MIN_SAMPLES      = 150;   // 15s warmup of own resid series
    static constexpr int  SECPAIR_ENTRY_Z100       = 250;
    static constexpr int  SECPAIR_EXIT_Z100        = 50;
    static constexpr int  SECPAIR_STOP_Z100        = 600;
    static constexpr int  SECPAIR_STOP_COOLDOWN_MS = 30'000;
    static constexpr int  SECPAIR_REBAL_MS         = 250;
    static constexpr int  SECPAIR_POS_CAP          = 30;    // per ticker per venue

    // ─── Cross-venue same-ticker arbitrage (global scan) ────────────
    // Every tick, for each ticker observed on ≥2 venues, find venue with
    // cheapest ask and venue with richest bid. If they differ and
    // bid - ask >= edge, fire paired IOC. No transaction fees, so edge=1c
    // is pure positive EV. Cooldown is per ticker (global, not per pair).
    static constexpr int  XVENUE_EDGE_CENTS    = 1;
    static constexpr int  XVENUE_SIZE          = 50;
    static constexpr int  XVENUE_POS_CAP       = 150;
    static constexpr int  XVENUE_COOLDOWN_MS   = 100; // per ticker
};

// ════════════════════════════════════════════════════════════════════
//  Exchanges
// ════════════════════════════════════════════════════════════════════
static constexpr int EXCHANGE_PORT = 9001;

static const std::vector<std::pair<std::string,std::string>> EXCHANGE_HOSTS = {
    {"NYSE","10.0.201.2"}, {"NASDAQ","10.0.202.2"}, {"SSE","10.0.203.2"},
    {"JPX","10.0.204.2"},  {"Euronext","10.0.205.2"}, {"LSE","10.0.206.2"},
    {"HKEX","10.0.207.2"}, {"NSE","10.0.208.2"}, {"TMX","10.0.209.2"},
    {"ZSE","10.0.210.2"},
};

static std::optional<std::string> lookup_host(const std::string& name) {
    for (auto& [n,h] : EXCHANGE_HOSTS) if (n==name) return h;
    return std::nullopt;
}

// ════════════════════════════════════════════════════════════════════
//  ETF definitions (per guide §5)
//  ETF fair value = simple equal-weighted mean of constituent prices.
//  We trade ETF arb only on venues where ALL constituents are listed
//  locally (full hedge, no cross-venue leg risk).
// ════════════════════════════════════════════════════════════════════
struct ETFDef {
    std::string ticker;
    std::vector<std::string> constituents;
    std::vector<std::string> hedge_venues;  // venues with ETF + ALL constituents
};

static const std::vector<ETFDef> ETFS = {
    // 6-constituent ETFs — only ZSE has full local hedge
    {"ETFA",  {"NGUP","OIT","KTST","FSR","JZRO","XFR"},   {"ZSE"}},
    {"ETFB",  {"KOTD","INA","HT","JNAF","DLKV","DDJH"},   {"ZSE"}},
    // 3-constituent subsets — more venues with full hedge
    {"ETFA3", {"NGUP","KTST","XFR"},                      {"ZSE","NYSE"}},
    {"ETFB3", {"KOTD","INA","DLKV"},                      {"ZSE","NASDAQ","HKEX"}},
    // Safe-haven (2 constituents)
    {"ETFSH", {"GOLD","XAG"},                             {"ZSE","Euronext","JPX"}},
};

// ════════════════════════════════════════════════════════════════════
//  Safe-haven spreads (per guide §4: GOLD/XAG move inversely to market)
//  Spread = market_etf + safe_etf  → expected ≈ 200 (both start at 100).
//  Trade z-score of deviation. ZSE only (full coverage).
// ════════════════════════════════════════════════════════════════════
struct SHSpread {
    std::string name;
    std::string venue;
    std::string market_etf;   // ETFA or ETFB (sector index)
    std::string safe_etf;     // ETFSH
};

static const std::vector<SHSpread> SH_SPREADS = {
    {"A_SH", "ZSE", "ETFA", "ETFSH"},
    {"B_SH", "ZSE", "ETFB", "ETFSH"},
};

// ════════════════════════════════════════════════════════════════════
//  Sectors (per guide §4) — used by sector pair z-arb
// ════════════════════════════════════════════════════════════════════
struct SectorDef {
    std::string name;
    std::vector<std::string> tickers;
};
static const std::vector<SectorDef> SECTORS_DEF = {
    {"A", {"NGUP","OIT","KTST","FSR","JZRO","XFR"}},
    {"B", {"KOTD","INA","HT","JNAF","DLKV","DDJH"}},
};

// ════════════════════════════════════════════════════════════════════
//  Order book / market state
// ════════════════════════════════════════════════════════════════════
struct Book {
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

    // List instruments observed on a venue (snapshot of names).
    std::vector<std::string> instruments_on(const std::string& exch) const {
        std::lock_guard lk(mtx_);
        std::vector<std::string> out;
        auto e = books_.find(exch); if (e==books_.end()) return out;
        out.reserve(e->second.size());
        for (auto& [k,_] : e->second) out.push_back(k);
        return out;
    }

    // ─── Warmup tracking ────────────────────────────────────────────
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

private:
    mutable std::mutex mtx_;
    std::map<std::string, std::map<std::string, Book>> books_;
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
        } catch (const std::exception&) {
            connected_ = false;
            return false;
        }
    }

    void disconnect() {
        connected_ = false;
        try { ws_->close(websocket::close_code::normal); } catch (...) {}
    }

    std::string next_rid() { return name_+"-"+std::to_string(++rid_); }

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

    std::mutex pos_mtx;
    std::unordered_map<std::string,int> positions;
    long cash_cents = 10'000'000;
    std::atomic<bool> killed{false};
    std::atomic<long> orders_sent_{0};
    std::atomic<long> orders_filled_{0};
    std::atomic<long> orders_rejected_{0};

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
//  Strategy
// ════════════════════════════════════════════════════════════════════
class Strategy {
public:
    Strategy(MarketState& s,
             std::unordered_map<std::string,std::unique_ptr<ExchangeConnection>>& c)
        : state_(s), conns_(c) {}

    void on_market_data(const std::string& exch);
    void reset_segment() {
        std::lock_guard lk1(sh_mtx_);
        sh_state_.clear();
        std::lock_guard lk2(etf_cd_mtx_);
        last_etf_.clear();
        std::lock_guard lk3(secpair_mtx_);
        secpair_state_.clear();
        std::lock_guard lk4(xvenue_mtx_);
        last_xvenue_.clear();
    }

private:
    bool can_buy(ExchangeConnection& c, const std::string& inst, int qty, int px);
    bool can_sell(ExchangeConnection& c, const std::string& inst, int qty);

    void etf_arb(const std::string& exch);
    void safehaven_spread(const std::string& exch);
    void sector_pair_zarb(const std::string& exch);
    void xvenue_arb();

    MarketState& state_;
    std::unordered_map<std::string,std::unique_ptr<ExchangeConnection>>& conns_;

    std::unordered_map<std::string,int64_t> last_run_;
    std::mutex last_run_mtx_;

    // Per-instrument cooldown to suppress order spam
    std::unordered_map<std::string,int64_t> last_inst_order_;
    std::mutex inst_cd_mtx_;
    bool inst_cooldown_ok(const std::string& inst) {
        std::lock_guard lk(inst_cd_mtx_);
        int64_t t = now_ms();
        auto it = last_inst_order_.find(inst);
        if (it != last_inst_order_.end() && t - it->second < Params::INST_COOLDOWN_MS)
            return false;
        last_inst_order_[inst] = t;
        return true;
    }

    // ETF arb cooldown: key = "ETF|venue"
    std::unordered_map<std::string,int64_t> last_etf_;
    std::mutex etf_cd_mtx_;

    // Safe-haven spread state
    struct SHState {
        std::vector<double> spread_hist;
        int64_t last_rebal_ms = 0;
        int target_state = 0;       // -1 short / +1 long / 0 flat
        int64_t stop_until_ms = 0;
    };
    std::unordered_map<std::string, SHState> sh_state_;
    std::mutex sh_mtx_;

    // Sector pair state per "venue|sector|ticker" key
    struct SecPairState {
        std::vector<double> resid_hist;
        std::vector<double> mid_hist;   // rolling mid prices for pairwise corr
        int64_t last_rebal_ms = 0;
        int target_state = 0;
        int64_t stop_until_ms = 0;
    };
    std::unordered_map<std::string, SecPairState> secpair_state_;
    std::mutex secpair_mtx_;

    // xvenue per-ticker cooldown
    std::unordered_map<std::string,int64_t> last_xvenue_;
    std::mutex xvenue_mtx_;
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

void Strategy::on_market_data(const std::string& exch) {
    {
        std::lock_guard lk(last_run_mtx_);
        auto t = now_ms();
        auto& last = last_run_[exch];
        if (t - last < Params::STRAT_MIN_INTERVAL_MS) return;
        last = t;
    }
    try {
        etf_arb(exch);
        safehaven_spread(exch);
        sector_pair_zarb(exch);
        xvenue_arb();
    } catch (const std::exception& e) {
        std::cerr << "[" << exch << "] strategy error: " << e.what() << "\n";
    } catch (...) {
        std::cerr << "[" << exch << "] strategy unknown error\n";
    }
}

// ─── ETF arbitrage ───────────────────────────────────────────────────
// Per guide §5: ETF fair value = simple mean of constituent prices.
// On a venue where ALL constituents are listed locally, we fully hedge:
//   if ETF underpriced (etf_ask < fair - EDGE):  BUY N*K ETFs, SELL K of each constituent
//   if ETF overpriced  (etf_bid > fair + EDGE):  SELL N*K ETFs, BUY K of each constituent
// where N = number of constituents and K = ETF_HEDGE_UNIT.
// Math: K ETFs at price ≈ fair carry exposure ≈ K*sum_of_const_prices/N.
//   Hedge with 1 share of each constituent (N total) → matches exactly.
//   So qty: etf_qty = N*K, const_qty = K each (gives same dollar exposure
//   only when using K, not N*K — see correction below).
// Correction: 1 ETF share is worth (sum)/N. To replicate: hold 1/N of
//   each constituent. Smallest hedgeable trade: N ETFs ↔ 1 of each.
//   So etf_qty = N*K, const_qty = K each.
void Strategy::etf_arb(const std::string& exch) {
    if (!state_.warmup_done()) return;
    int64_t now = now_ms();

    for (const auto& etf : ETFS) {
        // Only on venues where we can fully hedge locally.
        bool hedgeable = false;
        for (auto& v : etf.hedge_venues) if (v == exch) { hedgeable = true; break; }
        if (!hedgeable) continue;

        std::string etf_inst = exch + "-" + etf.ticker;
        auto book_etf = state_.get(exch, etf_inst);
        if (!book_etf) continue;
        auto etf_bid = book_etf->best_bid();
        auto etf_ask = book_etf->best_ask();
        auto etf_bid_qty = book_etf->best_bid_qty();
        auto etf_ask_qty = book_etf->best_ask_qty();
        if (!etf_bid || !etf_ask) continue;

        // Compute fair value from local constituent mids. Also collect
        // best bid/ask of each constituent for execution.
        const int N = (int)etf.constituents.size();
        double fair_sum = 0.0;
        bool all_ok = true;
        struct ConstSnap {
            std::string inst;
            int bid, ask, bid_qty, ask_qty;
        };
        std::vector<ConstSnap> snaps;
        snaps.reserve(N);
        for (auto& tkr : etf.constituents) {
            std::string ci = exch + "-" + tkr;
            auto bk = state_.get(exch, ci);
            if (!bk) { all_ok = false; break; }
            auto m = bk->mid();
            auto b = bk->best_bid(); auto a = bk->best_ask();
            auto bq = bk->best_bid_qty(); auto aq = bk->best_ask_qty();
            if (!m || !b || !a) { all_ok = false; break; }
            fair_sum += *m;
            snaps.push_back({ci, *b, *a, bq.value_or(0), aq.value_or(0)});
        }
        if (!all_ok) continue;

        double fair = fair_sum / N;
        int fair_int = (int)std::lround(fair);

        // Cooldown per (etf, venue)
        std::string cd_key = etf.ticker + "|" + exch;

        auto& c = *conns_.at(exch);
        int etf_pos = c.position_of(etf_inst);

        // ── Direction: ETF underpriced → BUY ETF, SELL constituents ──
        // ── or ETF overpriced → SELL ETF, BUY constituents ─────────
        int K = Params::ETF_HEDGE_UNIT;
        int etf_qty = N * K;     // total ETF shares per cycle
        int leg_qty = K;         // per-constituent shares per cycle

        bool buy_etf = false, sell_etf = false;
        int edge = 0;
        if (*etf_ask <= fair_int - Params::ETF_EDGE_CENTS) {
            buy_etf = true;
            edge = fair_int - *etf_ask;
        } else if (*etf_bid >= fair_int + Params::ETF_EDGE_CENTS) {
            sell_etf = true;
            edge = *etf_bid - fair_int;
        } else {
            continue;
        }

        // Cooldown gate
        {
            std::lock_guard lk(etf_cd_mtx_);
            auto it = last_etf_.find(cd_key);
            if (it != last_etf_.end() && now - it->second < Params::ETF_COOLDOWN_MS)
                continue;
        }

        // Position cap
        if (buy_etf  && etf_pos + etf_qty > Params::ETF_POS_CAP) continue;
        if (sell_etf && etf_pos - etf_qty < -Params::ETF_POS_CAP) continue;

        // Quantity available on each side
        int etf_side_qty = buy_etf ? etf_ask_qty.value_or(0) : etf_bid_qty.value_or(0);
        if (etf_side_qty < etf_qty) continue;

        // Verify each constituent has enough liquidity on opposite side
        bool liq_ok = true;
        for (auto& s : snaps) {
            int q = buy_etf ? s.bid_qty : s.ask_qty;
            if (q < leg_qty) { liq_ok = false; break; }
        }
        if (!liq_ok) continue;

        // Risk gates per leg
        if (buy_etf) {
            if (!can_buy(c, etf_inst, etf_qty, *etf_ask)) continue;
            for (auto& s : snaps)
                if (!can_sell(c, s.inst, leg_qty)) { liq_ok = false; break; }
        } else {
            if (!can_sell(c, etf_inst, etf_qty)) continue;
            for (auto& s : snaps)
                if (!can_buy(c, s.inst, leg_qty, s.ask)) { liq_ok = false; break; }
        }
        if (!liq_ok) continue;

        // Mark cooldown BEFORE firing to prevent races
        {
            std::lock_guard lk(etf_cd_mtx_);
            last_etf_[cd_key] = now;
        }

        // Fire ETF leg + all constituent hedge legs
        if (buy_etf) {
            c.place_ioc(etf_inst, "bid", *etf_ask, etf_qty);
            for (auto& s : snaps)
                c.place_ioc(s.inst, "ask", s.bid, leg_qty);
        } else {
            c.place_ioc(etf_inst, "ask", *etf_bid, etf_qty);
            for (auto& s : snaps)
                c.place_ioc(s.inst, "bid", s.ask, leg_qty);
        }
        std::cout << "[" << exch << " etf-" << etf.ticker << "] "
                  << (buy_etf ? "BUY" : "SELL") << " " << etf_qty
                  << "@" << (buy_etf ? *etf_ask : *etf_bid)
                  << " fair=" << fair_int << " edge=" << edge
                  << "¢ hedge " << N << "x" << leg_qty << "\n";
    }
}

// ─── Safe-haven spread (market_etf − safe_etf, mean-reverting) ───────────────────
// Per guide §4: GOLD/XAG (→ ETFSH) inversely correlated with sectors
// (→ ETFA, ETFB). Spread = market_etf − safe_etf. Both move opposite
// to each other, so the difference amplifies the inverse-correlation
// signal and is mean-reverting around its rolling mean.
void Strategy::safehaven_spread(const std::string& exch) {
    if (!state_.warmup_done()) return;
    int64_t now = now_ms();

    for (const auto& sh : SH_SPREADS) {
        if (sh.venue != exch) continue;

        std::string m_inst = exch + "-" + sh.market_etf;
        std::string s_inst = exch + "-" + sh.safe_etf;
        auto bm = state_.get(exch, m_inst);
        auto bs = state_.get(exch, s_inst);
        if (!bm || !bs) continue;
        auto mm = bm->mid(); auto ms = bs->mid();
        if (!mm || !ms) continue;

        double spread = *mm - *ms;  // ETFA − ETFSH (mean-reverting around rolling μ)

        SHState* st;
        {
            std::lock_guard lk(sh_mtx_);
            st = &sh_state_[sh.name];
            st->spread_hist.push_back(spread);
            if ((int)st->spread_hist.size() > Params::SH_HISTORY)
                st->spread_hist.erase(st->spread_hist.begin());
        }

        if ((int)st->spread_hist.size() < Params::SH_MIN_SAMPLES) continue;

        std::vector<double> snap;
        {
            std::lock_guard lk(sh_mtx_);
            snap = st->spread_hist;
        }
        double sum = 0, sumsq = 0;
        for (double v : snap) { sum += v; sumsq += v*v; }
        double n = snap.size();
        double mean = sum / n;
        double var = std::max(1e-12, sumsq/n - mean*mean);
        double sd = std::sqrt(var);
        if (sd < 1e-6) continue;
        double z = (spread - mean) / sd;

        const double entry = Params::SH_ENTRY_Z100 / 100.0;
        const double exit_ = Params::SH_EXIT_Z100  / 100.0;
        const double stop  = Params::SH_STOP_Z100  / 100.0;

        int prev = st->target_state;
        int target = prev;
        bool tripped = false;
        if (std::abs(z) >= stop) {
            if (prev != 0) { target = 0; tripped = true; }
            st->stop_until_ms = now + Params::SH_STOP_COOLDOWN_MS;
        }
        bool in_cooldown = now < st->stop_until_ms;
        if (!tripped) {
            // spread = ETFA − ETFSH unusually high → ETFA rich vs ETFSH
            //   → SHORT ETFA + LONG ETFSH (target = -1)
            // spread unusually low → ETFA cheap vs ETFSH
            //   → LONG ETFA + SHORT ETFSH (target = +1)
            if (z >= entry && !in_cooldown)        target = -1;
            else if (z <= -entry && !in_cooldown)  target = +1;
            else if (std::abs(z) <= exit_)         target = 0;
        }

        if (target == prev) continue;
        st->target_state = target;
        std::cout << "[" << exch << " sh-" << sh.name << "] z=" << z
                  << " (μ=" << mean << ", σ=" << sd << ")"
                  << " state " << prev << " → " << target
                  << (tripped ? " [STOP]" : "") << "\n";

        // Throttle rebalance
        {
            std::lock_guard lk(sh_mtx_);
            if (now - st->last_rebal_ms < Params::SH_REBAL_MS) continue;
            st->last_rebal_ms = now;
        }

        // Execute: target=+1 → LONG market_etf + SHORT safe_etf;
        // target=-1 → SHORT market_etf + LONG safe_etf;
        // target=0  → flatten both. SH_SIZE per leg.
        auto& c = *conns_.at(exch);
        struct Leg { std::string inst; int sign; };  // +1 long, -1 short relative to target
        std::vector<Leg> legs = {{m_inst, +1}, {s_inst, -1}};
        for (auto& L : legs) {
            int cur = c.position_of(L.inst);
            int desired = target * L.sign * Params::SH_SIZE;
            // cap
            if (desired > Params::SH_POS_CAP) desired = Params::SH_POS_CAP;
            if (desired < -Params::SH_POS_CAP) desired = -Params::SH_POS_CAP;
            int diff = desired - cur;
            if (diff == 0) continue;
            auto bk = state_.get(exch, L.inst);
            if (!bk) continue;
            if (diff > 0) {
                if (!bk->best_ask() || !bk->best_ask_qty()) continue;
                int qty = std::min({diff, *bk->best_ask_qty(), 50});
                if (qty <= 0 || !can_buy(c, L.inst, qty, *bk->best_ask())) continue;
                c.place_ioc(L.inst, "bid", *bk->best_ask(), qty);
            } else {
                int need = -diff;
                if (!bk->best_bid() || !bk->best_bid_qty()) continue;
                int qty = std::min({need, *bk->best_bid_qty(), 50});
                if (qty <= 0 || !can_sell(c, L.inst, qty)) continue;
                c.place_ioc(L.inst, "ask", *bk->best_bid(), qty);
            }
        }
    }
}

// ─── Sector pair z-arb (per-venue sector intra-correlation) ──────────
// For each sector with N≥2 tickers listed on this venue, compute
// for each ticker i:
//   resid_i(t) = price_i(t) − mean(price_j(t), j ≠ i in same sector on venue)
// Track rolling (μ, σ) of resid_i over SECPAIR_HISTORY samples.
// z = (resid − μ) / σ.
//   z ≥ +entry  → stock_i rich vs peers  → SHORT (N−1) of i + LONG 1 of each peer
//   z ≤ −entry  → stock_i cheap vs peers → LONG  (N−1) of i + SHORT 1 of each peer
//   |z| ≤ exit  → flatten this signal
//   |z| ≥ stop  → stop-loss flatten + cooldown
// All legs use mid-price-derived best bid/ask on this venue (full local).
void Strategy::sector_pair_zarb(const std::string& exch) {
    if (!state_.warmup_done()) return;
    int64_t now = now_ms();

    // Helper: pearson correlation of two equal-length return series
    auto pearson_returns = [](const std::vector<double>& a,
                              const std::vector<double>& b) -> double {
        size_t n = std::min(a.size(), b.size());
        if (n < 2) return 0.0;
        // Use last n-1 returns (need n >=2 mids to form n-1 returns)
        std::vector<double> ra(n-1), rb(n-1);
        for (size_t k = 1; k < n; ++k) {
            // log-ish return (relative diff); guard against zero
            double ap = a[a.size()-n+k-1], an = a[a.size()-n+k];
            double bp = b[b.size()-n+k-1], bn = b[b.size()-n+k];
            ra[k-1] = (ap > 1e-9) ? (an - ap) / ap : 0.0;
            rb[k-1] = (bp > 1e-9) ? (bn - bp) / bp : 0.0;
        }
        size_t m = ra.size();
        if (m < 2) return 0.0;
        double sa = 0, sb = 0;
        for (size_t k = 0; k < m; ++k) { sa += ra[k]; sb += rb[k]; }
        double ma = sa/m, mb = sb/m;
        double num = 0, va = 0, vb = 0;
        for (size_t k = 0; k < m; ++k) {
            double da = ra[k]-ma, db = rb[k]-mb;
            num += da*db; va += da*da; vb += db*db;
        }
        if (va < 1e-18 || vb < 1e-18) return 0.0;
        return num / std::sqrt(va*vb);
    };

    for (const auto& sec : SECTORS_DEF) {
        // Discover which sector tickers are listed on this venue.
        std::vector<std::string> peers;
        std::vector<double>      mids;
        std::vector<std::string> insts;
        std::vector<std::string> keys;
        for (auto& tkr : sec.tickers) {
            std::string inst = exch + "-" + tkr;
            auto bk = state_.get(exch, inst);
            if (!bk) continue;
            auto m = bk->mid();
            if (!m) continue;
            peers.push_back(tkr);
            mids.push_back(*m);
            insts.push_back(inst);
            keys.push_back(exch + "|" + sec.name + "|" + tkr);
        }
        const int N = (int)peers.size();
        if (N < 2) continue;  // need at least 2 to form residual

        // Push mid into each peer's rolling mid history (under one lock).
        {
            std::lock_guard lk(secpair_mtx_);
            for (int i = 0; i < N; ++i) {
                auto& s = secpair_state_[keys[i]];
                s.mid_hist.push_back(mids[i]);
                if ((int)s.mid_hist.size() > Params::SECPAIR_MID_HIST)
                    s.mid_hist.erase(s.mid_hist.begin());
            }
        }

        // Snapshot mid histories for all N tickers (locked copy).
        std::vector<std::vector<double>> mh(N);
        {
            std::lock_guard lk(secpair_mtx_);
            for (int i = 0; i < N; ++i)
                mh[i] = secpair_state_[keys[i]].mid_hist;
        }

        for (int i = 0; i < N; ++i) {
            // Compute correlation-weighted peer mean using rolling returns.
            // Fall back to equal weights until enough history accumulates.
            double peer_mean;
            bool have_corr = ((int)mh[i].size() >= Params::SECPAIR_CORR_MIN_SAMPLES);
            std::vector<double> w(N, 0.0);
            if (have_corr) {
                double wsum = 0.0;
                for (int j = 0; j < N; ++j) {
                    if (j == i) continue;
                    if ((int)mh[j].size() < Params::SECPAIR_CORR_MIN_SAMPLES) {
                        have_corr = false; break;
                    }
                    double c = pearson_returns(mh[i], mh[j]);
                    double wj = std::max(0.0, c);  // ignore negative / zero corr
                    w[j] = wj;
                    wsum += wj;
                }
                if (have_corr && wsum > 1e-9) {
                    double s = 0.0;
                    for (int j = 0; j < N; ++j) if (j != i) s += (w[j]/wsum) * mids[j];
                    peer_mean = s;
                } else {
                    have_corr = false;
                }
            }
            if (!have_corr) {
                double sum_others = 0.0;
                for (int j = 0; j < N; ++j) if (j != i) sum_others += mids[j];
                peer_mean = sum_others / (N - 1);
            }

            double resid = mids[i] - peer_mean;

            const std::string& key = keys[i];
            SecPairState* st;
            {
                std::lock_guard lk(secpair_mtx_);
                st = &secpair_state_[key];
                st->resid_hist.push_back(resid);
                if ((int)st->resid_hist.size() > Params::SECPAIR_HISTORY)
                    st->resid_hist.erase(st->resid_hist.begin());
            }
            if ((int)st->resid_hist.size() < Params::SECPAIR_MIN_SAMPLES) continue;

            std::vector<double> snap;
            {
                std::lock_guard lk(secpair_mtx_);
                snap = st->resid_hist;
            }
            double sum = 0, sumsq = 0;
            for (double v : snap) { sum += v; sumsq += v*v; }
            double n = snap.size();
            double mean = sum / n;
            double var = std::max(1e-12, sumsq/n - mean*mean);
            double sd = std::sqrt(var);
            if (sd < 1e-6) continue;
            double z = (resid - mean) / sd;

            const double entry = Params::SECPAIR_ENTRY_Z100 / 100.0;
            const double exit_ = Params::SECPAIR_EXIT_Z100  / 100.0;
            const double stop  = Params::SECPAIR_STOP_Z100  / 100.0;

            int prev = st->target_state;
            int target = prev;
            bool tripped = false;
            if (std::abs(z) >= stop) {
                if (prev != 0) { target = 0; tripped = true; }
                st->stop_until_ms = now + Params::SECPAIR_STOP_COOLDOWN_MS;
            }
            bool in_cooldown = now < st->stop_until_ms;
            if (!tripped) {
                if (z >= entry && !in_cooldown)        target = -1;
                else if (z <= -entry && !in_cooldown)  target = +1;
                else if (std::abs(z) <= exit_)         target = 0;
            }

            if (target == prev) continue;
            st->target_state = target;
            std::cout << "[" << exch << " sec-" << sec.name << "/" << peers[i] << "] z=" << z
                      << " (μ=" << mean << ", σ=" << sd << ", N=" << N
                      << ", corr=" << (have_corr ? "rolling" : "eq-w") << ")"
                      << " state " << prev << " → " << target
                      << (tripped ? " [STOP]" : "") << "\n";

            // Throttle rebalance per-key
            {
                std::lock_guard lk(secpair_mtx_);
                if (now - st->last_rebal_ms < Params::SECPAIR_REBAL_MS) continue;
                st->last_rebal_ms = now;
            }

            // Compute desired positions for stock_i and each peer.
            // weight: stock_i = +(N-1), peer = -1; scaled by target.
            // Cap each at SECPAIR_POS_CAP.
            auto& c = *conns_.at(exch);
            struct Move { std::string inst; int desired; };
            std::vector<Move> moves;
            int desired_i = target * (N - 1);
            if (desired_i >  Params::SECPAIR_POS_CAP) desired_i =  Params::SECPAIR_POS_CAP;
            if (desired_i < -Params::SECPAIR_POS_CAP) desired_i = -Params::SECPAIR_POS_CAP;
            moves.push_back({insts[i], desired_i});
            for (int j = 0; j < N; ++j) {
                if (j == i) continue;
                int dp = target * (-1);
                if (dp >  Params::SECPAIR_POS_CAP) dp =  Params::SECPAIR_POS_CAP;
                if (dp < -Params::SECPAIR_POS_CAP) dp = -Params::SECPAIR_POS_CAP;
                moves.push_back({insts[j], dp});
            }

            for (auto& mv : moves) {
                int cur = c.position_of(mv.inst);
                int diff = mv.desired - cur;
                if (diff == 0) continue;
                auto bk = state_.get(exch, mv.inst);
                if (!bk) continue;
                if (diff > 0) {
                    if (!bk->best_ask() || !bk->best_ask_qty()) continue;
                    int qty = std::min({diff, *bk->best_ask_qty(), 50});
                    if (qty <= 0 || !can_buy(c, mv.inst, qty, *bk->best_ask())) continue;
                    c.place_ioc(mv.inst, "bid", *bk->best_ask(), qty);
                } else {
                    int need = -diff;
                    if (!bk->best_bid() || !bk->best_bid_qty()) continue;
                    int qty = std::min({need, *bk->best_bid_qty(), 50});
                    if (qty <= 0 || !can_sell(c, mv.inst, qty)) continue;
                    c.place_ioc(mv.inst, "ask", *bk->best_bid(), qty);
                }
            }
        }
    }
}

// ─── Cross-venue same-ticker hedged arbitrage (global scan) ─────────
// Single pass per tick: enumerate every ticker observed on any venue,
// find venue with cheapest ask and venue with richest bid for that
// ticker, and if they differ with bid - ask >= XVENUE_EDGE_CENTS, fire
// a paired IOC (BUY on cheap venue, SELL on rich venue). No fees → any
// positive edge is pure profit. Cooldown is per-ticker.
void Strategy::xvenue_arb() {
    if (!state_.warmup_done()) return;

    // Gather union of all tickers across all connected venues.
    std::unordered_set<std::string> tickers;
    for (const auto& [vname, _] : conns_) {
        for (const auto& inst : state_.instruments_on(vname)) {
            auto dash = inst.find('-');
            if (dash != std::string::npos) tickers.insert(inst.substr(dash+1));
        }
    }

    int64_t t = now_ms();

    for (const auto& ticker : tickers) {
        // Per-ticker cooldown
        {
            std::lock_guard lk(xvenue_mtx_);
            auto it = last_xvenue_.find(ticker);
            if (it != last_xvenue_.end()
                && t - it->second < Params::XVENUE_COOLDOWN_MS) continue;
        }

        // Global scan: find min-ask venue and max-bid venue for this ticker.
        std::string best_ask_v;  int best_ask_px = INT_MAX; int best_ask_qty = 0;
        std::string best_bid_v;  int best_bid_px = INT_MIN; int best_bid_qty = 0;

        for (const auto& [vname, _] : conns_) {
            std::string inst = vname + "-" + ticker;
            auto book = state_.get(vname, inst);
            if (!book) continue;
            auto bid = book->best_bid(); auto ask = book->best_ask();
            if (ask && *ask < best_ask_px) {
                best_ask_px  = *ask;
                best_ask_v   = vname;
                best_ask_qty = book->best_ask_qty().value_or(0);
            }
            if (bid && *bid > best_bid_px) {
                best_bid_px  = *bid;
                best_bid_v   = vname;
                best_bid_qty = book->best_bid_qty().value_or(0);
            }
        }

        if (best_ask_v.empty() || best_bid_v.empty()) continue;
        if (best_ask_v == best_bid_v) continue;
        int edge = best_bid_px - best_ask_px;
        if (edge < Params::XVENUE_EDGE_CENTS) continue;

        std::string buy_inst  = best_ask_v + "-" + ticker;
        std::string sell_inst = best_bid_v + "-" + ticker;

        auto& c_buy  = *conns_.at(best_ask_v);
        auto& c_sell = *conns_.at(best_bid_v);

        int pos_buy  = c_buy.position_of(buy_inst);
        int pos_sell = c_sell.position_of(sell_inst);

        int headroom_buy  = Params::XVENUE_POS_CAP - pos_buy;
        int headroom_sell = Params::XVENUE_POS_CAP + pos_sell;
        int qty = std::min({Params::XVENUE_SIZE,
                            best_ask_qty, best_bid_qty,
                            headroom_buy, headroom_sell});
        if (qty <= 0) continue;

        if (!can_buy(c_buy, buy_inst, qty, best_ask_px)) continue;
        if (!can_sell(c_sell, sell_inst, qty)) continue;

        // Reserve cooldown only after passing all checks.
        {
            std::lock_guard lk(xvenue_mtx_);
            last_xvenue_[ticker] = t;
        }

        c_buy.place_ioc(buy_inst,   "bid", best_ask_px, qty);
        c_sell.place_ioc(sell_inst, "ask", best_bid_px, qty);
        std::cout << "[xvenue " << ticker << "] buy " << best_ask_v
                  << "@" << best_ask_px << " sell " << best_bid_v
                  << "@" << best_bid_px << " edge=" << edge
                  << "c qty=" << qty << "\n";
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
            state_.reset_warmup();
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
                    state_.note_venue_alive(name);
                    strategy_->on_market_data(name);
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