/*
 * AlgoTrade 2026 — Competition Bot (C++20)
 *
 * Two strategies, both verified on training data:
 *   1. Cointegrated basket stat-arb on ZSE (sector A)
 *   2. Cross-venue lead-lag arbitrage, gated by physical RTT matrix
 *
 * Build:
 *   g++ -std=c++20 -O2 -o bot bot.cpp -lpthread
 *
 * Run:
 *   ./bot                                  # all 10 exchanges, default LOC=ZSE
 *   LOC=NYSE ./bot                         # override our co-location
 *   EXCHANGES="NYSE,NASDAQ,ZSE" ./bot      # subset of venues
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

    // Per-instrument cooldown after sending an order (skipped by coint)
    static constexpr int  INST_COOLDOWN_MS = 250;

    // ─── Cointegrated basket stat-arb (ZSE only) ────────────────────
    static constexpr int  COINT_HISTORY     = 300;
    static constexpr int  COINT_MIN_SAMPLES = 60;
    static constexpr int  COINT_BASKET_K    = 7500;
    static constexpr int  COINT_ENTRY_Z100  = 250;   // |z| >= 2.50
    static constexpr int  COINT_EXIT_Z100   = 30;    // |z| <= 0.30
    static constexpr int  COINT_REBAL_MS    = 250;

    // ─── Lead-lag cross-venue arbitrage ─────────────────────────────
    static constexpr int  LEAD_LAG_EDGE_CENTS        = 10;
    static constexpr int  LEAD_LAG_SIZE              = 20;
    static constexpr int  LEAD_LAG_COOLDOWN_MS       = 150;
    static constexpr int  LEAD_LAG_POS_CAP           = 60;
    static constexpr int  LEAD_LAG_EXIT_CENTS        = 2;
    static constexpr int  LEAD_LAG_LATENCY_SAFETY_MS = 50;
    // Default co-location. Override at runtime: LOC=NYSE ./bot (NYSE/ZSE/HKEX).
    static constexpr const char* OUR_LOCATION = "ZSE";
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
//  Cointegrated baskets (verified on ZSE training data)
//
//  Sector A (Engle-Granger): single vector, |z|>=2.5 entry.
//  Sector B (Johansen): 2 cointegrating vectors at 95% conf
//    (trace r<=0=125.0/95.8 ✓, r<=1=72.8/69.8 ✓, r<=2 fails)
//    Both spreads ADF p<0.01 → stationary.
//
//  Per-basket K and entry override (0 = use Params defaults).
//  B has smaller β-coefficients → set tighter entry to demand more edge.
// ════════════════════════════════════════════════════════════════════
struct CointBasket {
    std::string name;
    std::string venue;
    std::vector<std::pair<std::string,double>> legs;  // (ticker, beta)
    int k_override = 0;          // 0 → Params::COINT_BASKET_K
    int entry_z100_override = 0; // 0 → Params::COINT_ENTRY_Z100
};

static const std::vector<CointBasket> COINT_BASKETS = {
    // Sector A — Engle-Granger single-vector
    {"A", "ZSE", {
        {"FSR",  +0.0074},
        {"JZRO", -0.0058},
        {"KTST", +0.0008},
        {"NGUP", -0.0008},
        {"OIT",  +0.0007},
        {"XFR",  +0.0186},
    }, 0, 0},
    // Sector B vector #1 (Johansen) — ADF p=0.0070
    {"B1", "ZSE", {
        {"DDJH", +0.0004},
        {"DLKV", +0.0008},
        {"HT",   +0.0051},
        {"INA",  +0.0002},
        {"JNAF", -0.0003},
        {"KOTD", -0.0013},
    }, 7500, 280},
    // Sector B vector #2 (Johansen) — ADF p=0.0004
    {"B2", "ZSE", {
        {"DDJH", +0.0022},
        {"DLKV", +0.0003},
        {"HT",   -0.0016},
        {"INA",  +0.0007},
        {"JNAF", -0.0006},
        {"KOTD", -0.0027},
    }, 7500, 280},
};

// ════════════════════════════════════════════════════════════════════
//  Cross-venue lead-lag arbitrage table
//  58 pairs from prior-round orderbook history with corr ≥ 0.95.
//  Strategy: when lead's mid moves away from lag's quote by ≥ EDGE,
//  hit the lag book before its MM updates.
// ════════════════════════════════════════════════════════════════════
struct LeadLagPair {
    std::string lead;
    std::string lag;
    std::string ticker;
    int lag_ms;
};

// Inter-exchange round-trip latency (ms) — verbatim from guide §7.
// Filter: half_RTT(us↔lead) + half_RTT(us↔lag) + safety < lag_ms.
static const std::unordered_map<std::string, std::unordered_map<std::string,int>> RTT_MS = {
    {"NYSE",     {{"NYSE",0},{"NASDAQ",1},{"SSE",165},{"JPX",152},{"Euronext",84},{"LSE",80},{"HKEX",180},{"NSE",174},{"TMX",11},{"ZSE",96}}},
    {"NASDAQ",   {{"NYSE",1},{"NASDAQ",0},{"SSE",165},{"JPX",152},{"Euronext",84},{"LSE",80},{"HKEX",180},{"NSE",174},{"TMX",11},{"ZSE",96}}},
    {"SSE",      {{"NYSE",165},{"NASDAQ",165},{"SSE",0},{"JPX",18},{"Euronext",160},{"LSE",156},{"HKEX",19},{"NSE",54},{"TMX",159},{"ZSE",145}}},
    {"JPX",      {{"NYSE",152},{"NASDAQ",152},{"SSE",18},{"JPX",0},{"Euronext",145},{"LSE",141},{"HKEX",37},{"NSE",53},{"TMX",145},{"ZSE",140}}},
    {"Euronext", {{"NYSE",84},{"NASDAQ",84},{"SSE",160},{"JPX",145},{"Euronext",0},{"LSE",6},{"HKEX",130},{"NSE",130},{"TMX",86},{"ZSE",22}}},
    {"LSE",      {{"NYSE",80},{"NASDAQ",80},{"SSE",156},{"JPX",141},{"Euronext",6},{"LSE",0},{"HKEX",135},{"NSE",134},{"TMX",82},{"ZSE",24}}},
    {"HKEX",     {{"NYSE",180},{"NASDAQ",180},{"SSE",19},{"JPX",37},{"Euronext",130},{"LSE",135},{"HKEX",0},{"NSE",53},{"TMX",174},{"ZSE",150}}},
    {"NSE",      {{"NYSE",174},{"NASDAQ",174},{"SSE",54},{"JPX",53},{"Euronext",130},{"LSE",134},{"HKEX",53},{"NSE",0},{"TMX",174},{"ZSE",95}}},
    {"TMX",      {{"NYSE",11},{"NASDAQ",11},{"SSE",159},{"JPX",145},{"Euronext",86},{"LSE",82},{"HKEX",174},{"NSE",174},{"TMX",0},{"ZSE",98}}},
    {"ZSE",      {{"NYSE",96},{"NASDAQ",96},{"SSE",145},{"JPX",140},{"Euronext",22},{"LSE",24},{"HKEX",150},{"NSE",95},{"TMX",98},{"ZSE",0}}},
};

inline int rtt_ms(const std::string& from, const std::string& to) {
    auto it = RTT_MS.find(from); if (it==RTT_MS.end()) return 200;
    auto j = it->second.find(to); if (j==it->second.end()) return 200;
    return j->second;
}

inline bool latency_ok(const std::string& our_loc,
                       const std::string& lead,
                       const std::string& lag,
                       int lag_ms, int safety_ms)
{
    int t_see  = rtt_ms(our_loc, lead) / 2;
    int t_send = rtt_ms(our_loc, lag)  / 2;
    return (t_see + t_send + safety_ms) < lag_ms;
}

inline const std::string& our_location() {
    static std::string loc = []{
        const char* e = std::getenv("LOC");
        return std::string(e && *e ? e : Params::OUR_LOCATION);
    }();
    return loc;
}

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
        std::lock_guard lk(coint_mtx_);
        coint_state_.clear();
    }

private:
    bool can_buy(ExchangeConnection& c, const std::string& inst, int qty, int px);
    bool can_sell(ExchangeConnection& c, const std::string& inst, int qty);

    void coint_basket_trade(const std::string& exch);
    void lead_lag_arb(const std::string& exch);

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

    struct CointState {
        std::vector<double> spread_hist;
        int64_t last_rebal_ms = 0;
        int target_state = 0;   // -1 short / +1 long / 0 flat
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

void Strategy::on_market_data(const std::string& exch) {
    {
        std::lock_guard lk(last_run_mtx_);
        auto t = now_ms();
        auto& last = last_run_[exch];
        if (t - last < Params::STRAT_MIN_INTERVAL_MS) return;
        last = t;
    }
    try {
        coint_basket_trade(exch);
        lead_lag_arb(exch);
    } catch (const std::exception& e) {
        std::cerr << "[" << exch << "] strategy error: " << e.what() << "\n";
    } catch (...) {
        std::cerr << "[" << exch << "] strategy unknown error\n";
    }
}

// ─── Cointegrated basket stat-arb ────────────────────────────────────
void Strategy::coint_basket_trade(const std::string& exch) {
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

        const int entry_z100 = cb.entry_z100_override > 0
                                 ? cb.entry_z100_override
                                 : Params::COINT_ENTRY_Z100;
        const double entry = entry_z100 / 100.0;
        const double exit_ = Params::COINT_EXIT_Z100 / 100.0;

        int prev = cs->target_state;
        int target = prev;
        if (z >= entry)                target = -1;
        else if (z <= -entry)          target = +1;
        else if (std::abs(z) <= exit_) target = 0;

        if (target != prev) {
            cs->target_state = target;
            any_changed = true;
            std::cout << "[ZSE coint-" << cb.name << "] z=" << z
                      << " (μ=" << mean << ", σ=" << sd << ")"
                      << " state " << prev << " → " << target << "\n";
        }
    }

    if (!any_changed) return;
    {
        std::lock_guard lk(coint_mtx_);
        auto& cs0 = coint_state_["__throttle_" + exch];
        if (now - cs0.last_rebal_ms < Params::COINT_REBAL_MS) return;
        cs0.last_rebal_ms = now;
    }

    // Aggregate per-leg target positions across all active baskets
    auto& c = *conns_.at(exch);
    std::unordered_map<std::string, int> target_by_inst;
    for (const auto* cbp : active) {
        const auto& cb = *cbp;
        int state;
        {
            std::lock_guard lk(coint_mtx_);
            state = coint_state_[cb.name].target_state;
        }
        for (auto& [tkr, beta] : cb.legs) {
            std::string inst = cb.venue + "-" + tkr;
            int K = cb.k_override > 0 ? cb.k_override : Params::COINT_BASKET_K;
            int leg_units = (int)std::lround(K * beta);
            target_by_inst[inst] += state * leg_units;
        }
    }

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

// ─── Cross-venue lead-lag arbitrage ──────────────────────────────────
// Two-phase per pair:
//   ENTRY: open when |lead_mid - lag_quote| >= EDGE and pos within ±POS_CAP
//   EXIT:  flatten once gap closed (≤ EXIT_CENTS)
void Strategy::lead_lag_arb(const std::string& exch) {
    if (!state_.warmup_done()) return;
    auto& c = *conns_.at(exch);

    for (const auto& p : LEAD_LAG_TABLE) {
        if (p.lag != exch) continue;

        // Latency feasibility: skip pairs we cannot physically beat.
        if (!latency_ok(our_location(), p.lead, p.lag,
                        p.lag_ms, Params::LEAD_LAG_LATENCY_SAFETY_MS))
            continue;

        std::string lead_inst = p.lead + "-" + p.ticker;
        std::string lag_inst  = p.lag  + "-" + p.ticker;

        auto lead_book = state_.get(p.lead, lead_inst); if (!lead_book) continue;
        auto lag_book  = state_.get(p.lag,  lag_inst);  if (!lag_book)  continue;

        auto lead_mid = lead_book->mid(); if (!lead_mid) continue;
        auto lag_bid = lag_book->best_bid();
        auto lag_ask = lag_book->best_ask();
        int  pos = c.position_of(lag_inst);

        // EXIT
        if (pos > 0 && lag_bid) {
            if (!lag_ask || (*lead_mid - *lag_ask) < Params::LEAD_LAG_EXIT_CENTS) {
                int qty = std::min({pos, Params::LEAD_LAG_SIZE,
                                    lag_book->best_bid_qty().value_or(0)});
                if (qty > 0 && inst_cooldown_ok(lag_inst))
                    c.place_ioc(lag_inst, "ask", *lag_bid, qty);
                continue;
            }
        }
        if (pos < 0 && lag_ask) {
            if (!lag_bid || (*lag_bid - *lead_mid) < Params::LEAD_LAG_EXIT_CENTS) {
                int qty = std::min({-pos, Params::LEAD_LAG_SIZE,
                                    lag_book->best_ask_qty().value_or(0)});
                if (qty > 0 && inst_cooldown_ok(lag_inst))
                    c.place_ioc(lag_inst, "bid", *lag_ask, qty);
                continue;
            }
        }

        // ENTRY: lead above lag's ask → buy lag (it's too cheap)
        if (lag_ask && (*lead_mid - *lag_ask) >= Params::LEAD_LAG_EDGE_CENTS
            && pos < Params::LEAD_LAG_POS_CAP) {
            int avail = lag_book->best_ask_qty().value_or(0);
            int headroom = Params::LEAD_LAG_POS_CAP - pos;
            int qty = std::min({Params::LEAD_LAG_SIZE, avail, headroom});
            if (qty > 0 && can_buy(c, lag_inst, qty, *lag_ask)
                && inst_cooldown_ok(lag_inst)) {
                c.place_ioc(lag_inst, "bid", *lag_ask, qty);
            }
        }
        // ENTRY: lead below lag's bid → sell lag (it's too expensive)
        else if (lag_bid && (*lag_bid - *lead_mid) >= Params::LEAD_LAG_EDGE_CENTS
            && pos > -Params::LEAD_LAG_POS_CAP) {
            int avail = lag_book->best_bid_qty().value_or(0);
            int headroom = Params::LEAD_LAG_POS_CAP + pos;
            int qty = std::min({Params::LEAD_LAG_SIZE, avail, headroom});
            if (qty > 0 && can_sell(c, lag_inst, qty)
                && inst_cooldown_ok(lag_inst)) {
                c.place_ioc(lag_inst, "ask", *lag_bid, qty);
            }
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
    std::cout << "[location] OUR_LOCATION=" << our_location()
              << "  safety=" << Params::LEAD_LAG_LATENCY_SAFETY_MS << "ms\n";
    {
        int kept=0, dropped=0;
        std::map<std::string,int> per_lag;
        for (auto& p : LEAD_LAG_TABLE) {
            bool ok = latency_ok(our_location(), p.lead, p.lag,
                                 p.lag_ms, Params::LEAD_LAG_LATENCY_SAFETY_MS);
            if (ok) { kept++; per_lag[p.lag]++; } else dropped++;
        }
        std::cout << "[lead-lag] " << kept << " pairs tradeable, "
                  << dropped << " filtered by latency\n";
        for (auto& [v,n] : per_lag)
            std::cout << "    " << v << ": " << n << " pairs\n";
    }

    Bot bot;
    bot.run(exchanges);
    return 0;
}