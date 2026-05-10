/*
 * SCREW BOT v2 — Volume-Weighted Mean Reversion (C++20)
 * Strategy: Track VWMA20 per asset. When VWMA20 > current_price,
 * sell aggressively and short. Maximize capital deployment.
 * Build: g++ -std=c++20 -O3 -march=native -o screw_v2 screw_v2.cpp -lpthread
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
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
#include <vector>
#include <deque>

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
//  AGGRESSIVE PARAMS
// ════════════════════════════════════════════════════════════════════
struct Params {
    static constexpr int MAX_LONG    = 200;
    static constexpr int MAX_SHORT   = -200;
    static constexpr long CASH_FLOOR = -10'000'000;  // Allow -$100k in debt
    
    // EXTREME SPEED
    static constexpr int STRAT_INTERVAL_MS  = 3;    // 3ms eval
    static constexpr int INST_COOLDOWN_MS   = 5;    // 5ms cooldown
    static constexpr int ORDER_TTL_MS       = 500;  // 0.5s TTL
    
    // MINIMAL WARMUP
    static constexpr int WARMUP_MIN_MS     = 500;   // 0.5s only
    static constexpr int WARMUP_MIN_VENUES = 3;     // 3 venues min
    
    // VWMA SETTINGS
    static constexpr int VWMA_PERIOD = 20;           // 20-period
    static constexpr int EDGE_PERCENT = 5;           // 5% above SMA = signal
    
    // AGGRESSIVE SIZING
    static constexpr int BASE_SIZE = 150;            // 150 per order
    static constexpr int AGGRESSIVE_MULTIPLIER = 2;  // 2x when short signal
};

static const std::vector<std::pair<std::string,std::string>> EXCHANGES = {
    {"NYSE","10.0.201.2"},   {"NASDAQ","10.0.202.2"}, {"SSE","10.0.203.2"},
    {"JPX","10.0.204.2"},    {"Euronext","10.0.205.2"}, {"LSE","10.0.206.2"},
    {"HKEX","10.0.207.2"},   {"NSE","10.0.208.2"},    {"TMX","10.0.209.2"},
    {"ZSE","10.0.210.2"},
};

static const std::vector<std::string> TARGET_ETFS = {"ETFA", "ETFB", "ETFA3", "ETFB3"};

// ════════════════════════════════════════════════════════════════════
//  Price with Volume (for VWMA calculation)
// ════════════════════════════════════════════════════════════════════
struct PricePoint {
    double price;    // in cents
    int64_t volume;
    int64_t ts_ms;
};

// ════════════════════════════════════════════════════════════════════
//  Book & Market State
// ════════════════════════════════════════════════════════════════════
struct Book {
    std::map<int,int> bids, asks;
    int64_t ts_ms = 0;
    int64_t last_volume = 0;  // cumulative volume
    
    std::optional<int> best_bid() const {
        return bids.empty() ? std::nullopt : std::optional<int>(bids.rbegin()->first);
    }
    std::optional<int> best_ask() const {
        return asks.empty() ? std::nullopt : std::optional<int>(asks.begin()->first);
    }
    std::optional<double> mid() const {
        auto b = best_bid(), a = best_ask();
        return (b && a) ? std::optional<double>((*b + *a) / 2.0) : std::nullopt;
    }
};

class MarketState {
    std::map<std::string, std::map<std::string, Book>> books_;
    std::map<std::string, std::map<std::string, std::deque<PricePoint>>> price_history_;
    mutable std::mutex mtx_;
    std::set<std::string> active_venues_;
    int64_t warmup_start_ms_ = 0;
    
public:
    void update_book(const std::string& exch, const std::string& inst,
                     const json& bids, const json& asks, int64_t t) {
        std::lock_guard lk(mtx_);
        active_venues_.insert(exch);
        if (warmup_start_ms_ == 0) warmup_start_ms_ = t;
        
        auto& b = books_[exch][inst];
        b.ts_ms = t;
        b.bids.clear(); b.asks.clear();
        
        try {
            if (bids.is_object())
                for (auto& [p, q] : bids.items())
                    if (q.is_number()) b.bids[std::stoi(p)] = q.get<int>();
            if (asks.is_object())
                for (auto& [p, q] : asks.items())
                    if (q.is_number()) b.asks[std::stoi(p)] = q.get<int>();
        } catch (...) {}
        
        // Extract mid and volume, add to history
        if (auto mid = b.mid()) {
            int64_t vol = 0;
            for (auto& [p, q] : b.asks) vol += q;
            
            auto& hist = price_history_[exch][inst];
            hist.push_back({*mid, vol, t});
            
            // Keep only last 50 periods for VWMA20 + buffer
            if (hist.size() > 50) hist.pop_front();
        }
    }
    
    std::optional<Book> get(const std::string& exch, const std::string& inst) const {
        std::lock_guard lk(mtx_);
        auto e = books_.find(exch); if (e == books_.end()) return std::nullopt;
        auto i = e->second.find(inst); if (i == e->second.end()) return std::nullopt;
        return i->second;
    }
    
    // Calculate Volume-Weighted Moving Average (VWMA)
    // VWMA = Σ(price_i * volume_i) / Σ(volume_i) for last N periods
    std::optional<double> get_vwma(const std::string& exch, const std::string& inst, int periods = 20) const {
        std::lock_guard lk(mtx_);
        auto eh = price_history_.find(exch);
        if (eh == price_history_.end()) return std::nullopt;
        auto ih = eh->second.find(inst);
        if (ih == eh->second.end()) return std::nullopt;
        
        const auto& hist = ih->second;
        if (hist.size() < periods) return std::nullopt;
        
        double sum_pv = 0, sum_v = 0;
        size_t count = std::min(hist.size(), (size_t)periods);
        
        for (size_t i = hist.size() - count; i < hist.size(); ++i) {
            sum_pv += hist[i].price * hist[i].volume;
            sum_v += hist[i].volume;
        }
        
        return sum_v > 0 ? std::optional<double>(sum_pv / sum_v) : std::nullopt;
    }
    
    bool is_warmed_up(int64_t now_ms) const {
        std::lock_guard lk(mtx_);
        return (now_ms - warmup_start_ms_ > Params::WARMUP_MIN_MS) &&
               (active_venues_.size() >= Params::WARMUP_MIN_VENUES);
    }
};

// ════════════════════════════════════════════════════════════════════
//  Exchange Connection (sends orders)
// ════════════════════════════════════════════════════════════════════
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
            auto results = r.resolve(host_, "9001");
            net::connect(ws_->next_layer(), results);
            ws_->handshake(host_+":9001", "/trade");
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
            {"expiry", 0}, {"side", side},
            {"quantity", qty}, {"order_type","ioc"},
        });
        return ok;
    }

    int position_of(const std::string& inst) const {
        std::lock_guard lk(pos_mtx_);
        auto it = positions_.find(inst);
        return it == positions_.end() ? 0 : it->second;
    }

    long get_cash() const {
        std::lock_guard lk(pos_mtx_);
        return cash_cents_;
    }

    void apply_response(const json& m) {
        if (!m.value("success", false)) return;
        if (!m.contains("data")) return;
        const auto& d = m["data"];
        std::lock_guard lk(pos_mtx_);
        if (d.contains("immediate_balance_change") && !d["immediate_balance_change"].is_null())
            cash_cents_ += d["immediate_balance_change"].get<long>();
    }

private:
    std::string name_, host_;
    std::unique_ptr<net::io_context> ioc_;
    std::unique_ptr<websocket::stream<tcp::socket>> ws_;
    std::atomic<bool> connected_{false};
    int rid_ = 0;
    std::mutex write_mtx_;
    
    mutable std::mutex pos_mtx_;
    std::unordered_map<std::string,int> positions_;
    long cash_cents_ = 10'000'000;
};

// ════════════════════════════════════════════════════════════════════
//  Position & Cash Tracker
// ════════════════════════════════════════════════════════════════════
class Positions {
    std::unordered_map<std::string, std::unique_ptr<ExchangeConnection>>& conns_;
    mutable std::mutex mtx_;
    
public:
    Positions(std::unordered_map<std::string, std::unique_ptr<ExchangeConnection>>& c)
        : conns_(c) {}
    
    int get_position(const std::string& ticker) const {
        // Sum across all exchanges
        int total = 0;
        for (auto& [_, conn] : conns_) {
            total += conn->position_of(ticker);
        }
        return total;
    }
    
    long get_total_cash() const {
        long total = 0;
        for (auto& [_, conn] : conns_) {
            total += conn->get_cash();
        }
        return total;
    }
    
    void print_status() const {
        std::cout << "\n=== PORTFOLIO ===\n";
        std::cout << "Total Cash: $" << (get_total_cash() / 100.0) << "\n";
        for (auto& [ticker, _] : conns_) {
            int pos = get_position(ticker);
            if (pos != 0) {
                std::cout << ticker << ": " << pos << "\n";
            }
        }
    }
};

// ════════════════════════════════════════════════════════════════════
//  WebSocket Listener
// ════════════════════════════════════════════════════════════════════
class ExchangeListener {
    std::string exchange_;
    MarketState& mkt_;
    std::atomic<bool> running_{false};
    std::thread listener_thread_;
    
    void run_listener() {
        try {
            auto host_it = std::find_if(EXCHANGES.begin(), EXCHANGES.end(),
                [this](const auto& p) { return p.first == exchange_; });
            if (host_it == EXCHANGES.end()) return;
            
            auto ioc = std::make_unique<net::io_context>();
            auto ws = std::make_unique<websocket::stream<tcp::socket>>(*ioc);
            
            tcp::resolver r(*ioc);
            auto results = r.resolve(host_it->second, "9001");
            net::connect(ws->next_layer(), results);
            
            ws->handshake(host_it->second + ":9001", "/trade");
            
            beast::flat_buffer welcome_buf;
            ws->read(welcome_buf);
            
            while (running_) {
                try {
                    beast::flat_buffer buffer;
                    ws->read(buffer);
                    auto str = beast::buffers_to_string(buffer.data());
                    auto msg = json::parse(str);
                    
                    if (msg.contains("instrument") && msg.contains("bids") && msg.contains("asks")) {
                        mkt_.update_book(exchange_, msg["instrument"].get<std::string>(),
                                        msg["bids"], msg["asks"], 
                                        std::chrono::system_clock::now().time_since_epoch().count() / 1'000'000);
                    }
                } catch (...) {}
            }
            ws->close(websocket::close_code::normal);
        } catch (const std::exception& e) {
            std::cerr << "[" << exchange_ << "] listener error: " << e.what() << "\n";
        }
    }
    
public:
    ExchangeListener(const std::string& exch, MarketState& mkt)
        : exchange_(exch), mkt_(mkt) {
        running_ = true;
        listener_thread_ = std::thread(&ExchangeListener::run_listener, this);
    }
    
    ~ExchangeListener() {
        running_ = false;
        if (listener_thread_.joinable()) listener_thread_.join();
    }
};

// ════════════════════════════════════════════════════════════════════
//  Trading Logic
// ════════════════════════════════════════════════════════════════════
class Trader {
    MarketState& mkt_;
    Positions& pos_;
    std::unordered_map<std::string, std::unique_ptr<ExchangeConnection>>& conns_;
    std::map<std::string, int64_t> last_trade_time_;
    std::mutex trade_mtx_;
    
public:
    Trader(MarketState& mkt, Positions& pos, 
           std::unordered_map<std::string, std::unique_ptr<ExchangeConnection>>& c)
        : mkt_(mkt), pos_(pos), conns_(c) {}
    
    void evaluate_asset(const std::string& etf) {
        std::lock_guard lk(trade_mtx_);
        int64_t now = std::chrono::system_clock::now().time_since_epoch().count() / 1'000'000;
        
        // Cooldown
        auto it = last_trade_time_.find(etf);
        if (it != last_trade_time_.end() && now - it->second < Params::INST_COOLDOWN_MS)
            return;
        
        // Collect prices and VWMA from all venues
        std::vector<std::pair<std::string, double>> current_prices;
        std::vector<std::pair<std::string, double>> vwmas;
        
        for (auto& [exch, _] : EXCHANGES) {
            auto book = mkt_.get(exch, etf);
            if (!book || !book->mid()) continue;
            
            auto vwma = mkt_.get_vwma(exch, etf, Params::VWMA_PERIOD);
            if (!vwma) continue;
            
            current_prices.push_back({exch, *book->mid()});
            vwmas.push_back({exch, *vwma});
        }
        
        if (current_prices.empty() || vwmas.empty()) return;
        
        // Calculate average current price and average VWMA
        double avg_price = 0, avg_vwma = 0;
        for (auto& [_, p] : current_prices) avg_price += p;
        for (auto& [_, v] : vwmas) avg_vwma += v;
        avg_price /= current_prices.size();
        avg_vwma /= vwmas.size();
        
        // SIGNAL: If VWMA > current_price by EDGE%, SELL AGGRESSIVELY + SHORT
        double threshold = avg_price * (1.0 + Params::EDGE_PERCENT / 100.0);
        
        if (avg_vwma > threshold) {
            std::cout << "[SIGNAL] " << etf 
                      << " VWMA=" << (avg_vwma/100.0) 
                      << " > Price=" << (avg_price/100.0) 
                      << " (threshold=" << (threshold/100.0) << ") → SELL/SHORT\n";
            
            int current_pos = pos_.get_position(etf);
            int avg_price_int = (int)avg_price;
            
            // Sell from existing position
            if (current_pos > 0) {
                int sell_qty = std::min(current_pos, Params::BASE_SIZE * Params::AGGRESSIVE_MULTIPLIER);
                for (auto& [exch, conn] : conns_) {
                    if (conn && conn->connected()) {
                        bool ok = conn->place_ioc(etf, "sell", avg_price_int, sell_qty / 10);
                        if (ok) std::cout << "  ├─ SELL order sent to " << exch << "\n";
                    }
                }
            }
            
            // SHORT aggressively
            if (current_pos >= Params::MAX_SHORT) {
                int short_qty = Params::BASE_SIZE * Params::AGGRESSIVE_MULTIPLIER;
                for (auto& [exch, conn] : conns_) {
                    if (conn && conn->connected()) {
                        bool ok = conn->place_ioc(etf, "sell", avg_price_int, short_qty / 10);
                        if (ok) std::cout << "  └─ SHORT order sent to " << exch << "\n";
                    }
                }
            }
        }
        
        last_trade_time_[etf] = now;
    }
};

// ════════════════════════════════════════════════════════════════════
//  Main
// ════════════════════════════════════════════════════════════════════
int main() {
    std::cout << "SCREW BOT v2 — Volume-Weighted Mean Reversion\n";
    std::cout << "Monitoring: " << TARGET_ETFS.size() << " ETFs across all venues\n";
    std::cout << "Strategy: VWMA20 > Current Price → Aggressive Sell/Short\n\n";
    
    MarketState mkt;
    
    // Create exchange connections for SENDING orders
    std::unordered_map<std::string, std::unique_ptr<ExchangeConnection>> conns;
    for (auto& [name, host] : EXCHANGES) {
        auto conn = std::make_unique<ExchangeConnection>(name, host);
        if (conn->connect()) {
            std::cout << "Connected to " << name << " (order channel)\n";
            conns[name] = std::move(conn);
        }
    }
    
    Positions pos(conns);
    Trader trader(mkt, pos, conns);
    
    // Start listeners for MARKET DATA
    std::vector<std::unique_ptr<ExchangeListener>> listeners;
    for (auto& [name, _] : EXCHANGES) {
        listeners.push_back(std::make_unique<ExchangeListener>(name, mkt));
        std::cout << "Listening to " << name << " (market data)\n";
    }
    
    // Wait for warmup
    std::cout << "\nWarmup phase...\n";
    int warmup_count = 0;
    while (!mkt.is_warmed_up(std::chrono::system_clock::now().time_since_epoch().count() / 1'000'000)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (++warmup_count % 10 == 0) std::cout << ".";
    }
    std::cout << "\nWarmup complete. Starting trading...\n\n";
    
    // Trading loop
    auto last_eval = std::chrono::system_clock::now();
    int eval_count = 0;
    
    while (true) {
        auto now = std::chrono::system_clock::now();
        if (std::chrono::duration_cast<std::chrono::milliseconds>(now - last_eval).count() < Params::STRAT_INTERVAL_MS) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        last_eval = now;
        
        // Evaluate all ETFs
        for (const auto& etf : TARGET_ETFS) {
            trader.evaluate_asset(etf);
        }
        
        // Print status every 100 evals
        if (++eval_count % 100 == 0) {
            pos.print_status();
        }
    }
    
    return 0;
}
