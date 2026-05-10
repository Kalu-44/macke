/*
 * SCREW BOT — Extreme ETF Arbitrage (C++20)
 * Strategy: Cross-venue ETF mean arbitrage
 * ETFA, ETFB, ETFA3, ETFB3 — buy below mean, sell above mean
 * No risk limits, extreme speed
 * Build: g++ -std=c++20 -O3 -march=native -o screw screw.cpp -lpthread
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
//  EXTREME PARAMS
// ════════════════════════════════════════════════════════════════════
struct Params {
    static constexpr int MAX_LONG    = 200;
    static constexpr int MAX_SHORT   = -200;
    
    // EXTREME SPEED
    static constexpr int STRAT_INTERVAL_MS  = 5;    // 5ms eval
    static constexpr int INST_COOLDOWN_MS   = 10;   // 10ms cooldown
    static constexpr int ORDER_TTL_MS       = 1000; // 1s TTL
    
    // MINIMAL WARMUP
    static constexpr int WARMUP_MIN_MS     = 500;   // 0.5s only
    static constexpr int WARMUP_MIN_VENUES = 3;     // 3 venues min
    
    // AGGRESSIVE ARBITRAGE
    static constexpr int EDGE_CENTS = 1;            // 1¢ edge
    static constexpr int ORDER_SIZE = 100;          // 100 per order
};

static const std::vector<std::pair<std::string,std::string>> EXCHANGES = {
    {"NYSE","10.0.201.2"},   {"NASDAQ","10.0.202.2"}, {"SSE","10.0.203.2"},
    {"JPX","10.0.204.2"},    {"Euronext","10.0.205.2"}, {"LSE","10.0.206.2"},
    {"HKEX","10.0.207.2"},   {"NSE","10.0.208.2"},    {"TMX","10.0.209.2"},
    {"ZSE","10.0.210.2"},
};

static const std::vector<std::string> TARGET_ETFS = {"ETFA", "ETFB", "ETFA3", "ETFB3"};

// ════════════════════════════════════════════════════════════════════
//  Book & Market State
// ════════════════════════════════════════════════════════════════════
struct Book {
    std::map<int,int> bids, asks;
    int64_t ts_ms = 0;
    
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
    }
    
    std::optional<Book> get(const std::string& exch, const std::string& inst) const {
        std::lock_guard lk(mtx_);
        auto e = books_.find(exch); if (e == books_.end()) return std::nullopt;
        auto i = e->second.find(inst); if (i == e->second.end()) return std::nullopt;
        return i->second;
    }
    
    bool is_warmed_up(int64_t now_ms) const {
        std::lock_guard lk(mtx_);
        return (now_ms - warmup_start_ms_ > Params::WARMUP_MIN_MS) &&
               (active_venues_.size() >= Params::WARMUP_MIN_VENUES);
    }
};

// ════════════════════════════════════════════════════════════════════
//  Position Tracker
// ════════════════════════════════════════════════════════════════════
class Positions {
    std::map<std::string, int> pos_;  // ticker -> qty
    mutable std::mutex mtx_;
    
public:
    void update(const std::string& ticker, int delta) {
        std::lock_guard lk(mtx_);
        pos_[ticker] += delta;
    }
    
    int get(const std::string& ticker) const {
        std::lock_guard lk(mtx_);
        auto it = pos_.find(ticker);
        return it != pos_.end() ? it->second : 0;
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
            
            // Handshake with correct path and host:port format
            ws->handshake(host_it->second + ":9001", "/trade");
            
            // Read welcome message
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
//  Main Trading Loop
// ════════════════════════════════════════════════════════════════════
int main() {
    std::cout << "SCREW BOT — Extreme ETF Arbitrage v1.0\n";
    std::cout << "Monitoring: ETFA, ETFB, ETFA3, ETFB3 across all venues\n";
    
    MarketState mkt;
    Positions pos;
    
    // Start listeners for all exchanges
    std::vector<std::unique_ptr<ExchangeListener>> listeners;
    for (auto& [name, _] : EXCHANGES) {
        listeners.push_back(std::make_unique<ExchangeListener>(name, mkt));
        std::cout << "Connected to " << name << "\n";
    }
    
    // Wait for warmup
    int64_t start = std::chrono::system_clock::now().time_since_epoch().count() / 1'000'000;
    while (!mkt.is_warmed_up(std::chrono::system_clock::now().time_since_epoch().count() / 1'000'000)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::cout << "Warmup complete. Starting arbitrage...\n\n";
    
    // Trading loop
    auto last_trade = std::chrono::system_clock::now();
    while (true) {
        auto now = std::chrono::system_clock::now();
        if (std::chrono::duration_cast<std::chrono::milliseconds>(now - last_trade).count() < Params::STRAT_INTERVAL_MS) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        last_trade = now;
        
        // For each target ETF
        for (const auto& etf : TARGET_ETFS) {
            std::vector<std::pair<std::string, double>> prices;  // (venue, mid_price)
            
            // Collect prices from all venues
            for (auto& [exch, _] : EXCHANGES) {
                auto book = mkt.get(exch, etf);
                if (book && book->mid()) {
                    prices.push_back({exch, *book->mid()});
                }
            }
            
            if (prices.size() < 2) continue;
            
            // Calculate mean
            double mean = 0;
            for (auto& [_, p] : prices) mean += p;
            mean /= prices.size();
            
            // For each venue: BUY below mean, SELL above mean
            for (auto& [venue, price] : prices) {
                double edge = price - mean;
                
                if (edge < -Params::EDGE_CENTS && pos.get(etf) < Params::MAX_LONG) {
                    // EDGE_CENTS cents cheaper → BUY
                    std::cout << "[" << venue << " " << etf << "] BUY @ " << price/100.0 
                              << " (mean=" << mean/100.0 << ", edge=" << edge/100.0 << "¢)\n";
                    pos.update(etf, Params::ORDER_SIZE);
                } 
                else if (edge > Params::EDGE_CENTS && pos.get(etf) > Params::MAX_SHORT) {
                    // EDGE_CENTS cents expensive → SELL
                    std::cout << "[" << venue << " " << etf << "] SELL @ " << price/100.0 
                              << " (mean=" << mean/100.0 << ", edge=" << edge/100.0 << "¢)\n";
                    pos.update(etf, -Params::ORDER_SIZE);
                }
            }
        }
    }
    
    return 0;
}
