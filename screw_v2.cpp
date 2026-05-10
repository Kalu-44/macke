/*
 * SCREW BOT v2 — Volume-Weighted Mean Reversion (C++20)
 * Strategy: Track VWMA20 per asset. When VWMA20 > current_price * (1+EDGE%),
 * sell aggressively and short. Maximize capital deployment.
 * Build: g++ -std=c++20 -O2 -o screw_v2 screw_v2.cpp -lpthread
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
//  PARAMS
// ════════════════════════════════════════════════════════════════════
struct Params {
    static constexpr int  MAX_LONG    = 200;
    static constexpr int  MAX_SHORT   = -200;
    static constexpr long CASH_FLOOR  = -10'000'000;

    static constexpr int INST_COOLDOWN_MS   = 25;
    static constexpr int RATE_LIMIT_PER_SEC = 800;

    static constexpr int WARMUP_MIN_MS     = 5'000;
    static constexpr int WARMUP_MIN_VENUES = 3;

    static constexpr int VWMA_PERIOD           = 20;
    static constexpr int EDGE_PERCENT          = 5;
    static constexpr int BASE_SIZE             = 50;
    static constexpr int AGGRESSIVE_MULTIPLIER = 2;
};

static constexpr int EXCHANGE_PORT = 9001;

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
struct PricePoint {
    double  price;
    int64_t volume;
    int64_t ts_ms;
};

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

static int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

class MarketState {
public:
    void update_book(const std::string& exch, const std::string& inst,
                     const json& bids, const json& asks, int64_t t) {
        std::lock_guard lk(mtx_);
        auto& b = books_[exch][inst];
        b.ts_ms = t; b.bids.clear(); b.asks.clear();
        try {
            if (bids.is_object())
                for (auto& [p,q] : bids.items())
                    if (q.is_number()) b.bids[std::stoi(p)] = q.get<int>();
            if (asks.is_object())
                for (auto& [p,q] : asks.items())
                    if (q.is_number()) b.asks[std::stoi(p)] = q.get<int>();
        } catch (...) {}

        if (auto mid = b.mid()) {
            int64_t vol = 0;
            for (auto& [p,q] : b.asks) vol += q;
            auto& hist = price_history_[exch][inst];
            hist.push_back({*mid, vol, t});
            if (hist.size() > 50) hist.pop_front();
        }
    }

    std::optional<Book> get(const std::string& exch, const std::string& inst) const {
        std::lock_guard lk(mtx_);
        auto e = books_.find(exch); if (e == books_.end()) return std::nullopt;
        auto i = e->second.find(inst); if (i == e->second.end()) return std::nullopt;
        return i->second;
    }

    // VWMA = Σ(price_i * volume_i) / Σ(volume_i) for last N periods
    std::optional<double> get_vwma(const std::string& exch, const std::string& inst,
                                    int periods = 20) const {
        std::lock_guard lk(mtx_);
        auto eh = price_history_.find(exch); if (eh == price_history_.end()) return std::nullopt;
        auto ih = eh->second.find(inst);     if (ih == eh->second.end())     return std::nullopt;
        const auto& hist = ih->second;
        if ((int)hist.size() < periods) return std::nullopt;
        double sum_pv = 0, sum_v = 0;
        for (size_t i = hist.size() - periods; i < hist.size(); ++i) {
            sum_pv += hist[i].price * hist[i].volume;
            sum_v  += hist[i].volume;
        }
        return sum_v > 0 ? std::optional<double>(sum_pv / sum_v) : std::nullopt;
    }

    void note_venue_alive(const std::string& exch) {
        std::lock_guard lk(warm_mtx_);
        if (warmup_first_ms_ == 0) warmup_first_ms_ = now_ms();
        warmup_venues_.insert(exch);
    }
    bool warmup_done() const {
        std::lock_guard lk(warm_mtx_);
        if (warmup_first_ms_ == 0) return false;
        if ((int)warmup_venues_.size() < Params::WARMUP_MIN_VENUES) return false;
        return (now_ms() - warmup_first_ms_) >= Params::WARMUP_MIN_MS;
    }
    void reset_warmup() {
        std::lock_guard lk(warm_mtx_);
        warmup_venues_.clear();
        warmup_first_ms_ = 0;
    }

private:
    mutable std::mutex mtx_;
    std::map<std::string, std::map<std::string, Book>> books_;
    std::map<std::string, std::map<std::string, std::deque<PricePoint>>> price_history_;

    mutable std::mutex warm_mtx_;
    std::set<std::string> warmup_venues_;
    int64_t warmup_first_ms_ = 0;
};

// ════════════════════════════════════════════════════════════════════
//  Exchange Connection — single WS per exchange, handles market data
//  AND order responses on the same socket (same as prsbot)
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
        return send({
            {"type","add_order"}, {"user_request_id", next_rid()},
            {"instrument_id", inst}, {"price", price},
            {"expiry", now_ms()+1000}, {"side", side},
            {"quantity", qty}, {"order_type","ioc"},
        });
    }

    bool get_inventory() {
        return send({{"type","get_inventory"}, {"user_request_id", next_rid()}});
    }

    // Public for inventory/position access
    std::mutex pos_mtx;
    std::unordered_map<std::string,int> positions;
    long cash_cents = 10'000'000;

    void apply_add_order_resp(const json& m) {
        if (!m.value("success", false)) return;
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
        return it == positions.end() ? 0 : it->second;
    }

    void on_segment_reset() {
        std::lock_guard lk(pos_mtx);
        positions.clear();
        cash_cents = 10'000'000;
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
//  Trader — VWMA20 strategy, evaluated per-exchange on every tick
// ════════════════════════════════════════════════════════════════════
class Trader {
public:
    Trader(MarketState& mkt,
           std::unordered_map<std::string, std::unique_ptr<ExchangeConnection>>& conns)
        : mkt_(mkt), conns_(conns) {}

    void evaluate(const std::string& exch) {
        if (!mkt_.warmup_done()) return;

        auto& conn = *conns_.at(exch);
        int64_t now = now_ms();

        for (const auto& etf : TARGET_ETFS) {
            // Instruments are stored as "EXCHANGE-TICKER" (e.g. "ZSE-ETFA")
            std::string inst   = exch + "-" + etf;
            std::string cd_key = exch + "|" + etf;

            {
                std::lock_guard lk(cd_mtx_);
                auto it = last_trade_ms_.find(cd_key);
                if (it != last_trade_ms_.end() && now - it->second < Params::INST_COOLDOWN_MS)
                    continue;
            }

            auto book = mkt_.get(exch, inst);
            if (!book || !book->mid()) continue;

            auto vwma = mkt_.get_vwma(exch, inst, Params::VWMA_PERIOD);
            if (!vwma) continue;

            double price     = *book->mid();
            double threshold = price * (1.0 + Params::EDGE_PERCENT / 100.0);

            if (*vwma > threshold) {
                auto bid = book->best_bid();
                if (!bid) continue;

                int pos = conn.position_of(inst);
                std::cout << "[SIGNAL] " << exch << " " << etf
                          << " VWMA=" << (*vwma / 100.0)
                          << " Price=" << (price / 100.0)
                          << " pos=" << pos << " → SELL/SHORT\n";

                // Liquidate any long first
                if (pos > 0) {
                    int qty = std::min(pos, Params::BASE_SIZE * Params::AGGRESSIVE_MULTIPLIER);
                    if (qty > 0) conn.place_ioc(inst, "ask", *bid, qty);
                }

                // Go short if headroom remains
                if (pos > Params::MAX_SHORT) {
                    int qty = std::min(Params::BASE_SIZE, pos - Params::MAX_SHORT);
                    if (qty > 0) conn.place_ioc(inst, "ask", *bid, qty);
                }
            }

            {
                std::lock_guard lk(cd_mtx_);
                last_trade_ms_[cd_key] = now;
            }
        }
    }

private:
    MarketState& mkt_;
    std::unordered_map<std::string, std::unique_ptr<ExchangeConnection>>& conns_;
    std::mutex cd_mtx_;
    std::unordered_map<std::string, int64_t> last_trade_ms_;
};

// ════════════════════════════════════════════════════════════════════
//  Main — one thread per exchange, single connection handles all msgs
// ════════════════════════════════════════════════════════════════════
int main() {
    std::cout << "SCREW BOT v2 — Volume-Weighted Mean Reversion\n";
    std::cout << "Monitoring " << TARGET_ETFS.size() << " ETFs across "
              << EXCHANGES.size() << " venues\n\n";

    MarketState mkt;
    std::unordered_map<std::string, std::unique_ptr<ExchangeConnection>> conns;
    for (auto& [name, host] : EXCHANGES)
        conns.emplace(name, std::make_unique<ExchangeConnection>(name, host));

    Trader trader(mkt, conns);
    std::atomic<bool> running{true};
    std::vector<std::thread> threads;

    // One thread per exchange — reads market data + order responses on same socket
    for (auto& [name, _] : EXCHANGES) {
        threads.emplace_back([&, exch = name]() {
            auto& conn = *conns.at(exch);
            int backoff = 1;
            while (running) {
                if (!conn.connect()) {
                    std::this_thread::sleep_for(std::chrono::seconds(backoff));
                    backoff = std::min(backoff * 2, 8);
                    continue;
                }
                backoff = 1;
                conn.on_segment_reset();
                mkt.reset_warmup();
                std::cout << "[" << exch << "] connected\n";

                while (running && conn.connected()) {
                    auto m = conn.recv();
                    if (!m) break;

                    auto type = m->value("type", "");
                    if (type == "market_data_update") {
                        int64_t srv_t = m->value("time", (int64_t)0);
                        if (m->contains("orderbook_depths")) {
                            for (auto& [inst, ob] : (*m)["orderbook_depths"].items())
                                mkt.update_book(exch, inst,
                                               ob.value("bids", json::object()),
                                               ob.value("asks", json::object()), srv_t);
                        }
                        mkt.note_venue_alive(exch);
                        trader.evaluate(exch);
                    } else if (type == "add_order_response") {
                        conn.apply_add_order_resp(*m);
                    } else if (type == "get_inventory_response") {
                        conn.apply_inventory(*m);
                    } else if (type == "end_of_round") {
                        std::cout << "[" << exch << "] end_of_round; reconnecting\n";
                        break;
                    } else if (type == "error") {
                        std::cerr << "[" << exch << "] error: " << m->value("message","") << "\n";
                    }
                }

                conn.disconnect();
                std::this_thread::sleep_for(std::chrono::seconds(2));
            }
        });
    }

    // Periodic inventory sync so position tracking stays accurate
    threads.emplace_back([&]() {
        while (running) {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            for (auto& [_, conn] : conns)
                if (conn->connected()) conn->get_inventory();
        }
    });

    for (auto& t : threads) t.join();
    return 0;
}
