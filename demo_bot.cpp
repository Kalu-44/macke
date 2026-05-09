/*
 * AlgoTrade 2026 — Demo Trading Bot (C++)
 *
 * A reference trading bot that demonstrates the full exchange API.
 * The bot is structured around a pluggable Strategy interface — subclass
 * Strategy and implement the virtual methods with your own logic.
 *
 * Architecture:
 *     ExchangeConnection  — manages one WebSocket connection to one exchange
 *     MarketState         — parsed, queryable snapshot of the latest market data
 *     Strategy            — abstract base class with virtual callbacks
 *     SimpleStrategy      — minimal example that prints top-of-book
 *     Bot                 — orchestrates connections and dispatches to strategy
 *
 * Dependencies:
 *     - Boost.Beast + Boost.Asio (WebSocket + networking)
 *     - nlohmann/json (header-only JSON library)
 *
 *     Install on Arch Linux:
 *         sudo pacman -S boost nlohmann-json
 *
 *     Install on Ubuntu/Debian:
 *         sudo apt install libboost-all-dev nlohmann-json3-dev
 *
 * Build:
 *     g++ -std=c++20 -O2 -o demo_bot demo_bot.cpp -lpthread
 *
 * Usage:
 *     # Defaults to all 10 production exchanges. Authentication is by team IP,
 *     # so no team secret is required. Optionally narrow the list:
 *     export EXCHANGES="NYSE,NASDAQ,LSE"
 *     ./demo_bot
 */

#include <cstdlib>
#include <cstring>
#include <ctime>
#include <chrono>
#include <functional>
#include <iostream>
#include <iomanip>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>
#include <atomic>

#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>

#include <nlohmann/json.hpp>

namespace beast     = boost::beast;
namespace websocket = beast::websocket;
namespace net       = boost::asio;
using tcp           = net::ip::tcp;
using json          = nlohmann::json;


// ──────────────────────────────────────────────────────────────────────
//  Configuration
// ──────────────────────────────────────────────────────────────────────

// Production exchange network — each exchange has a fixed IP, all on port 9001.
// Authentication is by team IP, so no team secret is sent in the URL.
static constexpr int EXCHANGE_PORT = 9001;

static const std::vector<std::pair<std::string, std::string>>& exchange_hosts() {
    static const std::vector<std::pair<std::string, std::string>> hosts = {
        {"NYSE",     "10.0.201.2"},
        {"NASDAQ",   "10.0.202.2"},
        {"SSE",      "10.0.203.2"},
        {"JPX",      "10.0.204.2"},
        {"Euronext", "10.0.205.2"},
        {"LSE",      "10.0.206.2"},
        {"HKEX",     "10.0.207.2"},
        {"NSE",      "10.0.208.2"},
        {"TMX",      "10.0.209.2"},
        {"ZSE",      "10.0.210.2"},
    };
    return hosts;
}

static std::optional<std::string> lookup_host(const std::string& name) {
    for (auto& [n, h] : exchange_hosts()) {
        if (n == name) return h;
    }
    return std::nullopt;
}

struct Config {
    std::string exchanges_str;       // comma-separated exchange names
    int         reconnect_max = 10;
    int         reconnect_delay_ms = 2000;
    int         default_expiry_ms  = 10000; // 10 seconds
};

static Config load_config() {
    Config cfg;
    if (const char* v = std::getenv("EXCHANGES")) {
        cfg.exchanges_str = v;
    } else {
        // Default: all 10 production exchanges.
        std::ostringstream ss;
        bool first = true;
        for (auto& [n, _] : exchange_hosts()) {
            if (!first) ss << ",";
            ss << n;
            first = false;
        }
        cfg.exchanges_str = ss.str();
    }
    return cfg;
}


// ──────────────────────────────────────────────────────────────────────
//  Data Structures
// ──────────────────────────────────────────────────────────────────────

struct OrderBookLevel {
    int price;    // cents
    int quantity; // shares
};

struct OrderBook {
    std::vector<OrderBookLevel> bids; // sorted descending by price
    std::vector<OrderBookLevel> asks; // sorted ascending by price

    std::optional<int> best_bid() const {
        return bids.empty() ? std::nullopt : std::optional<int>(bids.front().price);
    }

    std::optional<int> best_ask() const {
        return asks.empty() ? std::nullopt : std::optional<int>(asks.front().price);
    }

    std::optional<double> mid_price() const {
        auto bb = best_bid();
        auto ba = best_ask();
        if (bb && ba) return (*bb + *ba) / 2.0;
        return std::nullopt;
    }

    std::optional<int> spread() const {
        auto bb = best_bid();
        auto ba = best_ask();
        if (bb && ba) return *ba - *bb;
        return std::nullopt;
    }
};

struct Candle {
    std::optional<int> open, close, high, low, volume;
    int index = 0;
};

struct Trade {
    std::string instrument_id;
    int64_t passive_order_id;
    int64_t active_order_id;
    int quantity;
    int price;
    int64_t time;
};

struct Fill {
    std::string exchange;
    std::string instrument_id;
    int64_t order_id;
    std::string side;
    int price;
    int quantity;
    std::optional<int> inventory_change;
    std::optional<int> balance_change;
};

/// Aggregated market state across all exchanges.
struct MarketState {
    // exchange -> instrument_id -> OrderBook
    std::map<std::string, std::map<std::string, OrderBook>> order_books;
    // exchange -> instrument_id -> candle history
    std::map<std::string, std::map<std::string, std::vector<Candle>>> candles;
    // exchange -> server time
    std::map<std::string, int64_t> server_times;

    mutable std::mutex mtx;

    /// Get a copy of the order book (thread-safe).
    std::optional<OrderBook> get_book(const std::string& exchange,
                                      const std::string& instrument) const {
        std::lock_guard lock(mtx);
        auto eit = order_books.find(exchange);
        if (eit == order_books.end()) return std::nullopt;
        auto iit = eit->second.find(instrument);
        if (iit == eit->second.end()) return std::nullopt;
        return iit->second;
    }

    /// List all instruments seen on an exchange.
    std::vector<std::string> instruments_on(const std::string& exchange) const {
        std::lock_guard lock(mtx);
        std::vector<std::string> result;
        auto it = order_books.find(exchange);
        if (it != order_books.end()) {
            for (auto& [k, _] : it->second) result.push_back(k);
        }
        return result;
    }
};


// ──────────────────────────────────────────────────────────────────────
//  Market Data Parser
// ──────────────────────────────────────────────────────────────────────

static OrderBook parse_order_book(const json& raw) {
    OrderBook ob;

    if (raw.contains("bids") && raw["bids"].is_object()) {
        for (auto& [price_str, qty] : raw["bids"].items()) {
            ob.bids.push_back({std::stoi(price_str), qty.get<int>()});
        }
        std::sort(ob.bids.begin(), ob.bids.end(),
                  [](auto& a, auto& b) { return a.price > b.price; });
    }

    if (raw.contains("asks") && raw["asks"].is_object()) {
        for (auto& [price_str, qty] : raw["asks"].items()) {
            ob.asks.push_back({std::stoi(price_str), qty.get<int>()});
        }
        std::sort(ob.asks.begin(), ob.asks.end(),
                  [](auto& a, auto& b) { return a.price < b.price; });
    }

    return ob;
}

static Candle parse_candle(const json& raw) {
    Candle c;
    if (raw.contains("open")   && !raw["open"].is_null())   c.open   = raw["open"].get<int>();
    if (raw.contains("close")  && !raw["close"].is_null())  c.close  = raw["close"].get<int>();
    if (raw.contains("high")   && !raw["high"].is_null())   c.high   = raw["high"].get<int>();
    if (raw.contains("low")    && !raw["low"].is_null())    c.low    = raw["low"].get<int>();
    if (raw.contains("volume") && !raw["volume"].is_null()) c.volume = raw["volume"].get<int>();
    c.index = raw.value("index", 0);
    return c;
}

static Trade parse_trade(const json& event) {
    if (!event.contains("data")) return {};
    const auto& d = event["data"];
    return Trade{
        .instrument_id    = d.value("instrumentID", ""),
        .passive_order_id = d.value("passiveOrderID", (int64_t)0),
        .active_order_id  = d.value("activeOrderID", (int64_t)0),
        .quantity          = d.value("quantity", 0),
        .price             = d.value("price", 0),
        .time              = d.value("time", (int64_t)0),
    };
}

/// Update the MarketState from a market_data_update. Returns parsed trades.
static std::vector<Trade> update_market_state(
    MarketState& state, const std::string& exchange, const json& data)
{
    std::lock_guard lock(state.mtx);

    state.server_times[exchange] = data.value("time", (int64_t)0);

    // Order books
    if (data.contains("orderbook_depths")) {
        for (auto& [inst, raw_book] : data["orderbook_depths"].items()) {
            state.order_books[exchange][inst] = parse_order_book(raw_book);
        }
    }

    // Candles
    if (data.contains("candles") && data["candles"].contains("tradeable")) {
        for (auto& [inst, raw_candles] : data["candles"]["tradeable"].items()) {
            for (auto& rc : raw_candles) {
                state.candles[exchange][inst].push_back(parse_candle(rc));
            }
        }
    }

    // Events — extract trades
    std::vector<Trade> trades;
    if (data.contains("events")) {
        for (auto& evt : data["events"]) {
            if (evt.value("event_type", "") == "trade") {
                trades.push_back(parse_trade(evt));
            }
        }
    }

    return trades;
}


// ──────────────────────────────────────────────────────────────────────
//  Strategy Interface
// ──────────────────────────────────────────────────────────────────────

// Forward declaration
class Bot;

/**
 * Abstract strategy interface.
 *
 * Subclass this and override the virtual methods with your trading logic.
 * Every callback receives a Bot reference so you can place/cancel orders:
 *
 *     void on_market_data(Bot& bot, const std::string& exchange,
 *                         const MarketState& state) override {
 *         bot.place_order(exchange, "NYSE-CARD", "bid", 10000, 10);
 *     }
 *
 * Lifecycle:
 *     1. on_connected(bot, exchange)           — WebSocket connected
 *     2. on_market_data(bot, exchange, state)   — every 100ms per exchange
 *     3. on_fill(bot, fill)                     — your order was filled
 *     4. on_round_end(bot, exchange)            — round ended
 */
class Strategy {
public:
    virtual ~Strategy() = default;

    /// Called every time a market_data_update arrives from an exchange.
    virtual void on_market_data(Bot& bot, const std::string& exchange,
                                const MarketState& state) = 0;

    /// Called when a WebSocket connection is established.
    virtual void on_connected(Bot& bot, const std::string& exchange) {
        (void)bot; (void)exchange;
    }

    /// Called when one of your orders is filled.
    virtual void on_fill(Bot& bot, const Fill& fill) {
        (void)bot; (void)fill;
    }

    /// Called when the round ends.
    virtual void on_round_end(Bot& bot, const std::string& exchange) {
        (void)bot; (void)exchange;
    }
};


// ──────────────────────────────────────────────────────────────────────
//  Example: SimpleStrategy
// ──────────────────────────────────────────────────────────────────────

/**
 * Minimal example strategy — logs top-of-book data.
 *
 * This does NOT trade. Replace with your own logic.
 */
class SimpleStrategy : public Strategy {
public:
    void on_connected(Bot& bot, const std::string& exchange) override {
        (void)bot;
        std::cout << "[SimpleStrategy] Connected to " << exchange << "\n";
    }

    void on_market_data(Bot& bot, const std::string& exchange,
                        const MarketState& state) override {
        (void)bot;
        // Print once every ~10 ticks (1 second)
        tick_count_[exchange]++;
        if (tick_count_[exchange] % 10 != 0) return;

        auto instruments = state.instruments_on(exchange);
        if (instruments.empty()) return;

        std::sort(instruments.begin(), instruments.end());
        std::cout << "\n[SimpleStrategy] " << exchange
                  << " tick " << tick_count_[exchange] << "\n";

        int shown = 0;
        for (auto& inst : instruments) {
            if (shown >= 5) break;
            auto book = state.get_book(exchange, inst);
            if (!book || !book->best_bid() || !book->best_ask()) continue;

            std::cout << "  " << std::left << std::setw(20) << inst
                      << "  bid=" << std::setw(8) << *book->best_bid()
                      << "  ask=" << std::setw(8) << *book->best_ask()
                      << "  spread=" << std::setw(6) << *book->spread()
                      << "  mid=" << std::fixed << std::setprecision(1)
                      << *book->mid_price()
                      << "\n";
            shown++;
        }
    }

    void on_fill(Bot& bot, const Fill& fill) override {
        (void)bot;
        std::cout << "[SimpleStrategy] FILL on " << fill.exchange << ": "
                  << fill.instrument_id << " " << fill.side
                  << " " << fill.quantity << "@" << fill.price << "\n";
    }

    void on_round_end(Bot& bot, const std::string& exchange) override {
        (void)bot;
        std::cout << "[SimpleStrategy] Round ended on " << exchange << "\n";
    }

private:
    std::map<std::string, int> tick_count_;
};


// ──────────────────────────────────────────────────────────────────────
//  Utility
// ──────────────────────────────────────────────────────────────────────

static int64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()).count();
}


// ──────────────────────────────────────────────────────────────────────
//  Exchange Connection (Boost.Beast synchronous WebSocket)
// ──────────────────────────────────────────────────────────────────────

/**
 * Manages a single WebSocket connection to one exchange.
 * Uses Boost.Beast for WebSocket I/O (synchronous, run in a dedicated thread).
 */
class ExchangeConnection {
public:
    ExchangeConnection(const std::string& name, const std::string& host,
                       int port, const Config& cfg)
        : name_(name), host_(host), port_(std::to_string(port)),
          config_(cfg) {}

    const std::string& name() const { return name_; }
    bool connected() const { return connected_.load(); }

    /// Connect synchronously. Returns true on success.
    bool connect() {
        try {
            // Resolve
            tcp::resolver resolver(ioc_);
            auto results = resolver.resolve(host_, port_);

            // TCP connect
            auto ep = net::connect(ws_.next_layer(), results);

            // WebSocket handshake — production network authenticates by team IP.
            std::string target = "/trade";
            std::string ws_host = host_ + ":" + port_;
            ws_.handshake(ws_host, target);

            connected_ = true;

            // Read the welcome message
            beast::flat_buffer buf;
            ws_.read(buf);
            auto welcome = json::parse(beast::buffers_to_string(buf.data()));
            if (welcome.value("type", "") == "welcome") {
                std::cout << "[" << name_ << "] Connected: "
                          << welcome.value("message", "") << "\n";
            }

            return true;
        } catch (const std::exception& e) {
            std::cerr << "[" << name_ << "] Connection failed: " << e.what() << "\n";
            connected_ = false;
            return false;
        }
    }

    /// Read one message. Returns nullopt on error/close.
    std::optional<json> recv() {
        try {
            beast::flat_buffer buf;
            ws_.read(buf);
            return json::parse(beast::buffers_to_string(buf.data()));
        } catch (const std::exception&) {
            connected_ = false;
            return std::nullopt;
        }
    }

    /// Send a JSON message.
    bool send(const json& msg) {
        if (!connected_) return false;
        try {
            std::string text = msg.dump();
            ws_.write(net::buffer(text));
            return true;
        } catch (const std::exception& e) {
            std::cerr << "[" << name_ << "] Send failed: " << e.what() << "\n";
            connected_ = false;
            return false;
        }
    }

    /// Close the WebSocket connection.
    void disconnect() {
        connected_ = false;
        try {
            ws_.close(websocket::close_code::normal);
        } catch (...) {}
    }

    /// Generate a unique request ID.
    std::string next_request_id() {
        return name_ + "-" + std::to_string(++request_counter_);
    }

    // ── Order helpers ──

    bool place_order(const std::string& instrument_id, const std::string& side,
                     int price, int quantity, int expiry_ms = 0) {
        if (expiry_ms <= 0) expiry_ms = config_.default_expiry_ms;
        json msg = {
            {"type",            "add_order"},
            {"user_request_id", next_request_id()},
            {"instrument_id",   instrument_id},
            {"price",           price},
            {"expiry",          now_ms() + expiry_ms},
            {"side",            side},
            {"quantity",        quantity},
        };
        return send(msg);
    }

    bool cancel_order(int64_t order_id, const std::string& instrument_id) {
        json msg = {
            {"type",            "cancel_order"},
            {"user_request_id", next_request_id()},
            {"order_id",        order_id},
            {"instrument_id",   instrument_id},
        };
        return send(msg);
    }

    bool get_inventory() {
        json msg = {
            {"type",            "get_inventory"},
            {"user_request_id", next_request_id()},
        };
        return send(msg);
    }

    bool get_pending_orders() {
        json msg = {
            {"type",            "get_pending_orders"},
            {"user_request_id", next_request_id()},
        };
        return send(msg);
    }

private:
    std::string name_;
    std::string host_;
    std::string port_;
    Config config_;
    net::io_context ioc_;
    websocket::stream<tcp::socket> ws_{ioc_};
    std::atomic<bool> connected_{false};
    int request_counter_ = 0;
};


// ──────────────────────────────────────────────────────────────────────
//  Bot — Main Orchestrator
// ──────────────────────────────────────────────────────────────────────

/**
 * Main bot orchestrator.
 *
 * Manages connections to multiple exchanges, maintains the aggregated
 * MarketState, and dispatches events to the plugged-in Strategy.
 *
 * Usage:
 *     auto strategy = std::make_unique<SimpleStrategy>();
 *     Bot bot(std::move(strategy));
 *     bot.run(); // blocks until round ends or interrupted
 */
class Bot {
public:
    explicit Bot(std::unique_ptr<Strategy> strategy)
        : strategy_(std::move(strategy)) {}

    MarketState& state() { return state_; }
    const MarketState& state() const { return state_; }

    /// Get a connection by exchange name (nullptr if not found).
    ExchangeConnection* connection(const std::string& exchange) {
        std::lock_guard lock(conn_mtx_);
        auto it = connections_.find(exchange);
        return (it != connections_.end()) ? it->second.get() : nullptr;
    }

    // ── Public API for strategies ──

    bool place_order(const std::string& exchange, const std::string& instrument_id,
                     const std::string& side, int price, int quantity,
                     int expiry_ms = 0) {
        auto* conn = connection(exchange);
        if (!conn) return false;
        return conn->place_order(instrument_id, side, price, quantity, expiry_ms);
    }

    bool cancel_order(const std::string& exchange, int64_t order_id,
                      const std::string& instrument_id) {
        auto* conn = connection(exchange);
        if (!conn) return false;
        return conn->cancel_order(order_id, instrument_id);
    }

    // ── Main entry point ──

    void run() {
        auto cfg = load_config();
        auto exchanges = parse_exchanges(cfg.exchanges_str);

        if (exchanges.empty()) {
            std::cerr << "[Bot] No exchanges configured. Set EXCHANGES env var.\n"
                      << "[Bot] Format: EXCHANGES='NYSE,NASDAQ,LSE' (known: ";
            bool first = true;
            for (auto& [n, _] : exchange_hosts()) {
                if (!first) std::cerr << ",";
                std::cerr << n;
                first = false;
            }
            std::cerr << ")\n";
            return;
        }

        std::cout << "[Bot] Starting with " << exchanges.size() << " exchange(s): ";
        for (size_t i = 0; i < exchanges.size(); i++) {
            if (i > 0) std::cout << ", ";
            std::cout << std::get<0>(exchanges[i]);
        }
        std::cout << "\n";

        running_ = true;

        // Launch a thread per exchange
        std::vector<std::thread> threads;
        for (auto& [name, host, port] : exchanges) {
            auto conn = std::make_unique<ExchangeConnection>(
                name, host, port, cfg);

            auto* conn_ptr = conn.get();
            {
                std::lock_guard lock(conn_mtx_);
                connections_[name] = std::move(conn);
            }

            threads.emplace_back([this, conn_ptr]() {
                handle_exchange(*conn_ptr);
            });
        }

        std::cout << "[Bot] Running... Press Ctrl+C to stop.\n";

        // Wait for all threads to finish
        for (auto& t : threads) {
            if (t.joinable()) t.join();
        }

        std::cout << "[Bot] Shutdown complete\n";
    }

private:
    /// Parse "NYSE,NASDAQ,..." into vector of (name, host, port).
    /// Unknown names are skipped with a warning.
    static std::vector<std::tuple<std::string, std::string, int>> parse_exchanges(
        const std::string& str)
    {
        std::vector<std::tuple<std::string, std::string, int>> result;
        std::istringstream ss(str);
        std::string entry;
        while (std::getline(ss, entry, ',')) {
            auto start = entry.find_first_not_of(" \t");
            auto end   = entry.find_last_not_of(" \t");
            if (start == std::string::npos) continue;
            std::string name = entry.substr(start, end - start + 1);

            auto host = lookup_host(name);
            if (!host) {
                std::cerr << "[Bot] Unknown exchange: " << name << "\n";
                continue;
            }
            result.emplace_back(name, *host, EXCHANGE_PORT);
        }
        return result;
    }

    /// Main loop for one exchange connection (runs in its own thread).
    void handle_exchange(ExchangeConnection& conn) {
        // Connect with retry
        for (int attempt = 1; attempt <= 10; attempt++) {
            if (conn.connect()) break;
            if (attempt == 10) {
                std::cerr << "[" << conn.name()
                          << "] Failed to connect after 10 attempts\n";
                return;
            }
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }

        {
            std::lock_guard lock(strategy_mtx_);
            strategy_->on_connected(*this, conn.name());
        }

        // Message loop
        while (running_ && conn.connected()) {
            auto msg = conn.recv();
            if (!msg) {
                if (running_) {
                    std::cerr << "[" << conn.name()
                              << "] Connection lost\n";
                }
                break;
            }

            auto type = msg->value("type", "");

            if (type == "market_data_update") {
                update_market_state(state_, conn.name(), *msg);
                std::lock_guard lock(strategy_mtx_);
                strategy_->on_market_data(*this, conn.name(), state_);

            } else if (type == "add_order_response") {
                if (msg->value("success", false)) {
                    if (msg->contains("data")) {
                        auto& d = (*msg)["data"];
                        auto inv_change = d.value("immediate_inventory_change", 0);
                        if (inv_change != 0) {
                            Fill fill{
                                .exchange         = conn.name(),
                                .instrument_id    = "",
                                .order_id         = d.value("order_id", (int64_t)0),
                                .side             = "",
                                .price            = 0,
                                .quantity          = std::abs(inv_change),
                                .inventory_change = inv_change,
                                .balance_change   = d.value("immediate_balance_change", 0),
                            };
                            std::lock_guard lock(strategy_mtx_);
                            strategy_->on_fill(*this, fill);
                        }
                    }
                } else {
                    std::string err = "unknown";
                    if (msg->contains("data") && (*msg)["data"].contains("message"))
                        err = (*msg)["data"]["message"].get<std::string>();
                    auto req = msg->value("user_request_id", "?");
                    std::cerr << "[" << conn.name() << "] Order rejected ("
                              << req << "): " << err << "\n";
                }

            } else if (type == "cancel_order_response") {
                if (!msg->value("success", false)) {
                    std::cerr << "[" << conn.name() << "] Cancel failed: "
                              << msg->value("message", "unknown") << "\n";
                }

            } else if (type == "end_of_round") {
                std::cout << "[" << conn.name() << "] Round ended\n";
                {
                    std::lock_guard lock(strategy_mtx_);
                    strategy_->on_round_end(*this, conn.name());
                }
                running_ = false;
                break;

            } else if (type == "error") {
                std::cerr << "[" << conn.name() << "] Error: "
                          << msg->value("message", "") << "\n";
            }
        }

        conn.disconnect();
    }

    std::unique_ptr<Strategy> strategy_;
    std::mutex strategy_mtx_;   // protects strategy callbacks
    MarketState state_;
    std::map<std::string, std::unique_ptr<ExchangeConnection>> connections_;
    std::mutex conn_mtx_;       // protects connections_ map
    std::atomic<bool> running_{false};
};


// ──────────────────────────────────────────────────────────────────────
//  Entry Point
// ──────────────────────────────────────────────────────────────────────

/**
 * Run the demo bot with the SimpleStrategy.
 *
 * To use your own strategy:
 *     1. Subclass Strategy
 *     2. Override on_market_data() (and optionally on_fill, etc.)
 *     3. Replace SimpleStrategy below with your class
 */
int main() {
    auto strategy = std::make_unique<SimpleStrategy>();
    Bot bot(std::move(strategy));
    bot.run();
    return 0;
}
