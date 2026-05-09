*
 * AlgoTrade 2026 — History Recorder (C++20)
 *
 * Light-weight bot that DOES NOT TRADE — it only connects to all
 * exchanges and records every market_data_update to CSV files.
 * Run during testing rounds; analyze the CSVs offline (Python /
 * pandas / Excel) to look for patterns:
 *
 *   - Is SIMP a sinusoid? (FFT, autocorrelation)
 *   - Is CARD a random walk? (variance ratio test)
 *   - What's the typical MM spread per instrument?
 *   - How quickly do prices propagate across venues?
 *
 * Output (per exchange, in --output-dir, default ./market_data):
 *   <EXCH>_orderbook.csv  — top-of-book every tick
 *   <EXCH>_trades.csv     — every trade event
 *   <EXCH>_candles.csv    — every completed candle
 *
 * Build:
 *   g++ -std=c++20 -O2 -o recorder recorder.cpp -lpthread
 *
 * Run:
 *   ./recorder
 *   EXCHANGES="NYSE,ZSE" OUTPUT_DIR="./data" ./recorder
 */

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
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
namespace fs        = std::filesystem;

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

static int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// --- One CSV writer per exchange ---------------------------------------
class Recorder {
public:
    Recorder(const std::string& exch, const fs::path& dir)
        : exch_(exch)
    {
        fs::create_directories(dir);
        auto ts = std::to_string(now_ms());
        ob_     = std::ofstream(dir / (exch + "_orderbook_" + ts + ".csv"));
        trades_ = std::ofstream(dir / (exch + "_trades_"    + ts + ".csv"));
        candles_= std::ofstream(dir / (exch + "_candles_"   + ts + ".csv"));
        cancels_= std::ofstream(dir / (exch + "_cancels_"   + ts + ".csv"));

        ob_     << "wall_ms,server_ms,instrument,bid1,bid1q,bid2,bid2q,bid3,bid3q,"
                   "ask1,ask1q,ask2,ask2q,ask3,ask3q\n";
        trades_ << "wall_ms,server_ms,instrument,price,quantity,passive_id,active_id\n";
        candles_<< "wall_ms,instrument,index,open,high,low,close,volume\n";
        cancels_<< "wall_ms,server_ms,instrument,order_id,expired\n";
    }

    void on_market_data(const json& m) {
        int64_t srv = m.value("time", (int64_t)0);
        int64_t wall = now_ms();

        if (m.contains("orderbook_depths")) {
            for (auto& [inst, ob] : m["orderbook_depths"].items()) {
                write_ob(wall, srv, inst, ob);
            }
        }
        if (m.contains("events")) {
            for (auto& e : m["events"]) {
                auto et = e.value("event_type","");
                if (!e.contains("data")) continue;
                auto& d = e["data"];
                if (et == "trade") {
                    trades_ << wall << ',' << d.value("time",(int64_t)0) << ','
                            << d.value("instrumentID","") << ','
                            << d.value("price",0) << ','
                            << d.value("quantity",0) << ','
                            << d.value("passiveOrderID",(int64_t)0) << ','
                            << d.value("activeOrderID",(int64_t)0) << '\n';
                } else if (et == "cancel") {
                    cancels_ << wall << ',' << d.value("time",(int64_t)0) << ','
                             << d.value("instrumentID","") << ','
                             << d.value("orderID",(int64_t)0) << ','
                             << (d.value("expired",false) ? 1 : 0) << '\n';
                }
            }
        }
        if (m.contains("candles") && m["candles"].contains("tradeable")) {
            for (auto& [inst, arr] : m["candles"]["tradeable"].items()) {
                for (auto& c : arr) {
                    candles_ << wall << ',' << inst << ','
                             << c.value("index",0) << ','
                             << c.value("open",0) << ','
                             << c.value("high",0) << ','
                             << c.value("low",0) << ','
                             << c.value("close",0) << ','
                             << c.value("volume",0) << '\n';
                }
            }
        }
        // Periodic flush so we don't lose data if killed
        if (++flush_ctr_ % 50 == 0) { ob_.flush(); trades_.flush(); candles_.flush(); cancels_.flush(); }
    }

private:
    void write_ob(int64_t wall, int64_t srv, const std::string& inst, const json& ob) {
        // Top 3 levels each side
        std::vector<std::pair<int,int>> bids, asks;
        if (ob.contains("bids") && ob["bids"].is_object())
            for (auto& [p,q] : ob["bids"].items())
                bids.push_back({std::stoi(p), q.get<int>()});
        if (ob.contains("asks") && ob["asks"].is_object())
            for (auto& [p,q] : ob["asks"].items())
                asks.push_back({std::stoi(p), q.get<int>()});
        std::sort(bids.begin(), bids.end(), [](auto&a,auto&b){return a.first>b.first;});
        std::sort(asks.begin(), asks.end(), [](auto&a,auto&b){return a.first<b.first;});

        ob_ << wall << ',' << srv << ',' << inst;
        for (int i = 0; i < 3; ++i) {
            if (i < (int)bids.size()) ob_ << ',' << bids[i].first << ',' << bids[i].second;
            else                      ob_ << ",,";
        }
        for (int i = 0; i < 3; ++i) {
            if (i < (int)asks.size()) ob_ << ',' << asks[i].first << ',' << asks[i].second;
            else                      ob_ << ",,";
        }
        ob_ << '\n';
    }

    std::string exch_;
    std::ofstream ob_, trades_, candles_, cancels_;
    int flush_ctr_ = 0;
};

// --- Per-exchange WebSocket loop ---------------------------------------
static std::atomic<bool> g_running{true};

static void run_exchange(const std::string& name, const std::string& host,
                         const fs::path& outdir)
{
    Recorder rec(name, outdir);
    int backoff = 1;

    while (g_running) {
        try {
            net::io_context ioc;
            websocket::stream<tcp::socket> ws(ioc);
            tcp::resolver r(ioc);
            net::connect(ws.next_layer(), r.resolve(host, std::to_string(EXCHANGE_PORT)));
            ws.handshake(host+":"+std::to_string(EXCHANGE_PORT), "/trade");

            beast::flat_buffer buf;
            ws.read(buf);  // welcome
            std::cout << "[" << name << "] connected\n";
            backoff = 1;

            while (g_running) {
                buf.consume(buf.size());
                ws.read(buf);
                auto msg = json::parse(beast::buffers_to_string(buf.data()));
                auto type = msg.value("type","");
                if (type == "market_data_update") rec.on_market_data(msg);
                else if (type == "end_of_round") {
                    std::cout << "[" << name << "] end_of_round, will reconnect\n";
                    break;
                }
            }
            try { ws.close(websocket::close_code::normal); } catch (...) {}
        } catch (const std::exception& e) {
            std::cerr << "[" << name << "] " << e.what() << "\n";
        }
        std::this_thread::sleep_for(std::chrono::seconds(backoff));
        backoff = std::min(backoff*2, 8);
    }
}

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

    fs::path outdir = "./market_data";
    if (const char* v = std::getenv("OUTPUT_DIR")) outdir = v;

    std::cout << "Recording to " << outdir << " for exchanges:";
    for (auto& e : exchanges) std::cout << " " << e;
    std::cout << "\nPress Ctrl+C to stop.\n";

    std::vector<std::thread> threads;
    for (auto& name : exchanges) {
        auto host = lookup_host(name);
        if (!host) { std::cerr << "unknown: " << name << "\n"; continue; }
        threads.emplace_back([name, h=*host, outdir]{ run_exchange(name, h, outdir); });
    }
    for (auto& t : threads) t.join();
    return 0;
}