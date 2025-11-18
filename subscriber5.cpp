// subscriber5.cpp - AWS Kinesis Consumer with NRT Ring Buffer Architecture
// Consumes orderbook data from Kinesis streams published by the publisher
// Uses the same NRT ring buffer logic as subscriber4.cpp

#include <iostream>
#include <thread>
#include <vector>
#include <string>
#include <atomic>
#include <map>
#include <chrono>
#include <algorithm>
#include <tuple>
#include <memory>
#include <mutex>
#include <sstream>

// AWS SDK includes
#include <aws/core/Aws.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/DescribeStreamRequest.h>

// JSON parsing
#include <nlohmann/json.hpp>

#ifdef _WIN32
#include <windows.h>
#endif

using namespace std;
using json = nlohmann::json;

// --- 1. Data structures (same as subscriber4.cpp) ---
struct CleanData {
    uint64_t ts_event;
    string exchange;
    string symbol;
    int64_t seq;
    map<double, double> bids;
    map<double, double> asks;
};

struct MergedOrderBook {
    uint64_t window_start_time;
    string symbol;
    vector<tuple<double, double, string>> global_bids;
    vector<tuple<double, double, string>> global_asks;
};

// --- 2. NRT Ring Buffer (Reader -> Processor) ---
template<typename T, size_t Capacity>
class NrtRingBuffer {
public:
    NrtRingBuffer() : write_cursor_(0), buffer_(Capacity), capacity_(Capacity) {}

    void push(const T& item) {
        const uint64_t seq = write_cursor_.load(memory_order_relaxed);
        buffer_[seq % capacity_] = item;
        write_cursor_.store(seq + 1, memory_order_release);
    }

    uint64_t get_latest_cursor() const {
        return write_cursor_.load(memory_order_acquire);
    }

    const T& at(uint64_t seq) const {
        return buffer_[seq % capacity_];
    }

private:
    alignas(64) atomic<uint64_t> write_cursor_;
    vector<T> buffer_;
    const size_t capacity_;
};

// --- 3. NRT Latch (Processor -> Publisher) ---
template<typename T>
class NrtLatch {
public:
    NrtLatch() : version_(0) {}

    void store(shared_ptr<T> new_ptr) {
        lock_guard<mutex> lock(mutex_);
        data_ = new_ptr;
        version_.fetch_add(1, memory_order_release);
    }

    shared_ptr<T> load() const {
        lock_guard<mutex> lock(mutex_);
        return data_;
    }

    uint64_t get_version() const {
        return version_.load(memory_order_acquire);
    }

private:
    mutable mutex mutex_;
    shared_ptr<T> data_;
    atomic<uint64_t> version_;
};

// --- 4. CPU Pinning ---
void set_thread_affinity(int core_id) {
#ifdef _WIN32
    if (core_id < 0) return;
    DWORD_PTR mask = (static_cast<DWORD_PTR>(1) << core_id);
    SetThreadAffinityMask(GetCurrentThread(), mask);
    cout << "Pinned thread to CPU core " << core_id << endl;
#endif
}

// --- 5. Kinesis Reader Thread ---
void kinesis_reader_thread(
    const string& stream_name,
    const string& region,
    NrtRingBuffer<CleanData, 4096>& buffer,
    atomic<bool>& running)
{
    cout << "[KinesisReader-" << stream_name << "] Starting..." << endl;

    // Create Kinesis client
    Aws::Client::ClientConfiguration config;
    config.region = region.c_str();
    Aws::Kinesis::KinesisClient kinesis_client(config);

    // Get stream description to find shards
    Aws::Kinesis::Model::DescribeStreamRequest describe_req;
    describe_req.SetStreamName(stream_name.c_str());

    auto describe_outcome = kinesis_client.DescribeStream(describe_req);
    if (!describe_outcome.IsSuccess()) {
        cerr << "[KinesisReader-" << stream_name << "] Failed to describe stream: "
             << describe_outcome.GetError().GetMessage() << endl;
        return;
    }

    auto shards = describe_outcome.GetResult().GetStreamDescription().GetShards();
    if (shards.empty()) {
        cerr << "[KinesisReader-" << stream_name << "] No shards found!" << endl;
        return;
    }

    // Use the first shard (you can extend to multiple shards)
    string shard_id = shards[0].GetShardId();
    cout << "[KinesisReader-" << stream_name << "] Using shard: " << shard_id << endl;

    // Get shard iterator (LATEST - start from newest records)
    Aws::Kinesis::Model::GetShardIteratorRequest iterator_req;
    iterator_req.SetStreamName(stream_name.c_str());
    iterator_req.SetShardId(shard_id.c_str());
    iterator_req.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::LATEST);

    auto iterator_outcome = kinesis_client.GetShardIterator(iterator_req);
    if (!iterator_outcome.IsSuccess()) {
        cerr << "[KinesisReader-" << stream_name << "] Failed to get iterator: "
             << iterator_outcome.GetError().GetMessage() << endl;
        return;
    }

    string shard_iterator = iterator_outcome.GetResult().GetShardIterator();
    cout << "[KinesisReader-" << stream_name << "] Connected successfully!" << endl;

    uint64_t record_count = 0;

    while (running) {
        Aws::Kinesis::Model::GetRecordsRequest records_req;
        records_req.SetShardIterator(shard_iterator.c_str());
        records_req.SetLimit(100);

        auto records_outcome = kinesis_client.GetRecords(records_req);

        if (!records_outcome.IsSuccess()) {
            cerr << "[KinesisReader-" << stream_name << "] Failed to get records: "
                 << records_outcome.GetError().GetMessage() << endl;
            this_thread::sleep_for(chrono::seconds(1));
            continue;
        }

        auto result = records_outcome.GetResult();
        auto records = result.GetRecords();

        // Process each record
        for (const auto& record : records) {
            try {
                // Get the data blob
                const auto& data_blob = record.GetData();
                string json_str(reinterpret_cast<const char*>(data_blob.GetUnderlyingData()),
                              data_blob.GetLength());

                // Parse JSON
                json j = json::parse(json_str);

                CleanData clean;
                clean.exchange = j.value("exchange", "unknown");
                clean.symbol = j.value("symbol", "");
                clean.seq = j.value("seq", 0L);

                // Parse event timestamp (in seconds, convert to ms)
                double event_ts_sec = j.value("event_ts", 0.0);
                clean.ts_event = static_cast<uint64_t>(event_ts_sec * 1000.0);

                // Parse bids
                if (j.contains("bids") && j["bids"].is_array()) {
                    for (const auto& bid : j["bids"]) {
                        if (bid.is_array() && bid.size() >= 2) {
                            double price = bid[0].get<double>();
                            double size = bid[1].get<double>();
                            clean.bids[price] = size;
                        }
                    }
                }

                // Parse asks
                if (j.contains("asks") && j["asks"].is_array()) {
                    for (const auto& ask : j["asks"]) {
                        if (ask.is_array() && ask.size() >= 2) {
                            double price = ask[0].get<double>();
                            double size = ask[1].get<double>();
                            clean.asks[price] = size;
                        }
                    }
                }

                // Push to ring buffer
                buffer.push(clean);
                record_count++;

                if (record_count % 100 == 0) {
                    cout << "[KinesisReader-" << stream_name << "] Processed "
                         << record_count << " records" << endl;
                }

            } catch (const exception& e) {
                cerr << "[KinesisReader-" << stream_name << "] Error parsing record: "
                     << e.what() << endl;
            }
        }

        // Update iterator for next batch
        shard_iterator = result.GetNextShardIterator();

        // If no records, sleep a bit
        if (records.empty()) {
            this_thread::sleep_for(chrono::milliseconds(500));
        }
    }

    cout << "[KinesisReader-" << stream_name << "] Stopped. Total records: "
         << record_count << endl;
}

// --- 6. Processor Thread (same logic as subscriber4.cpp) ---
void processor_thread_func(
    const string& product_name,
    NrtRingBuffer<CleanData, 4096>& input_buffer,
    NrtLatch<MergedOrderBook>& output_latch,
    atomic<bool>& running,
    int core_id)
{
    set_thread_affinity(core_id);
    cout << "[Processor-" << product_name << "] Starting..." << endl;

    const uint64_t WINDOW_SIZE_MS = 100;      // 100ms windows
    const uint64_t WATERMARK_DELAY_MS = 50;   // 50ms delay for late arrivals
    const uint64_t MY_CAPACITY = 1000;

    uint64_t my_last_processed_seq = input_buffer.get_latest_cursor();
    uint64_t max_event_time_seen = 0;
    map<uint64_t, vector<CleanData>> window_buffers;
    map<string, map<double, double>> current_bids;
    map<string, map<double, double>> current_asks;

    while (running) {
        const uint64_t latest_available_seq = input_buffer.get_latest_cursor();

        if (latest_available_seq <= my_last_processed_seq) {
            this_thread::sleep_for(chrono::microseconds(100));
            continue;
        }

        const uint64_t seq_to_end = latest_available_seq;
        const uint64_t pending_items = latest_available_seq - my_last_processed_seq;
        uint64_t seq_to_start = (pending_items > MY_CAPACITY)
                                ? (seq_to_end - MY_CAPACITY + 1)
                                : (my_last_processed_seq + 1);

        // Read new events
        for (uint64_t i = seq_to_start; i <= seq_to_end; i++) {
            const CleanData& data = input_buffer.at(i);
            if (data.ts_event > max_event_time_seen) {
                max_event_time_seen = data.ts_event;
            }
            uint64_t window = (data.ts_event / WINDOW_SIZE_MS) * WINDOW_SIZE_MS;
            window_buffers[window].push_back(data);
        }
        my_last_processed_seq = seq_to_end;

        // Process completed windows
        uint64_t watermark = (max_event_time_seen > WATERMARK_DELAY_MS)
                           ? (max_event_time_seen - WATERMARK_DELAY_MS) : 0;

        while (!window_buffers.empty()) {
            auto it = window_buffers.begin();
            uint64_t window_to_process = it->first;
            uint64_t window_end_time = window_to_process + WINDOW_SIZE_MS;

            if (watermark < window_end_time) {
                break;
            }

            vector<CleanData>& events_to_process = it->second;
            for (const auto& evt : events_to_process) {
                current_bids[evt.exchange] = evt.bids;
                current_asks[evt.exchange] = evt.asks;
            }

            // Create merged snapshot
            auto snapshot_ptr = make_shared<MergedOrderBook>();
            snapshot_ptr->window_start_time = window_to_process;
            snapshot_ptr->symbol = product_name;

            for (auto const& [ex, book] : current_bids) {
                for (auto const& [p, s] : book) {
                    snapshot_ptr->global_bids.emplace_back(p, s, ex);
                }
            }
            sort(snapshot_ptr->global_bids.rbegin(), snapshot_ptr->global_bids.rend());

            for (auto const& [ex, book] : current_asks) {
                for (auto const& [p, s] : book) {
                    snapshot_ptr->global_asks.emplace_back(p, s, ex);
                }
            }
            sort(snapshot_ptr->global_asks.begin(), snapshot_ptr->global_asks.end());

            // Store snapshot
            if (!snapshot_ptr->global_bids.empty() || !snapshot_ptr->global_asks.empty()) {
                output_latch.store(snapshot_ptr);
            }

            window_buffers.erase(it);
        }
    }
}

// --- 7. Publisher Thread (displays merged orderbook) ---
void publisher_thread_func(
    const string& product_name,
    NrtLatch<MergedOrderBook>& input_latch,
    atomic<bool>& running,
    int core_id)
{
    set_thread_affinity(core_id);
    cout << "[Publisher-" << product_name << "] Starting..." << endl;

    uint64_t last_processed_version = 0;

    while (running) {
        uint64_t current_version = input_latch.get_version();

        if (current_version > last_processed_version) {
            auto current_ptr = input_latch.load();

            if (current_ptr) {
                cout << "[Publisher-" << product_name << "] Window "
                     << current_ptr->window_start_time << " ms (";

                if (!current_ptr->global_bids.empty()) {
                    auto [price, size, exchange] = current_ptr->global_bids[0];
                    cout << "Best Bid: " << price << " (" << exchange << ")";
                } else {
                    cout << "Best Bid: N/A";
                }

                cout << " / ";

                if (!current_ptr->global_asks.empty()) {
                    auto [price, size, exchange] = current_ptr->global_asks[0];
                    cout << "Best Ask: " << price << " (" << exchange << ")";
                } else {
                    cout << "Best Ask: N/A";
                }

                cout << ")" << endl;

                last_processed_version = current_version;
            }
        }

        this_thread::sleep_for(chrono::milliseconds(100));
    }
}

// --- 8. Main ---
int main() {
    cout << "=== AWS Kinesis Subscriber (NRT Architecture) ===" << endl;
    cout << "Consuming from 3 Kinesis streams:" << endl;
    cout << "  - orderbook-binance" << endl;
    cout << "  - orderbook-bybit" << endl;
    cout << "  - orderbook-okx" << endl;

    // Initialize AWS SDK
    Aws::SDKOptions aws_options;
    Aws::InitAPI(aws_options);

    {
        atomic<bool> running(true);
        const string region = "ap-northeast-2";

        // Create ring buffers for each stream
        NrtRingBuffer<CleanData, 4096> buffer_binance;
        NrtRingBuffer<CleanData, 4096> buffer_bybit;
        NrtRingBuffer<CleanData, 4096> buffer_okx;

        // Create latches for each product (we'll track BTCUSDT from all exchanges)
        NrtLatch<MergedOrderBook> latch_btc;
        NrtLatch<MergedOrderBook> latch_eth;
        NrtLatch<MergedOrderBook> latch_sol;

        // Start Kinesis reader threads
        thread reader_binance(kinesis_reader_thread, "orderbook-binance", region,
                            ref(buffer_binance), ref(running));
        thread reader_bybit(kinesis_reader_thread, "orderbook-bybit", region,
                          ref(buffer_bybit), ref(running));
        thread reader_okx(kinesis_reader_thread, "orderbook-okx", region,
                        ref(buffer_okx), ref(running));

        // Start processor threads (one per symbol)
        thread processor_btc(processor_thread_func, "BTCUSDT",
                           ref(buffer_binance), ref(latch_btc), ref(running), 3);
        thread processor_eth(processor_thread_func, "ETHUSDT",
                           ref(buffer_bybit), ref(latch_eth), ref(running), 4);
        thread processor_sol(processor_thread_func, "SOLUSDT",
                           ref(buffer_okx), ref(latch_sol), ref(running), 5);

        // Start publisher threads
        thread publisher_btc(publisher_thread_func, "BTCUSDT",
                           ref(latch_btc), ref(running), 6);
        thread publisher_eth(publisher_thread_func, "ETHUSDT",
                           ref(latch_eth), ref(running), 7);
        thread publisher_sol(publisher_thread_func, "SOLUSDT",
                           ref(latch_sol), ref(running), 8);

        cout << "\nPress Ctrl+C to stop..." << endl;

        // Run for 60 seconds (or until interrupted)
        this_thread::sleep_for(chrono::seconds(60));

        running = false;

        // Join all threads
        reader_binance.join();
        reader_bybit.join();
        reader_okx.join();
        processor_btc.join();
        processor_eth.join();
        processor_sol.join();
        publisher_btc.join();
        publisher_eth.join();
        publisher_sol.join();

        cout << "Aggregator stopped." << endl;
    }

    Aws::ShutdownAPI(aws_options);
    return 0;
}
