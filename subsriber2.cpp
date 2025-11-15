#include <iostream>
#include <thread>
#include <vector>
#include <string>
#include <atomic>
#include <map>
#include <chrono>
#include <algorithm>
#include <fstream>
#include <mutex>
#include <iomanip>

// Linux/POSIX-specific for CPU pinning
#ifdef __linux__
#include <pthread.h>
#endif

using namespace std;

// === Configuration Parameters ===
struct Config {
    size_t RING_BUFFER_CAPACITY = 1024;
    uint64_t WINDOW_SIZE_MS = 10;
    uint64_t WATERMARK_DELAY_MS = 5;
    uint64_t PROCESSOR_CAPACITY = 200;
    uint64_t TEST_DURATION_SEC = 30;
    bool ENABLE_LOGGING = false;  // Disabled for Windows compatibility
    string LOG_FILE = "performance.log";
};

// Global config
Config g_config;

// === Performance Statistics ===
struct PerformanceStats {
    atomic<uint64_t> total_windows_published{0};
    atomic<uint64_t> total_events_processed{0};
    atomic<uint64_t> total_events_skipped{0};
    chrono::steady_clock::time_point start_time{};
    mutex log_mutex;
    ofstream* log_file{nullptr};

    PerformanceStats() = default;
    ~PerformanceStats() {
        if (log_file && log_file->is_open()) {
            log_file->close();
            delete log_file;
        }
    }

    void start() {
        start_time = chrono::steady_clock::now();
        if (g_config.ENABLE_LOGGING) {
            log_file = new ofstream(g_config.LOG_FILE, ios::out);
            *log_file << "timestamp_ms,thread,event,value,details\n";
        }
    }

    void log(const string& thread_name, const string& event, uint64_t value, const string& details = "") {
        if (!g_config.ENABLE_LOGGING || !log_file) return;

        lock_guard<mutex> lock(log_mutex);
        auto now = chrono::steady_clock::now();
        auto elapsed_ms = chrono::duration_cast<chrono::milliseconds>(now - start_time).count();

        *log_file << elapsed_ms << "," << thread_name << "," << event << ","
                 << value << "," << details << "\n";
    }

    void print_summary() {
        auto end_time = chrono::steady_clock::now();
        auto duration_sec = chrono::duration_cast<chrono::seconds>(end_time - start_time).count();

        cout << "\n=== Performance Summary ===\n";
        cout << "Test Duration: " << duration_sec << " seconds\n";
        cout << "Ring Buffer Capacity: " << g_config.RING_BUFFER_CAPACITY << "\n";
        cout << "Window Size: " << g_config.WINDOW_SIZE_MS << " ms\n";
        cout << "Watermark Delay: " << g_config.WATERMARK_DELAY_MS << " ms\n";
        cout << "Processor Capacity: " << g_config.PROCESSOR_CAPACITY << "\n";
        cout << "Total Windows Published: " << total_windows_published.load() << "\n";
        cout << "Total Events Processed: " << total_events_processed.load() << "\n";
        cout << "Total Events Skipped: " << total_events_skipped.load() << "\n";
        cout << "Throughput: " << (total_windows_published.load() / (double)duration_sec) << " windows/sec\n";
        cout << "Event Processing Rate: " << (total_events_processed.load() / (double)duration_sec) << " events/sec\n";

        if (g_config.ENABLE_LOGGING && log_file) {
            log_file->flush();
            log_file->close();
            cout << "Performance log saved to: " << g_config.LOG_FILE << "\n";
        }
    }
};

PerformanceStats g_stats;

// === Processor Statistics (for latency tracking) ===
struct ProcStats {
    uint64_t total_snapshots = 0;
    uint64_t latency_sum = 0;
    uint64_t latency_min = UINT64_MAX;
    uint64_t latency_max = 0;
};

// --- 1. Data structure definitions ---
// Clean Order Book data sent by Validator(Task 1) to KDS
struct CleanData {
    uint64_t ts_event; // Event occurrence time (Watermark basis)
    string exchange;

    // (Simplified orderbook: actually uses map<price, size>)
    map<double, double> bids;
    map<double, double> asks;

    // (Fake data for KDS simulation)
    static CleanData create_fake(string ex, uint64_t ts) {
        return { ts, ex, {{101.5, 10.0}}, {{102.0, 5.0}} };
    }
};

// Global snapshot to be published by Aggregator
struct MergedOrderBook {
    uint64_t window_start_time;
    // [ [price, size, exchange], ... ]
    vector<tuple<double, double, string>> global_bids;
    vector<tuple<double, double, string>> global_asks;
};


// --- 2. NRT Ring Buffer ---
template<typename T>
class NrtRingBuffer {
public:
    // Capacity should be a power of 2 (e.g., 1024)
    NrtRingBuffer(size_t capacity) : write_cursor_(0), buffer_(capacity), capacity_(capacity) {}

    // Called only by Reader thread (Producer)
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

    // Rarely needed as data is typically not modified
    T& at(uint64_t seq) {
        return buffer_[seq % capacity_];
    }

private:
    // C++17: use alignas instead of hardcoding cacheline_size
    // uint64_t overflow would take ~500 years
    alignas(64) atomic<uint64_t> write_cursor_;

    vector<T> buffer_;
    const size_t capacity_;
};

// --- 3. CPU pinning helper function ---
void set_thread_affinity(thread& th, int core_id) {
#ifdef __linux__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset); // Set pinning info

    int rc = pthread_setaffinity_np(th.native_handle(), sizeof(cpu_set_t), &cpuset); // Actual pinning
    if (rc != 0) {
        cerr << "Error setting thread affinity for core " << core_id << ": " << rc << endl;
    }
    else {
        cout << "Successfully pinned thread " << th.get_id() << " to CPU core " << core_id << endl;
    }
#else
    cout << "Warning: CPU pinning is only supported on Linux. Thread " << th.get_id() << " is NOT pinned." << endl;
#endif
}

// --- 4. Thread logic (Reader & Processor) ---
void reader_thread_func(
    const string& product_name,
    NrtRingBuffer<CleanData>& buffer,
    atomic<bool>& running)
{
    cout << "[Reader-" << product_name << "] started." << endl;

    uint64_t events_pushed = 0;
    while (running) {
        // 1. Fetch data from KDS (simulation)
        uint64_t now_ms = chrono::duration_cast<chrono::milliseconds>(
            chrono::system_clock::now().time_since_epoch()).count();

        CleanData fake_binance = CleanData::create_fake("binance", now_ms - 10);
        CleanData fake_coinbase = CleanData::create_fake("coinbase", now_ms - 20);

        buffer.push(fake_binance);
        buffer.push(fake_coinbase);
        events_pushed += 2;

        // Intentional time interval (5ms)
        this_thread::sleep_for(chrono::milliseconds(5));
    }

    g_stats.log(product_name, "reader_total_pushed", events_pushed);
}

void processor_thread_func(
    const string& product_name,
    NrtRingBuffer<CleanData>& buffer,
    atomic<bool>& running,
    ProcStats& stats)
{
    cout << "[Processor-" << product_name << "] started." << endl;

    uint64_t my_last_processed_seq = buffer.get_latest_cursor();

    // --- Window logic variables ---
    uint64_t max_event_time_seen = 0;
    uint64_t current_window_start = 0;
    map<uint64_t, vector<CleanData>> window_buffers;
    map<string, map<double, double>> current_bids;
    map<string, map<double, double>> current_asks;

    uint64_t windows_published = 0;
    uint64_t events_processed = 0;
    uint64_t events_skipped = 0;

    while (running) {
        const uint64_t latest_available_seq = buffer.get_latest_cursor();

        if (latest_available_seq <= my_last_processed_seq) {
            // Nothing to process (busy waiting)
            continue;
        }

        // "Skip" logic
        const uint64_t seq_to_end = latest_available_seq;
        uint64_t seq_to_start = my_last_processed_seq + 1;

        seq_to_start = max(seq_to_start,
                           (seq_to_end > g_config.PROCESSOR_CAPACITY) ? (seq_to_end - g_config.PROCESSOR_CAPACITY + 1) : 0);

        // Count skipped events
        if (seq_to_start > my_last_processed_seq + 1) {
            uint64_t skipped = seq_to_start - (my_last_processed_seq + 1);
            events_skipped += skipped;
            g_stats.total_events_skipped.fetch_add(skipped);
        }

        // Process batch
        for (uint64_t i = seq_to_start; i <= seq_to_end; i++) {
            const CleanData& data = buffer.at(i);
            events_processed++;

            // 2. Update watermark
            if (data.ts_event > max_event_time_seen) {
                max_event_time_seen = data.ts_event;
            }

            // 3. Assign to window (configurable window size)
            uint64_t window = (data.ts_event / g_config.WINDOW_SIZE_MS) * g_config.WINDOW_SIZE_MS;
            window_buffers[window].push_back(data);
        }

        my_last_processed_seq = seq_to_end;

        // 4. Calculate watermark
        uint64_t watermark = (max_event_time_seen > g_config.WATERMARK_DELAY_MS) ?
                             (max_event_time_seen - g_config.WATERMARK_DELAY_MS) : 0;

        if (current_window_start == 0 && !window_buffers.empty()) {
            current_window_start = window_buffers.begin()->first;
        }

        // 5. Close and process windows
        while (watermark > current_window_start + g_config.WINDOW_SIZE_MS) {

            vector<CleanData>& events_to_process = window_buffers[current_window_start];

            // 5a. Apply events (update memory state - latch effect)
            for (const auto& evt : events_to_process) {
                current_bids[evt.exchange] = evt.bids; // (Overwritten with latest data)
                current_asks[evt.exchange] = evt.asks;
            }

            // 5b. Create global orderbook (snapshot)
            MergedOrderBook snapshot;
            snapshot.window_start_time = current_window_start;

            for (auto const& [exchange, book] : current_bids) {
                for (auto const& [price, size] : book) {
                    snapshot.global_bids.emplace_back(price, size, exchange);
                }
            }
            sort(snapshot.global_bids.rbegin(), snapshot.global_bids.rend());

            // 5c. Final publish + Latency measurement
            if (!snapshot.global_bids.empty()) {
                // Publish timestamp
                uint64_t publish_time_ms = chrono::duration_cast<chrono::milliseconds>(
                    chrono::system_clock::now().time_since_epoch()).count();

                // Latency = now - window_start_time
                uint64_t latency = publish_time_ms - snapshot.window_start_time;

                // Update stats
                stats.total_snapshots++;
                stats.latency_sum += latency;
                stats.latency_min = min(stats.latency_min, latency);
                stats.latency_max = max(stats.latency_max, latency);

                windows_published++;
                g_stats.total_windows_published.fetch_add(1);

                cout << "✓ [Processor-" << product_name << "] Window " << current_window_start
                     << " latency=" << latency << "ms"
                     << " (Top bid: " << get<0>(snapshot.global_bids[0])
                     << " / " << get<2>(snapshot.global_bids[0]) << ")" << endl;

                g_stats.log(product_name, "window_published", current_window_start,
                           "latency=" + to_string(latency) + "ms,top_bid=" + to_string(get<0>(snapshot.global_bids[0])));
            }

            // 5d. Delete processed window
            window_buffers.erase(current_window_start);
            current_window_start += g_config.WINDOW_SIZE_MS;
        }

        // (Prevent CPU busy-wait)
        if (window_buffers.empty()) {
            this_thread::sleep_for(chrono::milliseconds(1));
        }
    }

    g_stats.total_events_processed.fetch_add(events_processed);
    g_stats.log(product_name, "processor_events_processed", events_processed);
    g_stats.log(product_name, "processor_events_skipped", events_skipped);
    g_stats.log(product_name, "processor_windows_published", windows_published);
}


// --- 5. Main function (run 6 threads with pinning) ---
int main(int argc, char* argv[]) {
    // Parse command-line arguments
    if (argc > 1) g_config.RING_BUFFER_CAPACITY = stoull(argv[1]);
    if (argc > 2) g_config.WINDOW_SIZE_MS = stoull(argv[2]);
    if (argc > 3) g_config.WATERMARK_DELAY_MS = stoull(argv[3]);
    if (argc > 4) g_config.PROCESSOR_CAPACITY = stoull(argv[4]);
    if (argc > 5) g_config.TEST_DURATION_SEC = stoull(argv[5]);

    cout << "Aggregator starting... (LMAX NRT pattern, 4-core asymmetric pinning)" << endl;
    cout << "Config: buffer=" << g_config.RING_BUFFER_CAPACITY
         << " window=" << g_config.WINDOW_SIZE_MS << "ms"
         << " watermark=" << g_config.WATERMARK_DELAY_MS << "ms"
         << " proc_cap=" << g_config.PROCESSOR_CAPACITY
         << " duration=" << g_config.TEST_DURATION_SEC << "s" << endl;
    cout.flush();

    g_stats.start();
    cout << "Stats initialized" << endl;
    cout.flush();
    atomic<bool> running(true);

    // Create stats structures for latency tracking
    ProcStats stats_abc, stats_xyz, stats_foo;

    // Create 3 NRT ring buffers (shared buffers)
    cout << "Creating buffers..." << endl;
    NrtRingBuffer<CleanData> buffer_abc(g_config.RING_BUFFER_CAPACITY);
    NrtRingBuffer<CleanData> buffer_xyz(g_config.RING_BUFFER_CAPACITY);
    NrtRingBuffer<CleanData> buffer_foo(g_config.RING_BUFFER_CAPACITY);
    cout << "Buffers created" << endl;
    cout.flush();

    // --- Create threads ---//I.O Intensive                             //CPU Intensive
    cout << "Creating threads..." << endl;
    thread reader_abc(reader_thread_func, "ABC", ref(buffer_abc), ref(running));
    thread reader_xyz(reader_thread_func, "XYZ", ref(buffer_xyz), ref(running));
    thread reader_foo(reader_thread_func, "FOO", ref(buffer_foo), ref(running));

    thread processor_abc(processor_thread_func, "ABC", ref(buffer_abc), ref(running), ref(stats_abc));
    thread processor_xyz(processor_thread_func, "XYZ", ref(buffer_xyz), ref(running), ref(stats_xyz));
    thread processor_foo(processor_thread_func, "FOO", ref(buffer_foo), ref(running), ref(stats_foo));

    // --- CPU pinning ---
    set_thread_affinity(reader_abc, 2); //Because three readers are I.O intensive, pin them to same core
    set_thread_affinity(reader_xyz, 2);
    set_thread_affinity(reader_foo, 2);

    set_thread_affinity(processor_abc, 3);
    set_thread_affinity(processor_xyz, 4);
    set_thread_affinity(processor_foo, 5);

    // Run for configured duration (test)
    this_thread::sleep_for(chrono::seconds(g_config.TEST_DURATION_SEC));

    running = false; // Signal threads to terminate

    reader_abc.join();
    reader_xyz.join();
    reader_foo.join();
    processor_abc.join();
    processor_xyz.join();
    processor_foo.join();

    cout << "Aggregator terminated." << endl;

    // Print latency statistics for each processor
    auto print_stats = [](const string& name, const ProcStats& s) {
        cout << "\n=== Latency Stats for " << name << " ===\n";
        cout << "Total snapshots : " << s.total_snapshots << "\n";

        if (s.total_snapshots > 0) {
            double avg = static_cast<double>(s.latency_sum) / s.total_snapshots;
            cout << "Latency avg (ms): " << fixed << setprecision(2) << avg << "\n";
            cout << "Latency min (ms): " << s.latency_min << "\n";
            cout << "Latency max (ms): " << s.latency_max << "\n";
        }
    };

    print_stats("ABC", stats_abc);
    print_stats("XYZ", stats_xyz);
    print_stats("FOO", stats_foo);

    g_stats.print_summary();

    return 0;
}
