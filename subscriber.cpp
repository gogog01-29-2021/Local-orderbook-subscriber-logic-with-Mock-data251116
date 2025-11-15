#include <iostream>
#include <thread>
#include <vector>
#include <string>
#include <atomic>
#include <map>
#include <chrono> // For timestamps and sleep
#include <algorithm> // For std::sort

// Linux/POSIX-specific for CPU pinning
#ifdef __linux__
#include <pthread.h>
#endif

using namespace std;

// --- 1. 데이터 구조체 정의 ---
// Validator(Task 1)가 KDS로 보낸 Clean Order Book 데이터
struct CleanData {
    uint64_t ts_event; // 이벤트 발생 시각 (Watermark 기준)
    string exchange;

    // (간소화된 오더북: 실제로는 map<price, size> 사용)
    map<double, double> bids;
    map<double, double> asks;

    // (KDS 시뮬레이션을 위한 가짜 데이터)
    static CleanData create_fake(string ex, uint64_t ts) {
        return { ts, ex, {{101.5, 10.0}}, {{102.0, 5.0}} };
    }
};

// Aggregator가 최종 발행(Publish)할 글로벌 스냅샷
struct MergedOrderBook {
    uint64_t window_start_time;
    // [ [price, size, exchange], ... ]
    vector<tuple<double, double, string>> global_bids;
    vector<tuple<double, double, string>> global_asks;
};


// --- 2. 동시성 문제 핸들링: 락프리 SPSC 링 버퍼 ---
// (Reader 1개, Processor 1개가 사용할 버퍼)
template<typename T, size_t Capacity>
class LockFreeSPSCQueue {
public:
    LockFreeSPSCQueue() : write_index_(0), read_index_(0), buffer_(Capacity) {}

    // Reader 스레드(Producer)만 호출
    bool try_push(const T& item) {
        const size_t current_write = write_index_.load(memory_order_relaxed);
        const size_t next_write = (current_write + 1) % Capacity;

        if (next_write == read_index_.load(memory_order_acquire)) {
            // 큐가 꽉 참 (Processor가 느림) //buffersize=1024 -1     read:consumer ,Producer:write index    1)If we drop index:0R Race condition happens
            return false;
        }

        buffer_[current_write] = item;
        write_index_.store(next_write, memory_order_release);
        return true;
    }

    // Processor 스레드(Consumer)만 호출
    bool try_pop(T& item) {
        const size_t current_read = read_index_.load(memory_order_relaxed);

        if (current_read == write_index_.load(memory_order_acquire)) {
            // 큐가 비어있음
            return false;
        }

        item = buffer_[current_read];
        read_index_.store((current_read + 1) % Capacity, memory_order_release);
        return true;
    }

private:
    // C++17: cacheline_size를 하드코딩하는 대신 alignas 사용
    alignas(64) atomic<size_t> write_index_;
    alignas(64) atomic<size_t> read_index_;
    vector<T> buffer_;
};

// --- 3. CPU 피닝 헬퍼 함수 ---
void set_thread_affinity(thread& th, int core_id) {
#ifdef __linux__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset); // 피닝 정보 설정

    int rc = pthread_setaffinity_np(th.native_handle(), sizeof(cpu_set_t), &cpuset); // 실제 피닝
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

// --- 4. 스레드 로직 (Reader & Processor) ---

// KDS Reader 스레드 (I/O-Bound)
// (실제로는 KDS의 3개 샤드를 모두 구독하는 로직이 필요)
void reader_thread_func(
    const string& product_name,
    LockFreeSPSCQueue<CleanData, 1024>& buffer,
    atomic<bool>& running)
{
    cout << "[Reader-" << product_name << "] 시작." << endl;
    // (실제로는 KDS 클라이언트 초기화 및 get_shard_iterator() 호출)

    while (running) {
        // 1. KDS에서 데이터를 배치로 가져옴 (get_records() - 동기적/Blocking)
        // (가짜 데이터 시뮬레이션)
        uint64_t now_ms = chrono::duration_cast<chrono::milliseconds>(
            chrono::system_clock::now().time_since_epoch()).count();

        CleanData fake_binance = CleanData::create_fake("binance", now_ms - 10); // 10ms 지연 가정
        CleanData fake_coinbase = CleanData::create_fake("coinbase", now_ms - 20); // 20ms 지연 가정

        // 2. SPSC 큐(공유 버퍼)에 푸시
        if (!buffer.try_push(fake_binance)) {
            // (에러 처리: 버퍼가 꽉 참. Processor 스레드가 너무 느림)
        }
        if (!buffer.try_push(fake_coinbase)) {
            // ...
        }

        // KDS get_records()는 보통 200ms~1초 간격으로 폴링함
        this_thread::sleep_for(chrono::milliseconds(5));
    }
}

// Processor 스레드 (CPU-Bound)
void processor_thread_func(
    const string& product_name,
    LockFreeSPSCQueue<CleanData, 1024>& buffer,
    atomic<bool>& running)
{
    cout << "[Processor-" << product_name << "] 시작." << endl;

    const uint64_t WINDOW_SIZE_MS = 10;     // 10ms 윈도우
    const uint64_t WATERMARK_DELAY_MS = 5;  // 5ms 지연 허용

    uint64_t max_event_time_seen = 0;
    uint64_t current_window_start = 0;

    // 윈도우별 데이터 버퍼
    map<uint64_t, vector<CleanData>> window_buffers;

    // Aggregator의 현재 오더북 상태 (거래소별)
    map<string, map<double, double>> current_bids;
    map<string, map<double, double>> current_asks;

    while (running) {
        CleanData data;

        // 1. 락프리 버퍼에서 데이터 가져오기 (동시성 핸들링)
        while (buffer.try_pop(data)) {
            // 2. 워터마크 업데이트
            if (data.ts_event > max_event_time_seen) {
                max_event_time_seen = data.ts_event;
            }

            // 3. 윈도우 배정 (10ms 단위)
            uint64_t window = (data.ts_event / WINDOW_SIZE_MS) * WINDOW_SIZE_MS;
            window_buffers[window].push_back(data);
        }

        // 4. 워터마크 계산
        uint64_t watermark = (max_event_time_seen > WATERMARK_DELAY_MS) ? (max_event_time_seen - WATERMARK_DELAY_MS) : 0;

        if (current_window_start == 0 && !window_buffers.empty()) {
            current_window_start = window_buffers.begin()->first;
        }

        // 5. 윈도우 마감 및 처리
        // (워터마크가 현재 윈도우의 끝을 지났는가?)
        while (watermark > current_window_start + WINDOW_SIZE_MS) {

            // --- 윈도우 처리 로직 (10ms마다 실행) ---
            vector<CleanData>& events_to_process = window_buffers[current_window_start];

            // 5a. 이벤트 적용 (메모리 상태 업데이트)
            for (const auto& evt : events_to_process) {
                current_bids[evt.exchange] = evt.bids; // (간소화: 실제로는 merge)
                current_asks[evt.exchange] = evt.asks;
            }

            // 5b. 글로벌 오더북(스냅샷) 생성
            MergedOrderBook snapshot;
            snapshot.window_start_time = current_window_start;

            // (Bids/Asks를 가격순으로 정렬하는 로직...)
            for (auto const& [exchange, book] : current_bids) {
                for (auto const& [price, size] : book) {
                    snapshot.global_bids.emplace_back(price, size, exchange);
                }
            }
            sort(snapshot.global_bids.rbegin(), snapshot.global_bids.rend()); // 가격 내림차순

            // 5c. 최종 발행 (Publish)
            cout << "✅ [Processor-" << product_name << "] Window " << current_window_start << " 발행! (최고 매수가: "
                << get<0>(snapshot.global_bids[0]) << " / " << get<2>(snapshot.global_bids[0]) << ")" << endl;

            // (실제로는 KDS 'merged' 스트림으로 put_record)

            // 5d. 처리 완료된 윈도우 삭제
            window_buffers.erase(current_window_start);
            current_window_start += WINDOW_SIZE_MS;
            // ------------------------------------
        }

        // (CPU Busy-wait 방지. 실제로는 데이터가 없을 때 sleep)
        if (window_buffers.empty()) {
            this_thread::sleep_for(chrono::milliseconds(1));
        }
    }
}


// --- 5. 메인 함수 (스레드 6개 실행 및 피닝) ---
int main() {
    cout << "Aggregator 시작... (6-스레드, 4-코어 비대칭 피닝)" << endl;

    atomic<bool> running(true);

    // 3개의 SPSC 락프리 큐 (공유 버퍼) 생성
    LockFreeSPSCQueue<CleanData, 1024> buffer_abc;
    LockFreeSPSCQueue<CleanData, 1024> buffer_xyz;
    LockFreeSPSCQueue<CleanData, 1024> buffer_foo;

    // --- 스레드 생성 ---
    thread reader_abc(reader_thread_func, "ABC", ref(buffer_abc), ref(running));
    thread reader_xyz(reader_thread_func, "XYZ", ref(buffer_xyz), ref(running));
    thread reader_foo(reader_thread_func, "FOO", ref(buffer_foo), ref(running));

    thread processor_abc(processor_thread_func, "ABC", ref(buffer_abc), ref(running));
    thread processor_xyz(processor_thread_func, "XYZ", ref(buffer_xyz), ref(running));
    thread processor_foo(processor_thread_func, "FOO", ref(buffer_foo), ref(running));

    // --- CPU 피닝 (비대칭) ---
    // (이 코드는 Linux에서만 작동합니다)

    // 3개의 Reader 스레드를 '공유 코어 2'에 피닝
    set_thread_affinity(reader_abc, 2);
    set_thread_affinity(reader_xyz, 2);
    set_thread_affinity(reader_foo, 2);

    // 3개의 Processor 스레드를 '전용 코어 3, 4, 5'에 피닝
    set_thread_affinity(processor_abc, 3);
    set_thread_affinity(processor_xyz, 4);
    set_thread_affinity(processor_foo, 5);


    // 30초간 실행 (테스트용)
    this_thread::sleep_for(chrono::seconds(30));

    running = false; // 스레드 종료 신호

    reader_abc.join();
    reader_xyz.join();
    reader_foo.join();
    processor_abc.join();
    processor_xyz.join();
    processor_foo.join();

    cout << "Aggregator 종료." << endl;
    return 0;
}