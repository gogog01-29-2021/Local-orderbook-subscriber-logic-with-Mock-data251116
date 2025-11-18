#include <iostream>
#include <thread>
#include <vector>
#include <string>
#include <atomic>
#include <map>
#include <chrono>
#include <algorithm>
#include <tuple>
#include <cmath>
#include <memory> // For std::shared_ptr

// Linux/POSIX-specific for CPU pinning
#ifdef __linux__
#include <pthread.h>
#endif

using namespace std;

// --- 1. 데이터 구조체 정의 (변경 없음) ---
struct CleanData {
   uint64_t ts_event;
   string exchange;
   map<double, double> bids;
   map<double, double> asks;
   static CleanData create_fake(string ex, uint64_t ts) {
       return { ts, ex, {{101.5, 10.0}}, {{102.0, 5.0}} };
   }
};

struct MergedOrderBook {
   uint64_t window_start_time;
   vector<tuple<double, double, string>> global_bids;
   vector<tuple<double, double, string>> global_asks;
};


// --- 2. NRT 링 버퍼 (Reader -> Processor) (변경 없음) ---
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


// --- 3. NRT 래치 (Processor -> Publisher) (변경 없음) ---
template<typename T>
class NrtLatch {
public:
   NrtLatch() : data_ptr_(nullptr) {}
   void store(shared_ptr<T> new_ptr) {
       data_ptr_.store(new_ptr, memory_order_release);
   }
   shared_ptr<T> load() const {
       return data_ptr_.load(memory_order_acquire);
   }
private:
   atomic<shared_ptr<T>> data_ptr_;
};


// --- 4. CPU 피닝 헬퍼 함수 (변경 없음) ---
void set_thread_affinity(thread& th, int core_id) {
#ifdef __linux__
   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(core_id, &cpuset);
   int rc = pthread_setaffinity_np(th.native_handle(), sizeof(cpu_set_t), &cpuset);
   if (rc != 0) {
       cerr << "Error setting thread affinity for core " << core_id << ": " << rc << endl;
   } else {
       cout << "Successfully pinned thread " << th.get_id() << " to CPU core " << core_id << endl;
   }
#else
   cout << "Warning: CPU pinning is only supported on Linux. Thread " << th.get_id() << " is NOT pinned." << endl;
#endif
}

// --- 5. 스레드 로직 (Reader, Processor, Publisher) ---

// KDS Reader 스레드 (I/O-Bound) (변경 없음)
void reader_thread_func(
   const string& product_name,
   NrtRingBuffer<CleanData, 1024>& buffer,
   atomic<bool>& running)
{
   while (running) {
       uint64_t now_ms = chrono::duration_cast<chrono::milliseconds>(
           chrono::system_clock::now().time_since_epoch()).count();
       CleanData fake_binance = CleanData::create_fake("binance", now_ms - 10);
       buffer.push(fake_binance);
       CleanData fake_coinbase = CleanData::create_fake("coinbase", now_ms - 20);
       buffer.push(fake_coinbase);
       // KDS I/O 대기 시뮬레이션
       this_thread::sleep_for(chrono::milliseconds(5));
   }
}

// Processor 스레드 (CPU-Bound) (변경 없음)
void processor_thread_func(
   const string& product_name,
   NrtRingBuffer<CleanData, 1024>& input_buffer,
   NrtLatch<MergedOrderBook>& output_latch,
   atomic<bool>& running)
{
   cout << "[Processor-" << product_name << "] 시작." << endl;
   const uint64_t WINDOW_SIZE_MS = 10;
   const uint64_t WATERMARK_DELAY_MS = 5;
   const uint64_t MY_CAPACITY = 200;

   uint64_t my_last_processed_seq = input_buffer.get_latest_cursor();
   uint64_t max_event_time_seen = 0;
   map<uint64_t, vector<CleanData>> window_buffers;
   map<string, map<double, double>> current_bids;
   map<string, map<double, double>> current_asks;

   while (running) {
       // --- 1. NRT 배치 읽기 ---
       const uint64_t latest_available_seq = input_buffer.get_latest_cursor();
       const uint64_t pending_items = latest_available_seq - my_last_processed_seq;

       // [수정됨] 오버플로우는 고려하지 않음 (코드 단순성)
       if (latest_available_seq <= my_last_processed_seq) {
           continue; // Busy-waiting
       }

       const uint64_t seq_to_end = latest_available_seq;
       // [수정됨] 불필요한 BUFFER_CAPACITY 검사 제거됨
       uint64_t seq_to_start = (pending_items > MY_CAPACITY)
                               ? (seq_to_end - MY_CAPACITY + 1)
                               : (my_last_processed_seq + 1);    
       
       for (uint64_t i = seq_to_start; i <= seq_to_end; i++) {
           const CleanData& data = input_buffer.at(i);
           if (data.ts_event > max_event_time_seen) {
               max_event_time_seen = data.ts_event;

           }
           uint64_t window = (data.ts_event / WINDOW_SIZE_MS) * WINDOW_SIZE_MS;
           window_buffers[window].push_back(data);
       }
       my_last_processed_seq = seq_to_end;

       // --- 2. 윈도우 마감 및 처리 ---
       uint64_t watermark = (max_event_time_seen > WATERMARK_DELAY_MS) ? (max_event_time_seen - WATERMARK_DELAY_MS) : 0;

       while (!window_buffers.empty()) {
           auto it = window_buffers.begin();
           uint64_t window_to_process = it->first;
           uint64_t window_end_time = window_to_process + WINDOW_SIZE_MS;

           // [수정됨] "아직 마감되지 않은" 윈도우를 만나면 중단
           // (watermark가 window_end보다 작으면, 아직 닫힐 때가 아님)
           if (watermark < window_end_time) {
               break;
           }
           
           vector<CleanData>& events_to_process = it->second;
           for (const auto& evt : events_to_process) {
               current_bids[evt.exchange] = evt.bids;
               current_asks[evt.exchange] = evt.asks;
           }

           // [수정됨] 래치에 저장할 스냅샷 생성
           auto snapshot_ptr = make_shared<MergedOrderBook>();
           snapshot_ptr->window_start_time = window_to_process;
           
           for (auto const& [ex, book] : current_bids) {
               for (auto const& [p, s] : book) snapshot_ptr->global_bids.emplace_back(p, s, ex);
           }
           sort(snapshot_ptr->global_bids.rbegin(), snapshot_ptr->global_bids.rend());
           for (auto const& [ex, book] : current_asks) {
               for (auto const& [p, s] : book) snapshot_ptr->global_asks.emplace_back(p, s, ex);
           }
           sort(snapshot_ptr->global_asks.begin(), snapshot_ptr->global_asks.end());

           // 래치 덮어쓰기
           if (!snapshot_ptr->global_bids.empty() || !snapshot_ptr->global_asks.empty()) {
               output_latch.store(snapshot_ptr);
           }
     
     
           window_buffers.erase(it);
       }
   }
}

// Publisher 스레드 (I/O-Bound) (변경 없음)
void publisher_thread_func(
   const string& product_name,
   NrtLatch<MergedOrderBook>& input_latch,
   atomic<bool>& running)
{
   cout << "[Publisher-" << product_name << "] 시작." << endl;
   shared_ptr<MergedOrderBook> last_processed_ptr = nullptr;

   while (running) {
       auto current_ptr = input_latch.load();

       if (current_ptr && current_ptr != last_processed_ptr) {
           
           cout << "? [Publisher-" << product_name << "] Window " << current_ptr->window_start_time << " 발행! (";
           if (!current_ptr->global_bids.empty()) {
               cout << "Best Bid: " << get<0>(current_ptr->global_bids[0]);
           } else { cout << "Best Bid: N/A"; }
           cout << " / ";
           if (!current_ptr->global_asks.empty()) {
               cout << "Best Ask: " << get<0>(current_ptr->global_asks[0]);
           } else { cout << "Best Ask: N/A"; }
           cout << ")" << endl;

           last_processed_ptr = current_ptr;
       }
       
       // Publisher의 I/O 병목 시뮬레이션
       this_thread::sleep_for(chrono::milliseconds(50));
   }
}


// --- 6. [수정됨] 메인 함수 (피닝 로직 변경) ---
int main() {
   cout << "Aggregator 시작... (9-스레드, CPU-Bound 피닝)" << endl;
   cout << "(Cores 3,4,5 = Processors; 외에는 OS 스케줄러가 관리)" << endl;
   
   atomic<bool> running(true);

   // 3개의 NRT 링 버퍼 (Reader -> Processor)
   NrtRingBuffer<CleanData, 1024> buffer_abc;
   NrtRingBuffer<CleanData, 1024> buffer_xyz;
   NrtRingBuffer<CleanData, 1024> buffer_foo;

   // 3개의 NRT 래치 (Processor -> Publisher)
   NrtLatch<MergedOrderBook> latch_abc;
   NrtLatch<MergedOrderBook> latch_xyz;
   NrtLatch<MergedOrderBook> latch_foo;

   // --- 스레드 생성 (9개) ---
   thread reader_abc(reader_thread_func, "ABC", ref(buffer_abc), ref(running));
   thread reader_xyz(reader_thread_func, "XYZ", ref(buffer_xyz), ref(running));
   thread reader_foo(reader_thread_func, "FOO", ref(buffer_foo), ref(running));

   thread processor_abc(processor_thread_func, "ABC", ref(buffer_abc), ref(latch_abc), ref(running));
   thread processor_xyz(processor_thread_func, "XYZ", ref(buffer_xyz), ref(latch_xyz), ref(running));
   thread processor_foo(processor_thread_func, "FOO", ref(buffer_foo), ref(latch_foo), ref(running));

   thread publisher_abc(publisher_thread_func, "ABC", ref(latch_abc), ref(running));
   thread publisher_xyz(publisher_thread_func, "XYZ", ref(latch_xyz), ref(running));
   thread publisher_foo(publisher_thread_func, "FOO", ref(latch_foo), ref(running));

   // --- [수정됨] CPU 피닝 (Processor만) ---
   
   // 3개의 Reader 스레드는 (I/O-Bound) 피닝하지 않고 OS 스케줄러에 맡김.
   cout << "Reader 스레드 3개 (I/O-Bound)는 OS 스케줄러에 위임합니다." << endl;
   // set_thread_affinity(reader_abc, ...); // 제거됨

   // set_thread_affinity(reader_xyz, ...); // 제거됨
   // set_thread_affinity(reader_foo, ...); // 제거됨

   // 3개의 Processor 스레드(CPU-Bound)를 '전용 코어 3, 4, 5'에 피닝
   cout << "Processor 스레드 3개 (CPU-Bound)는 전용 코어 3, 4, 5에 피닝합니다." << endl;
   set_thread_affinity(processor_abc, 3);
   set_thread_affinity(processor_xyz, 4);
   set_thread_affinity(processor_foo, 5);

   // 3개의 Publisher 스레드(I/O-Bound)는 피닝하지 않고 OS 스케줄러에 맡김.
   cout << "Publisher 스레드 3개 (I/O-Bound)는 OS 스케줄러에 위임합니다." << endl;
   // set_thread_affinity(publisher_abc, ...); // 제거됨
   // set_thread_affinity(publisher_xyz, ...); // 제거됨
   // set_thread_affinity(publisher_foo, ...); // 제거됨

   // 30초간 실행
   this_thread::sleep_for(chrono::seconds(30));

   running = false;

   reader_abc.join();
   reader_xyz.join();
   reader_foo.join();
   processor_abc.join();
   processor_xyz.join();
   processor_foo.join();
   publisher_abc.join();
   publisher_xyz.join();
   publisher_foo.join();

   cout << "Aggregator 종료." << endl;
   return 0;
}