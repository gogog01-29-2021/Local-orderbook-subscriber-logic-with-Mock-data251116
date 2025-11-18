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


// --- 2. NRT 링 버퍼 ---
template<typename T, size_t Capacity>
class NrtRingBuffer {
public:
   // Capacity는 2의 거듭제곱이어야 함 (예: 1024)
   NrtRingBuffer() : write_cursor_(0), buffer_(Capacity), capacity_(Capacity) {}

   // Reader 스레드(Producer)만 호출
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
   
   // 데이터를 수정할 일이 사실상 없어서 거의 필요 없을듯
   T& at(uint64_t seq) {
       return buffer_[seq % capacity_];
   }

private:
   // C++17: cacheline_size를 하드코딩하는 대신 alignas 사용
   // uint64_t라 오버플로우 나려면 500걸림
   alignas(64) atomic<uint64_t> write_cursor_;
   
   vector<T> buffer_;
   const size_t capacity_;
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
void reader_thread_func(
   const string& product_name,
   NrtRingBuffer<CleanData, 1024>& buffer,
   atomic<bool>& running)
{
   cout << "[Reader-" << p
roduct_name << "] 시작." << endl;

   while (running) {
       // 1. KDS에서 데이터 가져오기 (시뮬레이션)
       uint64_t now_ms = chrono::duration_cast<chrono::milliseconds>(
           chrono::system_clock::now().time_since_epoch()).count();

       CleanData fake_binance = CleanData::create_fake("binance", now_ms - 10);
       CleanData fake_coinbase = CleanData::create_fake("coinbase", now_ms - 20);

       buffer.push(fake_binance);
       buffer.push(fake_coinbase);

       // 의도적 시간 간격(5ms)
       this_thread::sleep_for(chrono::milliseconds(5));
   }
}

void processor_thread_func(
   const string& product_name,
   NrtRingBuffer<CleanData, 1024>& buffer,
   atomic<bool>& running)
{
   cout << "[Processor-" << product_name << "] 시작." << endl;

   const uint64_t WINDOW_SIZE_MS = 10;
   const uint64_t WATERMARK_DELAY_MS = 5;
   
   const uint64_t BUFFER_CAPACITY = 1024;
   const uint64_t MY_CAPACITY = 200;

   uint64_t my_last_proces
sed_seq = buffer.get_latest_cursor();

   // --- 기존 윈도우 로직용 변수 (변경 없음) ---
   uint64_t max_event_time_seen = 0;
   uint64_t current_window_start = 0;
   map<uint64_t, vector<CleanData>> window_buffers;
   map<string, map<double, double>> current_bids;
   map<string, map<double, double>> current_asks;

   while (running) {
       const uint64_t latest_available_seq = buffer.get_latest_cursor();

       if (latest_available_seq <= my_last_processed_seq) {
           // 처리할 것 없음(busy waiting)
           continue;
       }

       // "건너뛰기" 로직
       const uint64_t seq_to_end = latest_available_seq;
       uint64_t seq_to_start = my_last_processed_seq + 1;

       seq_to_start = max(seq_to_start,
                          (seq_to_end > MY_CAPACITY) ? (seq_to_end - MY_CAPACITY + 1) : 0);

       // 처리 가능한 배치 만큼 처리
       for (uint64_t i = seq_to_start; i <= seq_to_end; i++) {
           const CleanData& data = buf
fer.at(i);

           // --- (이하는 기존 try_pop 루프 내부와 동일) ---
           // 2. 워터마크 업데이트
           if (data.ts_event > max_event_time_seen) {
               max_event_time_seen = data.ts_event;
           }

           // 3. 윈도우 배정 (10ms 단위)
           uint64_t window = (data.ts_event / WINDOW_SIZE_MS) * WINDOW_SIZE_MS;
           window_buffers[window].push_back(data);
           // --- (여기까지 기존 로직과 동일) ---
       }
       
       my_last_processed_seq = seq_to_end;

       // --- (이하는 기존 Processor 로직과 완전히 동일) ---

       // 4. 워터마크 계산
       uint64_t watermark = (max_event_time_seen > WATERMARK_DELAY_MS) ? (max_event_time_seen - WATERMARK_DELAY_MS) : 0;

       if (current_window_start == 0 && !window_buffers.empty()) {
           current_window_start = window_buffers.begin()->first;
       }

       // 5. 윈도우 마감 및 처리
       while (watermark > current_window_start + WINDOW_SIZE_MS
) {
           
           vector<CleanData>& events_to_process = window_buffers[current_window_start];

           // 5a. 이벤트 적용 (메모리 상태 업데이트 - 래치 효과)
           for (const auto& evt : events_to_process) {
               current_bids[evt.exchange] = evt.bids; // (최신 데이터로 덮어써짐)
               current_asks[evt.exchange] = evt.asks;
           }

           // 5b. 글로벌 오더북(스냅샷) 생성
           MergedOrderBook snapshot;
           snapshot.window_start_time = current_window_start;

           for (auto const& [exchange, book] : current_bids) {
               for (auto const& [price, size] : book) {
                   snapshot.global_bids.emplace_back(price, size, exchange);
               }
           }
           sort(snapshot.global_bids.rbegin(), snapshot.global_bids.rend());

           // 5c. 최종 발행 (Publish)
           if (!snapshot.global_bids.empty()) {
               cout << "? [Processor-" << product_
name << "] Window " << current_window_start << " 발행! (최고 매수가: "
                    << get<0>(snapshot.global_bids[0]) << " / " << get<2>(snapshot.global_bids[0]) << ")" << endl;
           }
           
           // 5d. 처리 완료된 윈도우 삭제
           window_buffers.erase(current_window_start);
           current_window_start += WINDOW_SIZE_MS;
       }

       // (CPU Busy-wait 방지. 기존 로직과 동일)
       if (window_buffers.empty()) {
           this_thread::sleep_for(chrono::milliseconds(1));
       }
   }
}


// --- 5. 메인 함수 (스레드 6개 실행 및 피닝) ---
int main() {
   cout << "Aggregator 시작... (LMAX NRT 패턴, 4-코어 비대칭 피닝)" << endl;

   atomic<bool> running(true);

   // 3개의 NRT 링 버퍼 (공유 버퍼) 생성
   NrtRingBuffer<CleanData, 1024> buffer_abc;
   NrtRingBuffer<CleanData, 1024> buffer_xyz;
   NrtRingBuffer<CleanData, 1024> buffer_foo;

   // --- 스레드 생성 (변경 없음) ---//I.O Intensive                             //CPU Intensive
   thread reader_abc(reader_thread_f
unc, "ABC", ref(buffer_abc), ref(running));
   thread reader_xyz(reader_thread_func, "XYZ", ref(buffer_xyz), ref(running));
   thread reader_foo(reader_thread_func, "FOO", ref(buffer_foo), ref(running));

   thread processor_abc(processor_thread_func, "ABC", ref(buffer_abc), ref(running));
   thread processor_xyz(processor_thread_func, "XYZ", ref(buffer_xyz), ref(running));
   thread processor_foo(processor_thread_func, "FOO", ref(buffer_foo), ref(running));

   // --- CPU 피닝 (변경 없음) ---
   set_thread_affinity(reader_abc, 2); //Because three readers are I.O intensive ,pin them to same core
   set_thread_affinity(reader_xyz, 2);
   set_thread_affinity(reader_foo, 2);

   set_thread_affinity(processor_abc, 3);
   set_thread_affinity(processor_xyz, 4);
   set_thread_affinity(processor_foo, 5);
    // IRQ, CPU Isolation is necessary.
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
