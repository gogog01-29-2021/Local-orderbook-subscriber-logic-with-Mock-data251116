#!/bin/bash

# Experiment Runner for Ring Buffer and Window Size Optimization
# Usage: ./run_experiments.sh

echo "=== Ring Buffer & Window Size Optimization Experiments ==="
echo ""

# Create results directory
mkdir -p experiment_results
RESULTS_DIR="experiment_results"

# Test counter
TEST_NUM=1

# Function to run a single test
run_test() {
    local buffer_size=$1
    local window_size=$2
    local watermark_delay=$3
    local proc_capacity=$4
    local duration=$5

    echo "[$TEST_NUM] Testing: buffer=$buffer_size window=${window_size}ms watermark=${watermark_delay}ms proc_cap=$proc_capacity duration=${duration}s"

    # Run the program and capture output
    ./subscriber2.exe $buffer_size $window_size $watermark_delay $proc_capacity $duration > "${RESULTS_DIR}/test_${TEST_NUM}_output.txt" 2>&1

    # Rename the performance log
    if [ -f "performance.log" ]; then
        mv performance.log "${RESULTS_DIR}/test_${TEST_NUM}_perf.csv"
    fi

    # Extract summary stats
    grep -A 10 "Performance Summary" "${RESULTS_DIR}/test_${TEST_NUM}_output.txt" >> "${RESULTS_DIR}/summary.txt"
    echo "---" >> "${RESULTS_DIR}/summary.txt"

    TEST_NUM=$((TEST_NUM + 1))
    sleep 2
}

# === EXPERIMENT 1: Ring Buffer Capacity ===
echo "Experiment 1: Testing different ring buffer capacities"
echo "Ring_Buffer_Capacity,Window_Size,Throughput,Events_Processed,Events_Skipped" > "${RESULTS_DIR}/buffer_capacity_results.csv"

for BUFFER_SIZE in 512 1024 2048 4096; do
    run_test $BUFFER_SIZE 10 5 200 10
done

# === EXPERIMENT 2: Window Size ===
echo ""
echo "Experiment 2: Testing different window sizes"
echo "Buffer_Capacity,Window_Size,Throughput,Events_Processed,Events_Skipped" > "${RESULTS_DIR}/window_size_results.csv"

for WINDOW_SIZE in 5 10 20 50 100; do
    run_test 1024 $WINDOW_SIZE 5 200 10
done

# === EXPERIMENT 3: Processor Capacity ===
echo ""
echo "Experiment 3: Testing different processor capacities"
echo "Buffer_Capacity,Window_Size,Proc_Capacity,Throughput,Events_Processed,Events_Skipped" > "${RESULTS_DIR}/proc_capacity_results.csv"

for PROC_CAP in 100 200 500 1000; do
    run_test 1024 10 5 $PROC_CAP 10
done

# === EXPERIMENT 4: Combined Optimization ===
echo ""
echo "Experiment 4: Testing combined configurations"

# Small buffer, small window (low latency)
run_test 512 5 2 100 10

# Medium buffer, medium window (balanced)
run_test 1024 10 5 200 10

# Large buffer, large window (high throughput)
run_test 4096 50 10 500 10

echo ""
echo "=== All experiments completed! ==="
echo "Results saved in: $RESULTS_DIR/"
echo ""
echo "Summary statistics:"
cat "${RESULTS_DIR}/summary.txt"
