#!/bin/bash

# Go Queue Test Runner
echo "=== Go Queue Test Suite ==="
echo "Running comprehensive tests for the Go message queue system..."
echo

# Change to project directory
cd "$(dirname "$0")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Test results tracking
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

run_test_suite() {
    local test_name="$1"
    local test_path="$2"
    local test_flags="$3"
    
    print_status "Running $test_name tests..."
    
    if go test $test_flags -v $test_path 2>&1 | tee /tmp/test_output.log; then
        local test_count=$(grep -c "=== RUN" /tmp/test_output.log || echo "0")
        local pass_count=$(grep -c "--- PASS:" /tmp/test_output.log || echo "0")
        local fail_count=$(grep -c "--- FAIL:" /tmp/test_output.log || echo "0")
        
        TOTAL_TESTS=$((TOTAL_TESTS + test_count))
        PASSED_TESTS=$((PASSED_TESTS + pass_count))
        FAILED_TESTS=$((FAILED_TESTS + fail_count))
        
        if [ $fail_count -eq 0 ]; then
            print_success "$test_name: $pass_count/$test_count tests passed"
        else
            print_error "$test_name: $fail_count/$test_count tests failed"
        fi
    else
        print_error "$test_name: Test suite failed to run"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
    echo
}

# Clean up any previous test artifacts
print_status "Cleaning up previous test artifacts..."
rm -rf /tmp/go-queue-test-*
rm -f /tmp/test_output.log

# Run unit tests
echo "🧪 UNIT TESTS"
echo "============="

run_test_suite "Client" "./client"
run_test_suite "Storage" "./internal/storage"
run_test_suite "Metadata" "./internal/metadata"

# Run integration tests (skip if broker not running)
echo "🔗 INTEGRATION TESTS"
echo "==================="

print_status "Checking if broker is running on localhost:9092..."
if nc -z localhost 9092 2>/dev/null; then
    print_success "Broker detected, running integration tests..."
    INTEGRATION_TEST=1 run_test_suite "Integration" "." "-run TestBrokerIntegration"
else
    print_warning "Broker not running on localhost:9092"
    print_warning "Skipping broker integration tests"
    print_status "Running storage integration tests only..."
    run_test_suite "Storage Integration" "." "-run TestManagerWithRealStorage"
    run_test_suite "Manager Lifecycle" "." "-run TestManagerLifecycle"
    run_test_suite "Client Reconnection" "." "-run TestClientReconnection"
    run_test_suite "Message Size Validation" "." "-run TestMessageSizeValidation"
    run_test_suite "Data Persistence" "." "-run TestDataPersistence"
fi

# Run benchmarks (optional)
echo "⚡ PERFORMANCE TESTS"
echo "==================="

print_status "Running benchmarks..."
if go test -bench=. -benchmem ./... > /tmp/bench_output.log 2>&1; then
    print_success "Benchmarks completed"
    echo "Benchmark results saved to /tmp/bench_output.log"
else
    print_warning "Benchmarks failed or not available"
fi
echo

# Test coverage
echo "📊 TEST COVERAGE"
echo "================"

print_status "Calculating test coverage..."
if go test -coverprofile=coverage.out ./... > /tmp/coverage_output.log 2>&1; then
    if command -v go >/dev/null 2>&1; then
        coverage=$(go tool cover -func=coverage.out | tail -1 | awk '{print $3}')
        print_success "Overall test coverage: $coverage"
        
        # Generate HTML coverage report
        go tool cover -html=coverage.out -o coverage.html
        print_status "HTML coverage report generated: coverage.html"
    fi
else
    print_warning "Coverage calculation failed"
fi
echo

# Cleanup
rm -f /tmp/test_output.log /tmp/coverage_output.log /tmp/bench_output.log

# Final summary
echo "📋 TEST SUMMARY"
echo "==============="

if [ $FAILED_TESTS -eq 0 ]; then
    print_success "All tests passed! ✨"
    print_success "Total: $TOTAL_TESTS tests, $PASSED_TESTS passed, $FAILED_TESTS failed"
    echo
    print_success "🎉 The Go message queue system is working correctly!"
else
    print_error "Some tests failed! ❌"
    print_error "Total: $TOTAL_TESTS tests, $PASSED_TESTS passed, $FAILED_TESTS failed"
    echo
    print_error "Please check the test output above for details."
    exit 1
fi

echo
echo "=== Test Suite Complete ===" 