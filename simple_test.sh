#!/bin/bash

# Simple Test Runner for Go Queue
echo "=== Go Queue Test Suite ==="
echo "Running comprehensive tests..."
echo

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# Test results
TOTAL_FAILED=0

run_test() {
    local test_name="$1"
    local test_path="$2"
    
    echo -e "${BLUE}Running $test_name tests...${NC}"
    
    if go test -v $test_path; then
        echo -e "${GREEN}✓ $test_name tests passed${NC}"
        echo
    else
        echo -e "${RED}✗ $test_name tests failed${NC}"
        TOTAL_FAILED=$((TOTAL_FAILED + 1))
        echo
    fi
}

# Run unit tests
echo "🧪 UNIT TESTS"
echo "============="
run_test "Client" "./client"
run_test "Storage" "./internal/storage"
run_test "Metadata" "./internal/metadata"

# Run integration tests
echo "🔗 INTEGRATION TESTS"
echo "==================="
run_test "Storage Integration" ". -run TestManagerWithRealStorage"
run_test "Manager Lifecycle" ". -run TestManagerLifecycle"
run_test "Client Reconnection" ". -run TestClientReconnection"
run_test "Message Size Validation" ". -run TestMessageSizeValidation"
run_test "Data Persistence" ". -run TestDataPersistence"

# Summary
echo "📋 SUMMARY"
echo "=========="
if [ $TOTAL_FAILED -eq 0 ]; then
    echo -e "${GREEN}🎉 All tests passed!${NC}"
    echo "The Go message queue system is working correctly."
else
    echo -e "${RED}❌ $TOTAL_FAILED test suite(s) failed.${NC}"
    echo "Please check the output above for details."
    exit 1
fi

echo
echo "=== Test Suite Complete ===" 