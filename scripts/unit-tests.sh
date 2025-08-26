#!/bin/bash

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[PASS]${NC} $1"
}

log_error() {
    echo -e "${RED}[FAIL]${NC} $1"
}

log_info "开始单元测试..."
log_info "================================"

# 清理之前的测试数据
rm -rf test-data test-coverage.out 2>/dev/null || true
mkdir -p test-data

# 运行所有包的单元测试
log_info "运行所有单元测试..."
if go test -v -race -coverprofile=test-coverage.out ./...; then
    log_success "单元测试通过"
else
    log_error "单元测试失败"
    exit 1
fi

# 生成覆盖率报告
log_info "生成测试覆盖率报告..."
if go tool cover -html=test-coverage.out -o test-coverage.html; then
    log_success "覆盖率报告生成成功: test-coverage.html"
else
    log_error "覆盖率报告生成失败"
fi

# 显示覆盖率统计
log_info "测试覆盖率统计:"
go tool cover -func=test-coverage.out | tail -1

# 检查代码质量
log_info "运行代码质量检查..."

# 检查代码格式
log_info "检查代码格式..."
if gofmt_output=$(gofmt -l .); then
    if [ -z "$gofmt_output" ]; then
        log_success "代码格式检查通过"
    else
        log_error "以下文件格式不正确:"
        echo "$gofmt_output"
        exit 1
    fi
fi

# 运行go vet
log_info "运行go vet..."
if go vet ./...; then
    log_success "go vet检查通过"
else
    log_error "go vet检查失败"
    exit 1
fi

# 检查模块依赖
log_info "检查模块依赖..."
if go mod verify; then
    log_success "模块依赖检查通过"
else
    log_error "模块依赖检查失败"
    exit 1
fi

# 检查未使用的依赖
log_info "检查未使用的依赖..."
if go mod tidy; then
    if git diff --exit-code go.mod go.sum; then
        log_success "依赖管理检查通过"
    else
        log_error "发现未使用的依赖，请运行 'go mod tidy'"
        exit 1
    fi
fi

log_info "================================"
log_success "🎉 所有单元测试和代码质量检查通过！" 