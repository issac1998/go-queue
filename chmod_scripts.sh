#!/bin/bash

# 设置脚本执行权限
chmod +x test_setup.sh
chmod +x test_cleanup.sh

echo "脚本权限设置完成"
echo "可以运行以下命令："
echo "  ./test_setup.sh    # 搭建测试环境"
echo "  ./test_cleanup.sh  # 清理测试环境"
echo "  go run run_integration_tests.go  # 运行集成测试" 