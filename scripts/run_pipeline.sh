#!/bin/bash
# 电商数据仓库全链路调度脚本
# 用法：bash scripts/run_pipeline.sh

set -e  # 任意步骤失败即退出

export HADOOP_HOME="C:/hadoop"
export PATH="$HADOOP_HOME/bin:$PATH"

PYTHON="C:/Users/26817/Desktop/data-analyst-ai/.venv/Scripts/python"
LOG_DIR="logs"
DATE=$(date +%Y%m%d_%H%M%S)

mkdir -p $LOG_DIR

echo "======================================"
echo "数据仓库调度开始：$DATE"
echo "======================================"

# ODS层
echo "[$(date +%H:%M:%S)] 开始 ODS 层..."
$PYTHON src/ods/ods_load.py >> $LOG_DIR/ods_$DATE.log 2>&1
if [ $? -eq 0 ]; then
    echo "[$(date +%H:%M:%S)] ODS 层完成 ✓"
else
    echo "[$(date +%H:%M:%S)] ODS 层失败 ✗，查看日志：$LOG_DIR/ods_$DATE.log"
    exit 1
fi

# DWD层
echo "[$(date +%H:%M:%S)] 开始 DWD 层..."
$PYTHON src/dwd/dwd_clean.py >> $LOG_DIR/dwd_$DATE.log 2>&1
if [ $? -eq 0 ]; then
    echo "[$(date +%H:%M:%S)] DWD 层完成 ✓"
else
    echo "[$(date +%H:%M:%S)] DWD 层失败 ✗，查看日志：$LOG_DIR/dwd_$DATE.log"
    exit 1
fi

# DWS层
echo "[$(date +%H:%M:%S)] 开始 DWS 层..."
$PYTHON src/dws/dws_agg.py >> $LOG_DIR/dws_$DATE.log 2>&1
if [ $? -eq 0 ]; then
    echo "[$(date +%H:%M:%S)] DWS 层完成 ✓"
else
    echo "[$(date +%H:%M:%S)] DWS 层失败 ✗，查看日志：$LOG_DIR/dws_$DATE.log"
    exit 1
fi

# ADS层
echo "[$(date +%H:%M:%S)] 开始 ADS 层..."
$PYTHON src/ads/ads_report.py >> $LOG_DIR/ads_$DATE.log 2>&1
if [ $? -eq 0 ]; then
    echo "[$(date +%H:%M:%S)] ADS 层完成 ✓"
else
    echo "[$(date +%H:%M:%S)] ADS 层失败 ✗，查看日志：$LOG_DIR/ads_$DATE.log"
    exit 1
fi

echo "======================================"
echo "全链路调度完成：$(date +%Y%m%d_%H%M%S)"
echo "日志目录：$LOG_DIR/"
echo "======================================"