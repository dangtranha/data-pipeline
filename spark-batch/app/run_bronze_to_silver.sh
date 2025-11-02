#!/bin/bash

echo "=========================================="
echo "Running Batch ETL: Bronze -> Silver"
echo "=========================================="

/opt/spark/bin/spark-submit \
  --master local[*] \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.2 \
  /opt/spark-apps/batch_bronze_to_silver.py

EXIT_CODE=$?

echo ""
echo "=========================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Bronze -> Silver ETL COMPLETED"
else
    echo "✗ Bronze -> Silver ETL FAILED (exit code: $EXIT_CODE)"
fi
echo "=========================================="

exit $EXIT_CODE
