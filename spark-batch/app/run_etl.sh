#!/bin/bash

echo "================================================================================================"
echo "                              BATCH ETL: BRONZE -> SILVER -> GOLD"
echo "================================================================================================"
echo ""

# Chạy Bronze to Silver
echo "Step 1/2: Running Bronze -> Silver ETL..."
echo "------------------------------------------------------------------------------------------------"
/opt/spark/bin/spark-submit \
  --master local[*] \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.2 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.com.amazonaws.sdk.disableEc2Metadata=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  /opt/spark-apps/batch_bronze_to_silver.py

if [ $? -eq 0 ]; then
    echo "✓ Bronze -> Silver completed successfully"
    echo ""
    
    # Chạy Silver to Gold
    echo "Step 2/2: Running Silver -> Gold ETL..."
    echo "------------------------------------------------------------------------------------------------"
    /opt/spark/bin/spark-submit \
      --master local[*] \
      --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.2 \
      --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
      --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
      --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
      --conf spark.hadoop.fs.s3a.access.key=minioadmin \
      --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
      --conf spark.hadoop.com.amazonaws.sdk.disableEc2Metadata=true \
      --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
      /opt/spark-apps/batch_silver_to_gold.py
    
    if [ $? -eq 0 ]; then
        echo "✓ Silver -> Gold completed successfully"
        echo ""
        echo "================================================================================================"
        echo "                              ALL ETL PROCESSES COMPLETED SUCCESSFULLY"
        echo "================================================================================================"
    else
        echo "✗ Silver -> Gold failed"
        exit 1
    fi
else
    echo "✗ Bronze -> Silver failed"
    exit 1
fi
