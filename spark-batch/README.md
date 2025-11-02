# Spark Batch ETL

## Mô tả
Batch ETL processing để xử lý dữ liệu từ Bronze layer sang Silver và Gold layers trong MinIO.

## Kiến trúc
```
Bronze (Raw Data) -> Silver (Cleaned Data) -> Gold (Analytical Data)
```

### Bronze Layer
- Dữ liệu thô từ Debezium (JSON format)
- Lưu trong MinIO bucket `bronze`
- Đường dẫn: `s3a://bronze/topics/{topic_name}/partition=0`

### Silver Layer  
- Dữ liệu đã được parse và clean
- Định dạng: Delta Lake
- Lưu trong MinIO bucket `silver`
- Đường dẫn: `s3a://silver/topics/{topic_name}`

### Gold Layer
- Dữ liệu analytical với dimensions và facts
- Định dạng: Delta Lake  
- Lưu trong MinIO bucket `gold`
- Tables:
  - **Dimensions**: dim_customers, dim_products, dim_employees
  - **Facts**: fact_orders
  - **Aggregations**: agg_sales_by_product, agg_sales_by_customer, agg_sales_by_month

## Cấu trúc thư mục
```
spark-batch/
├── app/
│   ├── batch_bronze_to_silver.py   # ETL Bronze -> Silver
│   ├── batch_silver_to_gold.py     # ETL Silver -> Gold
│   ├── main.py                      # Entry point chạy cả 2 ETL
│   └── requirements.txt             # Python dependencies
├── config/                          # Config files (nếu cần)
└── docker-compose.override.yml      # Docker compose config
```

## Chạy Batch ETL

### Cách 1: Chạy toàn bộ pipeline
```bash
cd spark-batch
docker-compose -f docker-compose.override.yml up
```

### Cách 2: Chạy từng bước

#### Bước 1: Bronze -> Silver
```bash
docker-compose -f docker-compose.override.yml run spark-batch \
  spark-submit \
  --master local[*] \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.2 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark-apps/batch_bronze_to_silver.py
```

#### Bước 2: Silver -> Gold
```bash
docker-compose -f docker-compose.override.yml run spark-batch \
  spark-submit \
  --master local[*] \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.2 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark-apps/batch_silver_to_gold.py
```

### Cách 3: Chạy interactive (để debug)
```bash
docker-compose -f docker-compose.override.yml run spark-batch bash
# Trong container:
spark-submit --master local[*] --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.2 /opt/spark-apps/batch_bronze_to_silver.py
```

## Kiểm tra kết quả

### Kiểm tra trong MinIO
1. Mở MinIO Console: http://localhost:9001
2. Login: minioadmin / minioadmin
3. Xem các buckets: silver, gold

### Kiểm tra bằng Spark
```python
# Đọc Silver data
df = spark.read.format("delta").load("s3a://silver/topics/northwind.public.customers")
df.show()

# Đọc Gold data
dim_customers = spark.read.format("delta").load("s3a://gold/dim_customers")
fact_orders = spark.read.format("delta").load("s3a://gold/fact_orders")
```

## Topics được xử lý
- northwind.public.customers
- northwind.public.orders
- northwind.public.order_details
- northwind.public.products
- northwind.public.categories
- northwind.public.employees
- northwind.public.shippers
- northwind.public.suppliers

## Dependencies
- Apache Spark 3.4.0
- Delta Lake 2.4.0
- Hadoop AWS 3.3.2 (cho S3A)
- MinIO (S3-compatible storage)

## Troubleshooting

### Lỗi kết nối MinIO
- Kiểm tra MinIO đã chạy: `docker ps | grep minio`
- Kiểm tra network: `docker network ls | grep data-pipeline-network`

### Lỗi không tìm thấy dữ liệu Bronze
- Kiểm tra Debezium đã ghi dữ liệu: http://localhost:9001
- Kiểm tra path trong code khớp với structure trong MinIO

### Lỗi Delta Lake
- Đảm bảo packages được load: `io.delta:delta-core_2.12:2.4.0`
- Kiểm tra Spark config có Delta extensions

## Notes
- Batch ETL chạy một lần và thoát (không phải streaming)
- Overwrite mode: dữ liệu cũ sẽ bị ghi đè
- Để chạy định kỳ, có thể dùng cron job hoặc Airflow
