import threading
import json
from pyspark.sql import SparkSession

from streaming_silver import run_silver
from streaming_gold import run_gold


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_spark(app_name="spark-streaming-rt"):
    builder = SparkSession.builder.appName(app_name)
    # Bật phần mở rộng Delta Lake trong Spark
    builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = builder.getOrCreate()
    return spark


def main():
    kafka_conf = load_json("/opt/config/kafka.conf")
    silver_conf = load_json("/opt/config/silver.conf")
    gold_conf = load_json("/opt/config/gold.conf")

    spark = build_spark()

    # --- Thiết lập cấu hình truy cập MinIO cho Spark (thông qua giao thức s3a)
    minio = silver_conf.get("minio", {})
    if minio:
        endpoint = minio.get("endpoint", "http://minio:9000")
        access = minio.get("access_key", "")
        secret = minio.get("secret_key", "")
        # Gán vào Hadoop config để s3a hoạt động
        hconf = spark._jsc.hadoopConfiguration()
        hconf.set("fs.s3a.endpoint", endpoint)
        hconf.set("fs.s3a.access.key", access)
        hconf.set("fs.s3a.secret.key", secret)
        hconf.set("fs.s3a.path.style.access", "true")
        # dùng implementation S3A
        hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # disable AWS instance metadata lookup (chặn delays)
        hconf.set("com.amazonaws.sdk.disableEc2Metadata", "true")
        # nếu MinIO không dùng SSL:
        hconf.set("fs.s3a.connection.ssl.enabled", "false")

    # Khởi động luồng xử lý Silver (đọc dữ liệu thô từ Bronze trong MinIO)
    t1 = threading.Thread(target=run_silver, args=(spark, kafka_conf, silver_conf), daemon=True)
    t1.start()

    # Khởi động luồng xử lý Gold (tổng hợp dữ liệu từ Silver)
    t2 = threading.Thread(target=run_gold, args=(spark, silver_conf, gold_conf), daemon=True)
    t2.start()

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Stopping streams...")
        for q in spark.streams.active:
            q.stop()


if __name__ == "__main__":
    main()
