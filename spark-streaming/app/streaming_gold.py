from pyspark.sql import functions as F

import time
import os
from delta import DeltaTable

def wait_for_silver_ready(spark, silver_path, max_wait=300):
    """
    Chờ cho tới khi Silver layer sẵn sàng (tức có Delta log metadata).
    max_wait: số giây tối đa chờ đợi.
    """
    start = time.time()
    print(f"[INFO] Waiting for Silver table to be ready at {silver_path} ...")

    while True:
        try:
            # Kiểm tra nếu thư mục Delta đã có log và metadata
            if DeltaTable.isDeltaTable(spark, silver_path):
                dt = DeltaTable.forPath(spark, silver_path)
                version = dt.history(1).collect()
                print(f"[INFO] Silver Delta table found (latest version: {version[0]['version'] if version else 0}).")
                return True
        except Exception as e:
            pass  # Delta chưa sẵn sàng

        elapsed = time.time() - start
        if elapsed > max_wait:
            print("[WARN] Timeout waiting for Silver table — no data yet.")
            return False

        time.sleep(10)

def run_gold(spark, silver_conf, gold_conf):
    """
    Đọc dữ liệu đã làm sạch từ Silver Layer (định dạng Delta) trong MinIO,
    thực hiện tổng hợp (aggregation) và ghi kết quả ra Gold Layer.
    """

    # Đường dẫn Silver Layer trong MinIO (nguồn dữ liệu đầu vào cho tầng Gold)
    silver_path = "s3a://silver/topics/northwind.public.customers/"

    # Đường dẫn Gold Layer trong MinIO (đích ghi dữ liệu sau khi tổng hợp)
    gold_path = "s3a://gold/aggregations/customers/"

    # Đường dẫn checkpoint để lưu trạng thái xử lý streaming
    checkpoint = "s3a://gold/topics/northwind.public.customers/checkpoints/gold/"

    # Cấu hình nhóm tổng hợp (aggregation) – mặc định nhóm theo cột "op"
    agg_conf = gold_conf.get("aggregation", {})
    group_cols = agg_conf.get("groupByColumns", ["op"])

    # Đợi cho Silver layer sẵn sàng    
    wait_for_silver_ready(spark, silver_path)

    # Đọc dữ liệu Silver Layer dưới dạng stream Delta
    silver_stream = spark.readStream.format("delta").load(silver_path)

    # Thực hiện phép tổng hợp đơn giản: đếm số bản ghi theo các cột nhóm
    agg_df = silver_stream.groupBy(*[F.col(c) for c in group_cols]).count()

    # Hàm xử lý từng micro-batch để ghi dữ liệu ra Gold Layer
    def _foreach_batch(micro_batch_df, batch_id):
        """
        Ghi kết quả tổng hợp ra Gold Layer sau mỗi micro-batch.
        Lưu ý: Ở đây sử dụng chế độ overwrite (ghi đè hoàn toàn).
        Trong môi trường sản xuất nên dùng cơ chế merge/upsert để tránh mất dữ liệu.
        """
        micro_batch_df.coalesce(1) \
            .write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(gold_path)

    # Khởi động luồng xử lý Gold streaming
    query = (
        agg_df.writeStream
        .outputMode("complete")
        .foreachBatch(_foreach_batch)
        .option("checkpointLocation", checkpoint)
        .start()
    )

    print(f"[START] Gold stream started.")
    print(f"        Silver source: {silver_path}")
    print(f"        Gold target:   {gold_path}")
    print(f"        Checkpoint:    {checkpoint}")

    return query
