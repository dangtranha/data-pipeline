from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestSpark") \
    .master("spark://LaptopcuaPhat.lan:7077") \
    .getOrCreate()

print("Spark version:", spark.version)
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
df.show()
spark.stop()
