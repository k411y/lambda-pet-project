import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

load_dotenv()

kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
kafka_topic = os.getenv('KAFKA_TOPIC')
s3_bucket_path = os.getenv('S3_OUTPUT_PATH')
checkpoint_path = os.getenv('S3_CHECKPOINT_PATH')

spark = SparkSession.builder \
    .appName("kafka_to_s3_batch") \
    .getOrCreate()

spark.conf.set("spark.sql.caseSensitive", "true")
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("t", LongType()),    # trade_id
    StructField("s", StringType()),  # symbol
    StructField("p", StringType()),  # price
    StructField("q", StringType()),  # quantity
    StructField("T", LongType()),    # trade_time
    StructField("E", LongType()),    # event_time
    StructField("m", BooleanType())  # is_buyer_maker
])

print("🚀 Запуск микро-батча: Чтение из Kafka...")

# Читаем из Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 50000) \
    .load()

# 5. Парсим JSON и переименовываем колонки
final_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(
        get_json_object(col("json_str"), "$.trade_id").cast("long").alias("trade_id"),
        get_json_object(col("json_str"), "$.symbol").cast("string").alias("symbol"),
        get_json_object(col("json_str"), "$.price").cast("double").alias("price"),
        get_json_object(col("json_str"), "$.quantity").cast("double").alias("quantity"),
        get_json_object(col("json_str"), "$.amount_usdt").cast("double").alias("amount_usdt"),
        get_json_object(col("json_str"), "$.trade_time").cast("long").alias("trade_time"),
        get_json_object(col("json_str"), "$.event_time").cast("long").alias("event_time"),
        get_json_object(col("json_str"), "$.is_buyer_maker").cast("boolean").alias("is_buyer_maker")
    )

# Запись в S3
print("💾 Запись данных в MinIO...")

query = final_df.writeStream \
    .format("parquet") \
    .option("path", s3_bucket_path) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()

print("✅ Микро-батч успешно завершен. Данные сохранены в S3 в формате Parquet.")