import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime

load_dotenv()

# Забираем переменные окружения
gp_host = os.getenv("GP_HOST")
gp_port = os.getenv("GP_PORT")
gp_db = os.getenv("GP_DB")
gp_user = os.getenv("GP_USER")
gp_password = os.getenv("GP_PASSWORD")
gp_schema = os.getenv("GP_SCHEMA")
gp_table_raw = os.getenv("GP_TABLE_RAW")

s3_bucket_path = os.getenv("S3_INPUT_PATH")
s3_archive_path = os.getenv("S3_ARCHIVE_PATH")
s3_root_uri = os.getenv("S3_ROOT_URI")

jdbc_url = f"jdbc:postgresql://{gp_host}:{gp_port}/{gp_db}"
full_gp_table = f"{gp_schema}.{gp_table_raw}"

# 1. Инициализация Spark
spark = SparkSession.builder.appName("S3_to_Greenplum").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("📥 Читаем исторические данные из S3 (MinIO)...")

# 2. Чтение файлов
try:
    # Используем переменную пути
    df = spark.read.parquet(f"{s3_bucket_path}/*.parquet")
except Exception as e:
    print("🤷‍♂️ Нет новых файлов для загрузки.")
    sys.exit(0)

# 3. Трансформация данных (BIGINT -> TIMESTAMP)
df_transformed = df \
    .withColumn("trade_time", from_unixtime(col("trade_time") / 1000).cast("timestamp")) \
    .withColumn("event_time", from_unixtime(col("event_time") / 1000).cast("timestamp"))

# 4. Запись в Greenplum
print(f"📤 Записываем данные в Greenplum (таблица {full_gp_table})...")

df_transformed.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", full_gp_table) \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("✅ Данные успешно загружены в Greenplum!")

# 5. Перенос файлов в архив
print("📦 Начинаем перенос обработанных файлов в архив...")

# Получаем доступ к файловой системе Hadoop, которая работает с S3
sc = spark.sparkContext
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
hadoop_conf = sc._jsc.hadoopConfiguration()

# Используем переменную для корня бакета
fs = FileSystem.get(URI(s3_root_uri), hadoop_conf)

source_dir = Path(s3_bucket_path)
archive_dir = Path(s3_archive_path)

if not fs.exists(archive_dir):
    fs.mkdirs(archive_dir)

# Получаем список файлов и переносим их по одному
files = fs.listStatus(source_dir)
moved_count = 0

for file_status in files:
    file_path = file_status.getPath()
    if file_path.getName().endswith(".parquet"):
        dest_path = Path(f"{s3_archive_path}/{file_path.getName()}")
        # rename в S3 = копирование + удаление оригинала
        fs.rename(file_path, dest_path)
        moved_count += 1

print(f"🧹 Успешно перемещено файлов в архив: {moved_count}")