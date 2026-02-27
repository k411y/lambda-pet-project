import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

ENV_CONFIG = {
    "GP_HOST": os.getenv('GP_HOST'),
    "GP_PORT": os.getenv('GP_PORT'),
    "GP_DB": os.getenv('GP_DB'),
    "GP_USER": os.getenv('GP_USER'),
    "GP_PASSWORD": os.getenv('GP_PASSWORD'),
    "GP_SCHEMA": os.getenv('GP_SCHEMA'),
    "GP_TABLE_MART": os.getenv('GP_TABLE_MART'),
    "GP_TABLE_RAW": os.getenv('GP_TABLE_RAW'),
    "CH_USER": os.getenv("CLICKHOUSE_USER", "default"),
    "CH_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD", ""),
    "CH_DB": os.getenv('CH_DB'),
    "CH_TABLE": os.getenv('CH_TABLE'),
    "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER"),
    "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD")
}

default_args = {
    'owner': 'k411y',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def run_update_mart(config):
    import psycopg2
    
    print("🔄 Подключение к Greenplum...")
    conn = psycopg2.connect(
        host=config['GP_HOST'],
        port=config['GP_PORT'],
        dbname=config['GP_DB'],
        user=config['GP_USER'],
        password=config['GP_PASSWORD']
    )
    cur = conn.cursor()
    
    schema = config['GP_SCHEMA']
    mart_table = config['GP_TABLE_MART']
    raw_table = config['GP_TABLE_RAW']

    update_mart_sql = f"""
        -- 1. Удаляем хвост
        DELETE FROM {schema}.{mart_table} 
        WHERE period_start >= (
            SELECT COALESCE(MAX(period_start) - INTERVAL '1 hour', '1970-01-01') 
            FROM {schema}.{mart_table}
        );

        -- 2. Вставляем новые
        INSERT INTO {schema}.{mart_table}
        SELECT 
            date_trunc('minute', trade_time) AS period_start,
            symbol,
            (array_agg(price ORDER BY trade_time ASC, trade_id ASC))[1] AS open_price,
            MAX(price) AS high_price,
            MIN(price) AS low_price,
            (array_agg(price ORDER BY trade_time DESC, trade_id DESC))[1] AS close_price,
            SUM(quantity) AS total_volume,
            SUM(amount_usdt) AS total_amount_usdt,
            COUNT(trade_id)::INTEGER AS trades_count
        FROM {schema}.{raw_table}
        WHERE date_trunc('minute', trade_time) >= (
            SELECT COALESCE(MAX(period_start), '1970-01-01') 
            FROM {schema}.{mart_table}
        )
        GROUP BY 1, 2;
    """
    
    cur.execute(update_mart_sql)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Витрина btc_ohlcv_1m успешно обновлена!")

def run_transfer_to_ch(config):
    import requests
    
    ch_url = 'http://clickhouse01:8123/'
    
    query = f"""
        INSERT INTO {config['CH_DB']}.{config['CH_TABLE']}
        SELECT * FROM postgresql(
            '{config['GP_HOST']}:{config['GP_PORT']}', 
            '{config['GP_DB']}', 
            '{config['GP_TABLE_MART']}',
            '{config['GP_USER']}', 
            '{config['GP_PASSWORD']}',
            '{config['GP_SCHEMA']}'
        );
    """
    
    print(f"🚀 Отправляем запрос в ClickHouse {ch_url}...")
    
    response = requests.post(
        ch_url, 
        data=query.encode('utf-8'),
        auth=(config['CH_USER'], config['CH_PASSWORD'])
    )
    
    if response.status_code != 200:
        raise Exception(f"ClickHouse error: {response.text}")
        
    print("✅ Данные успешно перелиты в ClickHouse!")

with DAG(
    dag_id='s3_to_gp',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1
) as dag:

    s3_to_gp = SparkSubmitOperator(
        task_id="spark_s3_to_gp_batch",
        application="/opt/airflow/scripts/transform/transform__s3.py",
        conn_id="spark_default",
        env_vars={
            "GP_HOST": ENV_CONFIG['GP_HOST'],
            "GP_PORT": ENV_CONFIG['GP_PORT'],
            "GP_DB": ENV_CONFIG['GP_DB'],
            "GP_USER": ENV_CONFIG['GP_USER'],
            "GP_PASSWORD": ENV_CONFIG['GP_PASSWORD'],
            "GP_SCHEMA": ENV_CONFIG['GP_SCHEMA'],
            "GP_TABLE_RAW": ENV_CONFIG['GP_TABLE_RAW']
        },
        conf={
            "spark.executor.instances": "1",
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": ENV_CONFIG['MINIO_ROOT_USER'],
            "spark.hadoop.fs.s3a.secret.key": ENV_CONFIG['MINIO_ROOT_PASSWORD'],
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
        },
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.5.0"
        )
    )

    update_mart = PythonOperator(
        task_id="update_ohlcv_1m_mart",
        python_callable=run_update_mart,
        op_kwargs={'config': ENV_CONFIG} 
    )

    transfer_to_ch = PythonOperator(
        task_id="transfer_gp_to_ch",
        python_callable=run_transfer_to_ch,
        op_kwargs={'config': ENV_CONFIG}
    )

    s3_to_gp >> update_mart >> transfer_to_ch
