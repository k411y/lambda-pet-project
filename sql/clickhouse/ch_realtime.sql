CREATE DATABASE IF NOT EXISTS k411y ON CLUSTER company_cluster;

-- 1. Создаем "Трубу" (Читает Кафку)
CREATE TABLE IF NOT EXISTS k411y.kafka_btc_queue ON CLUSTER company_cluster
(
    trade_id UInt64,
    symbol String,
    price Float64,
    quantity Float64,
    amount_usdt Float64,
    trade_time UInt64,
    event_time UInt64,
    is_buyer_maker Bool
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = 'k411y',
         kafka_group_name = 'ch_k411y_rt_consumer',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1;

-- 2. Локальная таблица с защитой от дублей
CREATE TABLE IF NOT EXISTS k411y.btc_trades_local ON CLUSTER company_cluster
(
    trade_id UInt64,
    symbol LowCardinality(String),
    price Float64,
    quantity Float64,
    amount_usdt Float64,
    trade_time DateTime64(3),
    event_time DateTime64(3),
    is_buyer_maker Bool,
    insert_date Date DEFAULT toDate(trade_time)
) 
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/k411y/btc_trades', '{replica}', trade_time)
PARTITION BY insert_date
ORDER BY (symbol, trade_id)
TTL toDateTime(trade_time) + INTERVAL 3 DAY DELETE;

-- 3. Вью для обновления таблицы
CREATE MATERIALIZED VIEW IF NOT EXISTS k411y.mv_btc_trades ON CLUSTER company_cluster
TO k411y.btc_trades_local
AS SELECT
    trade_id,
    symbol,
    price,
    quantity,
    amount_usdt,
    fromUnixTimestamp64Milli(trade_time) AS trade_time,
    fromUnixTimestamp64Milli(event_time) AS event_time,
    is_buyer_maker
FROM k411y.kafka_btc_queue;

-- 4. Распределенная таблица
CREATE TABLE IF NOT EXISTS k411y.btc_trades_all ON CLUSTER company_cluster
AS k411y.btc_trades_local
ENGINE = Distributed('company_cluster', 'k411y', 'btc_trades_local', rand());