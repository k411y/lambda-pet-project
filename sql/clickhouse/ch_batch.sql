CREATE TABLE IF NOT EXISTS k411y.btc_ohlcv_1m_local ON CLUSTER company_cluster
(
    period_start DateTime,
    symbol LowCardinality(String),
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    total_volume Float64,
    total_amount_usdt Float64,
    trades_count UInt32
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/k411y/btc_ohlcv_1m', 
    '{replica}'
)
PARTITION BY toYYYYMM(period_start)
ORDER BY (symbol, period_start);


CREATE TABLE IF NOT EXISTS k411y.btc_ohlcv_1m ON CLUSTER company_cluster
(
    period_start DateTime,
    symbol LowCardinality(String),
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    total_volume Float64,
    total_amount_usdt Float64,
    trades_count UInt32
)
ENGINE = Distributed(
    company_cluster,           
    'k411y',           
    'btc_ohlcv_1m_local',     
    cityHash64(symbol, period_start)
);