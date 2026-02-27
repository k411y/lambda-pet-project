CREATE TABLE k411y.btc_trades_raw (
	trade_id int8 NULL,
	symbol varchar(50) NULL,
	price numeric(20, 8) NULL,
	quantity numeric(20, 8) NULL,
	amount_usdt numeric(20, 8) NULL,
	trade_time timestamp NULL,
	event_time timestamp NULL,
	is_buyer_maker bool NULL
)
DISTRIBUTED BY (trade_id);


CREATE TABLE k411y.btc_ohlcv_1m (
    period_start TIMESTAMP,
    symbol VARCHAR(20),
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    total_volume DOUBLE PRECISION,
    total_amount_usdt DOUBLE PRECISION,
    trades_count INTEGER
)
DISTRIBUTED BY (period_start);