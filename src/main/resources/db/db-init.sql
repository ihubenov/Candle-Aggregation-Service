CREATE TABLE candles_1s (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    open DECIMAL(20, 8) NOT NULL,
    high DECIMAL(20, 8) NOT NULL,
    low DECIMAL(20, 8) NOT NULL,
    close DECIMAL(20, 8) NOT NULL,
    volume DECIMAL(20, 8) NOT NULL,
    PRIMARY KEY (time, symbol)
);

SELECT create_hypertable('candles_1s', 'time');

CREATE INDEX idx_candles_1s_symbol_time ON candles_1s (symbol, time DESC);

ALTER TABLE candles_1s SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'time DESC'
    );

SELECT add_compression_policy('candles_1s', INTERVAL '1 month');



CREATE MATERIALIZED VIEW candles_5s
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 seconds', time) AS time,
    symbol,
    FIRST(open, time) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close, time) AS close,
    SUM(volume) AS volume
FROM candles_1s
GROUP BY time_bucket('5 seconds', time), symbol
WITH NO DATA;

CREATE INDEX idx_candles_5s_symbol ON candles_5s (symbol, time DESC);

SELECT add_continuous_aggregate_policy(
    'candles_5s',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '5 seconds',
    schedule_interval => INTERVAL '1 minute');



CREATE MATERIALIZED VIEW candles_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS time,
    symbol,
    FIRST(open, time) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close, time) AS close,
    SUM(volume) AS volume
FROM candles_1s
GROUP BY time_bucket('1 minute', time), symbol
WITH NO DATA;

CREATE INDEX idx_candles_1m_symbol ON candles_1m (symbol, time DESC);

SELECT add_continuous_aggregate_policy(
    'candles_1m',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');



CREATE MATERIALIZED VIEW candles_15m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', time) AS time,
    symbol,
    FIRST(open, time) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close, time) AS close,
    SUM(volume) AS volume
FROM candles_1s
GROUP BY time_bucket('15 minutes', time), symbol
WITH NO DATA;

CREATE INDEX idx_candles_15m_symbol ON candles_15m (symbol, time DESC);

SELECT add_continuous_aggregate_policy(
    'candles_15m',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');



CREATE MATERIALIZED VIEW candles_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS time,
    symbol,
    FIRST(open, time) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close, time) AS close,
    SUM(volume) AS volume
FROM candles_1s
GROUP BY time_bucket('1 hour', time), symbol
WITH NO DATA;

CREATE INDEX idx_candles_1h_symbol ON candles_1h (symbol, time DESC);

SELECT add_continuous_aggregate_policy(
    'candles_1h',
    start_offset => INTERVAL '12 hours',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');
