package com.multibank.application.repository;

import com.multibank.application.entity.CandleEntity;
import com.multibank.application.model.Candle;
import com.multibank.application.model.CandleInterval;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.util.List;

@Repository
@RequiredArgsConstructor
@Slf4j
public class CandleCustomJdbcRepositoryImpl implements CandleCustomJdbcRepository {


    private final JdbcTemplate jdbcTemplate;

    @Override
    public List<Candle> find1sCandles(String symbol, Long from, Long to) {
        String sql =
                "SELECT time, open, high, low, close, volume " +
                        "FROM candles_1s " +
                        "WHERE symbol = ? " +
                        "AND time >= to_timestamp(? / 1000) " +
                        "AND time <= to_timestamp(? / 1000) " +
                        "ORDER BY time ASC";

        return jdbcTemplate.query(sql,
                (rs, rowNum) -> new Candle(
                        rs.getTimestamp("time").toInstant().toEpochMilli(),
                        rs.getBigDecimal("open").doubleValue(),
                        rs.getBigDecimal("high").doubleValue(),
                        rs.getBigDecimal("low").doubleValue(),
                        rs.getBigDecimal("close").doubleValue(),
                        rs.getBigDecimal("volume").intValue()
                ),
                symbol, from, to
        );
    }


    @Override
    public List<Candle> findCandles(CandleInterval interval, String symbol, Long from, Long to) {
        String sql = String.format(
                "SELECT time, open, high, low, close, volume " +
                        "FROM candles_%s " +
                        "WHERE symbol = ? " +
                        "AND time >= to_timestamp(?) " +
                        "AND time <= to_timestamp(?) " +
                        "ORDER BY time ASC",
                interval.getLabel()
        );

        return jdbcTemplate.query(sql,
                (rs, rowNum) -> new Candle(
                        rs.getTimestamp("time").toInstant().toEpochMilli(),
                        rs.getBigDecimal("open").doubleValue(),
                        rs.getBigDecimal("high").doubleValue(),
                        rs.getBigDecimal("low").doubleValue(),
                        rs.getBigDecimal("close").doubleValue(),
                        rs.getBigDecimal("volume").intValue()
                ),
                symbol, from, to
        );
    }

    /**
     * Batch upsert using JDBC batch operations for maximum performance.
     * Uses PostgreSQL's ON CONFLICT for efficient upserts.
     */
    @Override
    @Transactional
    public void batchUpsert(List<CandleEntity> candles) {
        if (candles.isEmpty()) {
            return;
        }

        String sql =
                "INSERT INTO candles_1s (time, symbol, open, high, low, close, volume) " +
                        "VALUES (to_timestamp(? / 1000.0), ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (time, symbol) DO UPDATE SET " +
                        "high = GREATEST(candles_1s.high, EXCLUDED.high), " +
                        "low = LEAST(candles_1s.low, EXCLUDED.low), " +
                        "close = EXCLUDED.close, " +
                        "volume = candles_1s.volume + EXCLUDED.volume";

        jdbcTemplate.batchUpdate(sql, candles, candles.size(),
                (PreparedStatement ps, CandleEntity candle) -> {
                    ps.setLong(1, candle.getId().getTime());
                    ps.setString(2, candle.getId().getSymbol());
                    ps.setDouble(3, candle.getOpen());
                    ps.setDouble(4, candle.getHigh());
                    ps.setDouble(5, candle.getLow());
                    ps.setDouble(6, candle.getClose());
                    ps.setLong(7, candle.getVolume());
                });
    }
}
