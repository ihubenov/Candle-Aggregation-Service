package com.multibank.application.repository;

import com.multibank.application.entity.CandleEntity;
import com.multibank.application.model.Candle;
import com.multibank.application.model.CandleInterval;

import java.util.List;

public interface CandleCustomJdbcRepository {

    List<Candle> find1sCandles(String symbol, Long from, Long to);

    List<Candle> findCandles(CandleInterval interval, String symbol, Long from, Long to);

    void batchUpsert(List<CandleEntity> candles);
}
