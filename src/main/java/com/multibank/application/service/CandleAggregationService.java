package com.multibank.application.service;

import com.multibank.application.model.BidAskEvent;
import com.multibank.application.model.Candle;
import com.multibank.application.model.CandleInterval;

import java.util.List;

public interface CandleAggregationService {
    void processEvent(BidAskEvent event);

    List<Candle> getHistoricalCandles(String symbol, CandleInterval interval, long from, long to);

    void shutdown();
}
