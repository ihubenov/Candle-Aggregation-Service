package com.multibank.application.controller;

import com.multibank.application.model.Candle;
import com.multibank.application.model.CandleInterval;
import com.multibank.application.service.CandleAggregationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequiredArgsConstructor
public class HistoryController {

    private final CandleAggregationService candleAggregationService;

    @GetMapping("/history")
    public ResponseEntity<Map<String, Object>> getHistory(
            @RequestParam String symbol,
            @RequestParam String interval,
            @RequestParam long from,
            @RequestParam long to
    ) {
        CandleInterval candleInterval = CandleInterval.fromLabel(interval);
        List<Candle> candles = candleAggregationService.getHistoricalCandles(symbol, candleInterval, from, to);

        Map<String, Object> response = new HashMap<>();
        response.put("s", "ok");
        response.put("t", candles.stream().map(Candle::time).toList());
        response.put("o", candles.stream().map(Candle::open).toList());
        response.put("h", candles.stream().map(Candle::high).toList());
        response.put("l", candles.stream().map(Candle::low).toList());
        response.put("c", candles.stream().map(Candle::close).toList());
        response.put("v", candles.stream().map(Candle::volume).toList());

        return ResponseEntity.ok(response);
    }
}
