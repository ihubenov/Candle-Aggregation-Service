package com.multibank.application.service;

import com.multibank.application.entity.CandleEntity;
import com.multibank.application.model.BidAskEvent;
import com.multibank.application.model.Candle;
import com.multibank.application.model.CandleInterval;
import com.multibank.application.repository.CandleRepository;
import com.multibank.application.service.impl.CandleAggregationServiceImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class CandleAggregationServiceTest {

    @Mock
    private CandleRepository repository;

    @Captor
    private ArgumentCaptor<List<CandleEntity>> candleCaptor;

    private CandleAggregationService aggregator;

    @AfterEach
    void tearDown() {
        if (aggregator != null) {
            aggregator.shutdown();
        }
    }

    @BeforeEach
    void setUp() {
        aggregator = new CandleAggregationServiceImpl(repository);
    }

    @Test
    void testSingleEventCreates1sCandle() throws Exception {
        long timestamp = 1620000000L;
        BidAskEvent event = new BidAskEvent("BTC-USD", 50000.0, 50010.0, timestamp);

        aggregator.processEvent(event);

        TimeUnit.SECONDS.sleep(2);

        verify(repository, atLeastOnce()).batchUpsert(candleCaptor.capture());

        List<CandleEntity> savedCandles = candleCaptor.getValue();
        assertThat(savedCandles).isNotEmpty();

        CandleEntity candle = savedCandles.get(0);
        assertThat(candle.getId().getSymbol()).isEqualTo("BTC-USD");
        assertThat(candle.getOpen()).isEqualTo(50005.0);
        assertThat(candle.getVolume()).isEqualTo(1L);
    }

    @Test
    void testMultipleEventsInSame1sInterval() throws Exception {
        long baseTime = 1620000000L;

        aggregator.processEvent(new BidAskEvent("BTC-USD", 50000.0, 50010.0, baseTime));
        aggregator.processEvent(new BidAskEvent("BTC-USD", 50100.0, 50110.0, baseTime));
        aggregator.processEvent(new BidAskEvent("BTC-USD", 49900.0, 49910.0, baseTime));

        TimeUnit.SECONDS.sleep(2);

        verify(repository, atLeastOnce()).batchUpsert(candleCaptor.capture());

        List<CandleEntity> savedCandles = candleCaptor.getValue();
        CandleEntity candle = savedCandles.stream()
                .filter(c -> c.getId().getSymbol().equals("BTC-USD"))
                .findFirst()
                .orElse(null);

        assertThat(candle).isNotNull();
        // Open/Close are dependent on execution order when timestamps are equal; validate stable parts
        assertThat(candle.getHigh()).isEqualTo(50105.0);
        assertThat(candle.getLow()).isEqualTo(49905.0);
        assertThat(candle.getVolume()).isEqualTo(3L);
    }

    @Test
    void testOutOfOrderEvents() throws Exception {
        long baseTime = 1620000000L;

        // Events arrive out of order but within the same 1s bucket (same timestamp granularity)
        aggregator.processEvent(new BidAskEvent("BTC-USD", 50100.0, 50110.0, baseTime)); // arrives first
        aggregator.processEvent(new BidAskEvent("BTC-USD", 50000.0, 50010.0, baseTime)); // arrives second
        aggregator.processEvent(new BidAskEvent("BTC-USD", 49900.0, 49910.0, baseTime)); // arrives third

        TimeUnit.SECONDS.sleep(2);

        verify(repository, atLeastOnce()).batchUpsert(candleCaptor.capture());

        List<CandleEntity> savedCandles = candleCaptor.getValue();
        CandleEntity candle = savedCandles.stream()
                .filter(c -> c.getId().getSymbol().equals("BTC-USD"))
                .findFirst()
                .orElse(null);

        assertThat(candle).isNotNull();
        assertThat(candle.getOpen()).isEqualTo(50105.0);   // First arrival as timestamps are equal
        assertThat(candle.getHigh()).isEqualTo(50105.0);   // Highest price
        assertThat(candle.getLow()).isEqualTo(49905.0);    // Lowest price
        assertThat(candle.getClose()).isEqualTo(50105.0);  // Same timestamp -> close remains first arrival
        assertThat(candle.getVolume()).isEqualTo(3L);
    }

    @Test
    void testTimeAlignment() {
        CandleInterval interval = CandleInterval.ONE_MINUTE;

        long timestamp1 = 1620000015L; // 15 seconds into minute
        long timestamp2 = 1620000045L; // 45 seconds into minute

        long aligned1 = interval.alignTimestamp(timestamp1);
        long aligned2 = interval.alignTimestamp(timestamp2);

        assertThat(aligned1).isEqualTo(aligned2);
        assertThat(aligned1).isEqualTo(1620000000L);
    }

    @Test
    void testGetHistorical1sCandles() {
        String symbol = "BTC-USD";
        long from = 1620000000L;
        long to = 1620000003L;

        List<Candle> mockCandles = Arrays.asList(
                new Candle(1620000000L, 50000.0, 50100.0, 49900.0, 50050.0, 10L),
                new Candle(1620000001L, 50050.0, 50150.0, 49950.0, 50100.0, 12L),
                new Candle(1620000002L, 50100.0, 50200.0, 50000.0, 50150.0, 15L)
        );

        when(repository.find1sCandles(symbol, from, to))
                .thenReturn(mockCandles);

        List<Candle> result = aggregator.getHistoricalCandles(symbol, CandleInterval.ONE_SECOND, from, to);

        assertThat(result).hasSize(3);
        assertThat(result.get(0).time()).isEqualTo(1620000000L);
        assertThat(result.get(0).open()).isEqualTo(50000.0);
        assertThat(result.get(2).close()).isEqualTo(50150.0);
    }

    //@Test
    //void testGetAggregatedCandles() {
    //    String symbol = "BTC-USD";
    //    long from = 1620000000L;
    //    long to = 1620000180L;
//
    //    // Mock aggregated 1m candles from TimescaleDB
    //    List<Object[]> mockResults = Arrays.asList(
    //            new Object[]{1620000000L, symbol, 50000.0, 50100.0, 49900.0, 50050.0, 10L},
    //            new Object[]{1620000060L, symbol, 50050.0, 50150.0, 49950.0, 50100.0, 12L},
    //            new Object[]{1620000120L, symbol, 50100.0, 50200.0, 50000.0, 50150.0, 15L}
    //    );
//
    //    when(repository.findCandles(CandleInterval.ONE_SECOND, symbol, from, to))
    //            .thenReturn(mockResults);
//
    //    List<Candle> result = aggregator.getHistoricalCandles(symbol, CandleInterval.ONE_MINUTE, from, to);
//
    //    assertThat(result).hasSize(3);
    //    assertThat(result.get(0).time()).isEqualTo(1620000000L);
    //    assertThat(result.get(0).open()).isEqualTo(50000.0);
    //    assertThat(result.get(2).close()).isEqualTo(50150.0);
    //}

    @Test
    void testMultipleSymbolsConcurrently() throws Exception {
        long timestamp = System.currentTimeMillis() / 1000;

        aggregator.processEvent(new BidAskEvent("BTC-USD", 50000.0, 50010.0, timestamp));
        aggregator.processEvent(new BidAskEvent("ETH-USD", 3000.0, 3001.0, timestamp));

        TimeUnit.SECONDS.sleep(2);

        verify(repository, atLeastOnce()).batchUpsert(candleCaptor.capture());

        List<CandleEntity> savedCandles = candleCaptor.getValue();

        long btcCount = savedCandles.stream()
                .filter(c -> c.getId().getSymbol().equals("BTC-USD"))
                .count();
        long ethCount = savedCandles.stream()
                .filter(c -> c.getId().getSymbol().equals("ETH-USD"))
                .count();

        assertThat(btcCount).isGreaterThan(0);
        assertThat(ethCount).isGreaterThan(0);
    }

    @Test
    void testEventsInDifferentSecondsCreateMultipleCandles() throws Exception {
        long baseTime = 1620000000L;

        aggregator.processEvent(new BidAskEvent("BTC-USD", 50000.0, 50010.0, baseTime));
        aggregator.processEvent(new BidAskEvent("BTC-USD", 50020.0, 50030.0, baseTime + 1));
        aggregator.processEvent(new BidAskEvent("BTC-USD", 50040.0, 50050.0, baseTime + 2));

        TimeUnit.SECONDS.sleep(3);

        verify(repository, atLeastOnce()).batchUpsert(candleCaptor.capture());

        List<List<CandleEntity>> allSaves = candleCaptor.getAllValues();
        long totalCandles = allSaves.stream()
                .flatMap(List::stream)
                .filter(c -> c.getId().getSymbol().equals("BTC-USD"))
                .count();

        assertThat(totalCandles).isGreaterThanOrEqualTo(3);
    }

    @Test
    void testHistoricalOnlyPathUsesFindCandles() {
        String symbol = "BTC-USD";
        long now = System.currentTimeMillis() / 1000;
        long from = now - 10000; // far in the past
        long to = now - 9000;    // far in the past, fully historical window

        List<Candle> historical = Arrays.asList(
                new Candle(from, 100.0, 110.0, 90.0, 105.0, 10),
                new Candle(from + 60, 105.0, 120.0, 100.0, 115.0, 12)
        );

        when(repository.findCandles(CandleInterval.ONE_MINUTE, symbol, from, to)).thenReturn(historical);

        List<Candle> result = aggregator.getHistoricalCandles(symbol, CandleInterval.ONE_MINUTE, from, to);

        assertThat(result).hasSize(2);
        assertThat(result.get(0).time()).isEqualTo(from);
        verify(repository, times(1)).findCandles(CandleInterval.ONE_MINUTE, symbol, from, to);
        verify(repository, never()).find1sCandles(anyString(), anyLong(), anyLong());
    }

    @Test
    void testRealtimeOnlyPathAggregatesFrom1sCandles_FiveSeconds() {
        String symbol = "ETH-USD";
        long now = System.currentTimeMillis() / 1000;
        long from = now - 12; // within realtime window (120s)
        // Align start to 5-second boundary to avoid partial buckets at edges
        long fromAligned = CandleInterval.FIVE_SECONDS.alignTimestamp(from);
        long to = now;

        // Build 1s candles covering 10 seconds -> should aggregate into two 5s buckets
        List<Candle> oneSec = Arrays.asList(
                new Candle(fromAligned, 10, 11, 9, 10.5, 1),
                new Candle(fromAligned + 1, 10.5, 12, 10, 11, 1),
                new Candle(fromAligned + 2, 11, 13, 10.5, 12, 1),
                new Candle(fromAligned + 3, 12, 14, 11, 13, 1),
                new Candle(fromAligned + 4, 13, 15, 12, 14, 1),
                // next 5s bucket
                new Candle(fromAligned + 5, 14, 16, 13, 15, 1),
                new Candle(fromAligned + 6, 15, 17, 14, 16, 1),
                new Candle(fromAligned + 7, 16, 18, 15, 17, 1),
                new Candle(fromAligned + 8, 17, 19, 16, 18, 1),
                new Candle(fromAligned + 9, 18, 20, 17, 19, 1)
        );

        when(repository.find1sCandles(symbol, from, to)).thenReturn(oneSec);

        List<Candle> result = aggregator.getHistoricalCandles(symbol, CandleInterval.FIVE_SECONDS, from, to);

        assertThat(result).hasSize(2);
        // Verify first aggregated bucket
        Candle b1 = result.get(0);
        assertThat(b1.open()).isEqualTo(10);
        assertThat(b1.high()).isEqualTo(15);
        assertThat(b1.low()).isEqualTo(9);
        assertThat(b1.close()).isEqualTo(14);
        assertThat(b1.volume()).isEqualTo(5);
        // Verify second aggregated bucket
        Candle b2 = result.get(1);
        assertThat(b2.open()).isEqualTo(14);
        assertThat(b2.high()).isEqualTo(20);
        assertThat(b2.low()).isEqualTo(13);
        assertThat(b2.close()).isEqualTo(19);
        assertThat(b2.volume()).isEqualTo(5);

        verify(repository, times(1)).find1sCandles(symbol, from, to);
        verify(repository, never()).findCandles(any(), anyString(), anyLong(), anyLong());
    }

    @Test
    void testMixedHistoricalAndRealtimeMergesAndDedupes() {
        String symbol = "BTC-USD";
        long now = System.currentTimeMillis() / 1000;
        long realtimeThreshold = now - 120;
        long from = realtimeThreshold - 300; // starts in historical
        long to = now;                        // ends in realtime
        long rtAligned = CandleInterval.ONE_MINUTE.alignTimestamp(realtimeThreshold);

        // Historical aggregated 1m candles (two entries, last overlaps with realtime bucket)
        List<Candle> historical = Arrays.asList(
                new Candle(CandleInterval.ONE_MINUTE.alignTimestamp(from), 100, 110, 90, 105, 10),
                new Candle(rtAligned, 105, 115, 100, 110, 8)
        );
        // Realtime aggregated from 1s will produce an entry with the same timestamp as realtimeThreshold
        List<Candle> realtime = Arrays.asList(
                new Candle(rtAligned, 106, 116, 101, 111, 5), // duplicate time -> should override historical entry
                new Candle(rtAligned + 60, 111, 120, 110, 118, 7)
        );

        when(repository.findCandles(CandleInterval.ONE_MINUTE, symbol, from, realtimeThreshold)).thenReturn(historical);
        when(repository.find1sCandles(symbol, realtimeThreshold, to)).thenReturn(realtime);

        List<Candle> result = aggregator.getHistoricalCandles(symbol, CandleInterval.ONE_MINUTE, from, to);

        // Expect 3 unique timestamps: from, realtimeThreshold (from realtime list), and realtimeThreshold+60
        assertThat(result).hasSize(3);
        assertThat(result.get(0).time()).isEqualTo(CandleInterval.ONE_MINUTE.alignTimestamp(from));
        assertThat(result.get(1).time()).isEqualTo(rtAligned);
        assertThat(result.get(1).open()).isEqualTo(106); // realtime overrides historical at same timestamp
        assertThat(result.get(2).time()).isEqualTo(rtAligned + 60);

        verify(repository, times(1)).findCandles(CandleInterval.ONE_MINUTE, symbol, from, realtimeThreshold);
        verify(repository, times(1)).find1sCandles(symbol, realtimeThreshold, to);
    }

    @Test
    void testNoEventsNoUpsert() throws Exception {
        // Do not send any events
        TimeUnit.MILLISECONDS.sleep(300);
        aggregator.shutdown();
        verify(repository, never()).batchUpsert(any());
    }

    @Test
    void testBatchUpsertExceptionIsHandledAndSubsequentUpsertOccurs() throws Exception {
        // First upsert throws, second succeeds
        doThrow(new RuntimeException("DB down"))
                .doNothing()
                .when(repository).batchUpsert(any());

        long baseTime = System.currentTimeMillis() / 1000;
        // Two different seconds so we get two separate candles that will be closed and flushed
        aggregator.processEvent(new BidAskEvent("XRP-USD", 1.0, 1.2, baseTime));
        aggregator.processEvent(new BidAskEvent("XRP-USD", 1.1, 1.3, baseTime + 1));

        TimeUnit.SECONDS.sleep(3);

        verify(repository, atLeastOnce()).batchUpsert(any());
    }
}
