package com.multibank.application.service.impl;

import com.multibank.application.entity.CandleEntity;
import com.multibank.application.model.BidAskEvent;
import com.multibank.application.model.Candle;
import com.multibank.application.model.CandleInterval;
import com.multibank.application.repository.CandleCustomJdbcRepository;
import com.multibank.application.service.CandleAggregationService;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;

@Slf4j
@Service
public class CandleAggregationServiceImpl implements CandleAggregationService {

    private final CandleCustomJdbcRepository repository;

    // Open candle buffer - keeps candles that are still accumulating ticks
    private final Map<String, CandleBuilder> openCandleBuffer;

    // Closed candle buffer - ready for batch upsert
    private final Map<String, CandleBuilder> closedCandleBuffer;

    private final ScheduledExecutorService scheduler;
    private final ExecutorService eventProcessor;

    private int flushIntervalMs = 150;

    private int candleCloseDelayMs = 100;

    private int realtimeWindowSeconds = 120;

    public CandleAggregationServiceImpl(CandleCustomJdbcRepository repository) {
        this.repository = repository;
        this.openCandleBuffer = new ConcurrentHashMap<>();
        this.closedCandleBuffer = new ConcurrentHashMap<>();
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.eventProcessor = Executors.newFixedThreadPool(8);

        startPeriodicTasks();
    }

    @Override
    public void processEvent(BidAskEvent event) {
        eventProcessor.submit(() -> {
            try {
                double price = event.midPrice();
                long eventTimestamp = event.timestamp();
                long alignedTime = CandleInterval.ONE_SECOND.alignTimestamp(eventTimestamp);
                String key = buildKey(event.symbol(), alignedTime);

                // Add price to open candle buffer with timestamp for order-independence
                openCandleBuffer.computeIfAbsent(key, k ->
                        new CandleBuilder(event.symbol(), alignedTime, System.currentTimeMillis())
                ).addPrice(price, eventTimestamp);

            } catch (Exception e) {
                log.error("Error processing event: {}", event, e);
            }
        });
    }

    @Override
    public List<Candle> getHistoricalCandles(String symbol, CandleInterval interval, long from, long to) {
        long now = System.currentTimeMillis() / 1000;
        long realtimeThreshold = now - realtimeWindowSeconds;

        if (interval == CandleInterval.ONE_SECOND) {
            // For 1s candles, always query directly from database
            return query1sCandles(symbol, from, to);
        }

        // For larger intervals, use hybrid approach
        if (to < realtimeThreshold) {
            // Fully historical
            return queryAggregatedCandles(symbol, interval, from, to);
        }

        if (from >= realtimeThreshold) {
            // Fully realtime - aggregate from 1s candles
            return aggregateFrom1sCandles(symbol, interval, from, to);
        }

        // Mixed: historical + realtime
        List<Candle> historical = queryAggregatedCandles(symbol, interval, from, realtimeThreshold);
        List<Candle> realtime = aggregateFrom1sCandles(symbol, interval, realtimeThreshold, to);

        // Merge and return. I use a treemap with timestamp as key to sort the results just in case
        // and to handle possible duplicates
        Map<Long, Candle> candleMap = new TreeMap<>();

        for (Candle c : historical) {
            candleMap.put(c.time(), c);
        }

        for (Candle c : realtime) {
            candleMap.put(c.time(), c);
        }

        return new ArrayList<>(candleMap.values());
    }

    /**
     * Query 1s candles directly from database (optimized with indexes)
     */
    private List<Candle> query1sCandles(String symbol, long from, long to) {
        List<Candle> entities = repository.find1sCandles(symbol, from, to);
        return entities;
    }

    /**
     * Query pre-aggregated candles using TimescaleDB time_bucket (fast for historical)
     */
    private List<Candle> queryAggregatedCandles(String symbol, CandleInterval interval, long from, long to) {
        return repository.findCandles(interval, symbol, from, to);
    }

    /**
     * Aggregate from raw 1s candles (accurate for recent data)
     * Queries database, not in-memory cache (simpler, stateless)
     */
    private List<Candle> aggregateFrom1sCandles(String symbol, CandleInterval interval, long from, long to) {
        List<Candle> raw1sCandles = repository.find1sCandles(symbol, from, to);

        return aggregateInMemory(raw1sCandles, interval);
    }

    /**
     * In-memory aggregation of 1s candles to larger intervals
     */
    private List<Candle> aggregateInMemory(List<Candle> raw1sCandles, CandleInterval interval) {
        Map<Long, CandleAggregator> aggregators = new LinkedHashMap<>();

        for (Candle candle : raw1sCandles) {
            long bucketTime = interval.alignTimestamp(candle.time());

            aggregators.computeIfAbsent(bucketTime, t -> new CandleAggregator())
                    .add(candle.open(), candle.high(), candle.low(),
                            candle.close(), candle.volume());
        }

        return aggregators.entrySet().stream()
                .map(e -> e.getValue().build(e.getKey()))
                .toList();
    }

    private void startPeriodicTasks() {
        // Task 1: Move open candles to closed buffer after candleCloseDelayMs
        scheduler.scheduleAtFixedRate(() -> {
            try {
                closeMaturedCandles();
            } catch (Exception e) {
                log.error("Error closing matured candles", e);
            }
        }, candleCloseDelayMs, 50, TimeUnit.MILLISECONDS);

        // Task 2: Batch upsert closed candles every flushIntervalMs
        scheduler.scheduleAtFixedRate(() -> {
            try {
                batchUpsertClosedCandles();
            } catch (Exception e) {
                log.error("Error batch upserting candles", e);
            }
        }, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Move candles from open buffer to closed buffer after candleCloseDelayMs.
     * This keeps the candle open for 100-200ms to accumulate all ticks from that second.
     */
    private void closeMaturedCandles() {
        long currentTimeMs = System.currentTimeMillis();
        List<String> keysToClose = new ArrayList<>();

        openCandleBuffer.forEach((key, builder) -> {
            long candleAgeMs = currentTimeMs - builder.getCreatedAtMs();
            long timeSinceCandleSecond = currentTimeMs - (builder.getStartTime());

            // Close candle if:
            // 1. It's been open for at least candleCloseDelayMs (100-200ms)
            // 2. We're in the next second (to avoid closing current second's candle too early)
            if (candleAgeMs >= candleCloseDelayMs && timeSinceCandleSecond >= 1000) {
                keysToClose.add(key);
            }
        });

        // Move to closed buffer
        for (String key : keysToClose) {
            CandleBuilder builder = openCandleBuffer.remove(key);
            if (builder != null) {
                closedCandleBuffer.put(key, builder);
                log.debug("Closed candle: {} {} after {}ms",
                        builder.getSymbol(), builder.getStartTime(),
                        System.currentTimeMillis() - builder.getCreatedAtMs());
            }
        }
    }

    /**
     * Batch upsert all closed candles to database.
     * Uses PostgreSQL ON CONFLICT for efficient upserts.
     */
    private void batchUpsertClosedCandles() {
        if (closedCandleBuffer.isEmpty()) {
            return;
        }

        // Snapshot and clear the closed buffer
        Map<String, CandleBuilder> snapshot = new HashMap<>(closedCandleBuffer);
        closedCandleBuffer.clear();

        // Build entities for batch upsert
        List<CandleEntity> entities = snapshot.values().stream()
                .map(builder -> {
                    Candle candle = builder.build();
                    return new CandleEntity(
                            new CandleEntity.CandleId(candle.time(), builder.getSymbol()),
                            candle.open(),
                            candle.high(),
                            candle.low(),
                            candle.close(),
                            candle.volume()
                    );
                })
                .toList();

        // Batch upsert to database
        long startTime = System.currentTimeMillis();
        try {
            repository.batchUpsert(entities);
            long duration = System.currentTimeMillis() - startTime;

            log.info("Batch upserted {} candles in {}ms (avg: {}ms/candle)",
                    entities.size(), duration,
                    entities.isEmpty() ? 0 : String.format("%.2f", (double)duration / entities.size()));

        } catch (Exception e) {
            log.error("Failed to batch upsert {} candles", entities.size(), e);
        }
    }

    @PreDestroy
    @Override
    public void shutdown() {
        log.info("Shutting down service");
        scheduler.shutdown();
        eventProcessor.shutdown();

        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            if (!eventProcessor.awaitTermination(5, TimeUnit.SECONDS)) {
                eventProcessor.shutdownNow();
            }

            // Final flush of all pending candles
            closeMaturedCandles();
            batchUpsertClosedCandles();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String buildKey(String symbol, long time) {
        return symbol + ":" + time;
    }

    /**
     * Thread-safe candle builder with creation timestamp tracking.
     * Handles out-of-order events by tracking timestamps for open and close prices.
     * Aggregates ticks into a candle
     */
    private static class CandleBuilder {
        private final String symbol;
        private final long startTime;
        private final long createdAtMs;

        // Timestamp tracking for open/close (handles out-of-order events)
        private long openTimestamp = Long.MAX_VALUE;
        private long closeTimestamp = Long.MIN_VALUE;

        private double open;
        private double high = Double.MIN_VALUE;
        private double low = Double.MAX_VALUE;
        private double close;
        private long volume;
        private boolean initialized;

        CandleBuilder(String symbol, long startTime, long createdAtMs) {
            this.symbol = symbol;
            this.startTime = startTime;
            this.createdAtMs = createdAtMs;
            this.initialized = false;
            this.volume = 0;
        }

        /**
         * Add a price tick with its timestamp.
         * Handles out-of-order events correctly:
         * - Open: price with earliest timestamp
         * - Close: price with latest timestamp
         * - High/Low: min/max regardless of order
         */
        synchronized void addPrice(double price, long eventTimestamp) {
            if (!initialized) {
                high = price;
                low = price;
                initialized = true;
            } else {
                high = Math.max(high, price);
                low = Math.min(low, price);
            }
            volume++;

            if (eventTimestamp < openTimestamp) {
                openTimestamp = eventTimestamp;
                open = price;
            }

            if (eventTimestamp > closeTimestamp) {
                closeTimestamp = eventTimestamp;
                close = price;
            }
        }

        synchronized Candle build() {
            return new Candle(startTime, open, high, low, close, volume);
        }

        String getSymbol() {
            return symbol;
        }

        long getStartTime() {
            return startTime;
        }

        long getCreatedAtMs() {
            return createdAtMs;
        }
    }

    // Aggregates 1s candles into larger interval candles
    private static class CandleAggregator {
        private double open = 0;
        private double high = Double.MIN_VALUE;
        private double low = Double.MAX_VALUE;
        private double close = 0;
        private long volume = 0;
        private boolean firstTick = true;

        void add(double o, double h, double l, double c, long v) {
            if (firstTick) {
                open = o;
                firstTick = false;
            }
            high = Math.max(high, h);
            low = Math.min(low, l);
            close = c;
            volume += v;
        }

        Candle build(long time) {
            return new Candle(time, open, high, low, close, volume);
        }
    }
}
