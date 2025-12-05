package com.multibank.application.datagenerator;

import com.multibank.application.model.BidAskEvent;
import com.multibank.application.service.CandleAggregationService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.*;

@Component
@Slf4j
@RequiredArgsConstructor
public class DataGenerator {

    private final CandleAggregationService candleAggregationService;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(8);

    private final Map<String, Double> initialPricePerSymbol = new HashMap<>() {{
        put("BTC-USD", 92300.0);
        put("ETH-USD", 3100.0);;
        put("USDT-USD", 1.0);
        put("XRP-USD", 2.1);
        put("BNB-USD", 899.66);
        put("USDC-USD", 0.997);
        put("SOL-USD", 139.2);
        put("TRX-USD", 0.285);
    }};

    private final int eventsPerSecondPerSymbol = 50;

    private final Map<String, Double> currentPricePerSymbol = new ConcurrentHashMap<>();

    @PostConstruct
    public void startDataGenerator() {
        currentPricePerSymbol.putAll(initialPricePerSymbol);
        Set<String> symbols = initialPricePerSymbol.keySet();

        long delayMs = 1000L / eventsPerSecondPerSymbol;

        for (String symbol : symbols) {
            scheduler.scheduleAtFixedRate(
                    () -> generateEvent(symbol),
                    0,
                    delayMs,
                    TimeUnit.MILLISECONDS
            );
        }

        log.info("Started market data simulation for {} symbols at {} events/sec",
                symbols.size(), eventsPerSecondPerSymbol);
    }

    private void generateEvent(String symbol) {
        try {
            double basePrice = currentPricePerSymbol.get(symbol);
            double onePercentOfPrice = basePrice / 100;
            double priceChange = ThreadLocalRandom.current().nextDouble(-1, 1) * onePercentOfPrice;
            double newPrice = basePrice + priceChange;

            currentPricePerSymbol.put(symbol, newPrice);

            double spread = newPrice * 0.0005;
            double bid = BigDecimal.valueOf(newPrice - spread / 2)
                    .setScale(4, RoundingMode.HALF_UP).doubleValue();
            double ask = BigDecimal.valueOf(newPrice + spread / 2)
                    .setScale(4, RoundingMode.HALF_UP).doubleValue();

            BidAskEvent event = new BidAskEvent(
                    symbol,
                    bid,
                    ask,
                    System.currentTimeMillis()
            );

            // System.err.println(event);
            // candleAggregationService.processEvent(event);
        } catch (Exception e) {
            log.error("Error generating event for {}", symbol, e);
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Stopping data generator");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
