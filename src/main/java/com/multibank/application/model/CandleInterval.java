package com.multibank.application.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum CandleInterval {
    ONE_SECOND("1s", 1),
    FIVE_SECONDS("5s", 5),
    ONE_MINUTE("1m", 60),
    FIFTEEN_MINUTES("15m", 900),
    ONE_HOUR("1h", 3600);

    private final String label;
    private final long seconds;

    public static CandleInterval fromLabel(String label) {
        for (CandleInterval interval : values()) {
            if (interval.label.equals(label)) {
                return interval;
            }
        }
        throw new IllegalArgumentException("Unknown interval: " + label);
    }

    public long alignTimestamp(long timestamp) {
        return (timestamp / seconds) * seconds;
    }
}