package com.multibank.application.entity;

import com.multibank.application.config.TimestampConverter;
import com.multibank.application.model.Candle;
import jakarta.persistence.*;
import lombok.*;

import java.io.Serializable;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Entity(name = "candles_1s")
public class CandleEntity {

    @EmbeddedId
    private CandleId id;

    @Column(nullable = false)
    private Double open;

    @Column(nullable = false)
    private Double high;

    @Column(nullable = false)
    private Double low;

    @Column(nullable = false)
    private Double close;

    @Column(nullable = false)
    private Long volume;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Embeddable
    public static class CandleId implements Serializable {
        @Convert(converter = TimestampConverter.class)
        private Long time;
        private String symbol;
    }
    
}
