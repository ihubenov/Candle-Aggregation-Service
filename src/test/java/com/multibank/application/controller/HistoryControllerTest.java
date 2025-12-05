package com.multibank.application.controller;

import com.multibank.application.model.Candle;
import com.multibank.application.model.CandleInterval;
import com.multibank.application.service.CandleAggregationService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.hamcrest.Matchers.containsString;

class HistoryControllerTest {

    @Test
    @DisplayName("HistoryController returns mapped arrays and ok status for valid request")
    void getHistory_ok() {
        String symbol = "BTC-USD";
        long from = 1_620_000_000L;
        long to = from + 180;

        List<Candle> candles = Arrays.asList(
                new Candle(from, 100, 110, 90, 105, 10),
                new Candle(from + 60, 105, 120, 100, 115, 12)
        );

        CandleAggregationService service = Mockito.mock(CandleAggregationService.class);
        when(service.getHistoricalCandles(symbol, CandleInterval.ONE_MINUTE, from, to)).thenReturn(candles);

        HistoryController controller = new HistoryController(service);

        ResponseEntity<Map<String, Object>> response = controller.getHistory(symbol, "1m", from, to);

        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        Map<String, Object> body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.get("s")).isEqualTo("ok");
        List<?> t = (List<?>) body.get("t");
        List<?> o = (List<?>) body.get("o");
        List<?> h = (List<?>) body.get("h");
        List<?> l = (List<?>) body.get("l");
        List<?> c = (List<?>) body.get("c");
        List<?> v = (List<?>) body.get("v");

        assertThat(t).hasSize(2);
        assertThat(((Number) t.get(0)).longValue()).isEqualTo(from);
        assertThat(((Number) t.get(1)).longValue()).isEqualTo(from + 60);

        assertThat(o).hasSize(2);
        assertThat(((Number) o.get(0)).doubleValue()).isEqualTo(100.0);
        assertThat(((Number) o.get(1)).doubleValue()).isEqualTo(105.0);

        assertThat(h).hasSize(2);
        assertThat(((Number) h.get(0)).doubleValue()).isEqualTo(110.0);
        assertThat(((Number) h.get(1)).doubleValue()).isEqualTo(120.0);

        assertThat(l).hasSize(2);
        assertThat(((Number) l.get(0)).doubleValue()).isEqualTo(90.0);
        assertThat(((Number) l.get(1)).doubleValue()).isEqualTo(100.0);

        assertThat(c).hasSize(2);
        assertThat(((Number) c.get(0)).doubleValue()).isEqualTo(105.0);
        assertThat(((Number) c.get(1)).doubleValue()).isEqualTo(115.0);

        assertThat(v).hasSize(2);
        assertThat(((Number) v.get(0)).longValue()).isEqualTo(10L);
        assertThat(((Number) v.get(1)).longValue()).isEqualTo(12L);

        verify(service).getHistoricalCandles(symbol, CandleInterval.ONE_MINUTE, from, to);
    }

    @Test
    @DisplayName("HistoryController returns error payload for invalid interval label")
    void getHistory_invalidInterval() {
        HistoryController controller = new HistoryController(Mockito.mock(CandleAggregationService.class));

        MockMvc mockMvc = MockMvcBuilders
                .standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();

        try {
            mockMvc.perform(get("/history")
                            .param("symbol", "ETH-USD")
                            .param("interval", "2m")
                            .param("from", String.valueOf(1620000000L))
                            .param("to", String.valueOf(1620000300L)))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.s").value("error"))
                    .andExpect(jsonPath("$.errmsg", containsString("Unknown interval")));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @DisplayName("HistoryController returns empty arrays when no candles are available")
    void getHistory_empty() {
        String symbol = "XRP-USD";
        long from = 1_620_000_000L;
        long to = from + 60;

        CandleAggregationService service = Mockito.mock(CandleAggregationService.class);
        when(service.getHistoricalCandles(symbol, CandleInterval.ONE_MINUTE, from, to)).thenReturn(Collections.emptyList());

        HistoryController controller = new HistoryController(service);

        ResponseEntity<Map<String, Object>> response = controller.getHistory(symbol, "1m", from, to);

        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        Map<String, Object> body = response.getBody();
        assertThat(body).isNotNull();
        assertThat((List<?>) body.get("t")).isEmpty();
        assertThat((List<?>) body.get("o")).isEmpty();
        assertThat((List<?>) body.get("h")).isEmpty();
        assertThat((List<?>) body.get("l")).isEmpty();
        assertThat((List<?>) body.get("c")).isEmpty();
        assertThat((List<?>) body.get("v")).isEmpty();
    }
}
