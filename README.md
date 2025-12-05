Project overview

This Spring Boot application ingests bid/ask tick events, aggregates them into OHLCV candles across multiple intervals, and exposes an HTTP endpoint to retrieve historical candles.

- Tech stack: Java 25, Spring Boot 4 (Web MVC, Data JPA), PostgreSQL (TimeScale DB)
- Key components: HistoryController (API), CandleAggregationService/Impl (aggregation), CandleRepository (persistence), CandleInterval (time buckets)
- Endpoint: GET /history?symbol=SYMBOL&interval=1m&from=FROM&to=TO returns arrays t/o/h/l/c/v with s=ok; invalid intervals return s=error with errmsg
- in src/main/resources/db/db-init.sql we can see the schema for generating the table and continuous materialized views in TimeScale DB. I have added also indexes on the main table and the materialized views, as well as compression policy on the main table

Assumptions or trade-offs

- Mid price (bid+ask)/2 is used for OHLC values; each event contributes volume = 1
- Timestamps are aligned to interval boundaries; supported intervals: 1s, 5s, 1m, 15m, 1h
- Minimal validation (no auth/pagination); focus on aggregation correctness and simple API
- Persistence prefers batch upserts to PostgreSQL; tests mock the repository (DB not required for running tests)

Instructions for running tests

- Prerequisites: Internet access so Gradle can download the toolchain and dependencies
- Windows (PowerShell/CMD): gradlew.bat test
- macOS/Linux: ./gradlew test
- Notes: Tests run on JUnit Platform and do not require a running database

Optional: run the application

- Configure PostgreSQL in src/main/resources/application.yml (defaults to localhost, database candle_aggregation_service, user postgres, password postgres)
- Initialize schema via src/main/resources/db/db-init.sql
- Start: Windows -> gradlew.bat bootRun; macOS/Linux -> ./gradlew bootRun
- Example request: http://localhost:8080/history?symbol=BTC-USD&interval=1m&from=1620000000&to=1620000300

Updated: 2025-12-05