package com.multibank.application.repository;

import com.multibank.application.entity.CandleEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CandleRepository extends JpaRepository<CandleEntity, CandleEntity.CandleId>, CandleCustomJdbcRepository {

    // @Query(value = "SELECT * FROM candles_1s WHERE symbol = ?1 " +
    //         "AND time >= to_timestamp(?2 / 1000) AND time <= to_timestamp(?3 / 1000) ORDER BY time ASC",
    //         nativeQuery = true)
    // List<CandleEntity> findCandles1s(
    //         @Param("symbol") String symbol,
    //         @Param("from") Long from,
    //         @Param("to") Long to
    // );
}
