package com.amanu.pracitce.kafastreamspractice;

import java.time.LocalDate;
import java.util.function.Function;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.amanu.pracitce.kafastreamspractice.domain.Balance;
import com.amanu.pracitce.kafastreamspractice.domain.BalanceKey;
import com.amanu.pracitce.kafastreamspractice.domain.DailyPosition;
import com.amanu.pracitce.kafastreamspractice.domain.DailyPositionKey;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class Processors {

    @Bean
    public Function<KStream<BalanceKey, Balance>, KStream<LocalDate, DailyPosition>> positions() {
        return input ->
                input
                        .peek((key, value) -> {
                            log.warn("Received balances {} {}", key, value);
                        })

//                        .selectKey((BalanceKey key, Balance value) -> DailyPositionKey.builder().date(key.getDate()).build())
//                        .peek((key, value) -> {
//                            log.info("Selected key {}", key);
//                        })
                        .map((key, value) -> {
                            return KeyValue.pair(key.getDate(), value.getBalance());
                        })

//                        .groupByKey(Grouped.with(new JsonSerde<>(DailyPositionKey.class), new JsonSerde<>(Balance.class).noTypeInfo()))
                        .groupByKey(Grouped.with(new JsonSerde<>(LocalDate.class), new Serdes.IntegerSerde()))

//                        .windowedBy(SessionWindows.with(Duration.ofSeconds(5)).grace(Duration.ZERO))

                        .reduce(Integer::sum)
/*
                        .aggregate(() -> 0,
                                (DailyPositionKey key, Balance value, Integer aggregate) -> aggregate + value.getBalance(),
                                Materialized.with(new JsonSerde<DailyPositionKey>().ignoreTypeHeaders(), new Serdes.IntegerSerde()))
*/
/*
                        .aggregate(() -> (Integer) 0,
                                (DailyPositionKey key, Balance value, Integer aggregate) -> aggregate + value.getBalance()*/
/*,
                                (DailyPositionKey aggKey, Integer aggOne, Integer aggTwo) -> aggOne + aggTwo*//*
                 */
/*,
                                Materialized.with(new JsonSerde<Windowed<DailyPositionKey>>(), new Serdes.IntegerSerde())*//*
)
*/

                        .toStream()
//                        .selectKey((Windowed<DailyPositionKey> key, Integer value) -> key.key())
//                        .selectKey((k, v) -> k.getDate())
                        .mapValues((Integer val) -> DailyPosition.builder().totalBalance(val).build())
                        .peek((key, value) -> {
                            log.warn("Received position {} {}", key, value);
                        });
    }
}
