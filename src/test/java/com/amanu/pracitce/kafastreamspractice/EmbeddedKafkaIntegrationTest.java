package com.amanu.pracitce.kafastreamspractice;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.time.LocalDate;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.assertj.core.util.Streams;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.amanu.pracitce.kafastreamspractice.domain.Balance;
import com.amanu.pracitce.kafastreamspractice.domain.BalanceKey;

import lombok.extern.slf4j.Slf4j;

@SpringBootTest
@EmbeddedKafka(partitions = 1,
        brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" },
        topics = { "${test.input}", "${test.positions.output}", "${test.balances.output}" })
@Slf4j
public class EmbeddedKafkaIntegrationTest {
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Value("${test.positions.output}")
    private String positionsOutput;
    @Value("${test.balances.output}")
    private String balancesOutput;


    @Test
    public void givenEmbeddedKafkaBroker_whenSendingtoSimpleProducer_thenMessageReceived() throws Exception {


        publishRecords(balancesOutput, BalanceKey.generate(LocalDate.now(), 10).stream()
                .map(balanceKey -> new KeyValue(balanceKey, Balance.builder().balance(RandomUtils.nextInt()).build())));

        Map<String, Object> consumerProps = consumerProps();

        Set<ConsumerRecord<Object, Object>> positions = getConsumerRecords(consumerProps, positionsOutput).collect(Collectors.toSet());

        log.info("Received positions {}", positions);
        assertThat(positions.size(), equalTo(1));
    }

    private <K, V> Stream<ConsumerRecord<K, V>> getConsumerRecords(final Map<String, Object> consumerProps, final String topic) {
        ConsumerFactory<K, V> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        Consumer<K, V> consumer = cf.createConsumer();

        this.embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, topic);
        return Streams.stream(KafkaTestUtils.getRecords(consumer, 10000000).records(topic).iterator());
    }

    private <K, V> void publishRecords(final String topic, Stream<KeyValue<K, V>> records) {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.springframework.kafka.support.serializer.JsonSerializer");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.springframework.kafka.support.serializer.JsonSerializer");
        producerProps.put("spring.json.trusted.packages", "com.amanu.pracitce.kafastreamspractice.domain");
        ProducerFactory<K, V> cf = new DefaultKafkaProducerFactory<>(producerProps);
        Producer<K, V> producer = cf.createProducer();


        records.forEach(p -> {

            producer.send(new ProducerRecord<>(topic, p.key, p.value));
        });
    }


    private Map<String, Object> consumerProps() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "true", this.embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.springframework.kafka.support.serializer.JsonDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.springframework.kafka.support.serializer.JsonDeserializer");
        consumerProps.put("spring.json.trusted.packages", "com.amanu.pracitce.kafastreamspractice.domain,java.time");
        return consumerProps;
    }
}
