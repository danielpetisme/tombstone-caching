package com.sample.app;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.sample.app.TestUtils.*;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

public class SimpleTest {

    private static final Logger logger = LoggerFactory.getLogger(SimpleTest.class);

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"));

    public static final String INPUT_TOPIC = "input";
    public static final String OUTPUT_TOPIC = "output";
    public static final String STATE_STORE_NAME = "DUMMY_STATE_STORE";
    public static final String CHANGELOG_TOPIC_NAME = "tombstone-caching-DUMMY_STATE_STORE-changelog";
    public static final int NUM_PARTITIONS = 1;
    public static final int REPLICATION_FACTOR = 1;

    Properties consumerProperties;

    Properties producerProperties;

    Properties streamProperties;

    KafkaStreams kafkaStreams;

    File stateDir;

    @BeforeEach
    public void beforeEach() throws ExecutionException, InterruptedException {
        kafka.start();

        this.stateDir = TestUtils.tempDirectory(SimpleTest.class.getName() + Math.random());

        streamProperties = toProperties(Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "tombstone-caching",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName(),
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
                // Use a temporary directory for storing state, which will be automatically removed after the test.
                StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath()
               // ,StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0
        ));

        consumerProperties = toProperties(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, SimpleTest.class.getName() + Math.random()
        ));

        producerProperties = toProperties(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        ));

        createTopic(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()),
                INPUT_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR, Collections.emptyMap()
        );

        var topology = buildTopology(INPUT_TOPIC, OUTPUT_TOPIC);
        logger.info(topology.describe().toString());

        this.kafkaStreams = new KafkaStreams(topology, streamProperties);
        Runtime.getRuntime().addShutdownHook(new Thread(this.kafkaStreams::close));

    }

    public static final Topology buildTopology(String inputTopic, String outputTopic) {
        StreamsBuilder builder = new StreamsBuilder();

        final StoreBuilder<KeyValueStore<Integer, String>> myStore = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore(STATE_STORE_NAME),
                        Serdes.Integer(), Serdes.String())
                //.withCachingDisabled()
                .withLoggingEnabled(new HashMap<>());
        builder.addStateStore(myStore);

        KStream<Integer, String> streams = builder.stream(inputTopic);
        streams
                .transform(MyTransformer::new, STATE_STORE_NAME)
                .to(outputTopic);
        return builder.build();
    }

    public static class MyTransformer implements Transformer<Integer, String, KeyValue<Integer, String>> {

        private KeyValueStore<Integer, String> myStore;

        @Override
        public void init(ProcessorContext context) {
            myStore = context.getStateStore(STATE_STORE_NAME);
        }

        @Override
        public KeyValue<Integer, String> transform(Integer key, String value) {
            myStore.put(key,value);
            if (key == 4) {
                int previousKey = 2;
                if (myStore.get(previousKey) != null) {
                    myStore.delete(previousKey);
                }
            }
            return KeyValue.pair(key, value);
        }

        @Override
        public void close() {

        }

    }

    @Test
    public void puzzle() throws Exception {
        kafkaStreams.start();

        Map<Integer, String> messages = new HashMap<>();
        for (var i = 0; i < 6; i++) {
            messages.put(i, "v" + i);
        }
        producerSendSync(producerProperties, INPUT_TOPIC, messages);

        var fromBeginningProperties = consumerProperties;
        fromBeginningProperties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        fromBeginningProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var messagesFromBeginning = new HashMap<>();
        consumerReadUntilMinKeyValueRecordsReceived(fromBeginningProperties, OUTPUT_TOPIC, messages.size(), Duration.ofSeconds(10))
                .forEach(record -> {
                    messagesFromBeginning.put(record.key(), record.value());
                });

        assertThat(messagesFromBeginning.size()).isEqualTo(messages.size());
        assertThat(messagesFromBeginning).containsAllEntriesOf(messages);

        assertThat(messagesFromBeginning).contains(
                entry(0, "v0"),
                entry(1, "v1"),
                entry(2, "v2"),
                entry(3, "v3"),
                entry(4, "v4"),
                entry(5, "v5")
        );

        var messagesChangelog = new HashMap<>();
        consumerReadUntilMinKeyValueRecordsReceived(fromBeginningProperties, CHANGELOG_TOPIC_NAME, 5, Duration.ofSeconds(10))
                .forEach(record -> {
                    messagesChangelog.put(record.key(), Optional.ofNullable(record.value()));
                });

        assertThat(messagesChangelog).contains(
                entry(0, Optional.of("v0")),
                entry(1, Optional.of("v1")),
                entry(2, Optional.empty()),
                entry(3, Optional.of("v3")),
                entry(4, Optional.of("v4"))
        );

        kafkaStreams.close(Duration.ofSeconds(3));
    }
}
