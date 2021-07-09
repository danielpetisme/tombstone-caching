package com.sample.app;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public final class TestUtils {

    static Properties toProperties(Map<String, Object> map) {
        Properties properties = new Properties();
        properties.putAll(map);
        return properties;
    }

    static void createTopic(Map<String, Object> adminClientConfig, String topicName, int numPartitions, int replicationFactor, Map<String, String> topicConfig) throws ExecutionException, InterruptedException {
        var admin = AdminClient.create(adminClientConfig);
        var topic = new NewTopic(topicName, numPartitions, (short) replicationFactor)
                .configs(topicConfig);
        admin.createTopics(Collections.singletonList(topic)).all().get();

    }

    static <K, V> void producerSendSync(Properties producerProperties, String topic, Map<K, V> entries) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<K, V>(producerProperties);
        for (var entry : entries.entrySet()) {
            var recordMetadata = producer.send(new ProducerRecord<>(topic, entry.getKey(), entry.getValue())).get();
        }
        producer.flush();
        producer.close();
    }

    static <K, V> List<ConsumerRecord<K, V>> consumerReadUntilMinKeyValueRecordsReceived(Properties consumerProperties, String topic, int expectedNumRecords) {
        return consumerReadUntilMinKeyValueRecordsReceived(consumerProperties, topic, expectedNumRecords, Duration.ofSeconds(10));
    }

    static <K, V> List<ConsumerRecord<K, V>> consumerReadUntilMinKeyValueRecordsReceived(Properties consumerProperties, String topic, int expectedNumRecords, Duration timeout) {
        var consumer = new KafkaConsumer<K, V>(consumerProperties);
        consumer.subscribe(Collections.singletonList(topic));
        final Duration pollInterval = Duration.ofMillis(100L);
        List<ConsumerRecord<K, V>> records = new ArrayList<>();
        final long startTimer = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTimer) < timeout.toMillis() && records.size() < expectedNumRecords) {
            var polledRecords = consumer.poll(pollInterval);
            System.out.println("Polled: " + polledRecords.count());
            for (var record : polledRecords) {
                records.add(record);
            }
        }
        consumer.close();
        return records;
    }

    static <K, V> List<ConsumerRecord<K, V>> consumerFromOffsetReadUntilMinKeyValueRecordsReceived(Properties consumerProperties, String topic, int partition, long fromOffset, int expectedNumRecords, Duration timeout) {
        var consumer = new KafkaConsumer<K, V>(consumerProperties);
        consumer.assign(Collections.singletonList(new TopicPartition(topic, partition)));
        consumer.seek(new TopicPartition(topic, partition), fromOffset);

        final Duration pollInterval = Duration.ofMillis(100L);
        List<ConsumerRecord<K, V>> records = new ArrayList<>();
        final long startTimer = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTimer) < timeout.toMillis() && records.size() < expectedNumRecords) {
            var polledRecords = consumer.poll(pollInterval);
//            System.out.println("Polled: " + polledRecords.count());
            for (var record : polledRecords) {
                records.add(record);
            }
        }
        consumer.close();
        return records;
    }

    public static File tempDirectory(String prefix) {
        return tempDirectory(null, prefix);
    }

    public static File tempDirectory() {
        return tempDirectory("kafka-");
    }

    public static File tempDirectory(Path parent, String prefix) {

        File file;
        try {
            file = parent == null ? Files.createTempDirectory(prefix).toFile() : Files.createTempDirectory(parent, prefix).toFile();
        } catch (IOException var4) {
            throw new RuntimeException("Failed to create a temp dir", var4);
        }

        file.deleteOnExit();
        Exit.addShutdownHook("delete-temp-file-shutdown-hook", () -> {
            try {
                Utils.delete(file);
            } catch (IOException var2) {
                System.out.println(" Error deleting " + file.getAbsolutePath() + ": " + var2);
            }

        });
        return file;
    }
}
