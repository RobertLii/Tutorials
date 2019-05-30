package consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class KafkaSimpleConsumer {
    private static KafkaConsumer<String, String> kafkaConsumer = null;

    static {
        Properties props = initConfig();
        kafkaConsumer = new KafkaConsumer(props);
    }

    private static Properties initConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("client.id", "test");
        props.put("key.deserialiser", StringDeserializer.class.getName());
        props.put("value.deserialiser", StringDeserializer.class.getName());

        return props;
    }

    public static void consumeAutoCommit() {
        Properties props = initConfig();
        props.put("enable.auto.commit", true);
        props.put("auto.commit.interval.ms", 10);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //subscribe to the specified topic
        consumer.subscribe(Arrays.asList("stock-quotation"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                long committedOffset = -1;
                for (TopicPartition topicPartition : partitions) {
                    committedOffset = consumer.committed(topicPartition).offset();
                    consumer.seek(topicPartition, committedOffset + 1);
                }
            }
        });

        //subscribe to the specified partition of a topic
        //consumer.assign(Arrays.asList(new TopicPartition("stock-quotation", 0), new TopicPartition("stock-quotation", 2)));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                printConsumerRecords(records);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    public static void consumeManualCommit() {
        Properties props = initConfig();
        props.put("enable.auto.commit", false);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("stock-quotation"));

        try {
            int minCommitSize = 10;
            int count = 0;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                printConsumerRecords(records);

                if (count >= minCommitSize) {
                    consumer.commitAsync(new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                            if (null == exception)
                                System.out.println("Committed successfully");
                            else
                                System.out.println("Committed failed");
                        }
                    });
                    count = 0;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    public static void consumeByTimeIndex() {
        Properties props = initConfig();
        props.put("enable.auto.commit", true);
        props.put("auto.commit.interval.ms", 10);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.assign(Arrays.asList(new TopicPartition("stock-quotation", 0)));

        try {
            Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
            TopicPartition partition = new TopicPartition("stock-quotation", 0);
            timestampsToSearch.put(partition, System.currentTimeMillis() - 12 * 3600 * 1000);
            Map<TopicPartition, OffsetAndTimestamp> offsetMap = consumer.offsetsForTimes(timestampsToSearch);
            OffsetAndTimestamp offsetAndTimestamp = null;

            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetMap.entrySet()) {
                offsetAndTimestamp = entry.getValue();
                if (offsetAndTimestamp != null)
                    consumer.seek(partition, entry.getValue().offset());
            }

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                printConsumerRecords(records);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    public static void pauseAndResume() {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        List<TopicPartition> partitions = Arrays.asList(new TopicPartition("stock-quotation", 0));
        consumer.assign(partitions);

        consumer.pause(partitions);
        Set<TopicPartition> pausedPatitions = consumer.paused();

        consumer.resume(partitions);
    }

    public static void printConsumerRecords(ConsumerRecords<String, String> records) {
        if (null == records)
            throw new IllegalArgumentException("ConsumerRecords is null");

        for (ConsumerRecord<String, String> record : records) {
            System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
        }
    }
}
