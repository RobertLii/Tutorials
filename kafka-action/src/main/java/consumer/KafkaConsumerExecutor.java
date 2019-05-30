package consumer;

import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaConsumerExecutor {
    private static Properties initConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.80.129:9092");
        props.put("group.id", "test");
        props.put("client.id", "test");
        props.put("key.deserialiser", StringDeserializer.class.getName());
        props.put("value.deserialiser", StringDeserializer.class.getName());
        props.put("enable.auto.commit", true);
        props.put("auto.commit.interval.ms", 10);

        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();

        for (int i = 0; i < 3; i++) {
            new KafkaConsumerThread(props, "stock-quotation").start();
        }
    }
}
