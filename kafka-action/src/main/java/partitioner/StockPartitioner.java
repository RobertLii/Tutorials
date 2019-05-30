package partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class StockPartitioner implements Partitioner {
    private static final Integer PARTITIONS_NUM = 3;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (null == key)
            return 0;
        String stockCode = String.valueOf(key);
        try {
            int partitionId = Integer.valueOf(stockCode.substring(stockCode.length() - 2)) % PARTITIONS_NUM;
            return partitionId;
        } catch (NumberFormatException e) {
            System.out.println("An exception occurs when parsing message key, key: " + stockCode);
            return 0;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
