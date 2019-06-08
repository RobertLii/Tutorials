package producer;

import entity.StockQuotationInfo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultiThreadProducer {
    //private static final Logger =
    private static final int MSG_SIZE = 100;
    private static final String TEST_TOPIC = "test";
    private static final String BROKER_LIST = "localhost:9092";
    private static KafkaProducer<String, String> producer = null;

    static {
        Properties configs = initConfig();
        producer = new KafkaProducer<>(configs);
    }

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BROKER_LIST);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        return properties;
    }

    private static StockQuotationInfo createStockQuotationInfo() {
        StockQuotationInfo quotation = new StockQuotationInfo();
        Random r = new Random();
        Integer stockCode = 60010 + r.nextInt(10);
        float random = (float) Math.random();

        if (random / 2 < 0.5)
            random = -random;

        DecimalFormat decimalFormat = new DecimalFormat(".00");
        quotation.setCurrentPrice(Float.valueOf(decimalFormat.format(11 + random)));
        quotation.setPreClosePrice(11.60f);
        quotation.setHighPrice(12.5f);
        quotation.setLowPrice(6.8f);
        quotation.setOpenPrice(8.5f);
        quotation.setStockCode(stockCode.toString());
        quotation.setStockName("stock-test");

        return quotation;
    }

    public static void main(String[] args) {
        ProducerRecord<String, String> record = null;
        StockQuotationInfo quotationInfo = null;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        try {
            int num = 0;
            for (int i = 0; i < MSG_SIZE; i++) {
                quotationInfo = createStockQuotationInfo();
                record = new ProducerRecord<>(TEST_TOPIC, null, quotationInfo.getTradeTime(), quotationInfo.getStockCode(), quotationInfo.toString());
                executor.submit(new KafkaProducerThread(producer, record));

                if (num++ % 10 == 0)
                    Thread.sleep(2000L);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
