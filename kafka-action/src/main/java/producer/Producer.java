package producer;

import entity.StockQuotationInfo;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Random;

public class Producer {
    //private static final Logger =
    private static final int MSG_SIZE = 100;
    private static final String TOPIC = "stock-quotation";
    private static final String BROKER_LIST = "localhost:9092";
    private static KafkaProducer<String, String> producer = null;

    static {
        Properties configs = initConfig();
        producer = new KafkaProducer<String, String>(configs);
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
        try {
            int num = 0;
            for (int i = 0; i < MSG_SIZE; i++) {
                quotationInfo = createStockQuotationInfo();
                record = new ProducerRecord<>(TOPIC, null, quotationInfo.getTradeTime(), quotationInfo.getStockCode(), quotationInfo.toString());
                //producer.send(record);
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            System.out.print("Producer send message failed!");
                        }
                        if (recordMetadata != null) {
                            System.out.println(String.format("offset: %s, partitions: %s, topic: %s", recordMetadata.offset(), recordMetadata.partition(), recordMetadata.topic()));
                        }
                    }
                });

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
