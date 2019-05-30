package deserializer;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class AvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (null == data)
            return null;
        try {
            SpecificRecordBase record = null;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void close() {

    }
}
