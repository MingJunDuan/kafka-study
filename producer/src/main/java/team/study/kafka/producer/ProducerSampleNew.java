package team.study.kafka.producer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author GYFeng by 2016/12/19
 * @version 1.0
 */
public class ProducerSampleNew {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerSampleNew.class);


    public static void main(String[] args) throws InterruptedException {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "centosvm1:9092");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            ProducerRecord<String, String> data = new ProducerRecord<>("test-topic-ProducerSample", "test-message:" + i);
            producer.send(data);
            LOGGER.info("send message {} success!", i);
            Thread.sleep(1000);
        }
        producer.close();
    }
}
