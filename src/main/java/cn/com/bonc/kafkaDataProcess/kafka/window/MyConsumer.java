package cn.com.bonc.kafkaDataProcess.kafka.window;

import cn.com.bonc.kafkaDataProcess.util.PropertyReaderUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * @author maokeluo
 * @desc
 * @create 18-1-16
 */
public class MyConsumer {


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");

        KafkaConsumer<String, Long> consumer = new KafkaConsumer<String, Long>(props);
        consumer.subscribe(Arrays.asList("out"));
        while (true) {
            autoOffset(consumer);
        }
    }
    public static void autoOffset(KafkaConsumer<String, Long> consumer ){
        ConsumerRecords<String, Long> records = consumer.poll(100);

        for (ConsumerRecord<String, Long> record : records){
            System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}
