package cn.com.bonc.kafkaDataProcess.kafka.window;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.stream.IntStream;

/**
 * @author maokeluo
 * @desc
 * @create 18-1-16
 */
public class MyProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        String value = "{'name':'jack','channel':'pc','area':'北京'}";
        IntStream.range(0,200000).forEach(p->{
            producer.send(new ProducerRecord<String, String>("test", "PV", value));
        });
        producer.close();
    }
}
