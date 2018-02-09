package cn.com.bonc.kafkaDataProcess.kafka.window;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author maokeluo
 * @desc
 * @create 18-1-16
 */
public class PVCounts {

    public static void main(String[] args) {
        Map<String, Object> props = new HashMap<>();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        StreamsConfig config = new StreamsConfig(props);


        //定义窗口时间间隔
        long windowSizeMs = TimeUnit.MINUTES.toMillis(1); // 5 * 60 * 1000L
        KStreamBuilder builder = new KStreamBuilder();
        KStream<Object, Object> stream = builder.stream("test");
        KTable<Windowed<String>, Long> counts = stream.flatMap((k, v) -> {
            Map<String, Object> map = JSONObject.parseObject(v.toString());
            return map.entrySet().stream()
                    .filter((entry) -> !"".equals(entry.getValue()))
                    .map((entry) -> new KeyValue<>(entry.getKey(), entry.getValue()))
                    .collect(Collectors.toList());
        })
                .groupByKey().aggregate(
                        () -> 0L,
                        (k, v, aggregate) -> aggregate + 1L,
                        TimeWindows.of(windowSizeMs),
                        Serdes.Long()
                );
        //输出计数结果
        counts.toStream((k,v) -> k.key()).to(Serdes.String(),Serdes.Long(),"out");
        //将源数据输出
        //stream.to("out-source");
        /*TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source","test")
                .addProcessor("process",CountProcessor::new,"source")
                .addStateStore(Stores.create("Counts1").withStringKeys().withStringValues().inMemory().build(),"process")
                .addSink("sink","out","process");*/
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }
}
