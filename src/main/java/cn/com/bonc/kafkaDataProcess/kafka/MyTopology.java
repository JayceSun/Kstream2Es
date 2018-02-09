package cn.com.bonc.kafkaDataProcess.kafka;

import cn.com.bonc.kafkaDataProcess.kafka.processor.WebRegionProcessor;
import cn.com.bonc.kafkaDataProcess.util.PropertyReaderUtil;
import com.alibaba.fastjson.JSONArray;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.ho.yaml.Yaml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * @author maokeluo
 * @desc
 * @create 18-1-2
 */
public class MyTopology {

    public static final Logger logger = LoggerFactory.getLogger(MyTopology.class);

    public static List<KafkaStreams> topology() {
        List<KafkaStreams> kafkaStreams = new ArrayList<>();
        Map<String, Object> props = new HashMap<>();
        PropertyReaderUtil reader = new PropertyReaderUtil();
        Map<String, String> serverMap = reader.readPropertyFile("kafkaServer.properties");

        //props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, serverMap.get("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        List<String> topices = new ArrayList<>();
        Map<String,List<String>> processes = new HashMap<>();
        try {
            File path = new File(System.getProperty("user.dir"));
            File file = new File(path,"topicConfig.yml");
            InputStream inputStream = new FileInputStream(file);
            JSONArray jsonArray = Yaml.loadType(inputStream, JSONArray.class);
            logger.info("topicConfig.yml",jsonArray.toString());
            IntStream.range(0,jsonArray.size())
                    .mapToObj(jsonArray::getJSONObject)
                    .forEach(p->{
                        String topic = p.getString("topic");
                        topices.add(topic);
                        JSONArray processorArray = p.getJSONArray("processor");
                        List<String> processList = new ArrayList<>();
                        if (processorArray != null) processorArray.stream().forEach(q->processList.add(q.toString()));
                        processes.put(topic,processList);
                    });
        } catch (FileNotFoundException e) {
            logger.error("加载topic配置文件失败,找不到配置文件",e);
        }
        Map<String,String> map = reader.readPropertyFile("processor.properties");
        for (String topic : topices){
            TopologyBuilder builder = new TopologyBuilder();
            String parentName = "source";
            builder.addSource("source",topic);
            List<String> processList = processes.get(topic);
            for (String process : processList) {
                /*try {
                    //工厂根据processor名称创建Processor实例
                    ProcessorSupplierFactory.processor(topic,process);
                } catch (Exception e) {
                    logger.error("创建processor失败,key:",process,e);
                }*/

                //为Builder添加processor拓扑
                builder.addProcessor(process, new ProcessorSupplierFactory(map.get(process)), parentName);
                //现阶段暂需要做聚合处理,所以不需要将历史的数据存在本地状态仓库
                //.addStateStore(Stores.create(process).withStringKeys().withStringValues().inMemory().build(),process);
                //将下一个processor的父节点修改为当前processor
                parentName = process;

                logger.info("创建拓扑成功"+process);
            }
            builder.addSink("sink_"+topic,"output-stream",processList.get(processList.size()-1));
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, topic);
            StreamsConfig config = new StreamsConfig(props);
            KafkaStreams streams = new KafkaStreams(builder, config);
            streams.start();
            kafkaStreams.add(streams);
        }
        return kafkaStreams;
    }
}
