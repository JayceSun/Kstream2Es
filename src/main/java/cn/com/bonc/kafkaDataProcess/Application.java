package cn.com.bonc.kafkaDataProcess;

import cn.com.bonc.kafkaDataProcess.kafka.MyKafkaConsumer;
import cn.com.bonc.kafkaDataProcess.kafka.MyTopology;
import cn.com.bonc.kafkaDataProcess.util.PropertyReaderUtil;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author maokeluo
 * @desc
 * @create 18-1-3
 */
public class Application {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);
    private static List<KafkaStreams> kafkaStreams = new ArrayList<>();
    private static AtomicBoolean ymlModified = new AtomicBoolean(false);
    public static void main(String[] args) {
        //启动消费者
        ExecutorService consumerExcutor = Executors.newSingleThreadExecutor();
        consumerExcutor.submit(()->MyKafkaConsumer.consumData());
        //启动kafka Streams处理
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        Runnable task = ()-> {
            ymlModified.set(PropertyReaderUtil.isYmlModified());
            if (ymlModified.get()){
                MyKafkaConsumer.getIndex();
                executor.submit(()-> kafkaStreams.stream().forEach(p -> p.close()));
                kafkaStreams = MyTopology.topology();
                logger.info("开启流拓扑");
            }
        };
        //每隔一段时间执行一次
        executorService.scheduleWithFixedDelay(task,0,30,TimeUnit.MINUTES);
    }
}
