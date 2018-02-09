package cn.com.bonc.kafkaDataProcess.kafka.window;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author maokeluo
 * @desc
 * @create 18-1-16
 */
public class CountProcessor implements Processor {
    private ProcessorContext context;
    private KeyValueStore kvStore;
    public static final Set<String> keys = new HashSet<>();

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.context.schedule(50);
        this.kvStore = (KeyValueStore) context.getStateStore("Counts1");
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        Runnable task = ()->{
            System.out.println("开始删除");
            keys.forEach(p->{
                this.kvStore.delete(p);
                System.out.println("删除key:"+p);
            });
        };
        executorService.scheduleWithFixedDelay(task,0,30, TimeUnit.SECONDS);
    }

    @Override
    public void process(Object o, Object o2) {
        JSONObject jsonObject = JSON.parseObject(o2.toString());

        for (String key : jsonObject.keySet()){
            Object jsonvalue = jsonObject.get(key);
            if (jsonvalue != null){
                Object value = this.kvStore.get(key);
                if (value == null || "".equals(value.toString().trim())) {
                    this.kvStore.put(key, "1");
                } else {
                    Integer newValue = Integer.valueOf(value.toString()) + 1;
                    this.kvStore.put(key, newValue.toString());
                }
            }
        }


    }

    @Override
    public void punctuate(long l) {
        KeyValueIterator iter = this.kvStore.all();
        while (iter.hasNext()) {
            KeyValue entry = (KeyValue) iter.next();
            keys.add(entry.key.toString());
            context.forward(entry.key, entry.value.toString());
            //删除本地状态存储,processor每次只读取当前流
            //kvStore.delete(entry.key);
        }
        iter.close();
        context.commit();
    }

    @Override
    public void close() {

    }
}
