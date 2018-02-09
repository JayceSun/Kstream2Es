package cn.com.bonc.kafkaDataProcess.kafka.processor;

import cn.com.bonc.kafkaDataProcess.util.NlpUtil;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @author maokeluo
 * @desc nlp分类
 * @create 18-1-2
 */
public class ClassifitionProcessor implements Processor{
    private ProcessorContext context;
    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public void process(Object o, Object o2) {
        NlpUtil nlpUtil = new NlpUtil(NlpUtil.CLASSIFITION,o2.toString());
        context.forward(o.toString(),nlpUtil.getResultJ());
        context.commit();
    }

    @Override
    public void punctuate(long l) {
        //现阶段暂需要做聚合处理,所以不需要将历史的数据存在本地状态仓库
        /*KeyValueIterator iter = this.kvStore.all();
        while (iter.hasNext()) {
            KeyValue entry = (KeyValue) iter.next();
            context.forward(entry.key, entry.value.toString());
            //删除本地状态存储,processor每次只读取当前流
            kvStore.delete(entry.key);
        }
        iter.close();
        context.commit();*/
    }

    @Override
    public void close() {
    }
}
