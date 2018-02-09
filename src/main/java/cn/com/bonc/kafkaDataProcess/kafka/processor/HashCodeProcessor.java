package cn.com.bonc.kafkaDataProcess.kafka.processor;

import cn.com.bonc.kafkaDataProcess.util.NlpUtil;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @author maokeluo
 * @desc
 * @create 18-1-4
 */
public class HashCodeProcessor implements Processor {
    private ProcessorContext context;
    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public void process(Object o, Object o2) {
        NlpUtil nlpUtil = new NlpUtil(NlpUtil.HASHCODE,o2.toString());
        context.forward(o.toString(),nlpUtil.getResultJ());
        context.commit();
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
