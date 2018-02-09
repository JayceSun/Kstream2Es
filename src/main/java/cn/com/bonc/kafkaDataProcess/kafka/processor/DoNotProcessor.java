package cn.com.bonc.kafkaDataProcess.kafka.processor;

import cn.com.bonc.kafkaDataProcess.util.NlpUtil;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class DoNotProcessor implements Processor {
    private ProcessorContext context;
    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public void process(Object o, Object o2) {
        context.forward(o.toString(),o2.toString());
        context.commit();
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
