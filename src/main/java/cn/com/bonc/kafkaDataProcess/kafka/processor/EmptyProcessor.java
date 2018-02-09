package cn.com.bonc.kafkaDataProcess.kafka.processor;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @author maokeluo
 * @desc
 * @create 18-1-23
 */
public class EmptyProcessor implements Processor{

    private ProcessorContext context;
    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public void process(Object o, Object o2) {
        context.forward(o,o2.toString());
        context.commit();
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
