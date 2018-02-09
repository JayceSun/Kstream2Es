package cn.com.bonc.kafkaDataProcess.kafka.processor;

import cn.com.bonc.kafkaDataProcess.util.NlpUtil;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @author maokeluo
 * @desc nlp关键字提取
 * @create 18-1-2
 */
public class KeywordProcessor implements Processor {
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public void process(Object o, Object o2) {
        /*NlpUtil nlpUtil = new NlpUtil();
        String keyword = nlpUtil.getKeyword(json.getString("content"));
        json.put("keywords",keyword);*/
        NlpUtil nlpUtil = new NlpUtil(NlpUtil.KEYWORD,o2.toString());
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
