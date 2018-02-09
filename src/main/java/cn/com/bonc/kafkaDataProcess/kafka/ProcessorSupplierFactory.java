package cn.com.bonc.kafkaDataProcess.kafka;

import cn.com.bonc.kafkaDataProcess.kafka.processor.EmptyProcessor;
import cn.com.bonc.kafkaDataProcess.util.PropertyReaderUtil;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author maokeluo
 * @desc processorSupplier工厂类
 * @create 18-1-2
 */
public class ProcessorSupplierFactory implements ProcessorSupplier {
    public static final Logger logger = LoggerFactory.getLogger(ProcessorSupplierFactory.class);

    private String processorName;
    public ProcessorSupplierFactory(String processorName){
        this.processorName = processorName;
    }
    @Override
    public Processor get() {
        Processor processor = null;
        try {
           processor = (Processor) Class.forName(processorName).newInstance();
        } catch (InstantiationException e) {
            logger.error("反射类失败",e);
        } catch (IllegalAccessException e) {
            logger.error("错误：",e);
        } catch (ClassNotFoundException e) {
            logger.error("找不到类",e);
        }
        return processor;
    }
}
