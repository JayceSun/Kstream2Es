package cn.com.bonc.kafkaDataProcess.kafka;

import cn.com.bonc.kafkaDataProcess.util.ESTools;
import cn.com.bonc.kafkaDataProcess.util.Es;
import cn.com.bonc.kafkaDataProcess.util.PropertyReaderUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.ho.yaml.Yaml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * @author maokeluo
 * @desc kafka消费者
 * @create 18-1-2
 */
public class MyKafkaConsumer {
    public static final Logger logger = LoggerFactory.getLogger(MyKafkaConsumer.class);

    private static KafkaConsumer<String, String> consumer;
    private final static AtomicBoolean closed = new AtomicBoolean(false);
    private static Map<String, String> indexMap;

    private static Client client = ESTools.buildclient();
    private static BulkRequestBuilder prepareBulk = client.prepareBulk();
    private static int counts = 0;

    static {
        PropertyReaderUtil reader = new PropertyReaderUtil();
        Map<String, String> serverMap = reader.readPropertyFile("kafkaServer.properties");
        Properties props = new Properties();
        serverMap.forEach((k, v) -> props.put(k, v));
        consumer = new KafkaConsumer<>(props);
    }

    /**
     * @param
     * @return void
     * @desc 消费数据
     * @author maokeluo
     * @methodName consumer
     * @create 18-1-10
     */
    public static void consumData() {

        consumer.subscribe(Arrays.asList("output-stream"));
        manualOffset(consumer);
       /* while (true) {
            autoOffset(consumer);
            //sendData(consumer);
        }*/
        /*try {
            consumer.subscribe(Arrays.asList("output-stream"));
            //manualOffset(consumer);
            while (!closed.get()) {
                //autoOffset(consumer);
                sendData(consumer);
            }
        } catch (WakeupException e) {
            //if (!closed.get()) throw e;
            logger.info("捕获到WakeupException异常,进行关闭消费者处理");
        } finally {
            consumer.close();
            logger.info("终止消费者成功", consumer);
        }*/
    }

    /**
     * @param consumer
     * @return void
     * @desc 自动提交偏移量
     * @author maokeluo
     * @methodName autoOffset
     * @create 18-1-2
     */
    public static void autoOffset(KafkaConsumer<String, String> consumer) {
        ConsumerRecords<String, String> records = consumer.poll(100);

        //消费kafka中的数据发送到ES中
        //Es.setData(record.value(), index, record.key());
        if (records.count() > 0) {
            for (ConsumerRecord<String, String> record : records) {
                String index = indexMap.get(record.key()) == null ? "xrs_db_null" : indexMap.get(record.key());
                counts++;
                try {
                    IndexRequestBuilder indexRequestBuilder = client.prepareIndex(index, record.key()).setSource(record.value());
                    prepareBulk.add(indexRequestBuilder);
                    logger.info("添加" + index + "," + record.key() + "成功！");
                } catch (Exception e) {
                    logger.error("添加数据失败:" + e);
                }
                if (counts % 500 == 0) {
                    prepareBulk.execute().actionGet();
                    logger.info("上传数据成功！");
                    prepareBulk = client.prepareBulk();
                }
            }
        }
        if (prepareBulk.numberOfActions() > 0) {
            prepareBulk.execute().actionGet();
            logger.info("上传数据成功！");
            prepareBulk = client.prepareBulk();
        }

    }

    /**
     * @param consumer
     * @return void
     * @desc 手动提交偏移量, 当数据达到一定数量时再开始消费
     * @author maokeluo
     * @methodName manualOffset
     * @create 18-1-2
     */
	public static void manualOffset(KafkaConsumer<String, String> consumer) {
		final int num = 100;

		BulkResponse bulkResponse = null;

		while (true) {
			int count=0;
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				
				count++;
				String index = indexMap.get(record.key()) == null ? "xrs_db_null" : indexMap.get(record.key());

				try {
					// 添加数据
					IndexRequestBuilder indexRequestBuilder = client.prepareIndex(index, record.key())
							.setSource(record.value());
					prepareBulk.add(indexRequestBuilder);
					logger.info("第" + record.offset() + "条数据添加" + index + "," + record.key() + "成功！");

					// 提交数据
					if (count % num == 0) {
						pushdata(bulkResponse, num, true);
					}
				} catch (Exception e) {
					logger.error("添加数据失败," + e.getMessage());
				}
				System.out.println(count);

			}

			if (prepareBulk.numberOfActions() > 0) {
				pushdata(bulkResponse, num, false);
			}
			
			
			 
//          if (buffer.size() >= minBatchSize) {
//              for (ConsumerRecord<String, String> record : records) {
//              	
//                  String index = indexMap.get(record.key()) == null ? "xrs_db_null" : indexMap.get(record.key());
//                  counts++;
//                  try {
//                  	
//                  	System.out.println(record.offset()+"---"+record.value());
//                  	
//						IndexRequestBuilder indexRequestBuilder = client.prepareIndex(index, record.key())
//								.setSource(record.value());
//                      prepareBulk.add(indexRequestBuilder);
//                      logger.info("添加" + index + "," + record.key() + "成功！");
//                  } catch (Exception e) {
//                      logger.error("添加数据失败," + e);
//                  }
//                  if (counts % 500 == 0) {
//                      
//                  }
//              }
//              if (prepareBulk.numberOfActions() > 0) {
//                  bulkResponse = prepareBulk.execute().actionGet();
//                  logger.info("上传数据成功！");
//                  prepareBulk = client.prepareBulk();
//              }
//              if (bulkResponse.hasFailures()){
//                  for (BulkItemResponse bulkItemResponse : bulkResponse) {
//                      System.out.println("--数据提交出错啦:"+bulkItemResponse.getFailureMessage());
//                      logger.error("--数据提交出错啦:"+bulkItemResponse.getFailureMessage());
//                  }
//              }
//              consumer.commitSync();
//              buffer.clear();
//          }
		}
	}

    public static void getIndex() {
        Map<String, String> map = new HashMap<>();
        File path = new File(System.getProperty("user.dir"));
        File file = new File(path, "topicConfig.yml");
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(file);
            JSONArray jsonArray = Yaml.loadType(inputStream, JSONArray.class);
            IntStream.range(0, jsonArray.size())
                    .mapToObj(jsonArray::getJSONObject)
                    .forEach(p -> {
                        String index = p.getString("index");
                        String topic = p.getString("topic");
                        map.put(topic, index);
                    });
            indexMap = map;
        } catch (FileNotFoundException e) {
            logger.error("创建文件流失败,没找到文件", e);
        }
    }

    /*public static void sendData(KafkaConsumer<String, String> consumer) {
        Client client = ESTools.buildclient();
        int counts = 0;
        BulkRequestBuilder builder = client.prepareBulk();
        ConsumerRecords<String, String> records = consumer.poll(100);

<<<<<<< HEAD
        for (ConsumerRecord<String, String> record : records) {
            System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            String index = indexMap.get(record.key());
            builder.add(client.prepareIndex(index, record.key()).setSource(record.value()));
            //每五百条上传一次
            long l = System.currentTimeMillis();
            if (counts % 500 == 0) {
                builder.execute().actionGet();
                //重置,重新创建一个builder
                builder = client.prepareBulk();
                logger.info("上传数据条数" + counts);
                long time = System.currentTimeMillis() - l;
                System.out.println("上传1000条所用时间:" + time);
            }
            counts++;
        }
        System.out.println("上传条数" + counts);
        builder.execute().actionGet();
    }*/

    /**
     * @param
     * @return void
     * @desc 终止消费者
     * @author maokeluo
     * @methodName restart
     * @create 18-1-10
     */
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

    public void start() {
        closed.set(false);
    }
    
    public static void pushdata(BulkResponse bulkResponse,int num,boolean falg){

		bulkResponse = prepareBulk.execute().actionGet();

		if (bulkResponse.hasFailures()) {
			for (BulkItemResponse bulkItemResponse : bulkResponse) {
				logger.error("满足" + num + "数据提交出错啦:" + bulkItemResponse.getFailureMessage());
				logger.error("当数据满足 "+num+" 为  "+falg+"  时,提交出错啦: "+bulkItemResponse.getFailureMessage());

			}
		} else {
			logger.info("当数据满足 "+num+" 为  "+falg+"  时--上传成功!!!!");
		}
		prepareBulk = client.prepareBulk();
	}
}
