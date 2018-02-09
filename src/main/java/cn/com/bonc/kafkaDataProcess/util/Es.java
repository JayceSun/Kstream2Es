package cn.com.bonc.kafkaDataProcess.util;


import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Es {

    private static Logger logger = LoggerFactory.getLogger(Es.class);

    public static void setData(String object, String index, String topic) {
        Client client = ESTools.buildclient();
        // 传输数据
        try {
            client.prepareIndex(index, topic).setSource(object)
                    .execute().actionGet();
            logger.info(index + "_" + topic + "：数据上传成功");
        } catch (Exception e) {
            logger.error("上传数据失败",e);
        }
        // 刷新
        try {
            client.admin().indices()
                    .refresh(new RefreshRequest(index)).actionGet();
        } catch (Exception e) {
            logger.error("刷新数据失败",e);
        }
    }
}
