package cn.com.bonc.kafkaDataProcess.util;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;


public class ESTools {

	private static Logger logger = LoggerFactory.getLogger(ESTools.class);
	private static final String IPCONFIG = "172.16.23.13";//"127.0.0.1";
	private static final int PORT = 9303;

	//创建私有对象
	private static TransportClient client;
	static Settings settings = Settings.builder()
			.put("cluster.name", "esfyb_cluster")
			//.put("cluster.name","test")
			.put("client.transport.sniff", true).build();
	static{
		try {
			logger.info(ESTools.class.getSimpleName()+"开始创建client");
			//TransportAddress node = new TransportAddress(InetAddress.getByName(IPCONFIG),PORT);
			InetSocketTransportAddress node = new InetSocketTransportAddress(InetAddress.getByName(IPCONFIG),PORT);
			client = TransportClient
					.builder()
					.settings(settings)
					.build();
			//client = new PreBuiltTransportClient(settings);
			//添加节点
			client.addTransportAddress(node);
			logger.info(ESTools.class.getSimpleName()+"创建Elasticsearch Client 结束");
		} catch (UnknownHostException e2) {
			logger.error(ESTools.class.getSimpleName()+"创建Client异常");
		}	
	}
	
	public static synchronized TransportClient buildclient(){
		return client;
	}
}
