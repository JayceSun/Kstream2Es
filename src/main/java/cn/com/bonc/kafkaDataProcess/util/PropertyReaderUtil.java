package cn.com.bonc.kafkaDataProcess.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * @author maokeluo
 * @desc
 * @create 18-1-2
 */
public class PropertyReaderUtil {

    private static final Logger logger = LoggerFactory.getLogger(PropertyReaderUtil.class);
    public static Map<String,String> map = new HashMap<>();
    public static final String CONFIG_NAME = "topicConfig.yml";
    private static Long lastModified = 0L;

   /**
    * @desc 加载指定配置文件
    * @author maokeluo
    * @methodName readPropertyFile
    * @param  fileName
    * @create 18-1-2
    * @return java.util.Map<java.lang.String,java.lang.String>
    */
    public Map<String, String> readPropertyFile(String fileName) {
        Properties pro = new Properties();
        InputStream in = getClass().getClassLoader().getResourceAsStream(fileName);
        try {
            pro.load(in);
            Iterator<String> iterator = pro.stringPropertyNames().iterator();
            while (iterator.hasNext()) {
                String key = iterator.next();
                String value = pro.getProperty(key);
                map.put(key, value);
            }
            in.close();
        } catch (IOException e) {
            logger.error("加载文件错误",fileName,e);
        }
        return map;
    }

    /**
     * @desc 判断文件是否被修改
     * @author maokeluo
     * @methodName isYmlModified
     * @param
     * @create 18-1-3
     * @return boolean
     */
    public static boolean isYmlModified(){
        boolean isYmlModified = false;
        File path = new File(System.getProperty("user.dir"));
        File file = new File(path,"topicConfig.yml");
        if (file.lastModified() > lastModified){
            logger.info("配置文件被修改");
            lastModified = file.lastModified();
            isYmlModified = true;
        }else {
            logger.info("配置文件暂未修改");
        }
        return isYmlModified;
    }
}
