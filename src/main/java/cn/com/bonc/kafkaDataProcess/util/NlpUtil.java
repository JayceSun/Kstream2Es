package cn.com.bonc.kafkaDataProcess.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bonc.text.sdk.client.TextAbstractClient;
import com.bonc.text.sdk.client.TextClassModelClient;
import com.bonc.text.sdk.client.TextClassRuleClient;
import com.bonc.text.sdk.client.TextSentimentAnalysisClient;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.client.Client;
import org.nlpcn.commons.lang.tire.GetWord;
import org.nlpcn.commons.lang.tire.domain.Forest;
import org.nlpcn.commons.lang.tire.library.Library;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.*;
import java.util.stream.IntStream;

/**
 * @author maokeluo
 * @desc 自然语言处理工具类
 * @create 18-1-4
 */
public class NlpUtil {
    private static final Logger logger = LoggerFactory.getLogger(NlpUtil.class);

    /**关键字**/
    public static final String KEYWORD = "keyword";
    /**内容地域标签**/
    public static final String CONTENT_REGION = "contentRegion";
    /**网站地域标签**/
    public static final String WEB_REGION = "webRegion";
    /**地域、地市来确定最终地域**/
    public static final String FINAL_REGION = "finalRegion";
    /**hash值**/
    public static final String HASHCODE = "hashCode";
    /**分类**/
    public static final String CLASSIFITION = "classifition";
    /**正负值**/
    public static final String NEGATIVE_SCORE = "negativeScore";
    /**企业类型**/
    public static final String ENTERPRISEYPE = "enterpriseType";
    /**摘要**/
    public static final String SUMMARY = "summary";
    /**地区配置**/
    public static final String REGIONRULE = "regionrule.txt";
    /**NLP ip地址**/
    private static String DOMAIN = "";
    /**返回值**/
    private String resultJ;


    public String getResultJ() {
        return resultJ;
    }

    static {
        PropertyReaderUtil propertyReaderUtil = new PropertyReaderUtil();
        Map<String, String> map = propertyReaderUtil.readPropertyFile("processor.properties");
        DOMAIN = map.get("nlpserver");
    }

    public NlpUtil() {
    }

    public NlpUtil(String operate, String jsonStr) {
        JSONObject json = JSONObject.parseObject(jsonStr);
        String content = json.getString("content");
        List<String> contentRegion;
        String webRegion = "";
        switch (operate){
            case KEYWORD:
                String keyword = getKeyword(content);
                json.put("keywords",keyword);
                break;
            case CONTENT_REGION:
                contentRegion = getRegion(content);
                String[] split = contentRegion.get(0).split("\t");//3:省 2: 市 1: 县 0 : 街道
                json.put("con_province",split[3]);
                json.put("con_city",split[2]);
                json.put("con_county",split[1]);
                json.put("con_road",split[0]);
                break;
            case WEB_REGION:
                webRegion = getRegion(json.getString("source")).get(0).split("\t")[2];
                json.put("regionSource",webRegion);
                break;
            case FINAL_REGION:
                contentRegion = getRegion(content);
                webRegion = getRegion(json.getString("source")).get(0).split("\t")[2];
                String finalRegion = getFinalRegion(contentRegion.get(2), webRegion);
                json.put("regionFinal",finalRegion);
                break;
            case HASHCODE:
                json.put("hashCode",getHashCode(content));
                break;
            case CLASSIFITION:
                String classifition = getClassifition(content);
                json.put("classname",classifition);
                break;
            case ENTERPRISEYPE:
                String kkname = json.getString("kkname");
                String enterpriseType = getEnterpriseType(kkname);
                json.put("categoryHY",enterpriseType);
                break;
            case NEGATIVE_SCORE:
                String negativeScore = getNegativeScore(content);
                json.put("poandnoCell",negativeScore);
                break;
            case SUMMARY:
                String title = json.getString("title");
                String summary = getSummary(title,content);
                json.put("summary",summary);
                break;
            default:
                logger.error("没有该操作",operate);
                break;
        }
        this.resultJ = json.toString();
    }


    /**
     * @desc 根据内容/网站打地域标签
     * @author maokeluo
     * @methodName getRegion
     * @param  content
     * @create 18-1-4
     * @return region
     */
    public List<String> getRegion(String content){
        Forest forest = null;
        InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(REGIONRULE);
        try {
            forest = Library.makeForest(resourceAsStream);
        } catch (Exception e) {
            logger.error("创建forest失败",e);
        }
        GetWord udg = forest.getWord(content);
        List<String> list = new ArrayList<>();
        String road = udg.getFrontWords();	//街道、村庄、社区
        String county = udg.getParam(0);	//县级
        String city = udg.getParam(1);		//市级
        String province = udg.getParam(2);	//省级
        if (road != null) {
            list.add(road + "\t" + county + "\t" + city + "\t" + province);
        }
        else if (road == null && county != null) {
            list.add(" " + "\t" + county + "\t" + city + "\t" + province);
        }
        else if (road == null && county == null && city != null) {
            list.add(" " + "\t" + " " + "\t" + city + "\t" + province);
        }
        else if (road == null && county == null && city == null && province != null) {
            list.add(" " + "\t" + " " + "\t" + " " + "\t" + province);
        }
        else if (road == null && county == null && city == null && province == null) {
            list.add(" " + "\t" + " " + "\t" + " " + "\t" + " ");
        }
        return list;
    }

    /**
     * @desc 根据地域、地市来确定最终地域
     * @author maokeluo
     * @methodName getFinalRegion
     * @param  contentRegion 内容地域标签
     * @Param webRegion 网站地域标签
     * @create 18-1-4
     * @return finalRegion
     */
    public String getFinalRegion(String contentRegion,String webRegion){
        String finalRegion="中国";
        if (!"".equals(contentRegion)) {
            finalRegion=contentRegion;
        }
        else if("".equals(contentRegion) && !"".equals(webRegion)){
            finalRegion=webRegion;
        }
        return finalRegion;
    }

    /**
     * @desc 根据内容获取关键字
     * @author maokeluo
     * @methodName getKeyword
     * @param  content
     * @create 18-1-4
     * @return keyword
     */
    public String getKeyword(String content) {
        Client client = ESTools.buildclient();
        AnalyzeResponse response = client.admin().indices().prepareAnalyze(content)// 内容
                .setAnalyzer("index_ansj")// 指定分词器
                .execute().actionGet();// 执行
        List<AnalyzeResponse.AnalyzeToken> tokens = response.getTokens();
        //String result = JSONArray.parseArray(tokens.toString()).toString();
        JSONArray words = JSONArray.parseArray(JSON.toJSONString(tokens));
        Set<String> set = new HashSet<>();
        IntStream.range(0,words.size())
                .mapToObj(words::getJSONObject)
                .forEach(p->{
                    String term = p.getString("term");
                    if (term.length() >= 2) set.add(term);
                });
        return set.toString();
    }

    /**
     * @desc 判断这条新闻是属于哪个企业类型
     * @author maokeluo
     * @methodName getEnterpriseType
     * @param  content
     * @create 18-1-4
     * @return java.lang.String
     */
    public String getEnterpriseType(String content){
        TextClassRuleClient client = TextClassRuleClient.getInstance();
        client.setDomain(DOMAIN);
        Map<String, List<String>> classifier = client.classifierByrules("品牌审计", content);
        List<String> enterpriseTypes = new ArrayList<>();
        classifier.keySet().forEach(p->{
            String[] split = p.split("-");
            enterpriseTypes.add(split[1]);
        });
        return enterpriseTypes.toString();
    }

    /** 
     * @desc 打正负值
     * @author maokeluo
     * @methodName getNegativeScore       
     * @param  content
     * @create 18-1-4
     * @return java.lang.String
     */
    public String getNegativeScore(String content){
        TextSentimentAnalysisClient client = TextSentimentAnalysisClient.getIntance();
        client.setDomain(DOMAIN);
        String result = client.getTextScore(content);
        JSONObject jsonObject = JSONObject.parseObject(result);
        String score = jsonObject.getString("score");
        return score == null ? "0" : score;
    }

    /**
     * @desc 获取分类
     * @author maokeluo
     * @methodName getClassifition
     * @param  content
     * @create 18-1-4
     * @return java.lang.String
     */
    public String getClassifition(String content){
        TextClassModelClient client = TextClassModelClient.getInstance();
        client.setDomain(DOMAIN);

        String resultJ = client.modelClassifierJ("", content);
        JSONObject jsonObject = JSONObject.parseObject(resultJ);
        String className = jsonObject.getString("classname");
        return className;
    }

    /** 
     * @desc 获取文章内容的hash值
     * @author maokeluo
     * @methodName getHashCode       
     * @param  content
     * @create 18-1-4
     * @return java.lang.String
     */
    public String getHashCode(String content){
        int hashCode = content.hashCode();
        return String.valueOf(hashCode);
    }

    /**
     * @desc 获取文章内容的摘要
     * @author maokeluo
     * @methodName getSummary
     * @param  title, content
     * @create 18-1-22
     * @return java.lang.String
     */
    public String getSummary(String title, String content){
        TextAbstractClient client = TextAbstractClient.getInstance();
        client.setDomain(DOMAIN);

        int numOfSub = 10;      //关键词个数
        int percent = 5;        //摘要占文章长度的百分比
        int numOfAbs = 2;       //摘要句数

        String summary = client.getAbstract(title, content, numOfSub, percent, numOfAbs).getSummary();
        return summary;
    }
}
