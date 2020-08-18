import DataCleanProc.DataClean;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CleanRuleEnum;
import utils.CodeStandardEnum;
import utils.ResultActionEnum;
import utils.StaticContent;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.Properties;


public class DataCleanConfig {
    private static Logger logger = LoggerFactory.getLogger(DataCleanConfig.class); //log.info() 调用

    public static Properties consumerProperties;

    public static Properties producerProperties;

    public static Properties databasesProperties;

    public static Properties configProperties;

    public static JSONObject configJson;

    public static JSONObject cleanRuleJson;

    public static JSONObject splitMsgJson;

    public static JSONObject codeStandardJson =new JSONObject();;


    public static Boolean loadDataCleanConfig(String dirPath) {

        configJson = new JSONObject();
        //获取配置文件路径
        if (dirPath == null || dirPath.length() == 0) {
            return false;
        }

        if (!dirPath.endsWith(File.separator)) {
            dirPath += File.separator;
        }
        System.out.println("配置文件路径:" + dirPath);
        logger.info("配置文件路径:{}", dirPath);

        //获取消费者配置
        consumerProperties = getPropertiesConfig(dirPath + "consumer.properties");
        if (consumerProperties == null) {
            System.out.println("获取消费者配置失败");
            logger.error("获取消费者配置失败");
            return false;
        }
        //System.out.println("consumer properties:" + consumerProperties);
        logger.info("consumer properties:{}", consumerProperties);

        //获取生产者配置
        producerProperties = getPropertiesConfig(dirPath + "producer.properties");
        if (producerProperties == null) {
            System.out.println("获取生产者配置失败");
            logger.error("获取生产者配置失败");
            return false;
        }
        //System.out.println("producer properties:" + producerProperties);
        logger.info("producer properties:{}", producerProperties);


        //获取消费者配置
        databasesProperties = getPropertiesConfig(dirPath + "databases.properties");
        if (databasesProperties == null) {
            System.out.println("获取gauss数据库配置失败");
            logger.error("获取gauss数据库配置失败");
            return false;
        }
        //System.out.println("databases properties:" + databasesProperties);
        logger.info("databases properties:{}", databasesProperties);


        cleanRuleJson = getJsonConfig(dirPath + "dataCleanRule.json");
        if (cleanRuleJson == null) {
            System.out.println("获取清洗规则失败");
            logger.error("获取清洗规则失败");
            return false;
        }

        //System.out.println("DataCleanRule.json:" + JSON.toJSONString(cleanRuleJson));
        logger.info("DataCleanRule.json", JSON.toJSONString(cleanRuleJson));
        configJson.put(StaticContent.ruleKey,cleanRuleJson);

        splitMsgJson = getJsonConfig(dirPath + "msgFrame.json");
        if (splitMsgJson == null) {
            System.out.println("获取拆分配置失败");
            logger.error("获取拆分配置失败");
            return false;
        }
        //System.out.println("msgFrame.json:" + JSON.toJSONString(splitMsgJson));
        logger.info("msgFrame.json", JSON.toJSONString(splitMsgJson));
        configJson.put(StaticContent.splitKey,splitMsgJson);

        configProperties = getPropertiesConfig(dirPath + "config.properties");
        if (configProperties == null) {
            System.out.println("获取config配置失败");
            logger.error("获取config配置失败");
            return false;
        }


        return true;
    }

    public static Properties getPropertiesConfig(String filepath) {

        try {
            // 使用InPutStream流读取properties文件
            Properties properties = new Properties();
            BufferedReader bufferedReader = new BufferedReader(new FileReader(filepath));
            properties.load(bufferedReader);

            return properties;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }


    /**
     * 读取数据清洗json文件
     * @param filePath
     * @return
     */
    public static JSONObject getJsonConfig(String filePath) {
        try {
            if (filePath != null) {
                //读取json文件，转实体
                File file = new File(filePath);
                String jsonString = IOUtils.toString(new FileInputStream(file));

                return JSONObject.parseObject(jsonString);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 校验配置的清洗规则正确性
     * @return
     */
    public static boolean checkCleanRule(){
        boolean ret = true;
        String ruleStr = "";
        JSONObject topicJson = null;
        String keyStr = "";
        String topicStr = "";
        int additionLeftIndex = 0;
        int additionRightIndex = 0;
        String additionStr = "";
        String tmpRule = "";
        CleanRuleEnum ruleEnum = null;
        StringBuffer errSb = new StringBuffer("ERROR:checkCleanRule ");

        for (Object topic : cleanRuleJson.keySet()) {
            topicStr = (String)topic;

            topicJson = cleanRuleJson.getJSONObject(topicStr);

            for (Object key : topicJson.keySet()) {
                keyStr = (String) key;
                ruleStr = topicJson.getString(keyStr);

                String[] ruleList = ruleStr.split(StaticContent.ruleSplitSymbol);
                for (String rule : ruleList) {

                    tmpRule = rule;
                    additionStr = "";
                    additionLeftIndex = rule.indexOf(StaticContent.leftBracket);
                    additionRightIndex = rule.indexOf(StaticContent.rightBracket);

                    if(additionLeftIndex > 0 || additionRightIndex > 0) {
                        if (additionRightIndex <= 0 || additionLeftIndex <= 0) {

                            ret = false;
                            break;
                        }
                    }

                    if (additionLeftIndex > 0 && additionRightIndex > 0) {
                        if((additionLeftIndex + 2) > additionRightIndex) {
                            ret = false;
                            break;
                        }
                        else{
                            tmpRule = rule.substring(0 , additionLeftIndex);
                            additionStr = rule.substring(additionLeftIndex + 1,additionRightIndex);
                        }
                    }

                    ruleEnum = CleanRuleEnum.getByValue(tmpRule);
                    if (null == CleanRuleEnum.getByValue(tmpRule)) {
                        ret = false;
                        break;
                    }
                    else if(ruleEnum == CleanRuleEnum.RULE_CODE_STANDARD && true != analyzeCodeDictStandard(additionStr))
                    {
                        ret = false;
                        break;
                    }

                    ret = DataClean.cleanProcRule(keyStr,rule,null,null,false);

                    if(true != ret){
                        break;
                    }

                }
                if(!ret) {

                    errSb.append("topic " + topicStr);
                    errSb.append(" key " + keyStr);
                    errSb.append(" rule " + ruleStr);
                    errSb.append(" tmpRule " + tmpRule);

                    System.out.println(errSb.toString());

                    break;
                }
            }
            if(!ret) {
                break;
            }
        }

        return ret;
    }


    public static boolean analyzeCodeDictStandard(String codeDictRuleStr){

        boolean ret = true;

        JSONObject codeDictJson = new JSONObject();

        String[] codeDictRuleList = codeDictRuleStr.split(",");
        if(codeDictRuleList.length != 5 && codeDictRuleList.length != 6)
        {
            return false;
        }

        String[] currentColumnTypeList = codeDictRuleList[1].split("-");

        codeDictJson.put(StaticContent.columeDataUnit,      codeDictRuleList[0]);
        codeDictJson.put(StaticContent.currentColumnType,   currentColumnTypeList[0]);
        codeDictJson.put(StaticContent.relationType,        codeDictRuleList[2]);
        codeDictJson.put(StaticContent.currentColumnDisplayType,    codeDictRuleList[3]);
        codeDictJson.put(StaticContent.actionType,    codeDictRuleList[4]);

        if(codeDictRuleList.length == 6) {

            if(currentColumnTypeList.length != 2){
                return false;
            }

            codeDictJson.put(StaticContent.currentColumnDisplayType,   currentColumnTypeList[1]);
            codeDictJson.put(StaticContent.codeGroupId,codeDictRuleList[5]);
        }
        else{
            codeDictJson.put(StaticContent.currentColumnDisplayType,   currentColumnTypeList[0]);
        }


        if(null == CodeStandardEnum.getByValue(codeDictJson.getString(StaticContent.currentColumnDisplayType).toLowerCase()) ||
                null == CodeStandardEnum.getByValue(codeDictJson.getString(StaticContent.currentColumnDisplayType).toLowerCase()) ||
                null == CodeStandardEnum.getByValue(codeDictJson.getString(StaticContent.relationType).toLowerCase()) ||
                null == ResultActionEnum.getByValue(codeDictRuleList[4].toLowerCase())){
            return false;
        }



        if(!codeStandardJson.containsKey(codeDictRuleList[0]))
        {
            codeStandardJson.put(codeDictRuleList[0],codeDictJson);
        }

        return ret;
    }
}
