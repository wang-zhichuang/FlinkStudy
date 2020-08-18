package DataCleanProc;

import com.alibaba.fastjson.JSONObject;
import utils.*;

import java.util.regex.Pattern;


/**
 * @Author: Henry
 * @Description: 数据清洗需要
 *          组装代码
 *
 *  创建kafka topic命令：
 *      ./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 5 --topic allData
 *      ./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 5 --topic allDataClean
 *
 * @Date: Create in 2019/5/25 17:47
 **/
public class CleanCheck {

    public static boolean checkLength(JSONObject msgJson, String msgKey, String additionContents, CleanRuleEnum ruleEnum, boolean isRuning) {

        boolean ret = false;
        String msgContentStr = "";
        int length = 0;
        String[] contentList = additionContents.split(",");

        if(contentList.length != 2)
        {
            return false;
        }

        ResultActionEnum actionEnum = ResultActionEnum.getByValue(contentList[1].toLowerCase());
        if(null == actionEnum){
            return false;
        }

        try{
            length = Integer.parseInt(contentList[0]);
        }
        catch(Exception err){
            return false;
        }

        if(isRuning != true){
            return true;
        }

        if(!msgJson.containsKey(msgKey)) {
            msgJson.put(msgKey,"");
        }

        msgContentStr = msgJson.getString(msgKey).trim();
        if(msgContentStr.length() != length)
        {
            DataCleanResult.result2Action(msgJson,msgKey,actionEnum,ruleEnum,false);
            ret = true;
        }

        return ret;
    }

    public static  Boolean cutColumn(JSONObject msgJson, String msgKey, String additionContents, CleanRuleEnum ruleEnum, boolean isRuning){
        String columnValue = "";
        String value = "";
        int begin,end;
        //sensorno:11-12
        String[] contentList = additionContents.split(",");

        if(contentList.length != 2)
        {
            return false;
        }


        ResultActionEnum actionEnum = ResultActionEnum.getByValue(contentList[1].toLowerCase());
        if(null == actionEnum){
            return false;
        }

        String[] columnList = contentList[0].split(":");
        if(columnList.length != 2)
        {
            return false;
        }

        String[] locationList = columnList[1].split("-");
        if(locationList.length != 2 ){
            return false;
        }

        try{
            begin = Integer.parseInt(locationList[0]);
            end = Integer.parseInt(locationList[1]);
        }
        catch(Exception err){
            return false;
        }

        if(begin >= end){
            return false;
        }

        if(isRuning != true){
            return true;
        }

        if(!msgJson.containsKey(columnList[0])){
            DataCleanResult.result2Action(msgJson,columnList[0],actionEnum,ruleEnum,false);
            return false;
        }
        else{
            columnValue = msgJson.getString(columnList[0]);
        }


        if(begin >= columnValue.length() || end > columnValue.length()){
            DataCleanResult.result2Action(msgJson,columnList[0],actionEnum,ruleEnum,false);
        }
        else
        {
            value = columnValue.substring(begin - 1,end);
        }
        msgJson.put(msgKey,value);
        return true;

    }
    /**
     * 添加默认值
     * @param msgKey
     * @param msgJson
     * @return
     */
    public static  Boolean addContent(JSONObject msgJson, String msgKey, String additionContents, CleanRuleEnum ruleEnum, boolean isRuning){

        String msgStr = "";

        String msgContentStr = "";
        String contentStr = "";
        StringBuffer contentSb = null;
        int index = 0;
        String[] contentList = additionContents.split(",");

        if(contentList.length != 2)
        {
            return false;
        }

        try{
            index = Integer.parseInt(contentList[0]);
            contentStr = contentList[1].trim();
        }
        catch(Exception err){
            index = Integer.MAX_VALUE;
        }

        if(isRuning != true){
            return true;
        }


        if(!msgJson.containsKey(msgKey)) {
            msgJson.put(msgKey,"");

        }

        msgContentStr = msgJson.getString(msgKey).trim();
        contentSb = new StringBuffer(msgContentStr);
        if(contentSb.length() < index) {
            contentSb.append(contentStr);
        }
        else{
            contentSb.insert(index-1,contentStr);
        }

        msgJson.put(msgKey,contentSb.toString());

        return true;
    }

    /**
     * 校验不能为空
     * @param msgKey
     * @param msgJson
     * @return
     */
    public static  Boolean checkNotEmpty(JSONObject msgJson, String msgKey, String additionContents, CleanRuleEnum ruleEnum, boolean isRuning){

        String msgStr = "";

        ResultActionEnum actionEnum = ResultActionEnum.getByValue(additionContents.toLowerCase());
        if(null == actionEnum){
            return false;
        }

        if(isRuning != true){
            return true;
        }


        if(null == msgJson.get(msgKey) || msgJson.getString(msgKey).trim().length() == 0) {

            DataCleanResult.result2Action(msgJson,msgKey,actionEnum,ruleEnum,false);
            return false;
        }


        msgStr = msgJson.getString(msgKey);
        if (0 == msgStr.trim().length()){
            return false;
        }
        return true;
    }

    /**
     * 校验是否为整型
     * @param msgKey
     * @param msgJson
     * @return
     */
    public static Boolean checkInterger(JSONObject msgJson, String msgKey, String additionContents, CleanRuleEnum ruleEnum, boolean isRuning){

        Long id = 0L;
        String msgStr = "";
        Boolean ret = false;

        ResultActionEnum actionEnum = ResultActionEnum.getByValue(additionContents.toLowerCase());
        if(null == actionEnum){
            return false;
        }

        if(isRuning != true){
            return true;
        }

        if(!msgJson.containsKey(msgKey))
        {
            msgJson.put(msgKey,"");
        }

        msgStr = msgJson.getString(msgKey).trim();
        try{
            id = Long.parseLong(msgStr);
            msgJson.put(msgKey,id.toString());
            ret = true;
        }
        catch (Exception e) {
            ret = false;
        }

        if(true != ret){
            DataCleanResult.result2Action(msgJson,msgKey,actionEnum,ruleEnum,false);
        }

        return ret;

    }

    /**
     * 校验是否满足小数格式
     * @param msgJson
     * @param msgKey
     * @param additionContents
     * @param ruleEnum
     * @param isRuning
     * @return
     */
    public static  Boolean checkDecimal(JSONObject msgJson, String msgKey, String additionContents, CleanRuleEnum ruleEnum, boolean isRuning){

        String msgStr = "";
        int intLen = 0;
        int decimalLen = 0;
        int commaIndex = 0;
        Boolean ret = false;
        String formatStr = "";
        String[] additionList = additionContents.split(StaticContent.comma);
        if(additionList.length != 3){
            return false;
        }

        ResultActionEnum actionEnum = ResultActionEnum.getByValue(additionList[2].toLowerCase());
        if(null == actionEnum){
            return false;
        }

        try {
            intLen = Integer.parseInt(additionList[0]);
            decimalLen = Integer.parseInt(additionList[1]);
        } catch (Exception e) {
            return false;
        }

        if(intLen < decimalLen)
        {
            return false;
        }

        if(isRuning != true){
            return true;
        }


        if(!msgJson.containsKey(msgKey))
        {
            msgJson.put(msgKey,"");
        }

        msgStr = msgJson.getString(msgKey).trim();
        try{

            Double valueMsg = Double.parseDouble(msgStr);
            msgStr = ToolsUtil.numerFormat(valueMsg,intLen,decimalLen);
            if(msgStr.length() > 0) {
                ret = true;
                msgJson.put(msgKey,msgStr);
            }

        }
        catch (Exception e) {
            ret = false;
        }

        if(true != ret){
            DataCleanResult.result2Action(msgJson,msgKey,actionEnum,ruleEnum,false);
        }

        return ret;
    }




    /**
     * 校验是否满足正则表达式
     * @param msgJson
     * @param msgKey
     * @param additionContents
     * @param ruleEnum
     * @param isRuning
     * @return
     */
    public static Boolean checkRegularExpression(JSONObject msgJson, String msgKey, String additionContents, CleanRuleEnum ruleEnum, boolean isRuning){

        String msgStr = "";
        Boolean ret = false;


        String[] contentsList = additionContents.split(",");

        if(contentsList.length != 2){
            return false;
        }

        ResultActionEnum actionEnum = ResultActionEnum.getByValue(contentsList[1].toLowerCase());
        if(null == actionEnum){
            return false;
        }

        if(isRuning != true){
            return true;
        }

        if(!msgJson.containsKey(msgKey))
        {
            msgJson.put(msgKey,"");
        }

        msgStr = msgJson.getString(msgKey).trim();
        ret = Pattern.matches(contentsList[0], msgStr);
        if(ret != true){

            DataCleanResult.result2Action(msgJson,msgKey,actionEnum,ruleEnum,false);
        }


        return ret;
    }



    /**
     * 时间标准化
     * @return
     */
    public static Boolean checkTimeStandardization(JSONObject msgJson, String msgKey, String additionContents, CleanRuleEnum ruleEnum, boolean isRuning) {

        String msgStr = "";
        Boolean ret = false;
        String timeStr = "";

        ResultActionEnum actionEnum = ResultActionEnum.getByValue(additionContents.toLowerCase());
        if(null == actionEnum){
            return false;
        }


        if(isRuning != true){
            return true;
        }

        if(!msgJson.containsKey(msgKey))
        {
            msgJson.put(msgKey,"");
        }

        msgStr = msgJson.getString(msgKey).trim();
        try {

            if(DateTimeUtil.isTime(msgStr))
            {
                timeStr = DateTimeUtil.getTimeStr(msgStr);
                msgJson.put(msgKey,timeStr);
                ret = true;
            }


        } catch (Exception e) {
            ret = false;
        }


        if(true != ret)
        {
            DataCleanResult.result2Action(msgJson,msgKey,actionEnum,ruleEnum,false);
        }

        return ret;
    }

    /**
     * 日期标准化
     * @return
     */
    public static Boolean checkdateStandardization(JSONObject msgJson, String msgKey, String additionContents, CleanRuleEnum ruleEnum, boolean isRuning) {

        String msgStr = "";
        Boolean ret = false;
        String dateStr = "";


        ResultActionEnum actionEnum = ResultActionEnum.getByValue(additionContents.toLowerCase());
        if(null == actionEnum){
            return false;
        }

        if(isRuning != true){
            return true;
        }

        if(!msgJson.containsKey(msgKey))
        {
            msgJson.put(msgKey,"");
        }

        msgStr = msgJson.getString(msgKey).trim();
        try {

            if(DateTimeUtil.isDate(msgStr))
            {
                dateStr = DateTimeUtil.getDateStr(msgStr);
                msgJson.put(msgKey,dateStr);
                ret = true;
            }


        } catch (Exception e) {
            ret = false;
        }

        if(true != ret)
        {
            DataCleanResult.result2Action(msgJson,msgKey,actionEnum,ruleEnum,false);
        }

        return ret;
    }

}
