package DataCleanProc;

import com.alibaba.fastjson.JSONObject;
import utils.CleanRuleEnum;
import utils.CompareEnum;
import utils.ResultActionEnum;

/**
 * @Description:
 * @Author: l0430
 * @Date: 2020/4/8 下午7:53
 */
public class DataCompare {

    public static Boolean checkCompareValue(JSONObject msgJson, String currentKey, String contents, CleanRuleEnum ruleEnum, boolean isRuning){

        boolean ret = false;
        String[] curretnRuleArray;
        int currentBeginIndex,currentEndIndex;
        String currentValue = "";
        CompareEnum compare = null;
        String anotherValue = "";
        String currentColumn = "";

        String[] associateList = contents.split(",");
        if(associateList.length != 4){
            return false;
        }

        ResultActionEnum actionEnum = ResultActionEnum.getByValue(associateList[3].toLowerCase());
        if(null == actionEnum){
            return false;
        }

        try{
            //当前列
            curretnRuleArray = associateList[0].split("-");
            currentBeginIndex = Integer.parseInt(curretnRuleArray[0]);
            currentEndIndex = Integer.parseInt(curretnRuleArray[1]);
        }
        catch (Exception err)
        {
            System.out.println(err.getMessage());
            return false;
        }

        compare = CompareEnum.getByValue(associateList[1].toLowerCase());
        if(null == compare)
        {
            return false;
        }

        if(currentBeginIndex <= currentEndIndex){
            return false;
        }

        if(isRuning != true){
            return true;
        }

        anotherValue = associateList[2];

        if(!msgJson.containsKey(currentKey)){
            msgJson.put(currentKey,"");
        }
        else{
            currentColumn = msgJson.getString(currentKey);
        }

        if(currentBeginIndex >= currentColumn.length() ||
                currentEndIndex > currentColumn.length())
        {
            DataCleanResult.result2Action(msgJson,currentKey,actionEnum,ruleEnum,false);
            return false;
        }

        currentValue = currentColumn.substring(currentBeginIndex - 1,currentEndIndex);

        switch (compare){
            case COMPARE_EQ:
                ret = checkCompareEQ(currentValue,anotherValue);
                break;
            case COMPARE_LT:
                ret = checkCompareLT(currentValue,anotherValue);
                break;
            case COMPARE_GT:
                ret = checkCompareGT(currentValue,anotherValue);
                break;
            case COMPARE_LE:
                ret = checkCompareLE(currentValue,anotherValue);
                break;
            case COMPARE_GE:
                ret = checkCompareGE(currentValue,anotherValue);
                break;
            case COMPARE_NE:
                ret = checkCompareNE(currentValue,anotherValue);
                break;
        }

        if(true != ret){
            DataCleanResult.result2Action(msgJson,currentKey,actionEnum,ruleEnum,false);
        }

        return ret;


    }

    /**
     * 关联行
     * @param associateContents
     * @param currentKey
     * @param msgJson
     * @return
     */
    public static Boolean checkCompareColumn(JSONObject msgJson, String currentKey, String associateContents, CleanRuleEnum ruleEnum, boolean isRuning){

        Boolean ret = false;
        String[] curretnRuleArray;
        int currentBeginIndex;
        int currentEndIndex;
        String compareStr;
        String associateAnother;
        String[] another;
        String anotherKey;
        String[] anotherIndexArray;
        int anotherBeginIndex;
        int anotherEndIndex;
        CompareEnum compare = null;
        String currentValue = "";
        String anotherValue = "";
        String currentColumn = "";
        String anotherColumn = "";

        String[] associateList = associateContents.split(",");

        if(associateList.length != 4){
            return false;
        }

        ResultActionEnum actionEnum = ResultActionEnum.getByValue(associateList[3].toLowerCase());
        if(null == actionEnum){
            return false;
        }

        try{
            //当前列
            curretnRuleArray = associateList[0].split("-");
            currentBeginIndex = Integer.parseInt(curretnRuleArray[0]);
            currentEndIndex = Integer.parseInt(curretnRuleArray[1]);

            //另外一列
            associateAnother = associateList[2];
            another = associateAnother.split(":");
            anotherKey = another[0];
            anotherIndexArray = another[another.length - 1].split("-");

            anotherBeginIndex = Integer.parseInt(anotherIndexArray[0]);
            anotherEndIndex = Integer.parseInt(anotherIndexArray[1]);

        }
        catch (Exception err)
        {
            System.out.println(err.getMessage());
            return false;
        }

        compare = CompareEnum.getByValue(associateList[1].toLowerCase());
        if(null == compare)
        {
            return false;
        }

        if(currentEndIndex <= currentBeginIndex || anotherEndIndex <= anotherBeginIndex){
            return false;
        }

        if(isRuning != true){
            return true;
        }


        if(!msgJson.containsKey(currentKey)){
            msgJson.put(currentKey,"");
        }
        else{
            currentColumn = msgJson.getString(currentKey);
        }

        if(!msgJson.containsKey(anotherKey)){
            msgJson.put(anotherKey,"");
        }
        else {
            anotherColumn = msgJson.getString(anotherKey);
        }

        if(currentBeginIndex >= currentColumn.length() ||
                currentEndIndex > currentColumn.length() ||
                anotherBeginIndex >= anotherColumn.length() ||
                anotherEndIndex > anotherColumn.length())
        {
            DataCleanResult.result2Action(msgJson,currentKey,actionEnum,ruleEnum,false);
            return false;
        }


        currentValue = currentColumn.substring(currentBeginIndex - 1,currentEndIndex);
        anotherValue = anotherColumn.substring(anotherBeginIndex - 1,anotherEndIndex);

        switch (compare){
            case COMPARE_EQ:
                ret = checkCompareEQ(currentValue,anotherValue);
                break;
            case COMPARE_LT:
                ret = checkCompareLT(currentValue,anotherValue);
                break;
            case COMPARE_GT:
                ret = checkCompareGT(currentValue,anotherValue);
                break;
            case COMPARE_LE:
                ret = checkCompareLE(currentValue,anotherValue);
                break;
            case COMPARE_GE:
                ret = checkCompareGE(currentValue,anotherValue);
                break;
            case COMPARE_NE:
                ret = checkCompareNE(currentValue,anotherValue);
                break;
        }

        if(true != ret){
            DataCleanResult.result2Action(msgJson,currentKey,actionEnum,ruleEnum,false);
        }

        return ret;


    }

    /***
     * 等于
     * @param currentValue
     * @param anotherValue
     * @return
     */
    public static Boolean checkCompareEQ(String currentValue,String anotherValue){
        Boolean ret = false;
        double value1 = 0;
        double value2 = 0;

        try
        {
            value1 = Double.parseDouble(currentValue);
            value2 = Double.parseDouble(anotherValue);
            if(value1 == value2){
                return true;
            }
        }
        catch (Exception err){

        }


        if(currentValue.equals(anotherValue))
        {
            ret = true;
        }
        return ret;

    }

    /***
     * 小于
     * @param currentValue
     * @param anotherValue
     * @return
     */
    public static Boolean checkCompareLT(String currentValue,String anotherValue){
        Boolean ret = false;
        double value1 = 0;
        double value2 = 0;

        try
        {
            value1 = Double.parseDouble(currentValue);
            value2 = Double.parseDouble(anotherValue);
            if(value1 < value2){
                return true;
            }
        }
        catch (Exception err){

        }


        if(currentValue.compareTo(anotherValue) < 0)
        {
            ret = true;
        }
        return ret;
    }

    /***
     * 大于
     * @param currentValue
     * @param anotherValue
     * @return
     */
    public static Boolean checkCompareGT(String currentValue,String anotherValue){
        Boolean ret = false;
        double value1 = 0;
        double value2 = 0;

        try
        {
            value1 = Double.parseDouble(currentValue);
            value2 = Double.parseDouble(anotherValue);
            if(value1 > value2){
                return true;
            }
        }
        catch (Exception err){

        }


        if(currentValue.compareTo(anotherValue) > 0)
        {
            ret = true;
        }
        return ret;
    }

    /***
     * 小于等于
     * @param currentValue
     * @param anotherValue
     * @return
     */
    public static Boolean checkCompareLE(String currentValue,String anotherValue){
        Boolean ret = false;
        double value1 = 0;
        double value2 = 0;

        try
        {
            value1 = Double.parseDouble(currentValue);
            value2 = Double.parseDouble(anotherValue);
            if(value1 <= value2){
                return true;
            }
        }
        catch (Exception err){

        }


        if(currentValue.compareTo(anotherValue) <= 0)
        {
            ret = true;
        }
        return ret;
    }

    /***
     * 不等于
     * @param currentValue
     * @param anotherValue
     * @return
     */
    public static Boolean checkCompareNE(String currentValue,String anotherValue){
        Boolean ret = false;
        double value1 = 0;
        double value2 = 0;

        try
        {
            value1 = Double.parseDouble(currentValue);
            value2 = Double.parseDouble(anotherValue);
            if(value1 != value2){
                return true;
            }
        }
        catch (Exception err){

        }


        if(currentValue.compareTo(anotherValue) != 0)
        {
            ret = true;
        }
        return ret;
    }

    /***
     * 大于等于
     * @param currentValue
     * @param anotherValue
     * @return
     */
    public static Boolean checkCompareGE(String currentValue,String anotherValue){
        Boolean ret = false;
        double value1 = 0;
        double value2 = 0;

        try
        {
            value1 = Double.parseDouble(currentValue);
            value2 = Double.parseDouble(anotherValue);
            if(value1 >= value2){
                return true;
            }
        }
        catch (Exception err){

        }


        if(currentValue.compareTo(anotherValue) >= 0)
        {
            ret = true;
        }
        return ret;
    }

}
