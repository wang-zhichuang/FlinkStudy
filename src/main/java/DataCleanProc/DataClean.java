package DataCleanProc;

import com.alibaba.fastjson.JSONObject;
import utils.CleanRuleEnum;
import utils.StaticContent;


/**
 * @Description:
 * @Author: l0430
 * @Date: 2020/2/6 下午1:20
 */
public class DataClean {
    public static Boolean cleanProcRule(String msgKey, String ruleStr, JSONObject msgJson, JSONObject codeStandardJson, boolean isRuning){

        Boolean ret = true;
        int ruleAdditionLeftIndex = 0;
        int ruleAdditionRightIndex = 0;
        String additionContents = "";
        StringBuffer errSb = new StringBuffer("ERROR: cleanProcRule ");
        StringBuffer errDepartmentSB = new StringBuffer();
        CleanRuleEnum ruleEnum = null;

        String[] ruleList = ruleStr.split(StaticContent.ruleSplitSymbol);

        for(String rule: ruleList){

            String tmpRule = rule;

            ruleAdditionLeftIndex = rule.indexOf(StaticContent.leftBracket);
            ruleAdditionRightIndex = rule.lastIndexOf(StaticContent.rightBracket);
            if( ruleAdditionLeftIndex > 0 && ruleAdditionRightIndex > 0)
            {
                tmpRule = rule.substring(0,ruleAdditionLeftIndex);

                additionContents = rule.substring(ruleAdditionLeftIndex + 1,ruleAdditionRightIndex);
            }

            ruleEnum = CleanRuleEnum.getByValue(tmpRule);
            if(null == ruleEnum){
                errSb.append("the clean rule:");
                errSb.append(tmpRule);
                errSb.append("is wrong\n");
                System.out.println(errSb.toString());
                continue;
            }

            Boolean tmpRet = true;
            try
            {
                switch (ruleEnum){
                    case RULE_GENERATE:
                        tmpRet = GenerateData.generateData(msgJson,msgKey,additionContents,ruleEnum,isRuning);
                        break;
                    case RULE_NOT_EMPTY:
                        tmpRet = CleanCheck.checkNotEmpty(msgJson,msgKey,additionContents,ruleEnum,isRuning);
                        break;
                    case RULE_INTERGER:
                        tmpRet = CleanCheck.checkInterger(msgJson,msgKey,additionContents,ruleEnum,isRuning);
                        break;
                    case RULE_DECIMAL:
                        tmpRet = CleanCheck.checkDecimal(msgJson,msgKey,additionContents,ruleEnum,isRuning);
                        break;
                    case RULE_DATE:
                        tmpRet = CleanCheck.checkdateStandardization(msgJson,msgKey,additionContents,ruleEnum,isRuning);
                        break;
                    case RULE_TIME:
                        tmpRet = CleanCheck.checkTimeStandardization(msgJson,msgKey,additionContents,ruleEnum,isRuning);
                        break;
                    case RULE_REGULAR:
                        tmpRet = CleanCheck.checkRegularExpression(msgJson,msgKey,additionContents,ruleEnum,isRuning);
                        break;
                    case RULE_COMPARE_COLUMN:
                        tmpRet = DataCompare.checkCompareColumn(msgJson,msgKey,additionContents,ruleEnum,isRuning);
                        break;
                    case RULE_CODE_STANDARD:
                        tmpRet = CodeStandardDict.procCodeStandard(msgJson,msgKey,additionContents,ruleEnum,codeStandardJson,isRuning);
                        break;
                    case RULE_ADD_CONTENT:
                        tmpRet = CleanCheck.addContent(msgJson,msgKey,additionContents,ruleEnum,isRuning);
                        break;
                    case RULE_CHECK_LENGTH:
                        tmpRet = CleanCheck.checkLength(msgJson,msgKey,additionContents,ruleEnum,isRuning);
                        break;
                    case RULE_COMPARE_VALUE:
                        tmpRet = DataCompare.checkCompareValue(msgJson,msgKey,additionContents,ruleEnum,isRuning);
                    case RULE_CUT_COLUMN:
                        tmpRet = CleanCheck.cutColumn(msgJson,msgKey,additionContents,ruleEnum,isRuning);
                        break;
                }
            }
            catch (Exception err)
            {
                errSb.append("key:" + msgKey);
                errSb.append(" rule:" + ruleStr);
                if(null != msgJson) {
                    errSb.append(" msgJson:" + msgJson.toJSONString());
                }
                errSb.append(" exception:" + err.toString());

                System.out.println(errSb.toString());
                tmpRet = false;
            }

            if(true != tmpRet ) {
                ret = tmpRet;
            }
        }

        return ret;
    }

    /**
     * 遍历消息key进行清洗
     * @param msgStr
     * @return
     */
    public static JSONObject cleanProcKey(JSONObject totalJson){

        String keyStr = "";
        String ruleStr = "";
        JSONObject codeStandardJson = null;


        JSONObject msgJson = totalJson.getJSONObject(StaticContent.valueKey);
        JSONObject ruleJson = totalJson.getJSONObject(StaticContent.ruleKey);
        if(totalJson.containsKey(StaticContent.codeStandardKey)) {
            codeStandardJson = totalJson.getJSONObject(StaticContent.codeStandardKey);
        }


        for (Object key : ruleJson.keySet()) {

            keyStr = key.toString();

            ruleStr  = ruleJson.getString(keyStr);
            if(ruleStr == null || ruleStr.trim().length() == 0) {
                continue;
            }
            cleanProcRule(keyStr,ruleStr,msgJson,codeStandardJson,true);
        }
        return msgJson;
    }



}
