package DataCleanProc;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import utils.CleanRuleEnum;
import utils.CodeStandardEnum;
import utils.ResultActionEnum;
import utils.StaticContent;

/**
 * @Description:
 * @Author: l0430
 * @Date: 2020/3/18 下午11:01
 */
public class CodeStandardDict {

    public static boolean procCodeStandard(JSONObject msgJson, String msgKey, String associateContents, CleanRuleEnum ruleEnum, JSONObject codeStandardJson, boolean isRuning){
        boolean ret = false;
        String valueStr = "";
        String relationValue = "";
        JSONObject tmpJson = null;
        JSONObject codeDictJson = new JSONObject();
        JSONArray dictJsonArray = null;
        ResultActionEnum actionEnum = null;

        String[] codeDictRuleList = associateContents.split(",");
        if(codeDictRuleList.length != 5 && codeDictRuleList.length != 6)
        {
            return false;
        }

        codeDictJson.put(StaticContent.columeDataUnit,      codeDictRuleList[0]);

        String[] currentColumnTypeList = codeDictRuleList[1].split("-");

        codeDictJson.put(StaticContent.currentColumnType,   currentColumnTypeList[0]);
        if(currentColumnTypeList.length > 1){
            codeDictJson.put(StaticContent.currentColumnDisplayType,   currentColumnTypeList[1]);
        }
        else{
            codeDictJson.put(StaticContent.currentColumnDisplayType,   currentColumnTypeList[0]);
        }

        codeDictJson.put(StaticContent.relationType,        codeDictRuleList[2]);
        codeDictJson.put(StaticContent.relationDisplayName,    codeDictRuleList[3]);
        codeDictJson.put(StaticContent.actionType,          codeDictRuleList[4]);

        if(codeDictRuleList.length == 6) {
            codeDictJson.put(StaticContent.codeGroupId,codeDictRuleList[5]);
        }

        actionEnum = ResultActionEnum.getByValue(codeDictRuleList[4].toLowerCase());

        if(null == actionEnum ||
                null == CodeStandardEnum.getByValue(codeDictJson.getString(StaticContent.currentColumnDisplayType).toLowerCase()) ||
                null == CodeStandardEnum.getByValue(codeDictJson.getString(StaticContent.currentColumnDisplayType).toLowerCase()) ||
                null == CodeStandardEnum.getByValue(codeDictJson.getString(StaticContent.relationType).toLowerCase())){
            return false;
        }



        if(true != isRuning)
        {
            return true;
        }

        if(null == codeStandardJson) {
            System.out.println("ERROR: the dict is null,please check the config of river is right");
            return true;
        }

        if(!codeStandardJson.containsKey(codeDictRuleList[0])){
            System.out.print("ERROR: the dict code rule isn'n exist:" + associateContents);
        }

        if(!msgJson.containsKey(msgKey)) {
            msgJson.put(msgKey,"");
        }

        dictJsonArray = codeStandardJson.getJSONArray(codeDictRuleList[0]);

        for (int index = 0; index < dictJsonArray.size(); index++) {
            tmpJson = dictJsonArray.getJSONObject(index);

            valueStr = tmpJson.getString(codeDictJson.getString(StaticContent.currentColumnType));
            if(valueStr.equals(msgJson.getString(msgKey).trim()))
            {
                ret = true;
                break;
            }

        }

        if(ret != true) {
            DataCleanResult.result2Action(msgJson,msgKey,actionEnum,ruleEnum,false);

            if (ResultActionEnum.ACTION_DEFAULT == actionEnum){
                msgJson.put(msgKey,"");
                msgJson.put(codeDictRuleList[3],"");

                ret = true;
            }
        }
        else
        {
            msgJson.put(msgKey,tmpJson.getString(codeDictJson.getString(StaticContent.currentColumnDisplayType)));
            msgJson.put(codeDictJson.getString(StaticContent.relationDisplayName),tmpJson.getString(codeDictJson.getString(StaticContent.relationType)));
        }

        return ret;
    }
}
