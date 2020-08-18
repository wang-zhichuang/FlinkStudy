package DataCleanProc;

import com.alibaba.fastjson.JSONObject;
import utils.CleanRuleEnum;
import utils.ResultActionEnum;
import utils.StaticContent;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Description:
 * @Author: l0430
 * @Date: 2020/3/28 下午2:51
 */
public class DataCleanResult {


    /**
     * 根据治理后的结果执行相关的动作
     * @param msgJson
     * @param msgKey
     * @param actionEnum
     * @param ruleEnum
     * @param isPass
     */
    public static void result2Action(JSONObject msgJson, String msgKey, ResultActionEnum actionEnum, CleanRuleEnum ruleEnum, boolean isPass) {


        if(null == actionEnum){
            System.out.printf("ERROR: the action %s is err");
            actionEnum = ResultActionEnum.ACTION_DROP;
        }

        if(true != isPass){
            if(actionEnum == ResultActionEnum.ACTION_DROP){
                putFailReason(msgJson,msgKey,StaticContent.failDropKey,ruleEnum.getRuleDescription());
            }
            else{
                putFailReason(msgJson,msgKey,StaticContent.failDefaultKey,ruleEnum.getRuleDescription());
                msgJson.put(msgKey,"");
            }
        }
    }

    /**
     * 填写失败原因
     * @param msgKey
     * @param failReason
     * @param msgJson
     */
    public static void putFailReason(JSONObject msgJson, String msgKey, String actiongStr, String failReason){

        String reasonStr = "";
        StringBuffer reasonSB = new StringBuffer();
        String DataStr = "";

        Date date = new Date();
        SimpleDateFormat dateFormat= new SimpleDateFormat(StaticContent.timeFormatStr);


        if(msgJson.containsKey(actiongStr))
        {
            reasonSB.append(msgJson.getString(actiongStr));
            reasonSB.append("|");
        }

        if(msgJson.containsKey(msgKey)){
            DataStr = msgJson.getString(msgKey);
        }


        reasonSB.append(msgKey);
        reasonSB.append(StaticContent.comma);
        reasonSB.append(DataStr);
        reasonSB.append(StaticContent.comma);
        reasonSB.append(failReason);


        msgJson.put(actiongStr,reasonSB.toString());

    }

}
