package DataCleanProc;

import com.alibaba.fastjson.JSONObject;
import utils.CleanRuleEnum;
import utils.DataTypeEnum;
import utils.StaticContent;
import utils.ToolsUtil;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;


/**
 * @Description:
 * @Author: l0430
 * @Date: 2020/3/5 上午10:24
 */
public class GenerateData {

    /**
     * 生成新数据
     * @param msgKey
     * @param msgJson
     * @return
     */
    public static  Boolean generateData(JSONObject msgJson, String msgKey, String additionContents, CleanRuleEnum ruleEnum, boolean isRuning){

        String newData = "";
        boolean ret = true;
        String dataType = "";

        String[] additionList = additionContents.split(",");


        dataType = additionList[0];

        if(isRuning != true){
            if(null != DataTypeEnum.getByValue(dataType)) {
                return true;
            }
            else{
                return false;
            }
        }

        try {
            switch (DataTypeEnum.getByValue(dataType)) {
                case TYPE_UUID:
                    newData = createUUID();
                    break;
                case TYPE_NOW:
                    newData = createNow();
                    break;
                case TYPE_MD5:
                    newData = createMD5(msgJson,additionList[1]);
                    break;
                case TYPE_MD5_ALL:
                    newData = createMD5All(msgJson);
                    break;
                default:
            }
        }
        catch (Exception err){
            ret = false;
        }

        msgJson.put(msgKey,newData);

        return ret;
    }

    /**
     * 获取32位随机字符串
     * @return
     */
    private static String createUUID(){
        String uuid = UUID.randomUUID().toString().replace("-", "").toLowerCase();
        return uuid;
    }

    /**
     * 获取当前时间
     * @return
     */
    private static String createNow(){
        SimpleDateFormat df = new SimpleDateFormat(StaticContent.timeFormatStr);
        String now = df.format(new Date());
        return now;
    }
    /**
     * 获取当前时间
     * @return
     */
    private static String createMD5All(JSONObject msgJson){

        StringBuffer sb = new StringBuffer();
        String keyStr = "";
        String valueStr = "";
        for (Object key : msgJson.keySet()) {

            keyStr = key.toString();

            valueStr  = msgJson.getString(keyStr);
            if(valueStr == null || valueStr.trim().length() == 0) {
                continue;
            }

            sb.append(valueStr);
        }
        return ToolsUtil.MD5(sb.toString());
    }

    private static String createMD5(JSONObject msgJson, String keys){

        StringBuffer sb = new StringBuffer();
        String[] keyList = keys.split("\\+");
        for(String key: keyList){
            if(!msgJson.containsKey(key)){
                continue;
            }
            sb.append(msgJson.getString(key));
        }

        return ToolsUtil.MD5(sb.toString());
    }
}
