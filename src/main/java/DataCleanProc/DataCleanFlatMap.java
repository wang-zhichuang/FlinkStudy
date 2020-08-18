package DataCleanProc;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import utils.StaticContent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;

/**
 * @Description:
 * @Author: l0430
 * @Date: 2020/2/6 下午1:21
 */
public class DataCleanFlatMap implements FlatMapFunction<ObjectNode, JSONObject> {

    @Override
    public void flatMap(ObjectNode inputNode, Collector<JSONObject> outCollector) throws Exception {

        Object msgObject = null;
        JSONObject configJson = null;
        JSONObject tmpJson = null;
        JSONObject ruleJson = null;
        JSONObject msgFrameJson = null;
        String topic = "";

        if(!inputNode.has(StaticContent.metadataKey) || !inputNode.has(StaticContent.configKey)||
                !inputNode.has(StaticContent.valueKey) || !inputNode.get(StaticContent.metadataKey).has(StaticContent.topicKey)){
            return;
        }

        String valueStr = inputNode.get(StaticContent.valueKey).toString();

        try{
            msgObject   = JSONObject.parse(valueStr);
            configJson  = JSONObject.parseObject(inputNode.get(StaticContent.configKey).toString());
            ruleJson    = configJson.getJSONObject(StaticContent.ruleKey);
            msgFrameJson   = configJson.getJSONObject(StaticContent.splitKey);
            topic =  inputNode.get(StaticContent.metadataKey).get(StaticContent.topicKey).textValue();

        }
        catch (Exception e) {
            System.out.println("WARNING:DataCleanFlatMap json format is error");
            System.out.println(e.getMessage());
            return;
        }

        if (msgObject instanceof JSONArray) {
            JSONArray msgJSONArray = (JSONArray) msgObject;
            for (int index = 0; index < msgJSONArray.size(); index++) {
                tmpJson = msgJSONArray.getJSONObject(index);
                procMsg(tmpJson,ruleJson,msgFrameJson,topic,outCollector);
            }
        }
        else if (msgObject instanceof JSONObject) {
            tmpJson = (JSONObject)msgObject;
            procMsg(tmpJson,ruleJson,msgFrameJson,topic,outCollector);

        }
        else{
            System.out.println("WARNING:DataCleanFlatMap the message is not json");
            return;
        }


    }


    private void procMsg(JSONObject msgJson, JSONObject ruleJson, JSONObject msgFrameJson, String topic, Collector<JSONObject> outCollector){
        JSONObject newMsgJson = null;
        JSONObject totalMsgJson = null;
        JSONObject topicRuleJson = null;
        Object datasObject = null;
        String splitValue = "";
        String datasKey = "";
        String splitKey = "";

        StringBuffer sb = new StringBuffer(topic);
        JSONObject topicFrameJson = msgFrameJson.getJSONObject(topic);

        if(topicFrameJson.containsKey(StaticContent.childKey)){

            if(topicFrameJson.containsKey(StaticContent.splitKeyKey) && topicFrameJson.containsKey(StaticContent.splitValueKey))
            {
                splitKey = topicFrameJson.getString(StaticContent.splitKeyKey);

                if(msgJson.containsKey(splitKey)) {
                    splitValue = msgJson.getString(splitKey);
                    sb.append("_");
                    sb.append(splitValue);
                }
                else{
                    System.out.printf("WARNING:procMsg the %s message is not contain the split key:%s\n",topic,splitKey);
                }

            }

            datasKey = topicFrameJson.getString(StaticContent.childKey);
            if(msgJson.containsKey(datasKey)) {
                datasObject = msgJson.get(datasKey);
            }
            else{
                System.out.printf("WARNING:procMsg the %s message is not contain the child json key:%s\n",topic,splitKey);
                return;
            }

            if (datasObject instanceof JSONArray) {
                JSONArray msgJSONArray = (JSONArray) datasObject;

                for (int index = 0; index < msgJSONArray.size(); index++) {
                    JSONObject tmpJson = msgJSONArray.getJSONObject(index);

                    totalMsgJson = new JSONObject();
                    newMsgJson = mergeMsgJson(datasKey,msgJson,tmpJson);

                    totalMsgJson.put(StaticContent.valueKey,newMsgJson);
                    totalMsgJson.put(StaticContent.topicKey,sb.toString());
                    if(ruleJson.containsKey(sb.toString())) {
                        topicRuleJson = ruleJson.getJSONObject(sb.toString());

                    }
                    else if(ruleJson.containsKey(topic)){
                        topicRuleJson = ruleJson.getJSONObject(topic);
                    }
                    else{
                        StringBuffer errSb = new StringBuffer("WARNING: the rule json don't set ");
                        errSb.append(topic + " rule" + "\n");
                        errSb.append("msgJson:" + JSONObject.toJSONString(msgJson) + "\n");
                        errSb.append("msgFrameJson:" + JSONObject.toJSONString(msgFrameJson) + "\n");
                        System.out.printf(errSb.toString());
                        return;
                    }
                    totalMsgJson.put(StaticContent.ruleKey, topicRuleJson);

                    outCollector.collect(totalMsgJson);
                }
            }
            else if(datasObject instanceof JSONObject)
            {
                totalMsgJson = new JSONObject();
                JSONObject tmpJson = (JSONObject)datasObject;

                newMsgJson = mergeMsgJson(datasKey,msgJson,tmpJson);
                totalMsgJson.put(StaticContent.valueKey,newMsgJson);
                totalMsgJson.put(StaticContent.topicKey,sb.toString());

                if(ruleJson.containsKey(sb.toString())) {
                    topicRuleJson = ruleJson.getJSONObject(sb.toString());
                }
                else
                {
                    StringBuffer errSb = new StringBuffer("WARNING: the rule json don't set ");
                    errSb.append(topic + " rule" + "\n");
                    errSb.append("msgJson:" + JSONObject.toJSONString(msgJson) + "\n");
                    errSb.append("msgFrameJson:" + JSONObject.toJSONString(msgFrameJson) + "\n");
                    System.out.printf(errSb.toString());
                    return;
                }

                totalMsgJson.put(StaticContent.ruleKey, topicRuleJson);
                outCollector.collect(totalMsgJson);
            }
            else{
                StringBuffer errSb = new StringBuffer("WARNING: the msg don't match  frame\n");
                errSb.append("msgJson:" + JSONObject.toJSONString(msgJson) + "\n");
                errSb.append("msgFrameJson:" + JSONObject.toJSONString(msgFrameJson) + "\n");
                System.out.printf(errSb.toString());
                return;
            }
        }
        else{

            if(topicFrameJson.containsKey(StaticContent.splitKeyKey) && topicFrameJson.containsKey(StaticContent.splitValueKey))
            {
                splitKey = topicFrameJson.getString(StaticContent.splitKeyKey);

                if(msgJson.containsKey(splitKey)) {
                    splitValue = msgJson.getString(splitKey);
                    sb.append("_");
                    sb.append(splitValue);
                }
                else{
                    System.out.printf("WARNING: procMsg the %s message is not contain the split key:%s\n",topic,splitKey);
                }
            }
            totalMsgJson = new JSONObject();

            totalMsgJson.put(StaticContent.valueKey,msgJson);
            totalMsgJson.put(StaticContent.topicKey,sb.toString());

            if(ruleJson.containsKey(sb.toString())) {
                topicRuleJson = ruleJson.getJSONObject(sb.toString());
            }
            else
            {
                StringBuffer errSb = new StringBuffer("WARNING:the rule json don't set ");
                errSb.append(sb.toString() + " rule" + "\n");
                errSb.append("msgJson:" + JSONObject.toJSONString(msgJson) + "\n");
                errSb.append("msgFrameJson:" + JSONObject.toJSONString(msgFrameJson) + "\n");
                System.out.printf(errSb.toString());
                return;
            }

            totalMsgJson.put(StaticContent.ruleKey, topicRuleJson);

            outCollector.collect(totalMsgJson);
        }
    }


    private JSONObject mergeMsgJson(String splitKey, JSONObject msgJson, JSONObject childJson){
        JSONObject resultJson = new JSONObject();

        for (Object key : msgJson.keySet()) {
            String keyStr = (String)key;

            if(!keyStr.equals(splitKey)){
                resultJson.put(keyStr,msgJson.get(keyStr));
            }
        }

        for (Object key : childJson.keySet()) {
            String keyStr = (String)key;
            resultJson.put(keyStr,childJson.get(keyStr));
        }

        return resultJson;
    }

    private boolean preDataClean(String topic, String splitKey, JSONObject msgJson, JSONObject ruleJson){

        JSONObject topicRuleJson = null;
        String ruleStr = "";
        if(ruleJson.containsKey(topic)){
            topicRuleJson = ruleJson.getJSONObject(topic);
        }
        else {
            System.out.printf("WARNING: the topic %s is not config clean rule\n",topic);
            return false;
        }

        if(topicRuleJson != null && topicRuleJson.containsKey(splitKey)){
            ruleStr = ruleJson.getJSONObject(topic).getString(splitKey);
        }

        if(ruleStr == null || ruleStr.length() == 0)
        {
            System.out.printf("WARNING: the topic %s splitKey %s is not config clean rule\n",topic,splitKey);
            return false;
        }

        return DataClean.cleanProcRule(splitKey,ruleStr,msgJson,null,true);

    }

}
