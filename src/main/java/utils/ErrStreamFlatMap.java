package utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Description:
 * @Author: l0430
 * @Date: 2020/3/11 下午11:41
 */
public class ErrStreamFlatMap implements FlatMapFunction<JSONObject, JSONObject> {

    @Override
    public void flatMap(JSONObject errResultJson, Collector<JSONObject> outCollector) throws Exception {

        String errData = "";
        String dropStr = "";
        String[] dropList = null;
        String defaultStr = "";
        String[] defaultList = null;

        Date now = new Date();
        SimpleDateFormat format = new SimpleDateFormat(StaticContent.timeFormatStr);

        if(errResultJson.containsKey(StaticContent.failDropKey)) {
            dropStr = errResultJson.getString(StaticContent.failDropKey);
            dropList = dropStr.split("\\|");
            errResultJson.remove(StaticContent.failDropKey);

            for (int index = 0; index < dropList.length; index++){
                String[] reasonList = dropList[index].split(StaticContent.comma);

                String key = reasonList[0];
                String reason = reasonList[2];
                errData = reasonList[1];

                JSONObject newJson = (JSONObject) errResultJson.clone();


                newJson.put(StaticContent.errData, errData);
                newJson.put(key,errData);
                newJson.put(StaticContent.errField, key);
                newJson.put(StaticContent.errDesc, reason);
                newJson.put(StaticContent.errDate, format.format(now));

                fillOldData(newJson,dropList);
                outCollector.collect(newJson);
            }
        }




        if(errResultJson.containsKey(StaticContent.failDefaultKey))
        {
            defaultStr = errResultJson.getString(StaticContent.failDefaultKey);
            defaultList = defaultStr.split("\\|");
            errResultJson.remove(StaticContent.failDefaultKey);

            for (int index = 0; index < defaultList.length; index++){
                String[] reasonList = defaultList[index].split(StaticContent.comma);

                String key = reasonList[0];
                String reason = reasonList[1];

                JSONObject newJson = (JSONObject) errResultJson.clone();


                errData = reasonList[1];

                newJson.put(StaticContent.errData, errData);
                newJson.put(key,errData);
                newJson.put(StaticContent.errField, key);
                newJson.put(StaticContent.errDesc, reason);
                newJson.put(StaticContent.errDate, format.format(now));
                fillOldData(newJson,defaultList);

                outCollector.collect(newJson);
            }
        }


    }

    private void fillOldData(JSONObject msgJson, String[] value){
        for (int index = 0; index < value.length; index++){
        String[] reasonList = value[index].split(StaticContent.comma);
            msgJson.put(reasonList[0],reasonList[1]);
        }

    }
}
