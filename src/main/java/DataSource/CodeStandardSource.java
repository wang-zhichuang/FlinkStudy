package DataSource;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import RiverDict.AuthFeiginInterceptor;
import RiverDict.RiverDictApi;
import utils.StaticContent;
import feign.Feign;
import feign.Request;
import feign.gson.GsonEncoder;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

/**
 * @Description:
 * @Author: l0430
 * @Date: 2020/3/17 下午4:37
 */
public class CodeStandardSource implements SourceFunction<JSONObject> {

    private final long SLEEP_MILLION = 43200000 ;
    private boolean isrunning = true;
    private JSONObject codeStandardJson;
    private Properties ConfigProperties;

    public CodeStandardSource(JSONObject codeStandardJson, Properties ConfigProperties){
        this.codeStandardJson = codeStandardJson;
        this.ConfigProperties = ConfigProperties;
    }


    public void run(SourceContext<JSONObject> ctx) throws Exception {

        while (isrunning){
            try{

                JSONObject retJson = getStandardDict();


                ctx.collect(retJson);

                //  一分钟提取一次
                Thread.sleep(SLEEP_MILLION);

            }// 捕获其他异常处理，通过日志记录
            catch (Exception e){

                StringBuffer errSb = new StringBuffer("ERROR: the CodeStandardSource is err\n");
                errSb.append(e.toString());

                System.out.println(errSb.toString());

            }
        }
    }

    /**
     *  任务停止，设置 false
     * */
    public void cancel() {
        isrunning = false;
        // 这样可以只获取一次连接在while一直用

    }



    public JSONObject getStandardDict(){

        String retStr = "";
        boolean ret = true;

        JSONArray dataJsonArray;
        JSONObject codeJson = null;
        String codeGroupId = "";
        JSONObject retJson = new JSONObject();
        StringBuffer errSb  = new StringBuffer("ERROR:");
        String river_tenantName = "";

        AuthFeiginInterceptor authFeiginInterceptor = new AuthFeiginInterceptor(ConfigProperties.getProperty("river_accessKeyID"),
                ConfigProperties.getProperty("river_accessKeySecret"),
                ConfigProperties.getProperty("river_tenantName"),
                ConfigProperties.getProperty("river_user"));


        Request.Options options = new Request.Options(10000,600000);
        RiverDictApi riverDictDsApi =  Feign.builder().options(options).requestInterceptor(authFeiginInterceptor).encoder(new GsonEncoder()).target(RiverDictApi.class,ConfigProperties.getProperty("river_apiURL"));

        if(!ConfigProperties.containsKey("river_scope") && ConfigProperties.getProperty("river_scope").toLowerCase().equals("global")){
            river_tenantName = ConfigProperties.getProperty("river_tenantName");
        }

        for (Object key: codeStandardJson.keySet()) {

            codeGroupId = "";
            codeJson = codeStandardJson.getJSONObject((String)key);
            if(codeJson.containsKey(StaticContent.codeGroupId)) {
                codeGroupId = codeJson.getString(StaticContent.codeGroupId);
            }



            dataJsonArray = getStandardTypeDict(riverDictDsApi,river_tenantName,(String)key,ConfigProperties.getProperty("river_scope"),codeGroupId);
            if(null != dataJsonArray){
                retJson.put((String)key,dataJsonArray);
            }
            else{
                errSb.append("get dict is err:" + JSONObject.toJSONString(codeJson));
            }
        }

        if(errSb.length() > 0){
            System.out.println(errSb.toString());
        }

        return retJson;
    }

    private JSONArray getStandardTypeDict(RiverDictApi riverDictDsApi, String departmentId, String dataDataUint, String scope, String groupId){

        JSONArray dataJsonArray = null;
        if(groupId.trim().length() == 0) {
            dataJsonArray = getGlobaltDict(riverDictDsApi,departmentId,dataDataUint,"GLOBAL");
        }
        else{
            dataJsonArray = getCustomDict(riverDictDsApi,departmentId,dataDataUint,groupId);

        }

        return dataJsonArray;
    }



    private JSONArray getGlobaltDict(RiverDictApi riverDictDsApi, String departmentId, String dataDataUint, String scope) {

        JSONArray dataJsonArray = null;
        JSONObject dataJson = null;
        JSONObject propertiesJson = null;
        JSONObject valueRangeJson = null;
        JSONObject configJson = null;
        String retStr = "";
        JSONArray retJsonArray = null;


        String result = riverDictDsApi.dataElementDetail(departmentId,dataDataUint);
        JSONObject retJson = JSONObject.parseObject(result);

        if(!retJson.getString("code").trim().equals("0"))
        {
            System.out.println("ERROR: the value of dataElementDetail is not 0");
            return null;
        }



        if(!retJson.containsKey("data")){
            System.out.println("ERROR: the value of dataElementDetail not contains data key");
            return null;
        }
        dataJson = retJson.getJSONObject("data");


        if(dataJson == null || !dataJson.containsKey("properties")){
            System.out.println("ERROR: the value of dataElementDetail not contains properties key");
            return  null;
        }
        propertiesJson = dataJson.getJSONObject("properties");

        if(propertiesJson == null || !propertiesJson.containsKey("value_range")){
            System.out.println("ERROR: the value of dataElementDetail not contains value_range key");
            return  null;
        }
        valueRangeJson = propertiesJson.getJSONObject("value_range");


        if(valueRangeJson == null || !valueRangeJson.containsKey("config")){
            System.out.println("ERROR: the value of dataElementDetail not contains config key");
            return  null;
        }

        configJson = valueRangeJson.getJSONObject("config");
        if(configJson == null || !configJson.containsKey("inner_identidify")){
            System.out.println("ERROR: the value of dataElementDetail not contains inner_identidify key");
            return  null;
        }

        if(valueRangeJson.getString("type").trim().equals("DICTIONARY")){

            String inner_identidify = configJson.getString("inner_identidify");

            retStr = riverDictDsApi.dataElementDefaultDict(inner_identidify,scope);
            JSONObject gobalDictJson = JSONObject.parseObject(retStr);
            dataJsonArray = gobalDictJson.getJSONArray("data");

        }

        if(null != dataJsonArray){

            JSONObject newJson = null;
            retJsonArray = new JSONArray();
            for (int index = 0; index < dataJsonArray.size(); index++) {
                JSONObject tmpJson = dataJsonArray.getJSONObject(index);
                newJson = new JSONObject();
                newJson.put("dictionaryName", tmpJson.getString("dictionaryName"));
                newJson.put("dictionaryValue", tmpJson.getString("dictionaryValue"));

                retJsonArray.add(newJson);
            }
        }

        return retJsonArray;
    }
    private JSONArray getCustomDict(RiverDictApi riverDictDsApi, String departmentId, String dataDataUint, String groupId) {
        JSONArray dataJsonArray = null;
        JSONObject dataJson = null;
        JSONObject propertiesJson = null;
        JSONObject valueRangeJson = null;
        JSONObject configJson = null;
        String instanceGuidStr;
        String retStr = "";
        JSONArray retJsonArray = null;

        String result = riverDictDsApi.dataElementDetail(departmentId,dataDataUint);
        JSONObject retJson = JSONObject.parseObject(result);

        if(!retJson.getString("code").trim().equals("0"))
        {
            System.out.println("ERROR: the value of dataElementDetail is not 0");
            return  null;
        }

        if(!retJson.containsKey("data")){
            System.out.println("ERROR: the value of dataElementDetail not contains data key");
            return  null;
        }
        dataJson = retJson.getJSONObject("data");

        if(dataJson == null || !dataJson.containsKey("properties") || !dataJson.containsKey("instanceGuid")){
            System.out.println("ERROR: the value of dataElementDetail not contains properties or instanceGuid key");
            return  null;
        }


        instanceGuidStr = dataJson.getString("instanceGuid");
        propertiesJson = dataJson.getJSONObject("properties");

        if(propertiesJson == null || !propertiesJson.containsKey("value_range")){
            System.out.println("ERROR: the value of dataElementDetail not contains value_range key");
            return  null;
        }

        valueRangeJson = propertiesJson.getJSONObject("value_range");
        if(valueRangeJson == null || !valueRangeJson.containsKey("config")){
            System.out.println("ERROR: the value of dataElementDetail not contains config key");
            return  null;
        }
        configJson = valueRangeJson.getJSONObject("config");
        if(valueRangeJson.getString("type").trim().equals("DICTIONARY")){

            String inner_identidify = configJson.getString("inner_identidify");
            retStr = riverDictDsApi.dataElementClustomDict(groupId,instanceGuidStr,inner_identidify);
            JSONObject gobalDictJson = JSONObject.parseObject(retStr);
            dataJsonArray = gobalDictJson.getJSONArray("data");
        }

        if(null != dataJsonArray){

            JSONObject newJson = null;
            retJsonArray = new JSONArray();
            for (int index = 0; index < dataJsonArray.size(); index++) {
                JSONObject tmpJson = dataJsonArray.getJSONObject(index);
                newJson = new JSONObject();
                newJson.put("name", tmpJson.getString("name"));
                newJson.put("dictionaryName", tmpJson.getString("dictionaryName"));
                newJson.put("dictionaryValue", tmpJson.getString("dictionaryValue"));

                retJsonArray.add(newJson);
            }
        }

        return retJsonArray;
    }


}
