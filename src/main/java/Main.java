import DataCleanProc.DataClean;
import DataCleanProc.DataCleanFlatMap;
import DataCleanProc.Json2RowMap;
import DataSource.CodeStandardSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import kafka.Consumer;
import kafka.Producer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.*;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Main {
    private static Logger logger = LoggerFactory.getLogger(Main.class); //log.info() 调用

    public static void main(String[] args) throws Exception {
        /*模拟参数*/
        args = new String[]{"--configPath",
                "C:\\MyProgram\\Project\\FlinkStudy\\src\\main\\java\\config",
                "--parallelism", "3",
                "--enableCheckpoint", "false"
        };
        JSONObject msgFrameJson = null;
        DataStream<JSONObject> codeStandardStream = null;
        System.out.println("执行命令参数:" + Arrays.toString(args));
        logger.info("执行命令参数:{}", Arrays.toString(args));

        ParameterRun parameterRun = new ParameterRun();
        if (!(parameterRun.checkAndLoadExecParam(args))) {
            System.out.println("检查加载命令参数失败");
            logger.error("检查加载命令参数失败");
            return;
        }

        System.out.println("加载配置:" +  JSON.toJSONString(parameterRun));
        logger.info("加载配置:{}", JSON.toJSONString(parameterRun));
        System.out.println(parameterRun.getConfigPath());
        //加载kafka生产者配置、消费者配置、数据清洗规则
        if (!DataCleanConfig.loadDataCleanConfig(parameterRun.getConfigPath())) {
            return;
        }

        if(!DataCleanConfig.checkCleanRule()) {
            return;
        }
        logger.info("参数、配置文件检查成功");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置固定延迟重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(10, TimeUnit.SECONDS)));
        // 修改并行度
        env.setParallelism(parameterRun.getParallelism());

        //checkpoint配置 检查点
        if (parameterRun.getEnableCheckpoint()) {
            env.enableCheckpointing(60000);  // 设置 1分钟=60秒
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
            env.getCheckpointConfig().setCheckpointTimeout(10000);
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            env.getCheckpointConfig().enableExternalizedCheckpoints(
                    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

            String checkpointDataUri = parameterRun.getCheckpointDataUri();
            if (checkpointDataUri != null && checkpointDataUri.length() > 0) {
                //设置statebackend
                logger.info("设置状态后台");
                System.out.println("设置状态后台");
                env.setStateBackend((StateBackend)new RocksDBStateBackend(checkpointDataUri, true));
            }
        }

        try {
            msgFrameJson = getMsgFrame(DataCleanConfig.configJson.getJSONObject(StaticContent.splitKey));
        }
        catch (Exception err)
        {
            System.out.println("ERROR: " + err.getMessage());
            return;
        }
        System.out.println("msgFrame: "+msgFrameJson);

        String topicStr = msgFrameJson.getString(StaticContent.originalTopicsKey);

        System.out.println("topicStr: "+topicStr);

        FlinkKafkaConsumer010 flinkKafkaConsumer = new FlinkKafkaConsumer010<ObjectNode>(Arrays.asList(topicStr.split(",")),
                new DataCleanNodeMsg(true,JSON.toJSONString(DataCleanConfig.configJson).getBytes()),
                DataCleanConfig.consumerProperties);

        flinkKafkaConsumer.setStartFromLatest();
        DataStream<ObjectNode> originalStream = env.addSource(flinkKafkaConsumer);

//        originalStream.print();

        DataStream<JSONObject> flatMapStream = originalStream.flatMap(new DataCleanFlatMap());

//        flatMapStream.print();

        SplitStream<JSONObject> splitStream = flatMapStream.split((OutputSelector<JSONObject>) valueJson -> {

            List<String> tags   = new ArrayList<String>();
            String  topic       = valueJson.getString(StaticContent.topicKey);
//            System.out.println(topic);
            tags.add(topic);
            return tags;
        });

        if(DataCleanConfig.codeStandardJson.keySet().size() > 0) {
            codeStandardStream = env.addSource(new CodeStandardSource(DataCleanConfig.codeStandardJson, DataCleanConfig.configProperties)).broadcast();    //  可以把数据发送到后面算子的所有并行实际例中进行计算，否则处理数据丢失数据
        }

        JSONObject procTopicJson = msgFrameJson.getJSONObject(StaticContent.procTopicsKey);
//        System.out.println(procTopicJson.toJSONString());

        for (Object key : procTopicJson.keySet()) {
            String procTopicStr = (String)key;
            DataStream<JSONObject> selectStream = splitStream.select(procTopicStr);

            JSONObject targetJson = procTopicJson.getJSONObject(procTopicStr);
            procStream(selectStream, codeStandardStream, targetJson);
        }

        env.execute("DTDREAM_DateClean");
//
    }
    private static void procStream(DataStream<JSONObject> msgStream,DataStream<JSONObject> codeStandardStream,JSONObject targetJson){


        DataStream<JSONObject> cleanStream = null;
        DataStream<byte[]> normalProtobufStream = null;

//        String driver   =   DataCleanConfig.databasesProperties.getProperty("db.connection.driverClass");
//        String url      =   DataCleanConfig.databasesProperties.getProperty("db.connection.url");
//        String username =   DataCleanConfig.databasesProperties.getProperty("db.connection.username");
//        String password =   DataCleanConfig.databasesProperties.getProperty("db.connection.password");


        if(null != codeStandardStream){

            cleanStream = msgStream.connect(codeStandardStream).flatMap(new CoFlatMapFunction<JSONObject, JSONObject, JSONObject>() {
                //  存储国家和大区的映射关系
                private JSONObject codeStandardJson = new JSONObject();

                //  flatMap1 处理 Kafka 中的数据
                public void flatMap1(JSONObject msgJson, Collector<JSONObject> out) throws Exception {

                    JSONObject retJSON ;
                    msgJson.put(StaticContent.codeStandardKey,codeStandardJson);

                    retJSON = DataClean.cleanProcKey(msgJson);
                    out.collect(retJSON);

                }
                //  flatMap2 处理 Redis 返回的 map 类型的数据
                public void flatMap2(JSONObject codeStandardStr,Collector<JSONObject> out) throws Exception {
                    this.codeStandardJson = codeStandardStr;

                    return;
                }
            });
        }
        else{

            cleanStream = msgStream.map(new MapFunction<JSONObject, JSONObject>() {
                @Override
                public JSONObject map(JSONObject msgJson) throws Exception {
                    return DataClean.cleanProcKey(msgJson);
                }
            });
        }

        SplitStream<JSONObject> splitStream = cleanStream.split(new OutputSelector<JSONObject>(){
            @Override
            public Iterable<String> select(JSONObject msgJson) {

                List<String> tags = new ArrayList<String>();

                if(msgJson.containsKey(StaticContent.failDropKey)) {
                    tags.add(StaticContent.selectErr);
                }
                else{
                    if(msgJson.containsKey(StaticContent.failDefaultKey)){
                        tags.add(StaticContent.selectErr);
                    }
                    tags.add(StaticContent.selectNormal);
                }

                return tags;
            }
        });


        DataStream<JSONObject> normalStream = splitStream.select(StaticContent.selectNormal);
        DataStream<JSONObject> errStream = splitStream.select(StaticContent.selectErr);

        normalStream.addSink(new FlinkKafkaProducer010<JSONObject>(
                targetJson.getString(StaticContent.targetTopicKey),
                new SimpleJSONSchema(),
                DataCleanConfig.producerProperties));




        //--------------------protobuf-------------------

        normalProtobufStream = normalStream.map(new MapFunction<JSONObject, byte[]>() {
            @Override
            public byte[] map(JSONObject msgJson) throws Exception {

                Column.Data.Builder data = Column.Data.newBuilder();

                for (Object key : msgJson.keySet()) {
                    Column.Col.Builder col = Column.Col.newBuilder();
                    col.setColumName(key.toString());

                    if(null != msgJson.get(key.toString())) {
                        col.setColumValue(msgJson.getString(key.toString()));
                    }
                    else
                    {
                        col.setColumValue("");
                    }
                    col.setColumType("string");
                    data.addColumns(col);
                }

                return data.build().toByteArray();
            }
        });

        normalProtobufStream.addSink(new FlinkKafkaProducer010<byte[]>(
                targetJson.getString(StaticContent.realtimeTopicKey),
                new SimpleByteSchema(),
                DataCleanConfig.producerProperties));


//        DataStream<JSONObject> errFlatMapStream = errStream.flatMap(new ErrStreamFlatMap());
//
//        errFlatMapStream.addSink(new FlinkKafkaProducer010<JSONObject>(
//                targetJson.getString(StaticContent.targetTopicErrKey),
//                new SimpleJSONSchema(),
//                DataCleanConfig.producerProperties));


//        DataStream<Row> normalRowStream = normalStream.map(new Json2RowMap(targetJson.getString(StaticContent.updateFIELDS)));

//        int[] typesArray = new int[Arrays.asList(targetJson.getString(StaticContent.updateFIELDS).split(",")).size()];
//        for(int index = 0;index < typesArray.length;index++){
//            typesArray[index] = Types.VARCHAR;
//        }

//        normalRowStream.writeUsingOutputFormat(JDBCOutputFormat.buildJDBCOutputFormat()
//                .setDrivername(driver)
//                .setDBUrl(url)
//                .setUsername(username)
//                .setPassword(password)
//                .setQuery(targetJson.getString(StaticContent.updateSQL))
//                .setSqlTypes(typesArray)
//                .setBatchInterval(0)
//                .finish());
    }

    private static JSONObject getMsgFrame(JSONObject msgFrameJson) throws Exception {

        JSONObject resultJson = new JSONObject();
        JSONObject splitProcJson = new JSONObject();
        StringBuffer sourceTopicSb = new StringBuffer();


        int index = 0;
        String targetTopic = "";
        String targetTopicErr = "";
        String realtimeTopicStr = "";
        String updateSql = "";
        String updateFields = "";
        String tmpUpdateSql = "";
        String tmpUpdateFields = "";
        JSONObject tmpJson = null;
        JSONObject childJson = null;

        for (Object key : msgFrameJson.keySet()) {
            String sourceTopic = (String)key;

            targetTopic = "";
            targetTopicErr = "";
            realtimeTopicStr = "";
            updateSql = "";
            updateFields = "";
            tmpUpdateSql = "";
            tmpUpdateFields = "";

            if(!sourceTopic.equals(StaticContent.DM_Topic)){
                if (index > 0) {
                    sourceTopicSb.append(",");
                }
                sourceTopicSb.append(sourceTopic);

                index++;
            }


            JSONObject sourceTopicJson = msgFrameJson.getJSONObject(sourceTopic);

            if(sourceTopicJson.containsKey(StaticContent.targetTopicKey)) {
                targetTopic = sourceTopicJson.getString(StaticContent.targetTopicKey);
            }
            if(sourceTopicJson.containsKey(StaticContent.realtimeTopicKey)) {
                realtimeTopicStr = sourceTopicJson.getString(StaticContent.realtimeTopicKey);
            }

            if(sourceTopicJson.containsKey(StaticContent.targetTopicErrKey)) {
                targetTopicErr = sourceTopicJson.getString(StaticContent.targetTopicErrKey);
            }

            if(sourceTopicJson.containsKey(StaticContent.updateSQL)) {
                updateSql = sourceTopicJson.getString(StaticContent.updateSQL);
            }

            if(sourceTopicJson.containsKey(StaticContent.updateFIELDS)) {
                updateFields = sourceTopicJson.getString(StaticContent.updateFIELDS);
            }

            if(updateSql.length() > 0 || updateFields.length() > 0){
                checkSqlParaCount(updateSql,updateFields);
            }

            tmpJson = new JSONObject();

            tmpJson.put(StaticContent.targetTopicKey,targetTopic);
            tmpJson.put(StaticContent.targetTopicErrKey,targetTopicErr);
            tmpJson.put(StaticContent.updateSQL,updateSql);
            tmpJson.put(StaticContent.updateFIELDS,updateFields);
            tmpJson.put(StaticContent.realtimeTopicKey,realtimeTopicStr);

            splitProcJson.put(sourceTopic,tmpJson);

            if(sourceTopicJson.containsKey(StaticContent.splitValueKey) && sourceTopicJson.containsKey(StaticContent.splitKeyKey)){
                JSONObject splitJson = sourceTopicJson.getJSONObject(StaticContent.splitValueKey);

                for (Object splitKey : splitJson.keySet()) {
                    String splitKeyStr = (String)splitKey;
                    StringBuffer tmpKeySB = new StringBuffer(sourceTopic);
                    tmpKeySB.append("_");
                    tmpKeySB.append(splitKey);

                    childJson = splitJson.getJSONObject(splitKeyStr);

                    if(!childJson.containsKey(StaticContent.targetTopicKey)) {
                        childJson.put(StaticContent.targetTopicKey,targetTopic);
                    }

                    if(!childJson.containsKey(StaticContent.targetTopicErrKey)) {
                        childJson.put(StaticContent.targetTopicErrKey,targetTopicErr);
                    }

                    if(!childJson.containsKey(StaticContent.realtimeTopicKey)) {
                        childJson.put(StaticContent.realtimeTopicKey,realtimeTopicStr);
                    }

                    if(!childJson.containsKey(StaticContent.updateSQL)) {
                        childJson.put(StaticContent.updateSQL,updateSql);
                    }
                    else{
                        tmpUpdateSql = tmpJson.getString(StaticContent.updateSQL);
                    }

                    if(!childJson.containsKey(StaticContent.updateFIELDS)) {
                        childJson.put(StaticContent.updateFIELDS,updateFields);
                    }
                    else{
                        tmpUpdateFields = tmpJson.getString(StaticContent.updateFIELDS);
                    }


                    if(tmpUpdateSql.length() > 0 || tmpUpdateFields.length() > 0){
                        checkSqlParaCount(tmpUpdateSql,tmpUpdateFields);
                    }

                    splitProcJson.put(tmpKeySB.toString(),childJson);
                }
            }

        }

        resultJson.put(StaticContent.originalTopicsKey,sourceTopicSb);
        resultJson.put(StaticContent.procTopicsKey,splitProcJson);

        return resultJson;
    }
    private static boolean checkSqlParaCount(String updateSqlStr,String updateFieldStr) throws Exception {
        int count = ToolsUtil.getSubStrCount(updateSqlStr,"?");
        String[] updateFieldArray = updateFieldStr.split(",");
        List<String> updateFieldList = Arrays.asList(updateFieldArray);

        if(updateFieldList.size() != count)
        {
            throw new Exception("ERROR: gauss info,because the updateSqlStr is not equal updateFieldStr");
        }
        return true;
    }

}
