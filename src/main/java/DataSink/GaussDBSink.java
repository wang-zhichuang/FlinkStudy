package DataSink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class GaussDBSink extends RichSinkFunction<String> {

    public GaussDBSink(){
        System.out.println("GaussDB running.....");
        System.out.println();
    }


    @Override
    public void invoke(String value, Context context) throws Exception {
        System.out.println("value: "+value);
        System.out.println("context"+ context);
    }

//    Request URL: http://10.96.0.207:47693/metadataManage/api/restful/v1/dataView/tables
//
//    Body
//    {
//         "startIndex":1,
//         "pageSize":50,
//         "dataSource":
//                ["explorer_test@预置默认引擎"],
//         "sourceType":"ALL_TABLES"
//    }

}
