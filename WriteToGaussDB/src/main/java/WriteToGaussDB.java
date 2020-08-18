import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import com.alibaba.fastjson.JSONObject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class WriteToGaussDB {
    public static void main(String[] args) {

    }
}
class StaticContent{
    static String timeFormatStr = "yyyy-MM-dd HH:mm:ss";
}
class GaussDBSink extends RichSinkFunction<JSONObject> {
    private PreparedStatement ps=null;
    private Connection connection=null;
    Properties databasesProperties = null;
    private String updateSqlStr = "";
    private List<String> updateFieldList = null;

    public GaussDBSink(Properties databasesProperties, String updateSqlStr, String updateFieldStr){
        this.databasesProperties = databasesProperties;
        this.updateSqlStr =  updateSqlStr;
        this.updateFieldList =   Arrays.asList(updateFieldStr.split(","));
    }



    /**
     * open()方法建立连接
     * 这样不用每次 invoke 的时候都要建立连接和释放连接
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);

    }



    /**
     * 每插入一条数据的调用一次invoke
     * @param valueJson
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(JSONObject valueJson, Context context) throws Exception {

        //Connection connection = DataBaseUtil.getConnection(databasesProperties);
        ps = connection.prepareStatement(this.updateSqlStr);

        String updateFieldStr = "";
        String valueStr = "";

        try {

            int index = 0;
            for(index = 0;index < this.updateFieldList.size();index++){
                updateFieldStr = this.updateFieldList.get(index);
                if(valueJson.containsKey(updateFieldStr)){
                    valueStr = valueJson.getString(updateFieldStr);
                }
                else
                {
                    valueStr = "";
                }

                ps.setString(index + 1,valueStr);

            }

            ps.executeUpdate();
        }
        catch (Exception err){

            Date date = new Date();

            SimpleDateFormat dateFormat= new SimpleDateFormat(StaticContent.timeFormatStr);

            StringBuffer sb = new StringBuffer("ERROR:\n");
            sb.append(err.getMessage());

            sb.append(dateFormat.format(date));

            sb.append("\nupdate gauss err:\n");

            sb.append("\nupdate gauss err:\n");
            sb.append(this.updateSqlStr);
            sb.append("\n");
            sb.append(this.updateFieldList.toString());
            sb.append("\n");
            sb.append(valueJson.toJSONString());
            sb.append("\n");

            sb.append(err.getStackTrace().toString());
            System.out.println(sb.toString());


        }

    }
    @Override
    public void close() throws Exception {
        Date date = new Date();
        SimpleDateFormat dateFormat= new SimpleDateFormat(StaticContent.timeFormatStr);

        System.out.println(dateFormat.format(date) + " the gaussdb connect is closing\n");

        super.close();
    }


}
