package DataCleanProc;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

/**
 * @Description:
 * @Author: l0430
 * @Date: 2020/6/21 上午12:40
 */
public class Json2RowMap implements MapFunction<JSONObject, Row> {

    private List<String> updateFieldList = null;
    public Json2RowMap(String UpdateSql){
        this.updateFieldList = Arrays.asList(UpdateSql.split(","));
    }

    @Override
    public Row map(JSONObject msgJson) throws Exception{
        Row msgRow = new Row(updateFieldList.size());
        String columnName = "";
        Object value = null;

        for(int index = 0;index < updateFieldList.size();index++)
        {
            columnName = updateFieldList.get(index);

            value = msgJson.get(columnName);
            if(null != value){
                msgRow.setField(index,value);
            }
            else {
                msgRow.setField(index,"");
            }
        }

        return msgRow;

    }
}
