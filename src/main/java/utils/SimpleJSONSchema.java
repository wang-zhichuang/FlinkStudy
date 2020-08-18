package utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @Description:
 * @Author: l0430
 * @Date: 2020/6/11 下午7:46
 */
public class SimpleJSONSchema implements SerializationSchema<JSONObject> {
    private static final long serialVersionUID = 1L;
    private transient Charset charset;

    public SimpleJSONSchema() {
        this(StandardCharsets.UTF_8);
    }

    public SimpleJSONSchema(Charset charset) {
        this.charset = (Charset) Preconditions.checkNotNull(charset);
    }

    public Charset getCharset() {
        return this.charset;
    }

    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    public byte[] serialize(JSONObject element) {

        //增加接受时间戳
        /*
        Date date = new Date();
        SimpleDateFormat dateFormat= new SimpleDateFormat(StaticContent.timeMSFormatStr);
        if(!element.containsKey(StaticContent.producertime)){
            element.put(StaticContent.producertime,dateFormat.format(date));
        }*/


        String elementStr = JSONObject.toJSONString(element);
        return elementStr.getBytes(this.charset);
    }

    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeUTF(this.charset.name());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        String charsetName = in.readUTF();
        this.charset = Charset.forName(charsetName);
    }
}
