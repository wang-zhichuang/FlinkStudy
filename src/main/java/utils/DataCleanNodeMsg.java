package utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DataCleanNodeMsg implements KeyedDeserializationSchema<ObjectNode> {

    private final boolean includeMetadata;
    private ObjectMapper mapper;
    private byte[] configJsonObject;

    public DataCleanNodeMsg(boolean includeMetadata) {
        this.includeMetadata = includeMetadata;
    }

    public DataCleanNodeMsg(boolean includeMetadata, byte[] configJsonObject) {
        this.includeMetadata = includeMetadata;
        this.configJsonObject = configJsonObject;
    }

    public ObjectNode deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        String tmpStr = "";
        if (this.mapper == null) {
            this.mapper = new ObjectMapper();
        }

        ObjectNode node = this.mapper.createObjectNode();

        try {
            if (messageKey != null && messageKey.length > 0) {
                node.set("key", (JsonNode) this.mapper.readValue(messageKey, JsonNode.class));
            }
            if (message != null && message.length > 0) {

                //增加接受时间戳
                Date date = new Date();
                SimpleDateFormat dateFormat= new SimpleDateFormat(StaticContent.timeMSFormatStr);
                ObjectNode msgNode = (ObjectNode) this.mapper.readValue(message, JsonNode.class);

                if(!msgNode.has(StaticContent.consumertime)) {
                    msgNode.put(StaticContent.consumertime, dateFormat.format(date));
                }

                node.set("value", msgNode);


            }
            if (this.includeMetadata) {
                node.putObject("metadata").put("offset", offset).put("topic", topic).put("partition", partition);
                if (configJsonObject != null && configJsonObject.length > 0) {
                    node.set("config", (JsonNode) this.mapper.readValue(configJsonObject, JsonNode.class));
                }
            }
        }
        catch (Exception e){
            tmpStr = new String(message,"UTF-8");

            System.out.println("ERR Messages:");
            System.out.println(tmpStr);
            System.out.println(e.getMessage());
        }

        return node;
    }

    public boolean isEndOfStream(ObjectNode nextElement) {
        return false;
    }

    public TypeInformation<ObjectNode> getProducedType() {
        return TypeExtractor.getForClass(ObjectNode.class);
    }
}
