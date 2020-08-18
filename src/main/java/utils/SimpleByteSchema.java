package utils;

import org.apache.flink.api.common.serialization.SerializationSchema;


/**
 * @Description:
 * @Author: l0430
 * @Date: 2020/3/20 下午8:10
 */
public class SimpleByteSchema implements SerializationSchema<byte[]> {
    private static final long serialVersionUID = 1L;

    public byte[] serialize(byte[] message) {
        return message;
    }
}
