package utils;

/**
 * @Description:
 * @Author: l0430
 * @Date: 2020/3/5 上午10:06
 */
public enum DataTypeEnum {

    TYPE_INT    ("int", "整型"),
    TYPE_UUID   ("uuid", "整型"),
    TYPE_MD5_ALL ("md5_all", "生成md5"),
    TYPE_MD5    ("md5", "生成md5"),
    TYPE_NOW    ("now", "当前时间");


    public String getDataType() {
        return dataType;
    }

    public String getDataDescription() {
        return dataDescription;
    }

    //数据类型
    private String dataType;

    //描述
    private String dataDescription;


    DataTypeEnum(String dataType, String dataDescription) {
        this.dataType = dataType;
        this.dataDescription = dataDescription;
    }


    // 根据value返回枚举类型,主要在switch中使用
    public static DataTypeEnum getByValue(String value) {
        for (DataTypeEnum type : values()) {
            if (type.getDataType().equals(value)) {
                return type;
            }
        }
        return null;
    }
}
