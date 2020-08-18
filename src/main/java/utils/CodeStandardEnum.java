package utils;

/**
 * @Description:
 * @Author: l0430
 * @Date: 2020/3/18 上午9:44
 */
public enum CodeStandardEnum {
    CODE_STANDARD_NAME("name"),
    CODE_STANDARD_DICT_NAME("dictionaryname"),
    CODE_STANDARD_DICT_VALUE("dictionaryvalue");

    public String getCodeStandardType() {
        return codeStandardType;
    }

    //清洗规则类型
    private String codeStandardType;

    CodeStandardEnum(String codeStandardType){
        this.codeStandardType = codeStandardType;
    }

    // 根据value返回枚举类型,主要在switch中使用
    public static CodeStandardEnum getByValue(String value) {
        for (CodeStandardEnum action : values()) {
            if (action.getCodeStandardType().equals(value)) {
                return action;
            }
        }
        return null;
    }
}
