package utils;

/**
 * @Description:
 * @Author: l0430
 * @Date: 2020/1/15 下午3:36
 */
public enum CompareEnum {
    COMPARE_LT("lt","小于"),
    COMPARE_GT("gt","大于"),
    COMPARE_LE("le","小于等于"),
    COMPARE_GE("ge","大于等于"),
    COMPARE_EQ("eq","等于"),
    COMPARE_NE("ne","不等于");

    //比较类型
    private String compareType;

    //描述
    private String compareDescription;

    CompareEnum(String compareType, String compareDescription) {
        this.compareType = compareType;
        this.compareDescription = compareDescription;
    }

    public String getCompareType() {
        return compareType;
    }

    public void setCompareType(String compareType) {
        this.compareType = compareType;
    }

    public String getCompareDescription() {
        return compareDescription;
    }

    public void setCompareDescription(String compareDescription) {
        this.compareDescription = compareDescription;
    }

    // 根据value返回枚举类型,主要在switch中使用
    public static CompareEnum getByValue(String value) {
        for (CompareEnum rule : values()) {
            if (rule.getCompareType().equals(value)) {
                return rule;
            }
        }
        return null;
    }
}
