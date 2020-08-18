package utils;

/**
 * @Description:
 * @Author: l0430
 * @Date: 2020/3/12 下午7:53
 */
public enum ResultActionEnum {
    ACTION_DROP     ("drop"),
    ACTION_DEFAULT  ("default");

    //清洗规则类型
    private String actionType;

    public String getActionType() {
        return actionType;
    }

    ResultActionEnum(String actionType) {
        this.actionType = actionType;
    }


    // 根据value返回枚举类型,主要在switch中使用
    public static ResultActionEnum getByValue(String value) {
        for (ResultActionEnum action : values()) {
            if (action.getActionType().equals(value)) {
                return action;
            }
        }
        return null;
    }
}
