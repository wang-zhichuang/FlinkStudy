package utils;

/**
 * @Description:
 * @Author: l0430
 * @Date: 2020/1/4 下午5:49
 */
public enum CleanRuleEnum {

    RULE_GENERATE("generateData","生成数据失败"),          //编写方式:generateData(xxx,drop),其中xxx为DataTypeEnum中的类型
    RULE_CUT_COLUMN("cutColumn","截取列失败"),          //从其他行截取字符串:generateData(A:m-n,drop),其中A为已经存在列，m-n为起止位置
    RULE_ADD_CONTENT("addContent","添加内容失败"),         //添加固定值
    RULE_CHECK_LENGTH("checkLength","不满足长度要求"),     //校验长度
    RULE_NOT_EMPTY("notEmpty","值域不能为空"),            //校验是否为空
    RULE_INTERGER("integerStandardization","值域必须为整型"),//校验是否为整形
    RULE_TIME("timeStandardization","时间格式错误"),          //校验是否为时间
    RULE_DATE("dateStandardization","日期格式错误"),      //校验是否为日期
    RULE_DECIMAL("decimal","小数格式不规范"),            //编写方式:decimal(x,y)，其中为数值的长度（不含小数点）,y为小数的长度
    RULE_REGULAR("regularExpression","值域不满足正则规范"),     //校验是否满足正则表达式:regularExpression(xxx) 其中xxx为正则表达式
    RULE_COMPARE_COLUMN("compareColumn","和其他列关联不匹配"),  //和其他列进行比较，编写方式:compareColumn(a-b,x,m:c-d),其中:a,b为当前列起止位置，x为比较类型CompareEnum，m为表中对比列,c,d为m列起止位置
    RULE_COMPARE_VALUE("compareValue","值域不满足要求"),       //和某一特定值进行比较compareValue(a-b,x,y),其中:a,b为当前列起止位置，x为比较类型CompareEnum,y为对应的值
    RULE_CODE_STANDARD("codeStandardization","字典标准化失败");  //编写方式:codeStandardization(a,m,n,x,y,z),其中:a数据元内部标识,m为当前列关联dictionaryName/dictionaryValue,n为dictionaryValue/dictionaryName，x关联值的名称，y为CodeStandardActionEnum类型,z为自定义组

    //清洗规则类型
    private String ruleType;


    //清洗规则名称
    private String ruleDescription;



    CleanRuleEnum(String ruleType, String ruleDescription) {
        this.ruleType = ruleType;
        this.ruleDescription = ruleDescription;
    }

    public String getRuleType() {
        return ruleType;
    }

    public void setRuleType(String ruleType) {
        this.ruleType = ruleType;
    }

    public String getRuleDescription() {
        return ruleDescription;
    }

    public void setRuleDescription(String ruleDescription) {
        this.ruleDescription = ruleDescription;
    }


     // 根据value返回枚举类型,主要在switch中使用
     public static CleanRuleEnum getByValue(String value) {
        for (CleanRuleEnum rule : values()) {
            if (rule.getRuleType().equals(value)) {
                return rule;
            }
        }
        return null;
    }
}
