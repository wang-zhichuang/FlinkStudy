package utils;

import java.math.BigDecimal;
import java.security.MessageDigest;
import java.util.UUID;

/**
 * @Description:
 * @Author: l0430
 * @Date: 2020/1/5 下午2:35
 */
public class ToolsUtil {

    public static String repeatString(String str, int n, String seg) {
        StringBuffer sb = new StringBuffer(str);

        for (int i = 0; i < n; i++) {
            sb.append(seg);
        }
        return sb.toString();
    }


    public static String numerFormat(double num,int intLen,int decimalLen){

        String retStr = "";
        int negativeIndex = 0;
        String value = String.valueOf(num);
        BigDecimal bigNum = new BigDecimal(num);


        Character negativeChar = value.charAt(0);
        if(negativeChar == '-'){
            negativeIndex = 1;
        }

        if(intLen < value.substring(negativeIndex,value.indexOf(".")).length()){
            return retStr;
        }

        retStr = bigNum.setScale(decimalLen,BigDecimal.ROUND_HALF_UP).toString();
        return  retStr;
    }

    public static String deleteString(String str, char delChar) {
        StringBuffer stringBuffer = new StringBuffer("");
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) != delChar) {
                stringBuffer.append(str.charAt(i));
            }
        }
        return stringBuffer.toString();
    }

    /**
     * 获取
     * @return
     */
    public static String getGuid(){
        UUID uuid = UUID.randomUUID();
        String str = uuid.toString();
        // 去掉"-"符号
        String tempUid = str.substring(0, 8) + str.substring(9, 13) + str.substring(14, 18) + str.substring(19, 23) + str.substring(24);
        return tempUid;
    }


    /**
     * 查找字符串中某一子字符串的数量
     * @param SourceStr
     * @param findStr
     * @return
     */
    public static int getSubStrCount(String SourceStr,String findStr) {
        int count = 0;
        int len = SourceStr.length();
        while(SourceStr.indexOf(findStr) != -1) {
            SourceStr = SourceStr.substring(SourceStr.indexOf(findStr) + 1,SourceStr.length());
            count++;
        }

        return count;
    }


    public static String MD5(String key) {
        char hexDigits[] = {
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
        };
        try {
            byte[] btInput = key.getBytes();
            // 获得MD5摘要算法的 MessageDigest 对象
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            // 使用指定的字节更新摘要
            mdInst.update(btInput);
            // 获得密文
            byte[] md = mdInst.digest();
            // 把密文转换成十六进制的字符串形式
            int j = md.length;
            char str[] = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = md[i];
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str[k++] = hexDigits[byte0 & 0xf];
            }
            return new String(str);
        } catch (Exception e) {
            return null;
        }
    }
}
