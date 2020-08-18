package cn.oneseek.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

public class DataUtil {



    public static boolean isValidPhoneNumber(String phoneNumber){
        String pattern = "0?(13|14|15|18|17)[0-9]{9}|[0-9-()（）]{7,18}";
        return Pattern.compile(pattern).matcher(phoneNumber).matches();
    }

    public static boolean isValidDate(String str){
        String datePatternStr1 = "^[1-9]\\d{3}(0?[1-9]|1[0-2])(0?[1-9]|[1-2][0-9]|3[0-1])$";
        return Pattern.compile(datePatternStr1).matcher(str).matches();
    }

    public static boolean isValidEmail(String str){
        String pattern = "\\w[-\\w.+]*@([A-Za-z0-9][-A-Za-z0-9]+\\.)+[A-Za-z]{2,14}";
        return Pattern.compile(pattern).matcher(str).matches();
    }
}
