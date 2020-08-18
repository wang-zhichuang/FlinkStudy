package cn.oneseek.utils;

import org.apache.commons.lang3.time.FastDateFormat;

import java.util.Date;


public class DateUtils {
    private static FastDateFormat SOURCE_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    private static FastDateFormat TARGET_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss");

    public static Long  getTime(String  time) throws Exception{
        return SOURCE_FORMAT.parse(time).getTime();
    }

    public static String parseMinute(String time) throws  Exception{
        return TARGET_FORMAT.format(new Date(getTime(time)));
    }


    public static void main(String[] args) throws Exception{
        String time = "2018-12-19 18:55:00";

        System.out.println(parseMinute(time));
    }
}

