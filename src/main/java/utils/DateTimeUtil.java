package utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * 时间处理工具类
 */
public class DateTimeUtil {

    public static final int yearLength = 4;
    public static final int monthLength = 2;
    public static final int dayLength = 2;

    //阿拉伯数字1-9开头 一个右下划线 三个连续的 d
    private  static  String timeMSPatternStr1 = "^[1-9]\\d{3}(0?[1-9]|1[0-2])(0?[1-9]|[1-2][0-9]|3[0-1])(0?[0-9]|1[0-9]|2[0-3])(0?[0-9]|[1-5][0-9])(0?[0-9]|[1-5][0-9])\\d{1,3}$";
    private  static  String timeMSPatternStr2 = "^[1-9]\\d{3}/(0?[1-9]|1[0-2])/(0?[1-9]|[1-2][0-9]|3[0-1])[ ](0?[0-9]|1[0-9]|2[0-3]):(0?[0-9]|[1-5][0-9]):(0?[0-9]|[1-5][0-9])[ ]\\d{1,3}$";
    private  static  String timeMSPatternStr3 = "^[1-9]\\d{3}-(0?[1-9]|1[0-2])-(0?[1-9]|[1-2][0-9]|3[0-1])[ ](0?[0-9]|1[0-9]|2[0-3]):(0?[0-9]|[1-5][0-9]):(0?[0-9]|[1-5][0-9])[ ]\\d{1,3}$";

    private  static  String timePatternStr1 = "^[1-9]\\d{3}(0?[1-9]|1[0-2])(0?[1-9]|[1-2][0-9]|3[0-1])(0?[0-9]|1[0-9]|2[0-3])(0?[0-9]|[1-5][0-9])(0?[0-9]|[1-5][0-9])$";
    private  static  String timePatternStr2 = "^[1-9]\\d{3}/(0?[1-9]|1[0-2])/(0?[1-9]|[1-2][0-9]|3[0-1])[ ](0?[0-9]|1[0-9]|2[0-3]):(0?[0-9]|[1-5][0-9]):(0?[0-9]|[1-5][0-9])$";
    private  static  String timePatternStr3 = "^[1-9]\\d{3}-(0?[1-9]|1[0-2])-(0?[1-9]|[1-2][0-9]|3[0-1])[ ](0?[0-9]|1[0-9]|2[0-3]):(0?[0-9]|[1-5][0-9]):(0?[0-9]|[1-5][0-9])$";


    private  static  String datePatternStr1 = "^[1-9]\\d{3}(0?[1-9]|1[0-2])(0?[1-9]|[1-2][0-9]|3[0-1])$";
    private  static  String datePatternStr2 = "^[1-9]\\d{3}/(0?[1-9]|1[0-2])/(0?[1-9]|[1-2][0-9]|3[0-1])$";
    private  static  String datePatternStr3 = "^[1-9]\\d{3}-(0?[1-9]|1[0-2])-(0?[1-9]|[1-2][0-9]|3[0-1])$";


    public static boolean isDate(String strDate) {

        Pattern timeMSPattern1 = Pattern.compile(timeMSPatternStr1);
        Pattern timeMSPattern2 = Pattern.compile(timeMSPatternStr2);
        Pattern timeMSPattern3 = Pattern.compile(timeMSPatternStr3);

        Pattern timePattern1 = Pattern.compile(timePatternStr1);
        Pattern timePattern2 = Pattern.compile(timePatternStr2);
        Pattern timePattern3 = Pattern.compile(timePatternStr3);

        Pattern datePattern1 = Pattern.compile(datePatternStr1);
        Pattern datePattern2 = Pattern.compile(datePatternStr2);
        Pattern datePattern3 = Pattern.compile(datePatternStr3);

        Matcher mst1 = timeMSPattern1.matcher(strDate);
        Matcher mst2 = timeMSPattern2.matcher(strDate);
        Matcher mst3 = timeMSPattern3.matcher(strDate);

        Matcher t1 = timePattern1.matcher(strDate);
        Matcher t2 = timePattern2.matcher(strDate);
        Matcher t3 = timePattern3.matcher(strDate);

        Matcher d1 = datePattern1.matcher(strDate);
        Matcher d2 = datePattern2.matcher(strDate);
        Matcher d3 = datePattern3.matcher(strDate);


        if (t1.matches() || t2.matches() || t3.matches() || d1.matches() || d2.matches() || d3.matches() || mst1.matches() || mst2.matches() || mst3.matches() ) {
            System.out.println(t1.matches());
            System.out.println(t2.matches());
            System.out.println(t3.matches());

            System.out.println(d1.matches());
            System.out.println(d2.matches());
            System.out.println(d3.matches());

            System.out.println(mst1.matches());
            System.out.println(mst2.matches());
            System.out.println(mst3.matches());



            return true;
        } else {
            return false;
        }
    }

    public static String getSplitDateStr(int len,StringBuffer sb){

        String retStr = "";

        int index = len - 1;
        boolean splitFlag = false;
        StringBuilder strBuilder = new StringBuilder();
        int index1 = sb.indexOf(" ");
        int index2 = sb.indexOf("/");
        int index3 = sb.indexOf(":");
        int index4 = sb.indexOf("-");


        if(index1 == index || index1 == index + 1) {
            index = index1;
            splitFlag = true;
        }

        if(index2 == index || index2 == index + 1) {
            index = index2;
            splitFlag = true;
        }

        if(index3 == index || index3 == index + 1) {
            index = index3;
            splitFlag = true;
        }

        if(index4 == index || index4 == index + 1) {
            index = index4;
            splitFlag = true;
        }

        if(splitFlag == true){
            retStr = sb.substring(0,index);
            sb.delete(0,index + 1);
        }
        else
        {
            if(len > sb.length()){
                len = sb.length();
            }
            retStr = sb.substring(0,len);
            sb.delete(0,len);
        }

        if(retStr.length() < 2)
        {
            retStr = "0" + retStr;
        }
        return retStr;
    }

    public static  String getDateStr(String strDate){

        String resStr = "";
        StringBuffer sb = new StringBuffer(strDate);
        StringBuilder strBuilder = new StringBuilder();


        resStr = getSplitDateStr(4,sb);
        strBuilder.append(resStr);

        strBuilder.append("-");
        resStr = getSplitDateStr(2,sb);
        strBuilder.append(resStr);

        strBuilder.append("-");
        resStr = getSplitDateStr(2,sb);
        strBuilder.append(resStr);


        return strBuilder.toString();
    }

    public static  String getTimeStr(String strTime){

        String resStr = "";
        StringBuffer sb = new StringBuffer(strTime);
        StringBuilder strBuilder = new StringBuilder();


        resStr = getSplitDateStr(4,sb);
        strBuilder.append(resStr);

        strBuilder.append("-");
        resStr = getSplitDateStr(2,sb);
        strBuilder.append(resStr);

        strBuilder.append("-");
        resStr = getSplitDateStr(2,sb);
        strBuilder.append(resStr);

        strBuilder.append(" ");
        resStr = getSplitDateStr(2,sb);
        strBuilder.append(resStr);

        strBuilder.append(":");
        resStr = getSplitDateStr(2,sb);
        strBuilder.append(resStr);

        strBuilder.append(":");
        resStr = getSplitDateStr(2,sb);
        strBuilder.append(resStr);

        return strBuilder.toString();

    }

    public static boolean isTime(String strDate) {

        Pattern timeMSPattern1 = Pattern.compile(timeMSPatternStr1);
        Pattern timeMSPattern2 = Pattern.compile(timeMSPatternStr2);
        Pattern timeMSPattern3 = Pattern.compile(timeMSPatternStr3);

        Pattern timePattern1 = Pattern.compile(timePatternStr1);
        Pattern timePattern2 = Pattern.compile(timePatternStr2);
        Pattern timePattern3 = Pattern.compile(timePatternStr3);

        Matcher ms1 = timeMSPattern1.matcher(strDate);
        Matcher ms2 = timeMSPattern2.matcher(strDate);
        Matcher ms3 = timeMSPattern3.matcher(strDate);

        Matcher t1 = timePattern1.matcher(strDate);
        Matcher t2 = timePattern2.matcher(strDate);
        Matcher t3 = timePattern3.matcher(strDate);


        if (t1.matches() || t2.matches() || t3.matches() || ms1.matches() || ms2.matches() || ms3.matches()) {
            return true;
        } else {
            return false;
        }
    }


}
