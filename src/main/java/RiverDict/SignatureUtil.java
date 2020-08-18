package RiverDict;

/**
 * ------------------------------------------------------------------
 * Copyright @ 2019 Hangzhou DtDream Technology Co.,Ltd. All rights reserved.
 * ------------------------------------------------------------------
 * Product: DataRiver
 * Module Name:
 * Date Created: 2019/1/24
 * Description:
 * ------------------------------------------------------------------
 * Modification History
 * DATE            Name           Description
 * ------------------------------------------------------------------
 * 2019/1/24        z0586
 * ------------------------------------------------------------------
 */

import org.springframework.http.HttpMethod;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.commons.codec.binary.Base64;


public class SignatureUtil {
    public static final String DMALL_HEAD_ALGORITHM = "Algorithm";
    public static final String DMALL_ACCESSKEY_ID = "AccessKeyId";
    public static final String DMALL_HEAD_SIGNATURE = "Signature";
    public static final String DMALL_TIMESTAMP = "TimeStamp";
    public static final String UTF_8 = "UTF-8";


    public static String getQueryParams(Map<String, Collection<String>> parameter) {
        Map<String, String> params = new HashMap();
        Iterator it = parameter.keySet().iterator();
        while (it.hasNext()) {
            String key = (String) it.next();
            Collection<String> temp = parameter.get(key);
            String[] values = temp.toArray(new String[temp.size()]);
            String value = "";
            if (values.length == 1) {
                value = values[0];
            } else if (values.length > 1) {
                value = Arrays.toString(values);
            }
            try {
                params.put(key, URLDecoder.decode(value, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("API 签名编码错误");
            }

        }
        return getQueryStr(params);
    }


    public static String getQueryStr(Map<String, String> queries) {
        StringBuilder buff = new StringBuilder();
        String[] sortedKeys = queries.keySet().toArray(new String[0]);
        Arrays.sort(sortedKeys);
        for (String sortedKey : sortedKeys) {
            buff.append("&").append(sortedKey).append("=").append(queries.get(sortedKey));
        }
        return buff.toString();
    }

    // 获取时间戳
    public static String getUTCTime() {
        SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
        // 1、取得本地时间：
        Calendar cal = Calendar.getInstance();
        // 2、取得时间偏移量：
        int zoneOffset = cal.get(Calendar.ZONE_OFFSET);
        // 3、取得夏令时差：
        int dstOffset = cal.get(Calendar.DST_OFFSET);
        // 4、从本地时间里扣除这些差量，即可以取得UTC时间：
        cal.add(Calendar.MILLISECOND, -(zoneOffset + dstOffset));
        return SDF.format(cal.getTime());
    }

    /**
     * 签名计算方法
     *
     * @param accessKeyId     密钥id
     * @param accessKeySecret 密钥secret
     * @param httpMethod      http请求方法类型，目前支持 POST, PUT, DELETE, GET四种
     * @param queryString     http请求参数，通常为?号之后的内容
     * @param timeStamp       时间戳, 格式 yyyy-MM-dd HH:mm:ss，注意时区为UTC时区，如何获取utc时间，参看getUTCTime方法
     *                        注意：为防止重放，调用方传入的时间与服务器时间不能偏差超过5分钟
     * @return 计算结果，该字段为String类型，上层需要把该字段放到http header中，其中key为Authorization，value为返回结果
     */
    public static String getAuthorization(String accessKeyId, String accessKeySecret, String queryString, String httpMethod, String timeStamp) {
        /* 构造计算签名的数据 */
        String signature = getSignString(queryString, httpMethod, timeStamp, accessKeySecret);
        if (null == signature) {
            return null;
        }
        StringBuilder auth = new StringBuilder();
        auth.append(DMALL_HEAD_ALGORITHM).append("=").append("HMAC-SHA256").append(",")
                .append(DMALL_ACCESSKEY_ID).append("=").append(accessKeyId).append(",")
                .append(DMALL_TIMESTAMP).append("=").append(timeStamp).append(",")
                .append(DMALL_HEAD_SIGNATURE).append("=").append(signature);
        return auth.toString();
    }

    public static String getSignString(String queryString, String httpMethod, String timeStamp, String accessKeySecret) {
        String strToSign = null;
        try {
            if (HttpMethod.POST.name().equalsIgnoreCase(httpMethod)) {
                //忽略POST方法URL上的参数，按技术规范POST数据应该填写在body里
                queryString = "";
            } else if (null != queryString) {
                queryString = URLDecoder.decode(queryString, UTF_8);
            }
            strToSign = buildCanonicalString(queryString, httpMethod, timeStamp);
        } catch (UnsupportedEncodingException e) {
            return strToSign;
        }
        return signWithHmac(strToSign, accessKeySecret);
    }

    public static String buildCanonicalString(String parameters, String httpMethod, String timeStamp) {
        StringBuilder builder = new StringBuilder();
        builder.append(httpMethod).append("&");
        try {
            builder.append(URLEncoder.encode("/", UTF_8)).append("&");
            if (null != timeStamp) {
                builder.append(URLEncoder.encode(timeStamp, UTF_8)).append("&");
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        if (null != parameters) {
            String queryString = parameters;
            // 去除#后面部分
            int fragIdx = parameters.indexOf('#');
            if (fragIdx >= 0) {
                queryString = parameters.substring(0, fragIdx);
            }
            builder.append(queryString);
        }

        return builder.toString();
    }


    private static byte[] hmacsha256Signature(byte[] data, byte[] key) {
        try {
            SecretKeySpec signingKey = new SecretKeySpec(key, "HmacSHA256");
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(signingKey);
            return mac.doFinal(data);
        } catch (Exception e) {
            throw new RuntimeException("appKey is null");
        }
    }

    private static String signWithHmac(String strToSign, String accessKey) {
        byte[] crypto = new byte[0];
        try {
            crypto = hmacsha256Signature(strToSign.getBytes("utf-8"), accessKey.getBytes("utf-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        if (null == crypto) {
            return null;
        }
        String signature = Base64.encodeBase64String(crypto).trim();
        return signature;
    }
}