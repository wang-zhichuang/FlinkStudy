package RiverDict;

import feign.Request;
import feign.RequestInterceptor;
import feign.RequestTemplate;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class AuthFeiginInterceptor implements RequestInterceptor {
    public static final String AUTHORIZATION = "Authorization";
    public static final String TENANT = "tenant";
    public static final String DTUI_SHORT_AUTH = "DTUI_SHORT_AUTH";
    public static final String DTUI_USER = "DTUI_USER";

    private String accessKeyID;
    private String accessKeySecret;
    private String tenantName;
    private String username;

    public AuthFeiginInterceptor(String accessKeyID,String accessKeySecret,String tenant,String user){
        this.accessKeyID = accessKeyID;
        this.accessKeySecret = accessKeySecret;
        this.tenantName = tenant;
        this.username = user;
    }

    public static String encode(String input) {
        if (input == null || input.equals("")) {
            return input;
        }
        try {
            return URLEncoder.encode(input, "utf-8");
        } catch (UnsupportedEncodingException e) {
            return input;
        }
    }

    @Override
    public void apply(RequestTemplate requestTemplate) {
        Request request = requestTemplate.request();
        requestTemplate.header("Content-Type", "application/json;charset=UTF-8");
        requestTemplate.header("accept", "application/json,text/plain,text/html");
        requestTemplate.header("accept-language","zh-CN,zh;q=0.9");
        requestTemplate.header("DT_RIVER_AUTH_DATE", System.currentTimeMillis() + "");
        String utcTime = SignatureUtil.getUTCTime();
        String signature = SignatureUtil.getAuthorization(accessKeyID, accessKeySecret, SignatureUtil.getQueryParams(requestTemplate.queries()), request.method(), utcTime);

        String encodeUserName = encode(username);
        String encodeTenant = encode(tenantName);
        String encodeSignature =  encode(signature);

        requestTemplate.header(AUTHORIZATION, encodeSignature);
        requestTemplate.header(DTUI_SHORT_AUTH,  String.format("%s:%s:%s", "", encodeUserName, encodeSignature));
        requestTemplate.header(DTUI_USER, encodeUserName);
        requestTemplate.header(TENANT, encodeTenant);
    }
}
