package com.tct.bigdata.etl;


import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

/**
 * @author yuanhang.liu@tcl.com
 * @description
 * @date 2020-02-25 10:12
 **/
public class KylinRest {

    private static String encoding="QURNSU46S1lMSU4=";
    protected static final String baseURL = "http://127.0.0.1:7070/kylin/api";
    protected static final String METHOD_GET = "GET";
    protected static final String METHOD_POST = "POST";
    protected static final String METHOD_PUT = "PUT";

    protected static final String BUILD_TYPE_BUILD = "BUILD";
    protected static final String BUILD_TYPE_MERGE = "MERGE";
    protected static final String BUILD_TYPE_REFRESH = "REFRESH";

    protected static final String SEGMENT_STATUS_REDAY = "READY";
    protected static final String SEGMENT_STATUS_NEW = "NEW";


    protected static final String KYLIN_USERNAME = "ADMIN";
    protected static final String KYLIN_PASSWORD = "KYLIN";


    private static final Gson gson = new Gson();

    private static Properties getDefaultHeader(){
        Properties headers = new Properties();
        headers.put("Content-Type", "application/json");
        headers.put("Authorization", "Basic " + getAuthorization());
        return headers;
    }

    private static String getReponseContent(CloseableHttpResponse response) throws Exception{
        String result = null;
        if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK){
            result = IOUtils.toString(response.getEntity().getContent());
        }
        response.close();
        return result;
    }


    public static String login() throws Exception{
        String url = baseURL + "/user/authentication";
        Properties headers = getDefaultHeader();
        CloseableHttpResponse response = HttpClient.post(url, null, headers);
        return getReponseContent(response);
    }

    public static String getAuthorization(){
        byte[] key = (KYLIN_USERNAME+":"+KYLIN_PASSWORD).getBytes();
        return new sun.misc.BASE64Encoder().encode(key);
    }


    public static String getCubeDes(String cubeName) throws Exception{
        String url = baseURL + "/cube_desc/"+cubeName;
        Properties headers = getDefaultHeader();
        CloseableHttpResponse response = HttpClient.get(url, headers);
        return getReponseContent(response);
    }

    /**
     * 获取cube信息
     * @param cubeName
     * @return
     * @throws Exception
     */
    public static CubeDes getCube(String cubeName) throws Exception{
        CubeDes cubeDes = null;
        Properties headers = getDefaultHeader();
        String url = baseURL + "/cubes/"+cubeName;
        CloseableHttpResponse response = HttpClient.get(url, headers);
        if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK){
            cubeDes = gson.fromJson(IOUtils.toString(response.getEntity().getContent()), CubeDes.class);
        }
        response.close();

        return cubeDes;
    }

    public static String refreshSegment(String cubeName, Long startTime, Long endTime) throws Exception{

        String url = baseURL + "/cubes/" + cubeName +"/build";
        JsonObject jsonObject= new JsonObject();
        jsonObject.addProperty("startTime", startTime);
        jsonObject.addProperty("endTime", endTime);
        jsonObject.addProperty("buildType", BUILD_TYPE_REFRESH);
        String json = gson.toJson(jsonObject);
        Properties headers = getDefaultHeader();
        CloseableHttpResponse response = HttpClient.put(url, json,headers);
        return getReponseContent(response);
    }

    private  static String excute(String para,String method,String body, Properties requestProps){

        StringBuilder out = new StringBuilder();
        try {
            URL url = new URL(baseURL+para);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod(method);
            connection.setDoOutput(true);
            connection.setRequestProperty  ("Content-Type", "application/json");
            connection.setRequestProperty  ("Authorization", "Basic " + encoding);

            if (requestProps != null){
                Set<String> names = requestProps.stringPropertyNames();
                Iterator<String> iterator = names.iterator();
                while (iterator.hasNext()){
                    String propertiesName = iterator.next();
                    connection.setRequestProperty(propertiesName, requestProps.getProperty(propertiesName));
                }
            }

            if(body !=null){
                byte[] outputInBytes = body.getBytes("UTF-8");
                OutputStream os = connection.getOutputStream();
                os.write(outputInBytes);
                os.close();
            }
            InputStream content = (InputStream)connection.getInputStream();
            BufferedReader in  = new BufferedReader (new InputStreamReader(content));
            String line;
            while ((line = in.readLine()) != null) {
                out.append(line);
            }
            in.close();
            connection.disconnect();

        } catch(Exception e) {
            e.printStackTrace();
        }
        return out.toString();
    }

    public static void main(String[] args)throws Exception{

        login();

        String cubeName = "app_active_count";
        CubeDes cubeDes = getCube(cubeName);

        List<Segment> segments = cubeDes.getSegments();

        Segment candidateSegment = null;

        for (int i = segments.size() - 1; i > 0; i--){
            Segment segment = segments.get(i);
            String status = segment.getStatus();
            if (SEGMENT_STATUS_REDAY.equalsIgnoreCase(status)){
                candidateSegment = segment;
                break;
            }
        }

        if (null != candidateSegment){
            refreshSegment(cubeName, candidateSegment.getDate_range_start(), candidateSegment.getDate_range_end());
        }

    }



}
