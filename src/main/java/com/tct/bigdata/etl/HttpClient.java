package com.tct.bigdata.etl;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import static org.apache.http.entity.ContentType.APPLICATION_JSON;

/**
 * @author yuanhang.liu@tcl.com
 * @description HttpClient
 * @date 2020-02-26 17:39
 **/
public class HttpClient {

    private static final Integer DEFAULT_CONNECT_TIMEOUT = 10000;
    private static final Integer DEFAULT_SOCKET_TIMEOUT = 10000;
    private static final Integer DEFAULT_REQUEST_TIMEOUT = 10000;

    public static void defaultRequestConfig(HttpRequestBase request){
        RequestConfig.Builder configBuilder = RequestConfig.custom();
        configBuilder.setConnectTimeout(DEFAULT_CONNECT_TIMEOUT)
                .setSocketTimeout(DEFAULT_SOCKET_TIMEOUT)
                .setConnectionRequestTimeout(DEFAULT_REQUEST_TIMEOUT);
        request.setConfig(configBuilder.build());
    }

    public static void assembleHeader(HttpRequestBase request, Properties headers){
        if (headers != null){
            Set<String> names = headers.stringPropertyNames();
            for (String name : names){
                request.addHeader(name, headers.getProperty(name));
            }
        }
    }

    public static CloseableHttpResponse get(String url, Properties headers) throws Exception{
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet get = new HttpGet(url);
        defaultRequestConfig(get);
        assembleHeader(get, headers);
        return httpclient.execute(get);
    }

    public static CloseableHttpResponse post(String url, String json, Properties headers) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpPost post = new HttpPost(url);
        defaultRequestConfig(post);
        assembleHeader(post, headers);
        if (StringUtil.isNotBlank(json)){
            HttpEntity httpEntity = new StringEntity(json, APPLICATION_JSON);
            post.setEntity(httpEntity);
        }
        return httpclient.execute(post);
    }

    public static CloseableHttpResponse put(String url, String json, Properties headers) throws Exception {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpPut put = new HttpPut(url);
        defaultRequestConfig(put);
        assembleHeader(put, headers);
        if (StringUtil.isNotBlank(json)){
            HttpEntity httpEntity = new StringEntity(json, APPLICATION_JSON);
            put.setEntity(httpEntity);
        }
        return httpclient.execute(put);
    }
}
