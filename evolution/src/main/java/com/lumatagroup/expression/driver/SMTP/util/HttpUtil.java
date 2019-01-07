package com.lumatagroup.expression.driver.SMTP.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

public class HttpUtil {
	private static Logger logger = Logger.getLogger(HttpUtil.class);
	
	public static String doGet(String url, String callingMethod, Date from, Date to) {
		if (logger.isDebugEnabled()) logger.debug("HttpUtil.doGet() start " + url + " " + from + " " + to);
		CloseableHttpClient httpClient = null;
		HttpGet getRequest = null;
		String jsonResponse = null;
		CloseableHttpResponse response = null;
		int httpTimeout = Conf.getHttpRequestTimeoutSecs()*1000;
		if (logger.isDebugEnabled()) logger.debug("HttpUtil.doGet() httpTimeoutVal = " + httpTimeout);
		RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(httpTimeout).setSocketTimeout(httpTimeout).setConnectionRequestTimeout(httpTimeout).build();
		httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
		String urlWithParams = SMTPUtil.createRequestUrl(url, from, to);
		if (logger.isTraceEnabled()) logger.trace("HTTPUtil request url for "+callingMethod+": "+urlWithParams);
		getRequest = new HttpGet(urlWithParams);
		try {
			response = httpClient.execute(getRequest);
			jsonResponse = EntityUtils.toString(response.getEntity());
		} catch (ClientProtocolException e) {
			logger.error("ClientProtocolException occured in HttpUtil.doGet(): "+e.getMessage());
		} catch (IOException e) {
			logger.error("IOException occured in HttpUtil.doGet(): "+e.getMessage());
		} finally {
			if (response != null) {
					try {
						response.close();
					} catch (IOException e) {
						logger.error("IOException occured in CloseableHttpResponse.close(): "+e.getMessage());
					}
			}
		}
		if (logger.isTraceEnabled()) logger.trace("HTTPUtil response string for "+callingMethod+": "+jsonResponse.replaceAll(".{200}(?=.)", "$0\n"));
		return jsonResponse;
	}

	// Issue here as we don't receive apiKey (IS THIS METHOD REALLY CALLED ??)
	public static String doGet(String url,String callingMethod, int httpTimeoutVal) {
		return doGet(url, callingMethod, null, null);
	}
	
	public static InputStream doPost(String url,HttpParams params){
		HttpClient httpClient = null;
		InputStream respStream = null;
		StringBuffer respString = null;
		try{
			httpClient = HttpClientBuilder.create().build();
			HttpPost postRequest = new HttpPost(url);
			postRequest.setParams(params);
			HttpResponse response = httpClient.execute(postRequest);
			if (response!=null && response.getStatusLine().getStatusCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());
			}
			if (response!=null && response.getEntity() != null) {
				try{
					respStream = response.getEntity().getContent();
					if(respStream != null){
						BufferedReader respbuffer = new BufferedReader(new InputStreamReader(respStream));
						respString = new StringBuffer();
						String output;
						while ((output = respbuffer.readLine()) != null) {
							respString.append(output);
						}
						logger.debug("DM RestAPI response for getStillToBeSentMessagesExcludeModule  " + respString);
					}
				}catch (RuntimeException runtimeException) {
					/* In case of an unexpected exception you may want to abort the HTTP request in order to shut down the underlying
		           connection immediately.*/
					logger.error("SMTPDriver: Third Party HTTP request API HttpUtil doPost error : "+runtimeException.getMessage());
					postRequest.abort();
					runtimeException.printStackTrace();
					try {
						respStream.close();
					} catch (Exception ignore) {}
				}
			}

		} catch (ClientProtocolException e) {
			e.printStackTrace();

		} catch (IOException e) {
			e.printStackTrace();
			
		}finally{
			if(httpClient!=null && httpClient.getConnectionManager()!=null)
				httpClient.getConnectionManager().shutdown();
		}
		return respStream;
	}

}