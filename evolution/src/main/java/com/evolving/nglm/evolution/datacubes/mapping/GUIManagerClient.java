package com.evolving.nglm.evolution.datacubes.mapping;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GUIManagerClient
{
  protected static final Logger log = LoggerFactory.getLogger(GUIManagerClient.class);
  
  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  private String guiManagerHost;
  private String guiManagerPort;
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public GUIManagerClient(String guiManagerHost, String guiManagerPort) 
  {
    this.guiManagerHost = guiManagerHost;
    this.guiManagerPort = guiManagerPort;
  }
  
  /*****************************************
  *
  *  callGUIManager
  *
  *****************************************/

  public JSONObject call(String api, JSONObject requestJSON)
  {
    /*****************************************
    *
    *  call
    *
    *****************************************/

    HttpResponse httpResponse = null;
    try
      {
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost httpPost = new HttpPost("http://" + guiManagerHost + ":" + guiManagerPort + "/nglm-guimanager/" + api);
        httpPost.setEntity(new StringEntity(requestJSON.toString(), ContentType.create("application/json")));
        httpResponse = httpClient.execute(httpPost);
        if (httpResponse == null || httpResponse.getStatusLine() == null || httpResponse.getStatusLine().getStatusCode() != 200 || httpResponse.getEntity() == null)
          {
            log.error("Error processing GUIManager REST api: {}", httpResponse.toString());
            return null;
          }

      }
    catch (IOException e)
      {
        StringWriter stackTraceWriter = new StringWriter();
        log.error("Exception processing GUIManager REST api: {}", stackTraceWriter.toString());
        return null;
      }

    /*****************************************
    *
    *  process response
    *
    *****************************************/

    JSONObject jsonResponse = null;
    try
      {
        jsonResponse = (JSONObject) (new JSONParser()).parse(EntityUtils.toString(httpResponse.getEntity(), "UTF-8"));
      }
    catch (IOException|ParseException e)
      {
        StringWriter stackTraceWriter = new StringWriter();
        log.error("Exception processing GUIManager REST api: {}", stackTraceWriter.toString());
        return null;
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return jsonResponse;
  }
}
