package com.evolving.nglm.evolution;

import java.io.IOException;
import java.util.Date;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;

public class GUIManagerService
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private HttpClient httpClient;
  private String guiManagerEndpoint;
  
  public GUIManagerService(String guiManagerEndpoint)
  {
    /*****************************************
    *
    *  guiManagerEndpoint
    *
    *****************************************/

    this.guiManagerEndpoint = guiManagerEndpoint;
    this.guiManagerEndpoint = "dev-node01:7081"; // RAJ K

    /*****************************************
    *
    *  http
    *
    *****************************************/

    //
    //  default connections
    //

    PoolingHttpClientConnectionManager httpClientConnectionManager = new PoolingHttpClientConnectionManager();
    httpClientConnectionManager.setDefaultMaxPerRoute(Deployment.getEvolutionEngineStreamThreads());
    httpClientConnectionManager.setMaxTotal(Deployment.getEvolutionEngineInstanceNumbers()*Deployment.getEvolutionEngineStreamThreads());

    //
    //  httpClient
    //

    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    httpClientBuilder.setConnectionManager(httpClientConnectionManager);
    this.httpClient = httpClientBuilder.build();
  }
  
  /*****************************************
  *
  *  processPOSTRequest
  *
  *****************************************/
  
  public JSONObject processPOSTRequest(JSONObject requestJSON, String method)
  {
    JSONObject result = null;
    
    /*****************************************
    *
    *  timeout
    *
    *****************************************/

    Date now = SystemTime.getCurrentTime();
    Date timeout = RLMDateUtils.addSeconds(now, 10);
    
    /*****************************************
    *
    *   httpPost
    *
    *****************************************/
    
    HttpPost httpPost = new HttpPost("http://" + guiManagerEndpoint + "/nglm-guimanager/" + method);
    long waitTime = timeout.getTime() - now.getTime();
    httpPost.setEntity(new StringEntity(requestJSON.toString(), ContentType.create("application/json")));
    httpPost.setConfig(RequestConfig.custom().setConnectTimeout((int) (waitTime > 0 ? waitTime : 1)).build());
    
    try
      {
        HttpResponse httpResponse = httpClient.execute(httpPost);
        if (200 == httpResponse.getStatusLine().getStatusCode())
          {
            String responseString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
            result = (JSONObject) new JSONParser().parse(responseString);
          }
      } 
    catch (IOException | ParseException e)
      {
        e.printStackTrace();
      }
    return result;
  }

}
