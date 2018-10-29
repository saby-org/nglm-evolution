/*****************************************************************************
*
*  CriteriaAPI.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import org.apache.http.HttpHost;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

public class CriteriaAPI
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  private enum API
  {
    evaluateProfileCriteria;
  }

  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(CriteriaAPI.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private static final int RESTAPIVersion = 1;
  private HttpServer restServer;
  private RestHighLevelClient elasticsearch;

  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args) throws Exception
  {
    NGLMRuntime.initialize();
    CriteriaAPI criteriaAPI = new CriteriaAPI();
    criteriaAPI.start(args);
  }

  /****************************************
  *
  *  start
  *
  *****************************************/

  private void start(String[] args)
  {
    /*****************************************
    *
    *  configuration
    *
    *****************************************/

    int apiRestPort = parseInteger("apiRestPort", args[0]);
    String elasticsearchServerHost = args[1];
    int elasticsearchServerPort = parseInteger("elasticsearchServerPort", args[2]);

    //
    //  log
    //

    log.info("main START: {} {} {}", apiRestPort, elasticsearchServerHost, elasticsearchServerPort);

    /*****************************************
    *
    *  Elasticsearch -- client
    *
    *****************************************/

    try
      {
        elasticsearch = new RestHighLevelClient(RestClient.builder(new HttpHost(elasticsearchServerHost, elasticsearchServerPort, "http")));
      }
    catch (ElasticsearchException e)
      {
        throw new ServerRuntimeException("could not initialize elasticsearch client", e);
      }
    
    /*****************************************
    *
    *  REST interface -- server and handlers
    *
    *****************************************/

    try
      {
        InetSocketAddress addr = new InetSocketAddress(apiRestPort);
        restServer = HttpServer.create(addr, 0);
        restServer.createContext("/nglm-criteriaapi/evaluateProfileCriteria", new APIHandler(API.evaluateProfileCriteria));
        restServer.setExecutor(Executors.newFixedThreadPool(10));
        restServer.start();
      }
    catch (IOException e)
      {
        throw new ServerRuntimeException("could not initialize REST server", e);
      }
  }

  /*****************************************
  *
  *  parseInteger
  *
  *****************************************/

  private int parseInteger(String field, String stringValue)
  {
    int result = 0;
    try
      {
        result = Integer.parseInt(stringValue);
      }
    catch (NumberFormatException e)
      {
        throw new ServerRuntimeException("bad " + field + " argument", e);
      }
    return result;
  }

  /*****************************************
  *
  *  handle -- HttpHandler
  *
  *****************************************/

  private synchronized void handleAPI(API api, HttpExchange exchange) throws IOException
  {
    try
      {
        /*****************************************
        *
        *  get the body
        *
        *****************************************/

        StringBuilder requestBodyStringBuilder = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()));
        while (true)
          {
            String line = reader.readLine();
            if (line == null) break;
            requestBodyStringBuilder.append(line);
          }
        reader.close();
        log.debug("API (raw request): {} {}",api,requestBodyStringBuilder.toString());
        JSONObject jsonRoot = (JSONObject) (new JSONParser()).parse(requestBodyStringBuilder.toString());

        /*****************************************
        *
        *  validate
        *
        *****************************************/

        int apiVersion = JSONUtilities.decodeInteger(jsonRoot, "apiVersion", true);
        if (apiVersion > RESTAPIVersion)
          {
            throw new ServerRuntimeException("unknown api version " + apiVersion);
          }
        jsonRoot.remove("apiVersion");

        /*****************************************
        *
        *  process
        *
        *****************************************/

        JSONObject jsonResponse = null;
        switch (api)
          {
            case evaluateProfileCriteria:
              jsonResponse = processEvaluateProfileCriteria(jsonRoot);
              break;
          }
        
        //
        //  validate
        //

        if (jsonResponse == null)
          {
            throw new ServerException("no handler for " + api);
          }

        /*****************************************
        *
        *  send response
        *
        *****************************************/

        //
        //  standard response fields
        //

        jsonResponse.put("apiVersion", RESTAPIVersion);

        //
        //  log
        //

        log.debug("API (raw response): {}", jsonResponse.toString());

        //
        //  send
        //

        exchange.sendResponseHeaders(200, 0);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      }
    catch (org.json.simple.parser.ParseException | IOException | ServerException | RuntimeException e )
      {
        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error("Exception processing REST api: {}", stackTraceWriter.toString());

        //
        //  send error response
        //

        HashMap<String,Object> response = new HashMap<String,Object>();
        response.put("responseCode", "systemError");
        response.put("responseMessage", e.getMessage());
        JSONObject jsonResponse = JSONUtilities.encodeObject(response);
        exchange.sendResponseHeaders(200, 0);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      }
  }

  /*****************************************
  *
  *  processEvaluateProfileCriteria
  *
  *****************************************/

  private JSONObject processEvaluateProfileCriteria(JSONObject jsonRoot)
  {
    /****************************************
    *
    *  response
    *
    ****************************************/
    
    HashMap<String,Object> response = new HashMap<String,Object>();

    /*****************************************
    *
    *  parse
    *
    *****************************************/

    List<EvaluationCriterion> criteriaList = new ArrayList<EvaluationCriterion>();
    try
      {
        JSONArray jsonCriteriaList = JSONUtilities.decodeJSONArray(jsonRoot, "profileCriteria", true);
        for (int i=0; i<jsonCriteriaList.size(); i++)
          {
            criteriaList.add(new EvaluationCriterion((JSONObject) jsonCriteriaList.get(i), CriterionContext.Profile));
          }
      }
    catch (JSONUtilitiesException|GUIManagerException e)
      {
        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
        
        //
        //  response
        //

        response.put("responseCode", "argumentError");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  construct query
    *
    *****************************************/

    BoolQueryBuilder query;
    try
      {
        query = QueryBuilders.boolQuery();
        for (EvaluationCriterion evaluationCriterion : criteriaList)
          {
            query = query.filter(evaluationCriterion.esQuery());
          }
      }
    catch (CriterionException e)
      {
        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
        
        //
        //  response
        //

        response.put("responseCode", "argumentError");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  excecute query
    *
    *****************************************/

    long result;
    try
      {
        SearchRequest searchRequest = new SearchRequest("subscriberprofile").source(new SearchSourceBuilder().sort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).query(query).size(0));
        SearchResponse searchResponse = elasticsearch.search(searchRequest);
        result = searchResponse.getHits().getTotalHits();
      }
    catch (IOException e)
      {
        //
        //  log
        //

        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
        
        //
        //  response
        //

        response.put("responseCode", "systemError");
        response.put("responseMessage", e.getMessage());
        response.put("responseParameter", null);
        return JSONUtilities.encodeObject(response);
      }

    /*****************************************
    *
    *  response
    *
    *****************************************/

    response.put("result", result);
    return JSONUtilities.encodeObject(response);
  }

  /*****************************************
  *
  *  class APIHandler
  *
  *****************************************/

  private class APIHandler implements HttpHandler
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private API api;

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    private APIHandler(API api)
    {
      this.api = api;
    }

    /*****************************************
    *
    *  handle -- HttpHandler
    *
    *****************************************/

    public void handle(HttpExchange exchange) throws IOException
    {
      handleAPI(api, exchange);
    }
  }
}
