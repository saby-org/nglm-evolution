package com.evolving.nglm.evolution.elasticsearch;

import org.json.simple.JSONObject;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;

public class ElasticsearchConnectionSettings
{
  private String id;
  private int connectTimeout;
  private int queryTimeout;
  
  public ElasticsearchConnectionSettings(String id, int connectTimeout, int queryTimeout) {
    this.id = id;
    this.connectTimeout = connectTimeout;
    this.queryTimeout = queryTimeout;
  }
  
  public ElasticsearchConnectionSettings(JSONObject jsonRoot) throws JSONUtilitiesException {
    this(
        JSONUtilities.decodeString(jsonRoot, "id", true),
        JSONUtilities.decodeInteger(jsonRoot, "connectTimeout", true),
        JSONUtilities.decodeInteger(jsonRoot, "queryTimeout", true)
    );
  }

  public String getId() { return id; }
  public int getConnectTimeout() { return this.connectTimeout; }
  public int getQueryTimeout() { return this.queryTimeout; }

}
