package com.evolving.nglm.evolution.elasticsearch;

import org.json.simple.JSONObject;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;

public class ElasticsearchConnectionSettings
{
  private int connectTimeout;
  private int queryTimeout;
  
  public ElasticsearchConnectionSettings(int connectTimeout, int queryTimeout) {
    this.connectTimeout = connectTimeout;
    this.queryTimeout = queryTimeout;
  }
  
  public ElasticsearchConnectionSettings(JSONObject jsonRoot) throws JSONUtilitiesException {
    this(JSONUtilities.decodeInteger(jsonRoot, "connectTimeout", true),
        JSONUtilities.decodeInteger(jsonRoot, "queryTimeout", true));
  }
  
  public int getConnectTimeout() { return this.connectTimeout; }
  public int getQueryTimeout() { return this.queryTimeout; }
}
