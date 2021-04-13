package com.evolving.nglm.evolution.elasticsearch;

import org.apache.http.HttpHost;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;

public class ElasticsearchConnectionSettings
{
  private String id;
  private HttpHost[] hosts;
  private String user;
  private String password;
  private int connectTimeout;
  private int queryTimeout;
  
  public ElasticsearchConnectionSettings(String id, HttpHost[] hosts, String user, String password, int connectTimeout, int queryTimeout) {
    this.id = id;
    this.hosts = hosts;
    this.user = user;
    this.password = password;
    this.connectTimeout = connectTimeout;
    this.queryTimeout = queryTimeout;
  }
  
  public ElasticsearchConnectionSettings(JSONObject jsonRoot) throws JSONUtilitiesException, IllegalArgumentException {
    this(
        JSONUtilities.decodeString(jsonRoot, "id", true),
        ElasticsearchClientAPI.parseServersConf(JSONUtilities.decodeString(jsonRoot, "hosts", true)),
        JSONUtilities.decodeString(jsonRoot, "user", true),
        JSONUtilities.decodeString(jsonRoot, "password", true),
        JSONUtilities.decodeInteger(jsonRoot, "connectTimeout", true),
        JSONUtilities.decodeInteger(jsonRoot, "queryTimeout", true)
    );
  }

  public String getId() { return id; }
  public HttpHost[] getHosts() { return hosts; }
  public String getUser() { return user; }
  public String getPassword() { return password; }
  public int getConnectTimeout() { return this.connectTimeout; }
  public int getQueryTimeout() { return this.queryTimeout; }

}
