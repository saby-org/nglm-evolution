package com.evolving.nglm.core;

import org.json.simple.JSONObject;

import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;

public class ConnectTaskConfiguration
{
  private int initialWait;
  private int retries;
  
  public ConnectTaskConfiguration(int initialWait, int retries) {
    this.initialWait = initialWait;
    this.retries = retries;
  }
  
  public ConnectTaskConfiguration(JSONObject jsonRoot) throws JSONUtilitiesException {
    this(JSONUtilities.decodeInteger(jsonRoot, "initialWait", true),
        JSONUtilities.decodeInteger(jsonRoot, "retries", true));
  }
  
  public int getInitialWait() { return this.initialWait; }
  public int getRetries() { return this.retries; }
}