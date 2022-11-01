package com.evolving.nglm.evolution;

import java.util.LinkedHashMap;
import java.util.Map;

import org.json.simple.JSONObject;

import com.evolving.nglm.core.JSONUtilities;

public class RetentionConfiguration
{
  private String configurationKey;
  private String configurationDisplay;
  private String configurationValue;
  private String configurationDescription;
  
  public RetentionConfiguration(String configurationKey, String configurationDisplay, String configurationValue, String configurationDescription)
  {
    this.configurationKey = configurationKey;
    this.configurationDisplay = configurationDisplay;
    this.configurationValue = configurationValue;
    this.configurationDescription = configurationDescription;
  }
  
  public JSONObject getJSONPresentation()
  {
    Map<String, Object> result = new LinkedHashMap<String, Object>();
    result.put("configurationKey", configurationKey);
    result.put("configurationDisplay", configurationDisplay);
    result.put("configurationValue", configurationValue);
    result.put("configurationDescription", configurationDescription);
    return JSONUtilities.encodeObject(result);
  }

}
