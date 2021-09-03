/*****************************************************************************
*
*  JourneyStatistic.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.SubscriberStreamOutput;
import org.apache.kafka.connect.data.*;
import org.json.simple.JSONObject;

public class ExternalAPIOutput extends SubscriberStreamOutput
{
 
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String topicID;
  private JSONObject json;
  private String keyFieldFromJson;
  private String encoding;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getTopicID() { return topicID; }
  public JSONObject getJson() { return json; }
  public String getKeyFieldFromJson() { return keyFieldFromJson; }
  public String getEncoding() { return encoding; }

  /*****************************************
  *
  *  constructor simple
  *
  *****************************************/

  public ExternalAPIOutput(String topicID, JSONObject json, String keyFieldFromJson, String encoding)
  {
    this.topicID = topicID;
    this.json = json;
    this.keyFieldFromJson = keyFieldFromJson;
    this.encoding = encoding;
  }

  /*****************************************
  *
  *  constructor unpack
  *
  *****************************************/

  public ExternalAPIOutput(SchemaAndValue schemaAndValue, String topicID, JSONObject json, String keyFieldFromJson, String encoding)
  {
    super(schemaAndValue);
    this.topicID = topicID;
    this.json = json;
    this.keyFieldFromJson = keyFieldFromJson;
    this.encoding = encoding;
  }
}
