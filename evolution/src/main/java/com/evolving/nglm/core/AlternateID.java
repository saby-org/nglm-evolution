/*****************************************************************************
*
*  AlternateID.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import org.json.simple.JSONObject;

/* IF COPY/PASTE FROM ANOTHER DeploymentManagedObject OBJECT
 * DO NOT FORGET TO ADD IT TO THE DeploymentManagedObject FACTORY
 * SEE DeploymentManagedObject.create */
public class AlternateID extends DeploymentManagedObject
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private boolean externalSubscriberID;
  private String subscriberManagerChangeLog;
  private boolean sharedID;
  private int redisCacheIndex;
  private Integer reverseRedisCacheIndex;
  private Integer redisCacheTTLOnDelete;
  private String esField;
  private String profileCriterionField;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public boolean getExternalSubscriberID() { return externalSubscriberID; }
  public String getRekeyedStreamTopic() { return "streams-subscribermanager-" + subscriberManagerChangeLog + "-rekeyed"; }
  public String getChangeLogKeyedBySubscriberID() { return subscriberManagerChangeLog + "-subscriberid"; }
  public String getChangeLogKeyedByAlternateID() { return subscriberManagerChangeLog + "-alternateid"; }
  public String getBackChannelKeyedBySubscriberIDTopic() { return "streams-subscribermanager-" + subscriberManagerChangeLog + "-backchannel-subscriberid"; }
  public String getBackChannelKeyedByAlternateIDTopic() { return "streams-subscribermanager-" + subscriberManagerChangeLog + "-backchannel-alternateid"; }
  public boolean getSharedID() { return sharedID; }
  public int getRedisCacheIndex() { return redisCacheIndex; }
  public Integer getReverseRedisCacheIndex() { return reverseRedisCacheIndex; }
  public Integer getRedisCacheTTLOnDelete() { return redisCacheTTLOnDelete; }
  public String getESField() { return esField; }
  public String getProfileCriterionField() { return profileCriterionField; }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public AlternateID(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
    this.externalSubscriberID = JSONUtilities.decodeBoolean(jsonRoot, "externalSubscriberID", Boolean.FALSE);
    this.subscriberManagerChangeLog = JSONUtilities.decodeString(jsonRoot, "subscriberManagerChangeLog", true);
    this.sharedID = JSONUtilities.decodeBoolean(jsonRoot, "sharedID", Boolean.FALSE);
    this.redisCacheIndex = JSONUtilities.decodeInteger(jsonRoot, "redisCacheIndex", true);
    this.reverseRedisCacheIndex = JSONUtilities.decodeInteger(jsonRoot, "reverseRedisCacheIndex", false);
    this.redisCacheTTLOnDelete = JSONUtilities.decodeInteger(jsonRoot, "redisCacheTTLOnDelete", false);
    this.esField = JSONUtilities.decodeString(jsonRoot, "esField", false);
    this.profileCriterionField = JSONUtilities.decodeString(jsonRoot, "profileCriterionField", false);
  }
}
