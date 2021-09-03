/*****************************************************************************
*
*  ExternalAPITopic.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.json.simple.JSONObject;

import com.evolving.nglm.core.DeploymentManagedObject;
import com.evolving.nglm.core.JSONUtilities;

/* IF COPY/PASTE FROM ANOTHER DeploymentManagedObject OBJECT
 * DO NOT FORGET TO ADD IT TO THE DeploymentManagedObject FACTORY
 * SEE DeploymentManagedObject.create */
public class ExternalAPITopic extends DeploymentManagedObject
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String keyFieldFromJson = null;
  private String encoding = null;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ExternalAPITopic(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    super(jsonRoot);
    keyFieldFromJson = JSONUtilities.decodeString(jsonRoot, "keyfieldfromjson", false);
    encoding = JSONUtilities.decodeString(jsonRoot, "encoding", false);    
  }

  public String getKeyFieldFromJson()
  {
    return keyFieldFromJson;
  }

  public void setKeyFieldFromJson(String keyFieldFromJson)
  {
    this.keyFieldFromJson = keyFieldFromJson;
  }

  public String getEncoding()
  {
    return encoding;
  }

  public void setEncoding(String encoding)
  {
    this.encoding = encoding;
  }  
}
