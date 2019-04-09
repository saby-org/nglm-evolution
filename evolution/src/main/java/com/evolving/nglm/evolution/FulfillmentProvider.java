/*****************************************************************************
*
*  FulfillmentProvider.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.json.simple.JSONObject;

import com.evolving.nglm.core.DeploymentManagedObject;
import com.evolving.nglm.core.JSONUtilities;

public class FulfillmentProvider extends DeploymentManagedObject
{

  private static final String PROVIDER_TYPE = "providerType";
  private static final String URL = "url";

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String providerType = "";
  private String url = "";

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public String getProviderID() { return getID(); }
  public String getProviderName() { return getName(); }
  public String getProviderType() { return providerType; }
  public String getUrl() { return url; }
  
  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public FulfillmentProvider(JSONObject jsonRoot)
  {
    super(jsonRoot);
    this.providerType = JSONUtilities.decodeString(jsonRoot, PROVIDER_TYPE, false);
    this.url = JSONUtilities.decodeString(jsonRoot, URL, false);
  }
  
  /*****************************************
  *
  *  toString
  *
  *****************************************/

  @Override
  public String toString() {
    return "FulfillmentProvider ["
        + "id=" + getProviderID() 
        + ", name=" + getProviderName()
        + ", providerType=" + getProviderType() 
        + ", url=" + getUrl() 
        + "]";
  }

}