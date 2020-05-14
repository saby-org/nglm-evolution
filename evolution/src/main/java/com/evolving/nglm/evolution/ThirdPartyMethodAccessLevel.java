/*****************************************************************************
*
*  ThirdPartyMethodAccessLevel.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.JSONUtilities;

public class ThirdPartyMethodAccessLevel
{
  
  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  private List<String> permissions;
  private boolean byPassAuth;
  
  //
  // accessors
  //
  
  public List<String> getPermissions() { return permissions; }
  public boolean isByPassAuth() { return byPassAuth; } 
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ThirdPartyMethodAccessLevel(JSONObject jsonRoot)
  {
    //
    //  permissions
    //
    
    JSONArray permissionsJSONArray = JSONUtilities.decodeJSONArray(jsonRoot, "permissions", true);
    List<String> configuredPermissions = new ArrayList<String>();
    for (int i=0; i<permissionsJSONArray.size(); i++)
      {
        configuredPermissions.add((String) permissionsJSONArray.get(i));
      }
    
    //
    //  set
    //
    
    this.permissions = configuredPermissions;
    this.byPassAuth = JSONUtilities.decodeBoolean(jsonRoot, "byPassAuth", Boolean.FALSE);
  }
  
  /*****************************************
  *
  *  toString
  *
  *****************************************/
  
  @Override public String toString()
  {
    return "ThirdPartyMethodAccessLevel [permissions=" + permissions + "]";
  }

}
