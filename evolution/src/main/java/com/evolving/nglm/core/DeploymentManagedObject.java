/*****************************************************************************
*
*  DeploymentManagedObject.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import org.json.simple.JSONObject;

import java.util.Objects;

public abstract class DeploymentManagedObject
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private JSONObject jsonRepresentation;
  private String id;
  private String name;
  private String display;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public JSONObject getJSONRepresentation() { return jsonRepresentation; }
  public String getID() { return id; }
  public String getName() { return name; }
  public String getDisplay() { return display; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DeploymentManagedObject(JSONObject jsonRoot)
  {
    this.jsonRepresentation = jsonRoot;
    this.id = JSONUtilities.decodeString(jsonRoot, "id", true);
    this.name = JSONUtilities.decodeString(jsonRoot, "name", false);
    this.display = JSONUtilities.decodeString(jsonRoot, "display", false);
    if (this.name == null)
      {
        this.name = this.id;
        this.jsonRepresentation.put("name", this.name);
      }
  }

  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof DeploymentManagedObject)
      {
        DeploymentManagedObject deploymentManagedObject = (DeploymentManagedObject) obj;
        result = true;
        result = result && Objects.equals(jsonRepresentation, deploymentManagedObject.getJSONRepresentation());
      }
    return result;
  }

  /*****************************************
  *
  *  hashCode
  *
  *****************************************/

  public int hashCode()
  {
    return jsonRepresentation.hashCode();
  }
}
