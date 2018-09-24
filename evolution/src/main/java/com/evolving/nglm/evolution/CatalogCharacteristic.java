/*****************************************************************************
*
*  CatalogCharacteristic.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.DeploymentManagedObject;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;

import org.json.simple.JSONObject;

import java.util.Objects;

public class CatalogCharacteristic extends DeploymentManagedObject
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  CatalogCharacteristicType
  //

  public enum CatalogCharacteristicType
  {
    Unit("unit"),
    Text("text"),
    Choice("choice"),
    List("list"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private CatalogCharacteristicType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static CatalogCharacteristicType fromExternalRepresentation(String externalRepresentation) { for (CatalogCharacteristicType enumeratedValue : CatalogCharacteristicType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private CatalogCharacteristicType catalogCharacteristicType;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public CatalogCharacteristicType getCatalogCharacteristicType() { return catalogCharacteristicType; }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public CatalogCharacteristic(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    //
    //  super
    //
    
    super(jsonRoot);

    //
    //  data
    //

    this.catalogCharacteristicType = CatalogCharacteristicType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "type", true));
  }
}
