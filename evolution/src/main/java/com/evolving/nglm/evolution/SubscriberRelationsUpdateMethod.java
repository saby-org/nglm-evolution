/*****************************************************************************
*
*  SubscriberHierarchyUpdate.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

public enum SubscriberRelationsUpdateMethod
{
  SetParent("set_parent"),
  AddChild("add_child"),
  RemoveChild("remove_child"),
  Unknown("(unknown)");
  private String externalRepresentation;
  private SubscriberRelationsUpdateMethod(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
  public String getExternalRepresentation() { return externalRepresentation; }
  public static SubscriberRelationsUpdateMethod fromExternalRepresentation(String externalRepresentation) 
  { 
    for (SubscriberRelationsUpdateMethod enumeratedValue : SubscriberRelationsUpdateMethod.values()) 
      { 
        if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation))
          return enumeratedValue; 
      } 
    return Unknown; 
  }
}