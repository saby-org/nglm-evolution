package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Map;

import com.evolving.nglm.evolution.datacubes.mapping.ModuleInformation.ModuleFeature;

public class ModulesMap extends ESObjectList<ModuleInformation>
{
  public static final String ESIndex = "mapping_modules";

  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public ModulesMap() 
  {
    super(ESIndex);
  }

  /*****************************************
  *
  * ESObjectList implementation
  *
  *****************************************/
  @Override
  protected void updateMapping(Map<String, Object> row)
  {
    this.mapping.put((String) row.get("moduleID"), new ModuleInformation((String) row.get("moduleName"), (String) row.get("moduleFeature")));
  }

  /*****************************************
  *
  * Getters
  *
  *****************************************/
  public String getDisplay(String id, String fieldName)
  {
    ModuleInformation result = this.mapping.get(id);
    if(result != null)
      {
        return result.moduleDisplay;
      }
    else
      {
        logWarningOnlyOnce("Unable to retrieve display for "+fieldName+" id: " + id);
        return id; // When missing, return default.
      }
  }
  
  public ModuleFeature getFeature(String id, String fieldName)
  {
    ModuleInformation result = this.mapping.get(id);
    if(result != null)
      {
        return result.moduleFeature;
      }
    else
      {
        logWarningOnlyOnce("Unable to retrieve feature for " + fieldName + " id: " + id);
        return ModuleFeature.None; // When missing, return default.
      }
  }
}
