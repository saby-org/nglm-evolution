package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Map;

import com.evolving.nglm.evolution.datacubes.mapping.ModuleInformation.ModuleFeature;

public class ModulesMap extends ESObjectList<ModuleInformation>
{
  public static final String ESIndex = "mapping_modules";
  
  public ModulesMap() 
  {
    super(ESIndex);
  }

  @Override
  protected void updateMapping(Map<String, Object> row)
  {
    this.mapping.put((String) row.get("moduleID"), new ModuleInformation((String) row.get("moduleName"), (String) row.get("moduleFeature")));
  }

  /*****************************************
  *
  *  getters
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
        logWarningOnlyOnce("Unable to retrieve "+fieldName+".display for "+fieldName+".id: " + id);
        return id; // When missing, return the ID by default.
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
        logWarningOnlyOnce("Unable to retrieve "+fieldName+".feature for "+fieldName+".id: " + id);
        return ModuleFeature.None; // When missing, return None by default.
      }
  }
}
