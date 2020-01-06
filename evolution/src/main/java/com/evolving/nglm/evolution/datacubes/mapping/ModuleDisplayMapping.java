package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Map;

import com.evolving.nglm.evolution.datacubes.mapping.ModuleInformation.ModuleFeature;

public class ModuleDisplayMapping extends DisplayMapping<ModuleInformation>
{
  public static final String ESIndex = "mapping_modules";
  
  public ModuleDisplayMapping() 
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
  
  public String getDisplay(String id)
  {
    ModuleInformation result = this.mapping.get(id);
    if(result != null)
      {
        return result.moduleDisplay;
      }
    else
      {
        logWarningOnlyOnce(id, "Unable to retrieve module.display and module.feature for module.id: " + id);
        return id; // When missing, return the ID by default.
      }
  }
  
  public ModuleFeature getFeature(String id)
  {
    ModuleInformation result = this.mapping.get(id);
    if(result != null)
      {
        return result.moduleFeature;
      }
    else
      {
        logWarningOnlyOnce(id, "Unable to retrieve module.display and module.feature for module.id: " + id);
        return ModuleFeature.None; // When missing, return None by default.
      }
  }
}
