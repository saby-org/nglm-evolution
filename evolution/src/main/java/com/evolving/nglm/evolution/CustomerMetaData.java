package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.DeploymentManagedObject;
import com.evolving.nglm.core.JSONUtilities;

public class CustomerMetaData
{
  //
  //  data
  //
  
  private List<Metadata> generalDetailsMetadataList = new ArrayList<Metadata>();
  private List<Metadata> kpisMetaDataList = new ArrayList<Metadata>();
  
  //
  // accessor
  //
  
  public List<Metadata> getGeneralDetailsMetadata() { return generalDetailsMetadataList; }
  public List<Metadata> getKpiMetaData() { return kpisMetaDataList; }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public CustomerMetaData(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    JSONArray generalDetailsMetadata = JSONUtilities.decodeJSONArray(jsonRoot, "generalDetailsMetadata", true);
    for (int i=0; i<generalDetailsMetadata.size(); i++)
      {
        JSONObject generalDetailsMetadataJson = (JSONObject) generalDetailsMetadata.get(i);
        generalDetailsMetadataJson.put("guiPosition", i+1);
        generalDetailsMetadataList.add(new Metadata(generalDetailsMetadataJson));
      }
    
    JSONArray kpisMetaData = JSONUtilities.decodeJSONArray(jsonRoot, "kpisMetaData", true);
    for (int i=0; i<kpisMetaData.size(); i++)
      {
        JSONObject kpiMetadataJson = (JSONObject) kpisMetaData.get(i);
        kpiMetadataJson.put("guiPosition", i+1);
        kpisMetaDataList.add(new Metadata(kpiMetadataJson));
      }
  }
  
  /********************************
   * 
   * class Metadata
   *
   ********************************/
  
  public class Metadata extends DeploymentManagedObject
  {

    /*****************************************
    *
    *  constructor
    *
    *****************************************/
    
    public Metadata(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
    {
      super(jsonRoot);
    }
    
  }

}
