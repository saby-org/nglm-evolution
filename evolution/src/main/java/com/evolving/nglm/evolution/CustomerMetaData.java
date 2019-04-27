package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.DeploymentManagedObject;
import com.evolving.nglm.core.JSONUtilities;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;

public class CustomerMetaData
{
  //
  //  data
  //
  
  private List<MetaData> generalDetailsMetaDataList = new ArrayList<MetaData>();
  private List<MetaData> kpisMetaDataList = new ArrayList<MetaData>();
  
  //
  // accessor
  //
  
  public List<MetaData> getGeneralDetailsMetaData() { return generalDetailsMetaDataList; }
  public List<MetaData> getKpiMetaData() { return kpisMetaDataList; }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public CustomerMetaData(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    JSONArray generalDetailsMetaData = JSONUtilities.decodeJSONArray(jsonRoot, "generalDetailsMetaData", true);
    for (int i=0; i<generalDetailsMetaData.size(); i++)
      {
        JSONObject generalDetailsMetaDataJson = (JSONObject) generalDetailsMetaData.get(i);
        generalDetailsMetaDataJson.put("guiPosition", i+1);
        generalDetailsMetaDataList.add(new MetaData(generalDetailsMetaDataJson));
      }
    
    JSONArray kpisMetaData = JSONUtilities.decodeJSONArray(jsonRoot, "kpisMetaData", true);
    for (int i=0; i<kpisMetaData.size(); i++)
      {
        JSONObject kpiMetaDataJson = (JSONObject) kpisMetaData.get(i);
        kpiMetaDataJson.put("guiPosition", i+1);
        kpisMetaDataList.add(new MetaData(kpiMetaDataJson));
      }
  }
  
  /********************************
  * 
  *  class MetaData
  *
  ********************************/
  
  public class MetaData extends DeploymentManagedObject
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String name;
    private CriterionDataType dataType;
    private boolean editable;
    private boolean alternateID;

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public String getName() { return name; }
    public CriterionDataType getDataType() { return dataType; }
    public boolean getEditable() { return editable; }
    public boolean getAlternateID() { return alternateID; }
    
    /*****************************************
    *
    *  constructor
    *
    *****************************************/
    
    public MetaData(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
    {
      super(jsonRoot);
      this.name = JSONUtilities.decodeString(jsonRoot, "name", true);
      this.dataType = CriterionDataType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "dataType", true));
      this.editable = JSONUtilities.decodeBoolean(jsonRoot, "editable", Boolean.FALSE);
      this.alternateID = JSONUtilities.decodeBoolean(jsonRoot, "alternateID", Boolean.FALSE);
    }
  }
}
