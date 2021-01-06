/*****************************************************************************
*
*  UCGRule.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import org.apache.kafka.connect.data.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.*;
import java.util.Date;

public class UCGRule extends GUIManagedObject
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum UCGRuleCalculationType
  {
    Percent("percent", "PERCENT"),
    Numeric("numeric", "NUMERIC"),
    File("file", "FILE"),
    Unknown("(unknown)", "(unknown)");
    private String externalRepresentation;
    private String guiRepresentation;
    private UCGRuleCalculationType(String externalRepresentation, String guiRepresentation) { this.externalRepresentation = externalRepresentation; this.guiRepresentation = guiRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public String getGUIRepresentation() { return guiRepresentation; }
    public static UCGRuleCalculationType fromExternalRepresentation(String externalRepresentation) { for (UCGRuleCalculationType enumeratedValue : UCGRuleCalculationType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
    public static UCGRuleCalculationType fromGUIRepresentation(String guiRepresentation) { for (UCGRuleCalculationType enumeratedValue : UCGRuleCalculationType.values()) { if (enumeratedValue.getGUIRepresentation().equalsIgnoreCase(guiRepresentation)) return enumeratedValue; } return Unknown; }
  }

  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("ucgrule");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("selectedDimensions", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schemaBuilder.field("calculationType", Schema.STRING_SCHEMA);
    schemaBuilder.field("size",Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("percentageOfRefresh",Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("noOfDaysForStayOut",Schema.INT32_SCHEMA);
    schemaBuilder.field("refreshEpoch",Schema.OPTIONAL_INT32_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<UCGRule> serde = new ConnectSerde<UCGRule>(schema, false, UCGRule.class, UCGRule::pack, UCGRule::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<UCGRule> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private List<String> selectedDimensions;
  private UCGRuleCalculationType calculationType;
  private Integer size;
  private Integer percentageOfRefresh;
  private int noOfDaysForStayOut;
  private Integer refreshEpoch;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getUCGRuleID() { return getGUIManagedObjectID(); }
  public String getUCGRuleName() { return getGUIManagedObjectName(); }
  public List<String> getSelectedDimensions() { return selectedDimensions; }
  public UCGRuleCalculationType getCalculationType() {return calculationType; }
  public Integer getSize(){return size; }
  public Integer getPercentageOfRefresh(){return percentageOfRefresh; }
  public int getNoOfDaysForStayOut() {return noOfDaysForStayOut; }
  public Integer getRefreshEpoch() { return refreshEpoch; }

  /*****************************************
  *
  *  setters
  *
  *****************************************/

  public void setRefreshEpoch(int refreshEpoch) { this.refreshEpoch = refreshEpoch; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public UCGRule(SchemaAndValue schemaAndValue, List<String> selectedDimensions, UCGRuleCalculationType calculationType, Integer size, Integer percentageOfRefresh, int noOfDaysForStayOut, Integer refreshEpoch)
  {
    super(schemaAndValue);
    this.selectedDimensions = selectedDimensions;
    this.calculationType = calculationType;
    this.size = size;
    this.percentageOfRefresh = percentageOfRefresh;
    this.noOfDaysForStayOut = noOfDaysForStayOut;
    this.refreshEpoch = refreshEpoch;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    UCGRule ucgRule = (UCGRule) value;
    Struct struct = new Struct(schema);
    packCommon(struct, ucgRule);
    struct.put("selectedDimensions", ucgRule.getSelectedDimensions());
    struct.put("calculationType", ucgRule.getCalculationType().getExternalRepresentation());
    struct.put("size", ucgRule.getSize());
    struct.put("percentageOfRefresh", ucgRule.getPercentageOfRefresh());
    struct.put("noOfDaysForStayOut", ucgRule.getNoOfDaysForStayOut());
    struct.put("refreshEpoch", ucgRule.getRefreshEpoch());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static UCGRule unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    List<String> selectedDimensions = (List<String>) valueStruct.get("selectedDimensions");
    UCGRuleCalculationType calculationType = UCGRuleCalculationType.fromExternalRepresentation(valueStruct.getString("calculationType"));
    Integer size = valueStruct.getInt32("size");
    Integer percentageOfRefresh = valueStruct.getInt32("percentageOfRefresh");
    int noOfDaysForStayOut = valueStruct.getInt32("noOfDaysForStayOut");
    Integer refreshEpoch = valueStruct.getInt32("refreshEpoch");

    //
    //  return
    //

    return new UCGRule(schemaAndValue, selectedDimensions, calculationType, size, percentageOfRefresh, noOfDaysForStayOut, refreshEpoch);
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public UCGRule(JSONObject jsonRoot, long epoch, GUIManagedObject existingUCGRuleUnchecked, int tenantID) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingUCGRuleUnchecked != null) ? existingUCGRuleUnchecked.getEpoch() : epoch, tenantID);

    /*****************************************
    *
    *  existingUCGRule
    *
    *****************************************/

    UCGRule existingUCGRule = (existingUCGRuleUnchecked != null && existingUCGRuleUnchecked instanceof UCGRule) ? (UCGRule) existingUCGRuleUnchecked : null;

    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.selectedDimensions = decodeSelectedDimensions(JSONUtilities.decodeJSONArray(jsonRoot, "selectedDimensions", true));
    this.calculationType = UCGRuleCalculationType.fromGUIRepresentation(JSONUtilities.decodeString(jsonRoot,"calculationType",true));
    this.size = JSONUtilities.decodeInteger(jsonRoot,"size",false);
    this.percentageOfRefresh = JSONUtilities.decodeInteger(jsonRoot,"percentageOfRefresh",false);
    this.noOfDaysForStayOut = JSONUtilities.decodeInteger(jsonRoot,"noOfDaysForStayOut",true);
    this.refreshEpoch = null;

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingUCGRule))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  decodeCatalogCharacteristics
  *
  *****************************************/

  private List<String> decodeSelectedDimensions(JSONArray jsonArray) throws GUIManagerException
  {
    List<String> selectedDimensions = new ArrayList<String>() ;
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject selectedDimensionJSON = (JSONObject) jsonArray.get(i);
        selectedDimensions.add(JSONUtilities.decodeString(selectedDimensionJSON, "selectedDimensionID", true));
      }
    return selectedDimensions;
  }

  /*****************************************
  *
  *  validate
  *
  *****************************************/

  public void validate(UCGRuleService ucgRuleService, SegmentationDimensionService segmentationDimensionService, Date date) throws GUIManagerException
  {
    /*****************************************
    *
    *  segmentationDimensions
    *   
    *****************************************/

    for (String segmentationDimensionID : selectedDimensions)
      {
        /*****************************************
        *
        *  retrieve segmentationDimension
        *
        *****************************************/

        SegmentationDimension segmentationDimension = segmentationDimensionService.getActiveSegmentationDimension(segmentationDimensionID, date);
        
        /*****************************************
        *
        *  validate the segmentationDimenstion exists and is active
        *
        *****************************************/

        if (segmentationDimension == null)
          {
            log.info("ucgRule {} uses unknown segmentation dimension: {}", getUCGRuleID(), segmentationDimensionID);
            throw new GUIManagerException("unknown segmentation dimension", segmentationDimensionID);
          }
      }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(UCGRule ucgRule)
  {
    if (ucgRule != null && ucgRule.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), ucgRule.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(selectedDimensions, ucgRule.getSelectedDimensions());
        epochChanged = epochChanged || ! Objects.equals(calculationType, ucgRule.getCalculationType());
        epochChanged = epochChanged || ! Objects.equals(size, ucgRule.getSize());
        epochChanged = epochChanged || ! Objects.equals(percentageOfRefresh, ucgRule.getPercentageOfRefresh());
        epochChanged = epochChanged || ! Objects.equals(noOfDaysForStayOut, ucgRule.getNoOfDaysForStayOut());
        epochChanged = epochChanged || ! Objects.equals(refreshEpoch, ucgRule.getRefreshEpoch());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
}
