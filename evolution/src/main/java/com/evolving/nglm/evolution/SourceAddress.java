package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class SourceAddress extends GUIManagedObject
{
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
    schemaBuilder.name("sourceaddress");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("communicationChannelId", Schema.STRING_SCHEMA);    
    schemaBuilder.field("isDefault", SchemaBuilder.bool().defaultValue(false).schema());
    schema = schemaBuilder.build();
  };
  
  //
  //  serde
  //

  private static ConnectSerde<SourceAddress> serde = new ConnectSerde<SourceAddress>(schema, false, SourceAddress.class, SourceAddress::pack, SourceAddress::unpack);
  
  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SourceAddress> serde() { return serde; }
  
  /****************************************
  *
  *  data
  *
  ****************************************/
  
  private String communicationChannelId;
  private boolean isDefault;
  
  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getSourceAddressId() { return getGUIManagedObjectID(); }
  public String getSourceAddressName() { return getGUIManagedObjectName(); }
  public String getSourceAddressDisplay() { return getGUIManagedObjectDisplay(); }
  public String getCommunicationChannelId() { return communicationChannelId; } 
  public  boolean getIsDefault() { return isDefault; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public SourceAddress(SchemaAndValue schemaAndValue,String communicationChannelId, boolean isDefault)
  {
    super(schemaAndValue);
    this.communicationChannelId = communicationChannelId;   
    this.isDefault = isDefault;
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SourceAddress sourceAddress = (SourceAddress) value;
    Struct struct = new Struct(schema);
    packCommon(struct, sourceAddress);
    struct.put("communicationChannelId", sourceAddress.getCommunicationChannelId());    
    struct.put("isDefault", sourceAddress.getIsDefault());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static SourceAddress unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String communicationChannelId = valueStruct.getString("communicationChannelId");
    boolean isDefault = valueStruct.getBoolean("isDefault");
    
    //
    //  return
    //

    return new SourceAddress(schemaAndValue, communicationChannelId, isDefault);
  }
  
  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public SourceAddress(JSONObject jsonRoot, long epoch, GUIManagedObject existingSourceAddressUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingSourceAddressUnchecked != null) ? existingSourceAddressUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingSourceAddress
    *
    *****************************************/

    SourceAddress existingSourceAddress = (existingSourceAddressUnchecked != null && existingSourceAddressUnchecked instanceof SourceAddress) ? (SourceAddress) existingSourceAddressUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.communicationChannelId = JSONUtilities.decodeString(jsonRoot, "communicationChannelId", true);
    this.isDefault = JSONUtilities.decodeBoolean(jsonRoot, "default", Boolean.FALSE);
    
    /*****************************************
    *
    *  validate
    *
    *****************************************/

    //
    //  no effective dates
    //
    
    if (getRawEffectiveStartDate() != null) throw new GUIManagerException("unsupported start date", JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", false));
    if (getRawEffectiveEndDate() != null) throw new GUIManagerException("unsupported end date", JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", false));

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingSourceAddress))
      {
        this.setEpoch(epoch);
      }
  }
  
  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(SourceAddress existingSourceAddress)
  {
    if (existingSourceAddress != null && existingSourceAddress.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingSourceAddress.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(communicationChannelId, existingSourceAddress.getCommunicationChannelId());       
        epochChanged = epochChanged || ! Objects.equals(isDefault, existingSourceAddress.getIsDefault());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
  
  /*****************************************
  *
  *  validate
  *
  *****************************************/
  
  public void validate(CommunicationChannelService communicationChannelService, Date date) throws GUIManagerException
  {
    /*****************************************
    *
    *  validate communication channel exist and is active
    *
    *****************************************/
    
    if (communicationChannelService.getActiveCommunicationChannel(communicationChannelId, date) == null) throw new GUIManagerException("unknown communicationChannel ", communicationChannelId);
  }
}
