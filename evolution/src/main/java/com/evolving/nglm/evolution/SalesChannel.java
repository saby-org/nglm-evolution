/*****************************************************************************
*
*  SalesChannel.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import com.evolving.nglm.core.JSONUtilities;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class SalesChannel extends GUIManagedObject
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
    schemaBuilder.name("sales_channel");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),2));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("callingChannelIDs", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schemaBuilder.field("partnerIDs", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<SalesChannel> serde = new ConnectSerde<SalesChannel>(schema, false, SalesChannel.class, SalesChannel::pack, SalesChannel::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SalesChannel> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private List<String> callingChannelIDs;
  private List<String> partnerIDs;
  
  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getSalesChannelID() { return getGUIManagedObjectID(); }
  public String getSalesChannelName() { return getGUIManagedObjectName(); }
  public List<String> getCallingChannelIDs() { return callingChannelIDs; }
  public List<String> getPartnerIDs() { return partnerIDs; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public SalesChannel(SchemaAndValue schemaAndValue, List<String> callingChannelIDs, List<String> partnerIDs)
  {
    super(schemaAndValue);
    this.callingChannelIDs = callingChannelIDs;
    this.partnerIDs = partnerIDs;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    SalesChannel salesChannel = (SalesChannel) value;
    Struct struct = new Struct(schema);
    packCommon(struct, salesChannel);
    struct.put("callingChannelIDs", salesChannel.getCallingChannelIDs());
    struct.put("partnerIDs", salesChannel.getPartnerIDs());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static SalesChannel unpack(SchemaAndValue schemaAndValue)
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
    List<String> callingChannelIDs = (List<String>) valueStruct.get("callingChannelIDs");
    List<String> partnerIDs = (List<String>) valueStruct.get("partnerIDs");
    //
    //  return
    //

    return new SalesChannel(schemaAndValue, callingChannelIDs, partnerIDs);
  }
  
  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public SalesChannel(JSONObject jsonRoot, long epoch, GUIManagedObject existingSalesChannelUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingSalesChannelUnchecked != null) ? existingSalesChannelUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingSalesChannel
    *
    *****************************************/

    SalesChannel existingSalesChannel = (existingSalesChannelUnchecked != null && existingSalesChannelUnchecked instanceof SalesChannel) ? (SalesChannel) existingSalesChannelUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.callingChannelIDs = decodeIDs(JSONUtilities.decodeJSONArray(jsonRoot, "callingChannelIDs", true));
    this.partnerIDs = decodeIDs(JSONUtilities.decodeJSONArray(jsonRoot, "partnerIDs", false));
    
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

    //
    //  unique calling channels
    //

    Set<String> uniqueCallingChannels = new HashSet<String>();
    for (String callingChannelID : callingChannelIDs)
      {
        if (! uniqueCallingChannels.add(callingChannelID)) throw new GUIManagerException("calling channel specified multiple times", callingChannelID);
      }

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingSalesChannel))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  constructor -- JSON without context -- for externals read-only (such as datacubes & reports)
  *
  *****************************************/

  public SalesChannel(JSONObject jsonRoot) throws GUIManagerException
  {
    this(jsonRoot, 0, null);
  }

  /*****************************************
  *
  *  decodeIDs
  *
  *****************************************/

  private List<String> decodeIDs(JSONArray jsonArray)
  {
    List<String> IDs = null;
    if (jsonArray != null)
      {
        IDs = new ArrayList<String>();
        for (int i=0; i<jsonArray.size(); i++)
          {
            String ID = (String) jsonArray.get(i);
            IDs.add(ID);
          }
      }
    return IDs;
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(SalesChannel existingSalesChannel)
  {
    if (existingSalesChannel != null && existingSalesChannel.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingSalesChannel.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(callingChannelIDs, existingSalesChannel.getCallingChannelIDs());
        epochChanged = epochChanged || ! Objects.equals(partnerIDs, existingSalesChannel.getPartnerIDs());
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

  public void validate(CallingChannelService callingChannelService, PartnerService partnerService, Date date) throws GUIManagerException
  {
    /*****************************************
    *
    *  validate all calling channels exist and are active
    *
    *****************************************/

    for (String callingChannelID : callingChannelIDs)
      {
        if (callingChannelService.getActiveCallingChannel(callingChannelID, date) == null) throw new GUIManagerException("unknown calling channel", callingChannelID);
      }
    
    //
    //  validate the calling channel start/end dates include the sales channel active period
    //

    for (String callingChannelID : callingChannelIDs)
      {
        CallingChannel callingChannel = callingChannelService.getActiveCallingChannel(callingChannelID, date);
        if (! callingChannelService.isActiveCallingChannelThroughInterval(callingChannel, this.getEffectiveStartDate(), this.getEffectiveEndDate())) throw new GUIManagerException("invalid calling channel (start/end dates)", callingChannelID);
      }
    
    if(partnerIDs != null && !partnerIDs.isEmpty())
      {
        for(String partnerID : partnerIDs) {
          if (partnerService.getActivePartner(partnerID, date) == null) throw new GUIManagerException("unknown sales partner", partnerID);
          
      }
    }
  }
}
