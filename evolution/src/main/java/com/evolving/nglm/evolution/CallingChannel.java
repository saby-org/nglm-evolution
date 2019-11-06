/*****************************************************************************
*
*  CallingChannel.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

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

public class CallingChannel extends GUIManagedObject
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
    schemaBuilder.name("calling_channel");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),2));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<CallingChannel> serde = new ConnectSerde<CallingChannel>(schema, false, CallingChannel.class, CallingChannel::pack, CallingChannel::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<CallingChannel> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  // none currently

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getCallingChannelID() { return getGUIManagedObjectID(); }
  public String getDisplay() { return getGUIManagedObjectDisplay(); }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public CallingChannel(SchemaAndValue schemaAndValue)
  {
    super(schemaAndValue);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    CallingChannel callingChannel = (CallingChannel) value;
    Struct struct = new Struct(schema);
    packCommon(struct, callingChannel);
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static CallingChannel unpack(SchemaAndValue schemaAndValue)
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
    
    //
    //  return
    //

    return new CallingChannel(schemaAndValue);
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public CallingChannel(JSONObject jsonRoot, long epoch, GUIManagedObject existingCallingChannelUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingCallingChannelUnchecked != null) ? existingCallingChannelUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingCallingChannel
    *
    *****************************************/

    CallingChannel existingCallingChannel = (existingCallingChannelUnchecked != null && existingCallingChannelUnchecked instanceof CallingChannel) ? (CallingChannel) existingCallingChannelUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    // none

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    if (getRawEffectiveStartDate() != null) throw new GUIManagerException("unsupported start date", JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", false));
    if (getRawEffectiveEndDate() != null) throw new GUIManagerException("unsupported end date", JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", false));

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingCallingChannel))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(CallingChannel existingCallingChannel)
  {
    if (existingCallingChannel != null && existingCallingChannel.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingCallingChannel.getGUIManagedObjectID());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
}
