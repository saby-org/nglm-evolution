/*****************************************************************************
*
*  Deliverable.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import com.evolving.nglm.core.JSONUtilities;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class Deliverable extends GUIManagedObject
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
    schemaBuilder.name("deliverable");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("fulfillmentProviderID", Schema.STRING_SCHEMA);
    schemaBuilder.field("unitaryCost", Schema.INT32_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<Deliverable> serde = new ConnectSerde<Deliverable>(schema, false, Deliverable.class, Deliverable::pack, Deliverable::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<Deliverable> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String fulfillmentProviderID;
  private int unitaryCost;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getDeliverableID() { return getGUIManagedObjectID(); }
  public String getFulfillmentProviderID() { return fulfillmentProviderID; }
  public int getUnitaryCost() { return unitaryCost; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Deliverable(SchemaAndValue schemaAndValue, String fulfillmentProviderID, int unitaryCost)
  {
    super(schemaAndValue);
    this.fulfillmentProviderID = fulfillmentProviderID;
    this.unitaryCost = unitaryCost;
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    Deliverable deliverable = (Deliverable) value;
    Struct struct = new Struct(schema);
    packCommon(struct, deliverable);
    struct.put("fulfillmentProviderID", deliverable.getFulfillmentProviderID());
    struct.put("unitaryCost", deliverable.getUnitaryCost());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static Deliverable unpack(SchemaAndValue schemaAndValue)
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
    String fulfillmentProviderID = valueStruct.getString("fulfillmentProviderID");
    int unitaryCost = valueStruct.getInt32("unitaryCost");
    
    //
    //  return
    //

    return new Deliverable(schemaAndValue, fulfillmentProviderID, unitaryCost);
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public Deliverable(JSONObject jsonRoot, long epoch, GUIManagedObject existingDeliverableUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingDeliverableUnchecked != null) ? existingDeliverableUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingDeliverable
    *
    *****************************************/

    Deliverable existingDeliverable = (existingDeliverableUnchecked != null && existingDeliverableUnchecked instanceof Deliverable) ? (Deliverable) existingDeliverableUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.fulfillmentProviderID = JSONUtilities.decodeString(jsonRoot, "fulfillmentProviderID", true);
    this.unitaryCost = JSONUtilities.decodeInteger(jsonRoot, "unitaryCost", true);

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    // validate fulfillment provider

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingDeliverable))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(Deliverable existingDeliverable)
  {
    if (existingDeliverable != null && existingDeliverable.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingDeliverable.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(fulfillmentProviderID, existingDeliverable.getFulfillmentProviderID());
        epochChanged = epochChanged || ! (unitaryCost == existingDeliverable.getUnitaryCost());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
}
