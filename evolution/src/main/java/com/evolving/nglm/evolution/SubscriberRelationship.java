/*****************************************************************************
*
*  SubscriberRelationship.java
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

public class SubscriberRelationship extends GUIManagedObject
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
    schemaBuilder.name("subscriber_relationship");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<SubscriberRelationship> serde = new ConnectSerde<SubscriberRelationship>(schema, false, SubscriberRelationship.class, SubscriberRelationship::pack, SubscriberRelationship::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SubscriberRelationship> serde() { return serde; }

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

  public String getRelationshipID() { return getGUIManagedObjectID(); }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public SubscriberRelationship(SchemaAndValue schemaAndValue)
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
    SubscriberRelationship relationship = (SubscriberRelationship) value;
    Struct struct = new Struct(schema);
    packCommon(struct, relationship);
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static SubscriberRelationship unpack(SchemaAndValue schemaAndValue)
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

    return new SubscriberRelationship(schemaAndValue);
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public SubscriberRelationship(JSONObject jsonRoot, long epoch, GUIManagedObject existingRelationshipUnchecked, int tenantID) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingRelationshipUnchecked != null) ? existingRelationshipUnchecked.getEpoch() : epoch, tenantID);

    /*****************************************
    *
    *  existingSupplier
    *
    *****************************************/

    SubscriberRelationship existingRelationship = (existingRelationshipUnchecked != null && existingRelationshipUnchecked instanceof SubscriberRelationship) ? (SubscriberRelationship) existingRelationshipUnchecked : null;
    
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

    if (epochChanged(existingRelationship))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(SubscriberRelationship existingRelationship)
  {
    if (existingRelationship != null && existingRelationship.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingRelationship.getGUIManagedObjectID());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
}
