/*****************************************************************************
*
*  Supplier.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.TimeUnit;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SchemaUtilities;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;

import org.json.simple.JSONObject;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class Supplier extends GUIManagedObject
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
    schemaBuilder.name("supplier");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<Supplier> serde = new ConnectSerde<Supplier>(schema, false, Supplier.class, Supplier::pack, Supplier::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<Supplier> serde() { return serde; }

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

  public String getSupplierID() { return getGUIManagedObjectID(); }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Supplier(SchemaAndValue schemaAndValue)
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
    Supplier supplier = (Supplier) value;
    Struct struct = new Struct(schema);
    packCommon(struct, supplier);
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static Supplier unpack(SchemaAndValue schemaAndValue)
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

    return new Supplier(schemaAndValue);
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public Supplier(JSONObject jsonRoot, long epoch, GUIManagedObject existingSupplierUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingSupplierUnchecked != null) ? existingSupplierUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingSupplier
    *
    *****************************************/

    Supplier existingSupplier = (existingSupplierUnchecked != null && existingSupplierUnchecked instanceof Supplier) ? (Supplier) existingSupplierUnchecked : null;
    
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

    if (epochChanged(existingSupplier))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(Supplier existingSupplier)
  {
    if (existingSupplier != null && existingSupplier.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingSupplier.getGUIManagedObjectID());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
}
