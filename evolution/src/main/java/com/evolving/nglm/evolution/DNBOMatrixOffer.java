/*****************************************************************************
*
kj*  DNBOMatrixOffer.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class DNBOMatrixOffer
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
    schemaBuilder.name("dnbo_matrixoffer");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("offerId", Schema.STRING_SCHEMA);
    schemaBuilder.field("position", Schema.STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<DNBOMatrixOffer> serde = new ConnectSerde<DNBOMatrixOffer>(schema, false, DNBOMatrixOffer.class, DNBOMatrixOffer::pack, DNBOMatrixOffer::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<DNBOMatrixOffer> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String offerId;
  private String position;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getOfferId()  {    return offerId;  }
  public String getPosition()  {    return position;  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DNBOMatrixOffer(String offerId, String position)
  {
    this.offerId = offerId;
    this.position = position;
  }
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    DNBOMatrixOffer dnboMatrixOffer = (DNBOMatrixOffer) value;
    Struct struct = new Struct(schema);
    struct.put("offerId", dnboMatrixOffer.getOfferId());
    struct.put("position", dnboMatrixOffer.getPosition());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static DNBOMatrixOffer unpack(SchemaAndValue schemaAndValue)
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
    String offerId = (String) valueStruct.get("offerId");
    String position = (String) valueStruct.get("position");

    //
    //  return
    //

    return new DNBOMatrixOffer(offerId, position);
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DNBOMatrixOffer(JSONObject jsonRoot) throws GUIManagerException
  {
    this.offerId = JSONUtilities.decodeString(jsonRoot, "offerId", true);
    this.position = JSONUtilities.decodeString(jsonRoot, "position", true);
  }

  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof DNBOMatrixOffer)
      {
        DNBOMatrixOffer dnboMatrixOffer = (DNBOMatrixOffer) obj;
        result = true;
        result = result && Objects.equals(offerId, dnboMatrixOffer.getOfferId());
        result = result && Objects.equals(position, dnboMatrixOffer.getPosition());
      }
    return result;
  }
}
