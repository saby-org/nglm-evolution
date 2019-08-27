/*****************************************************************************
*
*  DNBOMatrixSegment.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class DNBOMatrixSegment
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
    schemaBuilder.name("dnbo_matrixsegment");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("segmentId", Schema.STRING_SCHEMA);
    schemaBuilder.field("offers", SchemaBuilder.array(DNBOMatrixOffer.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<DNBOMatrixSegment> serde = new ConnectSerde<DNBOMatrixSegment>(schema, false, DNBOMatrixSegment.class, DNBOMatrixSegment::pack, DNBOMatrixSegment::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<DNBOMatrixSegment> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String segmentId;
  private List<DNBOMatrixOffer> offers;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getSegmentId() { return segmentId; }
  public List<DNBOMatrixOffer> getOffers() { return offers; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DNBOMatrixSegment(String segmentId, List<DNBOMatrixOffer> offers)
  {
    this.segmentId = segmentId;
    this.offers = offers;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    DNBOMatrixSegment dnboMatrixSegment = (DNBOMatrixSegment) value;
    Struct struct = new Struct(schema);
    struct.put("segmentId", dnboMatrixSegment.getSegmentId());
    struct.put("offers", packDNBOMatrixOffers(dnboMatrixSegment.getOffers()));
    return struct;
  }

  /****************************************
  *
  *  packDNBOMatrixSegments
  *
  ****************************************/

  private static List<Object> packDNBOMatrixOffers(List<DNBOMatrixOffer> dnboMatrixOffers)
  {
    List<Object> result = new ArrayList<Object>();
    for (DNBOMatrixOffer dnboMatrixOffer : dnboMatrixOffers)
      {
        result.add(DNBOMatrixOffer.pack(dnboMatrixOffer));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static DNBOMatrixSegment unpack(SchemaAndValue schemaAndValue)
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
    String segmentId = (String) valueStruct.get("segmentId");
    List<DNBOMatrixOffer> offers = unpackDNBOMatrixOffers(schema.field("offers").schema(), valueStruct.get("offers"));

    //
    //  return
    //

    return new DNBOMatrixSegment(segmentId, offers);
  }
  
  /*****************************************
  *
  *  unpackDNBOMatrixOffers
  *
  *****************************************/

  private static List<DNBOMatrixOffer> unpackDNBOMatrixOffers(Schema schema, Object value)
  {
    //
    //  get schema for DNBOMatrixOffer
    //

    Schema dnboMatrixOfferSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<DNBOMatrixOffer> result = new ArrayList<DNBOMatrixOffer>();
    List<Object> valueArray = (List<Object>) value;
    for (Object dnboMatrixOffer : valueArray)
      {
        result.add(DNBOMatrixOffer.unpack(new SchemaAndValue(dnboMatrixOfferSchema, dnboMatrixOffer)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DNBOMatrixSegment(JSONObject jsonRoot) throws GUIManagerException
  {
    this.segmentId = JSONUtilities.decodeString(jsonRoot, "segmentId", true);
    this.offers = decodeOffers(JSONUtilities.decodeJSONArray(jsonRoot, "offers", true));
  }

  /*****************************************
  *
  *  decodeOffers
  *
  *****************************************/

  private List<DNBOMatrixOffer> decodeOffers(JSONArray jsonArray) throws GUIManagerException
  {
    List<DNBOMatrixOffer> result = new ArrayList<DNBOMatrixOffer>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new DNBOMatrixOffer((JSONObject) jsonArray.get(i)));
      }
    return result;
  }

  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof DNBOMatrixSegment)
      {
        DNBOMatrixSegment dnboMatrixSegment = (DNBOMatrixSegment) obj;
        result = true;
        result = result && Objects.equals(segmentId, dnboMatrixSegment.getSegmentId());
        result = result && Objects.equals(offers, dnboMatrixSegment.getOffers());
      }
    return result;
  }
}
