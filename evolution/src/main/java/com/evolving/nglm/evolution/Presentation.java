package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

public class Presentation
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
    //
    //  schema
    //
    
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("presentation");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("date", Timestamp.builder().schema());
    schemaBuilder.field("offerIDs", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schema = schemaBuilder.build();
  };

  //
  //  accessor
  //

  public static Schema schema() { return schema; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private Date date;
  private List<String> offerIDs;
  
  public Date getDate() {return date; }
  public List<String> getOfferIDs() { return offerIDs; }
  
  public void setDate(Date date) { this.date = date; }
  public void setOfferIDs(List<String> offerIDs) { this.offerIDs = offerIDs; }

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  public Presentation(Date date, List<String> offerIDs)
  {
    this.date = date;
    this.offerIDs = offerIDs;
  }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<Presentation> serde()
  {
    return new ConnectSerde<Presentation>(schema, false, Presentation.class, Presentation::pack, Presentation::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    Presentation presentation = (Presentation) value;
    Struct struct = new Struct(schema);
    struct.put("date", presentation.getDate());
    struct.put("offerIDs", presentation.getOfferIDs());
    return struct;
  }
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static Presentation unpack(SchemaAndValue schemaAndValue)
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
    Date date = (Date) valueStruct.get("date");
    List<String> offerIDs = (List<String>) valueStruct.get("offerIDs");
     
    //
    //  construct
    //

    Presentation result = new Presentation(date, offerIDs);

    //
    //  return
    //

    return result;
  }
  
  @Override
  public String toString()
  {
    return "Presentation [" + (date != null ? "date=" + date + ", " : "") + (offerIDs != null ? "offerIDs=" + offerIDs : "") + "]";
  }


  
}
