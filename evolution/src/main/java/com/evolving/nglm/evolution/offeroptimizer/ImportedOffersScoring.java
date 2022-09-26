package com.evolving.nglm.evolution.offeroptimizer;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.evolution.DeliveryRequest.DeliveryPriority;
import com.evolving.nglm.evolution.EvolutionEngineEvent;

public class ImportedOffersScoring implements EvolutionEngineEvent, SubscriberStreamEvent
{
  //
  // schema
  //

  private static Schema schema = null;
  static
    {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      schemaBuilder.name("imported_offers_scoring");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("importedTypeID", Schema.STRING_SCHEMA);
      schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
      schemaBuilder.field("importedOffersList", SchemaBuilder.array(Schema.STRING_SCHEMA).defaultValue(new ArrayList<String>()).schema());
      schema = schemaBuilder.build();
    };
    
    private String importedTypeID;
    private String subscriberID;
    private List<String> importedOffersList;
    
    public ImportedOffersScoring(String importedTypeID, String subscriberID, List<String> importedOffersList)
    {
      super();
      this.importedTypeID = importedTypeID;
      this.subscriberID = subscriberID;
      this.importedOffersList = importedOffersList;
    }

    //
    // serde
    //

    private static ConnectSerde<ImportedOffersScoring> serde = new ConnectSerde<ImportedOffersScoring>(schema, false, ImportedOffersScoring.class, ImportedOffersScoring::pack, ImportedOffersScoring::unpack);
    
    //
    // accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<ImportedOffersScoring> serde() { return serde; }
    
    public String getImportedTypeID()
    {
      return importedTypeID;
    }
    public String getSubscriberID()
    {
      return subscriberID;
    }
    public List<String> getImportedOffersList()
    {
      return importedOffersList;
    }
    
    /*****************************************
    *
    *  pack
    *
    *****************************************/

   public static Object pack(Object value)
   {
     ImportedOffersScoring imported = (ImportedOffersScoring) value;
     Struct struct = new Struct(schema);
     struct.put("subscriberID", imported.getSubscriberID());
     struct.put("importedTypeID", imported.getImportedTypeID());
     struct.put("importedOffersList", imported.getImportedOffersList());
     return struct;
   }
   
   /*****************************************
    *
    * unpack
    *
    *****************************************/

   public static ImportedOffersScoring unpack(SchemaAndValue schemaAndValue)
   {
     //
     // data
     //

     Schema schema = schemaAndValue.schema();
     Object value = schemaAndValue.value();
     Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

     //
     // unpack
     //

     Struct valueStruct = (Struct) value;
     String subscriberID = valueStruct.getString("subscriberID");
     String importedTypeID = valueStruct.getString("importedTypeID");
     List<String> importedOffersList = valueStruct.getArray("importedOffersList");

     //
     // return
     //

     return new ImportedOffersScoring(importedTypeID, subscriberID, importedOffersList);
   }
   
  @Override
  public String toString()
  {
    return "ImportedOffersScoring [importedTypeID=" + importedTypeID + ", subscriberID=" + subscriberID + ", importedOffersList=" + importedOffersList + "]";
  }
  
  @Override
  public Schema subscriberStreamEventSchema()
  {
    return schema();
  }
  @Override
  public Object subscriberStreamEventPack(Object value)
  {
    return pack(value);
  }
  @Override
  public DeliveryPriority getDeliveryPriority()
  {
    return DeliveryPriority.Standard;
  }
  @Override
  public String getEventName()
  {
    return "importedScoring";
  }
    
}
