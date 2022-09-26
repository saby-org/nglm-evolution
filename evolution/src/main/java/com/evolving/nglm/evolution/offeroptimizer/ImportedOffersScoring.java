package com.evolving.nglm.evolution.offeroptimizer;

import java.util.ArrayList;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import com.evolving.nglm.core.SchemaUtilities;

public class ImportedOffersScoring
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
    
    
    
    
}
