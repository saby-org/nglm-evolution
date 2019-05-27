
package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SchemaUtilities;

public class PropensityKey
{

  /*****************************************
  *
  *  Logger
  *
  *****************************************/
  
  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(EvolutionEngine.class);

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
    schemaBuilder.name("propensity_key");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("offerID", Schema.STRING_SCHEMA);
    schemaBuilder.field("propensityStratum", SchemaBuilder.array(Schema.STRING_SCHEMA));
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
  *  propensityStratum is defined by a list of segments. 
  *   One segment for each dimension specified in propensity rules, and in the same position !
  *   The position of each segment is important because propensityStratum will be part of the Key and must be unique.
  *
  *****************************************/

  private String offerID;
  private List<String> propensityStratum;

  /*****************************************
  *
  *  private constructor
  *  
  *  This constructor is kept private because the position of each segment in the list is internal (retrieved from propensity rules)
  *  and must be maintained to keep the key unique.
  *
  *****************************************/

  private PropensityKey(String offerID, List<String> propensityStratum)
  {
    this.offerID = offerID;
    this.propensityStratum = propensityStratum;
  }

  /*****************************************
  *
  *  constructor (from subscriber profile)
  *  
  *  The propensity stratum will be extracted from this subscriber profile.
  *
  *****************************************/
  
  public PropensityKey(String offerID, SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    this.offerID = offerID;
    this.propensityStratum = new ArrayList<String>();
    
    //
    // Construct the propensity stratum from the subscriber profile
    // Segments will be arranged in the same order than the one specified in Propensity Rule
    //
    
    List<String> dimensions = Deployment.getPropensityRule().getSelectedDimensions();
    Map<String, String> subscriberGroups = subscriberProfile.getSegmentsMap(subscriberGroupEpochReader);
    
    for(String dimensionID : dimensions) {
      String segmentID = subscriberGroups.get(dimensionID);
      if(segmentID == null) {
        log.error("Requiered dimension for propensity could not be found in the subscriber profile. Propensity will not be relevant.");
      }
      
      this.propensityStratum.add(segmentID);
    }
    
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getOfferID() { return offerID; }
  public List<String> getPropensityStratum() { return propensityStratum; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<PropensityKey> serde()
  {
    return new ConnectSerde<PropensityKey>(schema, true, PropensityKey.class, PropensityKey::pack, PropensityKey::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PropensityKey propensityKey = (PropensityKey) value;
    Struct struct = new Struct(schema);
    struct.put("offerID", propensityKey.getOfferID());
    struct.put("propensityStratum", propensityKey.getPropensityStratum());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static PropensityKey unpack(SchemaAndValue schemaAndValue)
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
    String offerID = valueStruct.getString("offerID");
    List<String> propensityStratum = (List<String>) valueStruct.get("propensityStratum");

    //
    //  return
    //

    return new PropensityKey(offerID, propensityStratum);
  }


  /*****************************************
  *
  *  equals/hashCode
  *
  *****************************************/

  //
  //  equals
  //
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    PropensityKey other = (PropensityKey) obj;
    if (offerID == null) {
      if (other.offerID != null)
        return false;
    } else if (!offerID.equals(other.offerID))
      return false;
    if (propensityStratum == null) {
      if (other.propensityStratum != null)
        return false;
    } else if (!propensityStratum.equals(other.propensityStratum))
      return false;
    return true;
  }

  //
  //  hashCode
  //

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((offerID == null) ? 0 : offerID.hashCode());
    result = prime * result + ((propensityStratum == null) ? 0 : propensityStratum.hashCode());
    return result;
  }
}
