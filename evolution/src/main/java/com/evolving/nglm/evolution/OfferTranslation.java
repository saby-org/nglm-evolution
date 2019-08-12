/*****************************************************************************
*
*  OfferTranslation.java
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

public class OfferTranslation
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
    schemaBuilder.name("offer_translation");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("display", Schema.STRING_SCHEMA);
    schemaBuilder.field("description", Schema.STRING_SCHEMA);
    schemaBuilder.field("imageURL", Schema.STRING_SCHEMA);
    schemaBuilder.field("languageID", Schema.STRING_SCHEMA);
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

  private String display;
  private String description;
  private String imageURL;
  private String languageID;
  
  /*****************************************
   *
   *  constructor -- simple
   *
   *****************************************/

  private OfferTranslation(String display, String description, String imageURL, String languageID)
  {
    this.display = display;
    this.description = description;
    this.imageURL = imageURL;
    this.languageID = languageID;
  }

  /*****************************************
   *
   *  constructor -- external JSON
   *
   *****************************************/

  OfferTranslation(JSONObject jsonRoot) throws GUIManagerException
  {
    //
    //  basic fields
    //

    this.display = JSONUtilities.decodeString(jsonRoot, "display", false);
    this.description = JSONUtilities.decodeString(jsonRoot, "description", false);
    this.imageURL = JSONUtilities.decodeString(jsonRoot, "imageURL", false);
    this.languageID = JSONUtilities.decodeString(jsonRoot, "languageID", false);

    //
    //  validate 
    //

  }

  /*****************************************
   *
   *  accessors
   *
   *****************************************/

  public String getDisplay() { return display; }
  public String getDescription() { return description; }
  public String getImageURL() { return imageURL; }
  public String getLanguageID() { return languageID; }
  
  /*****************************************
   *
   *  serde
   *
   *****************************************/

  public static ConnectSerde<OfferTranslation> serde()
  {
    return new ConnectSerde<OfferTranslation>(schema, false, OfferTranslation.class, OfferTranslation::pack, OfferTranslation::unpack);
  }

  /*****************************************
   *
   *  pack
   *
   *****************************************/

  public static Object pack(Object value)
  {
    OfferTranslation offerTranslation = (OfferTranslation) value;
    Struct struct = new Struct(schema);
    struct.put("display", offerTranslation.getDisplay());
    struct.put("description", offerTranslation.getDescription());
    struct.put("imageURL", offerTranslation.getImageURL());
    struct.put("languageID", offerTranslation.getLanguageID());
    return struct;
  }

  /*****************************************
   *
   *  unpack
   *
   *****************************************/

  public static OfferTranslation unpack(SchemaAndValue schemaAndValue)
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
    String display = valueStruct.getString("display");
    String description = valueStruct.getString("description");
    String imageURL = valueStruct.getString("imageURL");
    String languageID = valueStruct.getString("languageID");

    //
    //  validate
    //

    //
    //  return
    //

    return new OfferTranslation(display, description, imageURL, languageID);
  }

  /*****************************************
   *
   *  equals
   *
   *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof OfferTranslation)
      {
        OfferTranslation offerTranslation = (OfferTranslation) obj;
        result = true;
        result = result && Objects.equals(display, offerTranslation.getDisplay());
        result = result && Objects.equals(description, offerTranslation.getDescription());
        result = result && Objects.equals(imageURL, offerTranslation.getImageURL());
        result = result && Objects.equals(languageID, offerTranslation.getLanguageID());
      }
    return result;
  }

  /*****************************************
   *
   *  hashCode
   *
   *****************************************/

  public int hashCode()
  {
    return display.hashCode();
  }


}
