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

public class BadgeTranslation
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
    schemaBuilder.name("badge_translation");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("display", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("description", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("imageURL", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("languageID", Schema.OPTIONAL_STRING_SCHEMA);
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

  private BadgeTranslation(String display, String description, String imageURL, String languageID)
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

  public BadgeTranslation(JSONObject jsonRoot) throws GUIManagerException
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

  public static ConnectSerde<BadgeTranslation> serde()
  {
    return new ConnectSerde<BadgeTranslation>(schema, false, BadgeTranslation.class, BadgeTranslation::pack, BadgeTranslation::unpack);
  }

  /*****************************************
   *
   *  pack
   *
   *****************************************/

  public static Object pack(Object value)
  {
    BadgeTranslation badgeTranslation = (BadgeTranslation) value;
    Struct struct = new Struct(schema);
    struct.put("display", badgeTranslation.getDisplay());
    struct.put("description", badgeTranslation.getDescription());
    struct.put("imageURL", badgeTranslation.getImageURL());
    struct.put("languageID", badgeTranslation.getLanguageID());
    return struct;
  }

  /*****************************************
   *
   *  unpack
   *
   *****************************************/

  public static BadgeTranslation unpack(SchemaAndValue schemaAndValue)
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

    return new BadgeTranslation(display, description, imageURL, languageID);
  }

  /*****************************************
   *
   *  equals
   *
   *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof BadgeTranslation)
      {
        BadgeTranslation badgeTranslation = (BadgeTranslation) obj;
        result = true;
        result = result && Objects.equals(display, badgeTranslation.getDisplay());
        result = result && Objects.equals(description, badgeTranslation.getDescription());
        result = result && Objects.equals(imageURL, badgeTranslation.getImageURL());
        result = result && Objects.equals(languageID, badgeTranslation.getLanguageID());
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
