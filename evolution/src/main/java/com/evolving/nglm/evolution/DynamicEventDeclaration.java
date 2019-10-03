/*****************************************************************************
*
*  DynamicEventDeclaration.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.EvolutionEngineEventDeclaration.EventRule;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class DynamicEventDeclaration
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
    schemaBuilder.name("dynamic_event_declaration");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("name", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventClassName", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventTopic", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventRule", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventCriterionFields", SchemaBuilder.map(Schema.STRING_SCHEMA, CriterionField.schema()).name("dynamic_event_criterion_fields").schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<DynamicEventDeclaration> serde = new ConnectSerde<DynamicEventDeclaration>(schema, false, DynamicEventDeclaration.class, DynamicEventDeclaration::pack, DynamicEventDeclaration::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<DynamicEventDeclaration> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private EvolutionEngineEventDeclaration dynamicEventDeclaration;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public EvolutionEngineEventDeclaration getDynamicEventDeclaration() { return dynamicEventDeclaration; }

  /*****************************************
  *
  *  constructor
   * @throws GUIManagerException 
  *
  *****************************************/

  public DynamicEventDeclaration(String name, String eventClassName, String eventTopic, EventRule eventRule, Map<String,CriterionField> eventCriterionFields) throws GUIManagerException
  {
    this.dynamicEventDeclaration = new EvolutionEngineEventDeclaration(name, eventClassName, eventTopic, eventRule, eventCriterionFields);
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public DynamicEventDeclaration(SchemaAndValue schemaAndValue, String name, String eventClassName, String eventTopic, EventRule eventRule, Map<String,CriterionField> eventCriterionFields)
  {
    try {
      this.dynamicEventDeclaration = new EvolutionEngineEventDeclaration(name, eventClassName, eventTopic, eventRule, eventCriterionFields);
    }
    catch(GUIManagerException e) {
      throw new SerializationException("DynamicEventDeclarations unpack",e);
    }
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    DynamicEventDeclaration dynamicEventDeclaration = (DynamicEventDeclaration) value;
    Struct struct = new Struct(schema);
    struct.put("name", dynamicEventDeclaration.getDynamicEventDeclaration().getName());
    struct.put("eventClassName", dynamicEventDeclaration.getDynamicEventDeclaration().getEventClassName());
    struct.put("eventTopic", dynamicEventDeclaration.getDynamicEventDeclaration().getEventTopic());
    struct.put("eventRule", dynamicEventDeclaration.getDynamicEventDeclaration().getEventRule().getExternalRepresentation());
    struct.put("eventCriterionFields", packEventCriterionFields(dynamicEventDeclaration.getDynamicEventDeclaration().getEventCriterionFields()));
    return struct;
  }
  
  /****************************************
  *
  *  packEventCriterionFields
  *
  ****************************************/

  private static Map<String,Object> packEventCriterionFields(Map<String,CriterionField> eventCriterionFields)
  {
    Map<String,Object> result = new LinkedHashMap<String,Object>();
    for (String eventCriterionFieldName : eventCriterionFields.keySet())
      {
        CriterionField criterionField = eventCriterionFields.get(eventCriterionFieldName);
        result.put(eventCriterionFieldName,CriterionField.pack(criterionField));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static DynamicEventDeclaration unpack(SchemaAndValue schemaAndValue)
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
    String name = valueStruct.getString("name");
    String eventClassName = valueStruct.getString("eventClassName");
    String eventTopic = valueStruct.getString("eventTopic");
    EventRule eventRule = valueStruct.getString("eventRule") != null ? EventRule.fromExternalRepresentation(valueStruct.getString("eventRule")) : EventRule.Standard;
    Map<String,CriterionField> eventCriterionFields = unpackEventCriterionFields(schema.field("eventCriterionFields").schema(), (Map<String,Object>) valueStruct.get("eventCriterionFields"));
    

    //
    //  return
    //

    return new DynamicEventDeclaration(schemaAndValue, name, eventClassName, eventTopic, eventRule, eventCriterionFields);
  }
  
  /*****************************************
  *
  *  unpackEventCriterionFields
  *
  *****************************************/

  private static Map<String,CriterionField> unpackEventCriterionFields(Schema schema, Map<String,Object> eventCriterionFields)
  {
    Map<String,CriterionField> result = new LinkedHashMap<String,CriterionField>();
    for (String criterionFieldName : eventCriterionFields.keySet())
      {
        CriterionField criterionField = CriterionField.unpack(new SchemaAndValue(schema.valueSchema(), eventCriterionFields.get(criterionFieldName)));
        result.put(criterionFieldName, criterionField);
      }
    return result;
  }
}
