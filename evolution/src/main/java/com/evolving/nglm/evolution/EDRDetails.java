/*****************************************************************************
*
*  EDRDetails.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;

public class EDRDetails extends SubscriberStreamOutput implements SubscriberStreamEvent
{
  //
  // logger
  //

  private static final Logger log = LoggerFactory.getLogger(EDRDetails.class);

  /*****************************************
   *
   * schema
   *
   *****************************************/

  //
  // schema
  //

  private static Schema schema = null;
  private static int currentSchemaVersion = 1;
  static
    {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      schemaBuilder.name("edr_details");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(), currentSchemaVersion));
      for (Field field : subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(), field.schema());
      schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
      schemaBuilder.field("eventID", Schema.STRING_SCHEMA);
      schemaBuilder.field("eventName", Schema.STRING_SCHEMA);
      schemaBuilder.field("eventDate", Timestamp.builder().schema());
      schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
      schemaBuilder.field("parameterMap", ParameterMap.schema());
      schema = schemaBuilder.build();
    };
    
    //
    //  serde
    //

    private static ConnectSerde<EDRDetails> serde = new ConnectSerde<EDRDetails>(schema, false, EDRDetails.class, EDRDetails::pack, EDRDetails::unpack);
    
    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<EDRDetails> serde() { return serde; }
    public Schema subscriberStreamEventSchema() { return schema(); }
    
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String subscriberID;
    private String eventID;
    private String eventName;
    private Date eventDate;
    private int tenantID;
    private ParameterMap parameterMap;
    
    public String getSubscriberID()
    {
      return subscriberID;
    }
    public String getEventID()
    {
      return eventID;
    }
    public String getEventName()
    {
      return eventName;
    }
    public Date getEventDate()
    {
      return eventDate;
    }
    public ParameterMap getParameterMap()
    {
      return parameterMap;
    }
    public int getTenantID() { return tenantID; }
    
    /*****************************************
    *
    *  constructor -- enter
    *
    *****************************************/

    public EDRDetails(EvolutionEventContext context, String subscriberID, String eventID, String eventName, Date eventDate, ParameterMap parameterMap, int tenantID)
    {
      this.subscriberID = subscriberID;
      this.eventID = eventID;
      this.eventName = eventName;
      this.eventDate = eventDate;
      this.parameterMap = parameterMap;
      this.tenantID = tenantID;
    }
    
    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    private EDRDetails(SchemaAndValue schemaAndValue, String subscriberID, String eventID, String eventName, Date eventDate, ParameterMap parameterMap, int tenantID)
    {
      super(schemaAndValue);
      this.subscriberID = subscriberID;
      this.eventID = eventID;
      this.eventName = eventName;
      this.eventDate = eventDate;
      this.parameterMap = parameterMap;
      this.tenantID = tenantID;
    }
    
    /*****************************************
    *
    *  constructor -- copy
    *
    *****************************************/

    public EDRDetails(EDRDetails edrDetails)
    {
      super(edrDetails);
      this.subscriberID = edrDetails.getSubscriberID();
      this.eventID = edrDetails.getEventID();
      this.eventName = edrDetails.getEventName();
      this.eventDate = edrDetails.getEventDate();
      this.parameterMap = edrDetails.getParameterMap();
      this.tenantID = edrDetails.getTenantID();
    }
    
    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      EDRDetails edrDetails = (EDRDetails) value;
      Struct struct = new Struct(schema);
      packSubscriberStreamOutput(struct, edrDetails);
      struct.put("subscriberID", edrDetails.getSubscriberID());
      struct.put("eventID", edrDetails.getEventID());
      struct.put("eventName", edrDetails.getEventName());
      struct.put("eventDate", edrDetails.getEventDate());
      struct.put("parameterMap", ParameterMap.pack(edrDetails.getParameterMap()));
      struct.put("tenantID", (short) edrDetails.getTenantID());
      return struct;
    }
    
    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static EDRDetails unpack(SchemaAndValue schemaAndValue)
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
      String subscriberID = valueStruct.getString("subscriberID");
      String eventID = valueStruct.getString("eventID");
      String eventName = valueStruct.getString("eventName");
      Date eventDate = (Date) valueStruct.get("eventDate");
      ParameterMap parameterMap = ParameterMap.unpack(new SchemaAndValue(schema.field("parameterMap").schema(), valueStruct.get("parameterMap")));
      int tenantID = valueStruct.getInt16("tenantID");
      
      //
      //  return
      //

      return new EDRDetails(schemaAndValue, subscriberID, eventID, eventName, eventDate, parameterMap, tenantID);
    }
    
    @Override public Object subscriberStreamEventPack(Object value) { return pack(value); }
}
