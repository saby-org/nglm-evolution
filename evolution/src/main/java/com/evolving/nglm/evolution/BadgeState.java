package com.evolving.nglm.evolution;

import java.time.Duration;
import java.util.Date;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.Badge.BadgeType;
import com.evolving.nglm.evolution.Badge.CustomerBadgeStatus;
import com.evolving.nglm.evolution.retention.Cleanable;
import com.evolving.nglm.evolution.retention.RetentionService;

public class BadgeState implements Cleanable
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
    schemaBuilder.name("badge_subscriber_state");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("badgeType", Schema.STRING_SCHEMA);
    schemaBuilder.field("badgeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("customerBadgeStatus", Schema.STRING_SCHEMA);
    schemaBuilder.field("badgeAwardDate", Timestamp.builder().schema());
    schemaBuilder.field("badgeRemoveDate", Timestamp.builder().optional().schema());
    schema = schemaBuilder.build();
  };
  
  //
  //  serde
  //

  private static ConnectSerde<BadgeState> serde = new ConnectSerde<BadgeState>(schema, false, BadgeState.class, BadgeState::pack, BadgeState::unpack);
  
  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private BadgeType badgeType;
  private String badgeID;
  private CustomerBadgeStatus customerBadgeStatus;
  private Date badgeAwardDate;
  private Date badgeRemoveDate;

  public BadgeType getBadgeType()
  {
    return badgeType;
  }

  public void setBadgeType(BadgeType badgeType)
  {
    this.badgeType = badgeType;
  }

  public String getBadgeID()
  {
    return badgeID;
  }

  public void setBadgeID(String badgeID)
  {
    this.badgeID = badgeID;
  }

  public CustomerBadgeStatus getCustomerBadgeStatus()
  {
    return customerBadgeStatus;
  }

  public void setCustomerBadgeStatus(CustomerBadgeStatus customerBadgeStatus)
  {
    this.customerBadgeStatus = customerBadgeStatus;
  }

  public Date getBadgeAwardDate()
  {
    return badgeAwardDate;
  }

  public void setBadgeAwardDate(Date badgeAwardDate)
  {
    this.badgeAwardDate = badgeAwardDate;
  }

  public Date getBadgeRemoveDate()
  {
    return badgeRemoveDate;
  }

  public void setBadgeRemoveDate(Date badgeRemoveDate)
  {
    this.badgeRemoveDate = badgeRemoveDate;
  }

  @Override
  public Date getExpirationDate(RetentionService retentionService) { return getBadgeRemoveDate(); }

  @Override
  public Duration getRetention(RetentionType retentionType, RetentionService retentionService)
  {
    switch (retentionType)
    {
      case KAFKA_DELETION:
        return Duration.ofDays(Deployment.getKafkaRetentionDaysLoyaltyPrograms()); //RAJ K
    }
    return null;
  }
  
  /*****************************************
  *
  *  constructor 
  *
  *****************************************/

  public BadgeState(String badgeID, BadgeType badgeType, CustomerBadgeStatus customerBadgeStatus, Date badgeAwardDate, Date badgeRemoveDate)
  {
    this.badgeID = badgeID;
    this.badgeType = badgeType;
    this.customerBadgeStatus = customerBadgeStatus;
    this.badgeAwardDate = badgeAwardDate;
    this.badgeRemoveDate = badgeRemoveDate;
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    BadgeState badgeState = (BadgeState) value;
    Struct struct = new Struct(schema);
    struct.put("badgeID", badgeState.getBadgeID());
    struct.put("badgeType", badgeState.getBadgeType().getExternalRepresentation());
    struct.put("customerBadgeStatus", badgeState.getCustomerBadgeStatus().getExternalRepresentation());
    struct.put("badgeAwardDate", badgeState.getBadgeAwardDate());
    struct.put("badgeRemoveDate", badgeState.getBadgeRemoveDate());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static BadgeState unpack(SchemaAndValue schemaAndValue)
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
    String badgeID = valueStruct.getString("badgeID");
    String badgeType = valueStruct.getString("badgeType");
    String customerBadgeStatus = valueStruct.getString("customerBadgeStatus");
    Date badgeAwardDate = (Date) valueStruct.get("badgeAwardDate");
    Date badgeRemoveDate = (Date) valueStruct.get("badgeRemoveDate");
    
    //
    //  return
    //

    return new BadgeState(badgeID, BadgeType.fromExternalRepresentation(badgeType), CustomerBadgeStatus.fromExternalRepresentation(customerBadgeStatus), badgeAwardDate, badgeRemoveDate);
  }

}
