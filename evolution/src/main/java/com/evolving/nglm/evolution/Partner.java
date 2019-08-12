/*****************************************************************************
*
*  Partner.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;

import org.json.simple.JSONObject;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import java.util.Date;
import java.util.Objects;

public class Partner extends GUIManagedObject
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
    schemaBuilder.name("partner");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("website", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("phone", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("email", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("contactPerson", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("address", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("mobile", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("alternateMobile", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("partnerType", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("billingMode", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("contractNumber", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("billingCode", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("paymentDetails", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<Partner> serde = new ConnectSerde<Partner>(schema, false, Partner.class, Partner::pack, Partner::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<Partner> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/
  
  private String website; 
  private String phone;
  private String email; 
  private String contactPerson; 
  private String address; 
  private String mobile; 
  private String alternateMobile; 
  private String partnerType; 
  private String billingMode; 
  private String contractNumber; 
  private String billingCode; 
  private String paymentDetails; 
  
  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getPartnerID() { return getGUIManagedObjectID(); }
  public String getPartnerName() { return getGUIManagedObjectName(); }
  public String getWebsite(){ return website; }
  public String getPhone(){ return phone; }
  public String getEmail(){ return email; }
  public String getContactPerson(){ return contactPerson; }
  public String getAddress(){ return address; }
  public String getMobile(){ return mobile; }
  public String getAlternateMobile(){ return alternateMobile; }
  public String getPartnerType(){ return partnerType; }
  public String getBillingMode(){ return billingMode; }
  public String getContractNumber() { return contractNumber; }
  public String getBillingCode() { return billingCode; }
  public String getPaymentDetails() { return paymentDetails; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Partner(SchemaAndValue schemaAndValue, String website, String phone, String email, String contactPerson, String address, String mobile, String alternateMobile, String partnerType, String billingMode, String contractNumber, String billingCode, String paymentDetails)
  {
    super(schemaAndValue);
    this.website = website;
    this.phone = phone;
    this.email = email;
    this.contactPerson = contactPerson;
    this.address = address;
    this.mobile = mobile;
    this.alternateMobile = alternateMobile;
    this.partnerType = partnerType;
    this.billingMode = billingMode;
    this.contractNumber = contractNumber;
    this.billingCode = billingCode;
    this.paymentDetails = paymentDetails;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    Partner partner = (Partner) value;
    Struct struct = new Struct(schema);
    packCommon(struct, partner);
    struct.put("website", partner.getWebsite());
    struct.put("phone", partner.getPhone());
    struct.put("email", partner.getEmail());
    struct.put("contactPerson", partner.getContactPerson());
    struct.put("address", partner.getAddress());
    struct.put("mobile", partner.getMobile());
    struct.put("alternateMobile", partner.getAlternateMobile());
    struct.put("partnerType", partner.getPartnerType());
    struct.put("billingMode", partner.getBillingMode());
    struct.put("contractNumber", partner.getContractNumber());
    struct.put("billingCode", partner.getBillingCode());
    struct.put("paymentDetails", partner.getPaymentDetails());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static Partner unpack(SchemaAndValue schemaAndValue)
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
    String website = valueStruct.getString("website");
    String phone = valueStruct.getString("phone");
    String email = valueStruct.getString("email");
    String contactPerson = valueStruct.getString("contactPerson");
    String address = valueStruct.getString("address");
    String mobile = valueStruct.getString("mobile");
    String alternateMobile = valueStruct.getString("alternateMobile");
    String partnerType = valueStruct.getString("partnerType");
    String billingMode = valueStruct.getString("billingMode");
    String contractNumber = valueStruct.getString("contractNumber");
    String billingCode = valueStruct.getString("billingCode");
    String paymentDetails = valueStruct.getString("paymentDetails");
    //
    //  return
    //

    return new Partner(schemaAndValue, website, phone, email, contactPerson, address, mobile, alternateMobile, partnerType, billingMode, contractNumber, billingCode, paymentDetails);
  }
  
  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public Partner(JSONObject jsonRoot, long epoch, GUIManagedObject existingPartnerUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingPartnerUnchecked != null) ? existingPartnerUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingPartner
    *
    *****************************************/

    Partner existingPartner = (existingPartnerUnchecked != null && existingPartnerUnchecked instanceof Partner) ? (Partner) existingPartnerUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/
    
    this.website = JSONUtilities.decodeString(jsonRoot, "website", false);
    this.phone = JSONUtilities.decodeString(jsonRoot, "phone", false);
    this.email = JSONUtilities.decodeString(jsonRoot, "email", false);
    this.contactPerson = JSONUtilities.decodeString(jsonRoot, "contactPerson", false);
    this.address = JSONUtilities.decodeString(jsonRoot, "address", false);
    this.mobile = JSONUtilities.decodeString(jsonRoot, "mobile", false);
    this.alternateMobile = JSONUtilities.decodeString(jsonRoot, "alternateMobile", false);
    this.partnerType = JSONUtilities.decodeString(jsonRoot, "partnerType", false);
    this.billingMode = JSONUtilities.decodeString(jsonRoot, "billingMode", false);
    this.contractNumber = JSONUtilities.decodeString(jsonRoot, "contractNumber", false);
    this.billingCode = JSONUtilities.decodeString(jsonRoot, "billingCode", false);
    this.paymentDetails = JSONUtilities.decodeString(jsonRoot, "paymentDetails", false);

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingPartner))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(Partner existingPartner)
  {
    if (existingPartner != null && existingPartner.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingPartner.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(getWebsite(), existingPartner.getWebsite());
        epochChanged = epochChanged || ! Objects.equals(getPhone(), existingPartner.getPhone());
        epochChanged = epochChanged || ! Objects.equals(getEmail(), existingPartner.getEmail());
        epochChanged = epochChanged || ! Objects.equals(getContactPerson(), existingPartner.getContactPerson());
        epochChanged = epochChanged || ! Objects.equals(getAddress(), existingPartner.getAddress());
        epochChanged = epochChanged || ! Objects.equals(getMobile(), existingPartner.getMobile());
        epochChanged = epochChanged || ! Objects.equals(getAlternateMobile(), existingPartner.getAlternateMobile());
        epochChanged = epochChanged || ! Objects.equals(getPartnerType(), existingPartner.getPartnerType());
        epochChanged = epochChanged || ! Objects.equals(getBillingMode(), existingPartner.getBillingMode());
        epochChanged = epochChanged || ! Objects.equals(getContractNumber(), existingPartner.getContractNumber());
        epochChanged = epochChanged || ! Objects.equals(getBillingCode(), existingPartner.getBillingCode());
        epochChanged = epochChanged || ! Objects.equals(getPaymentDetails(), existingPartner.getPaymentDetails());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
  
  /*****************************************
  *
  *  validate
  *
  *****************************************/

  public void validate(Date date) throws GUIManagerException
  {
    /*****************************************
    *
    *  validate
    *
    *****************************************/

  }
}
