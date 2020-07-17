/*****************************************************************************
*
*  Reseller.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@GUIDependencyDef(objectType = "reseller", serviceClass = ResellerService.class, dependencies = { })
public class Reseller extends GUIManagedObject
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
    schemaBuilder.name("reseller");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),3));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("website", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("phone", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("email", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("contactPerson", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("address", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("mobile", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("alternateMobile", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("billingModeID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("contractNumber", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("billingCode", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("paymentDetails", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("userIDs", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().schema());
    schemaBuilder.field("parentResellerID", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<Reseller> serde = new ConnectSerde<Reseller>(schema, false, Reseller.class, Reseller::pack, Reseller::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<Reseller> serde() { return serde; }

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
  private String billingModeID; 
  private String contractNumber; 
  private String billingCode; 
  private String paymentDetails; 
  private List<String> userIDs;
  private String parentResellerID; 
  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getResellerID() { return getGUIManagedObjectID(); }
  public String getResellerName() { return getGUIManagedObjectName(); }
  public String getWebsite(){ return website; }
  public String getPhone(){ return phone; }
  public String getEmail(){ return email; }
  public String getContactPerson(){ return contactPerson; }
  public String getAddress(){ return address; }
  public String getMobile(){ return mobile; }
  public String getAlternateMobile(){ return alternateMobile; }
  public String getBillingModeID(){ return billingModeID; }
  public String getContractNumber() { return contractNumber; }
  public String getBillingCode() { return billingCode; }
  public String getPaymentDetails() { return paymentDetails; }
  public String getParentResellerID() { return parentResellerID; }
  public List<String> getUserIDs() { return userIDs; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Reseller(SchemaAndValue schemaAndValue, String website, String phone, String email, String contactPerson, String address, String mobile, String alternateMobile, String billingModeID, String contractNumber, String billingCode, String paymentDetails, List<String> userIDs, String parentResellerID)
  {
    super(schemaAndValue);
    this.website = website;
    this.phone = phone;
    this.email = email;
    this.contactPerson = contactPerson;
    this.address = address;
    this.mobile = mobile;
    this.alternateMobile = alternateMobile;
    this.billingModeID = billingModeID;
    this.contractNumber = contractNumber;
    this.billingCode = billingCode;
    this.paymentDetails = paymentDetails;
    this.userIDs = userIDs;
    this.parentResellerID = parentResellerID;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    Reseller reseller = (Reseller) value;
    Struct struct = new Struct(schema);
    packCommon(struct, reseller);
    struct.put("website", reseller.getWebsite());
    struct.put("phone", reseller.getPhone());
    struct.put("email", reseller.getEmail());
    struct.put("contactPerson", reseller.getContactPerson());
    struct.put("address", reseller.getAddress());
    struct.put("mobile", reseller.getMobile());
    struct.put("alternateMobile", reseller.getAlternateMobile());
    struct.put("billingModeID", reseller.getBillingModeID());
    struct.put("contractNumber", reseller.getContractNumber());
    struct.put("billingCode", reseller.getBillingCode());
    struct.put("paymentDetails", reseller.getPaymentDetails());
    struct.put("userIDs", reseller.getUserIDs());
    struct.put("parentResellerID", reseller.getParentResellerID());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static Reseller unpack(SchemaAndValue schemaAndValue)
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
    String billingModeID = valueStruct.getString("billingModeID");
    String contractNumber = valueStruct.getString("contractNumber");
    String billingCode = valueStruct.getString("billingCode");
    String paymentDetails = valueStruct.getString("paymentDetails");    
    List<String> userIDs = (schemaVersion >= 2) ? (List<String>) valueStruct.get("userIDs"):new ArrayList<String>();
    String parentResellerID = (schemaVersion >= 3) ? valueStruct.getString("parentResellerID") : null;
    //List<String> users = schemaVersion >= 2 ? unpackTokenChanges(schema.field("users").schema(), valueStruct.get("users")) : new ArrayList<TokenChange>();

    
    //
    //  return
    //

    return new Reseller(schemaAndValue, website, phone, email, contactPerson, address, mobile, alternateMobile, billingModeID, contractNumber, billingCode, paymentDetails, userIDs, parentResellerID);
  }
  
  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public Reseller(JSONObject jsonRoot, long epoch, GUIManagedObject existingResellerUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingResellerUnchecked != null) ? existingResellerUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingReseller
    *
    *****************************************/

    Reseller existingReseller = (existingResellerUnchecked != null && existingResellerUnchecked instanceof Reseller) ? (Reseller) existingResellerUnchecked : null;
    
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
    this.billingModeID = JSONUtilities.decodeString(jsonRoot, "billingModeID", false);
    this.contractNumber = JSONUtilities.decodeString(jsonRoot, "contractNumber", false);
    this.billingCode = JSONUtilities.decodeString(jsonRoot, "billingCode", false);
    this.paymentDetails = JSONUtilities.decodeString(jsonRoot, "paymentDetails", false);
    this.userIDs = decodeUsers(JSONUtilities.decodeJSONArray(jsonRoot, "userIDs", false));
    this.parentResellerID = JSONUtilities.decodeString(jsonRoot, "parentResellerID", false);

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingReseller))
      {
        this.setEpoch(epoch);
      }
  }
  /*****************************************
  *
  *  decodeIDs
  *
  *****************************************/

  private List<String> decodeUsers(JSONArray jsonArray)
  {
    List<String> userIDs = null;
    if (jsonArray != null)
      {
        userIDs = new ArrayList<String>();
        for (int i=0; i<jsonArray.size(); i++)
          {
            String ID = (String) jsonArray.get(i);
            userIDs.add(ID);
          }
      }
    return userIDs;
  }


  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(Reseller existingReseller)
  {
    if (existingReseller != null && existingReseller.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingReseller.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(getWebsite(), existingReseller.getWebsite());
        epochChanged = epochChanged || ! Objects.equals(getPhone(), existingReseller.getPhone());
        epochChanged = epochChanged || ! Objects.equals(getEmail(), existingReseller.getEmail());
        epochChanged = epochChanged || ! Objects.equals(getContactPerson(), existingReseller.getContactPerson());
        epochChanged = epochChanged || ! Objects.equals(getAddress(), existingReseller.getAddress());
        epochChanged = epochChanged || ! Objects.equals(getMobile(), existingReseller.getMobile());
        epochChanged = epochChanged || ! Objects.equals(getAlternateMobile(), existingReseller.getAlternateMobile());
        epochChanged = epochChanged || ! Objects.equals(getBillingModeID(), existingReseller.getBillingModeID());
        epochChanged = epochChanged || ! Objects.equals(getContractNumber(), existingReseller.getContractNumber());
        epochChanged = epochChanged || ! Objects.equals(getBillingCode(), existingReseller.getBillingCode());
        epochChanged = epochChanged || ! Objects.equals(getPaymentDetails(), existingReseller.getPaymentDetails());
        epochChanged = epochChanged || ! Objects.equals(getUserIDs(), existingReseller.getUserIDs());
        epochChanged = epochChanged || ! Objects.equals(getParentResellerID(), existingReseller.getParentResellerID());
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

  public void validate(ResellerService resellerService, Date date) throws GUIManagerException
  {
    /*****************************************
    *
    *  validate
    *
    *****************************************/
    
    
    //
    //  ensure billingModeID (if specified)
    //
    
    if (billingModeID != null)
      {
        BillingMode billingMode = Deployment.getBillingModes().get(billingModeID);
        if (billingMode == null) throw new GUIManagerException("unknown billingModeID ", billingModeID);
      } 
  }
  
  @Override public Map<String, List<String>> getGUIDependencies()
  {
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    return result;
  }
}
