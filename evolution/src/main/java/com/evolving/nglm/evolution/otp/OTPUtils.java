package com.evolving.nglm.evolution.otp;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Future;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.DeliveryManagerDeclaration;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.RESTAPIGenericReturnCodes;
import com.evolving.nglm.evolution.SubscriberProfile;
import com.evolving.nglm.evolution.DeliveryRequest.Module;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentStatus;
import com.evolving.nglm.evolution.ThirdPartyManager.ThirdPartyManagerException;
import com.evolving.nglm.evolution.complexobjects.ComplexObjectInstance;
import com.evolving.nglm.evolution.complexobjects.ComplexObjectTypeSubfield;
import com.evolving.nglm.evolution.datamodel.DataModelFieldValue;
import com.evolving.nglm.evolution.otp.OTPInstance.OTPStatus;

public class OTPUtils
{  
  
  private static Random random = new Random();
  private static final Logger log = LoggerFactory.getLogger(OTPUtils.class);
    
  private static OTPTypeService otpTypeService;
  
  static
  {
	  otpTypeService = new OTPTypeService(System.getProperty("broker.servers"), "otpinstance-otptypeservice", Deployment.getOTPTypeTopic(), false);
	  otpTypeService.start();
  }  
  
  
  
  private class OTPCreationDateComparator implements Comparator<OTPInstance> {
	    @Override
	    public int compare(OTPInstance o1, OTPInstance o2) {
	        return o1.getCreationDate().compareTo(o2.getCreationDate());
	    }
	}


	
  private OTPInstance retrieveDummyOTPInstance() {
	  Date now = new Date();
	  return new OTPInstance("Dummy",OTPStatus.New,"DUMMY",0,0,now,now,null, null,DateUtils.addYears(now,1));
  }
  

  private List<OTPInstance> retrieveDummyOTPInstancesList() {
	  ArrayList<OTPInstance> list = new ArrayList<OTPInstance>();
	  list.add(retrieveDummyOTPInstance());
	  list.add(retrieveDummyOTPInstance());
	  return list;
  }

  private OTPType retrieveDummyOTPType() {
	  JSONObject fieldTypeJSON = new JSONObject();
	  // ?? ID ?? name ??
//	    this.jsonRepresentation = jsonRoot;
//	    this.guiManagedObjectID = JSONUtilities.decodeString(jsonRoot, "id", true);
//	    this.guiManagedObjectName = JSONUtilities.decodeString(jsonRoot, "name", false);
//	    this.guiManagedObjectDisplay = JSONUtilities.decodeString(jsonRoot, "display", false);
//	    this.guiManagedObjectType = guiManagedObjectType;
//	    this.epoch = epoch;
//	    this.effectiveStartDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", false));
//	    this.effectiveEndDate = parseDateField(JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", false));
//	    this.readOnly = JSONUtilities.decodeBoolean(jsonRoot, "readOnly", Boolean.FALSE);
//	    this.internalOnly = JSONUtilities.decodeBoolean(jsonRoot, "internalOnly", Boolean.FALSE);
//	    this.active = JSONUtilities.decodeBoolean(jsonRoot, "active", Boolean.TRUE);
//	    this.deleted = JSONUtilities.decodeBoolean(jsonRoot, "deleted", Boolean.FALSE);
//	    this.userID = JSONUtilities.decodeString(jsonRoot, "userID", false);
//	    this.userName = JSONUtilities.decodeString(jsonRoot, "userName", false);
//	    this.groupID = JSONUtilities.decodeString(jsonRoot, "groupID", false);
//	    this.createdDate = null;
//	    this.updatedDate = null;
//	    this.tenantID = tenantID;

	  fieldTypeJSON.put("id","666");
	  fieldTypeJSON.put("name","typename0");
	  fieldTypeJSON.put("displayName","DUMMY");
	  fieldTypeJSON.put("active",true);
	  fieldTypeJSON.put("maxWrongCheckAttemptsByInstance", 1);
	  fieldTypeJSON.put("maxWrongCheckAttemptsByTimeWindow", 10);
	  fieldTypeJSON.put("maxConcurrentWithinTimeWindow", 1);
	  fieldTypeJSON.put("timeWindow", 600);
	  fieldTypeJSON.put("banPeriod", 10800);
	  fieldTypeJSON.put("instanceExpirationDelay", 600);
	  fieldTypeJSON.put("valueGenerationDigits", 6);
	  fieldTypeJSON.put("valueGenerationRegex", "[123456789][0123456789]{5}");
	  OTPType returnedOTPType = new OTPType(fieldTypeJSON,System.currentTimeMillis(),1);
	  
	return 	returnedOTPType;
	
  }
  
  
  public OTPInstance checkOTP(SubscriberProfile profile, String otpType, String otpValue, int tenantID) {
	  return checkOTP(profile, otpType, otpValue, false, tenantID);
  }

  public OTPInstance checkOTP(SubscriberProfile profile, String otpType, String otpValue, Boolean forceBurn, int tenantID) {

//	  Collections.sort(relevantOTP, new OTPUtils.OTPCreationDateComparator());
//	    
//	  Database.arrayList.sort();
//	  
//	  
//	  
//	  Boolean flagNewError = false;
//	  int ttlerrorsinwindow = 0;
//	  Date now = new Date();
//	  
//	  ArrayList<OTPInstance> relevantOTP = new ArrayList<OTPInstance>();
//	  ArrayList<OTPInstance> nonRelevantOTP = new ArrayList<OTPInstance>();
//	  // retrieve OTP from customer
//	  for (OTPInstance otpinst : profile.getOTPInstances()) {
//		  if (otpinst.getOTPTypeDisplayName().equals(otpType)) {
//			  relevantOTP.add(otpinst);
//		  } else {
//			  nonRelevantOTP.add(otpinst);
//		  }
//	  }
//	  
//	  relevantOTP.sort((o1, o2) -> o1.getCreationDate().compareTo(o2.getCreationDate()));
//	  relevantOTP.size()
//			  
//	  
//	  
//	  
//	  
//	  
//		  }
//	  OTPInstance latestRelevant = null;
//	  OTPInstance previousRelevant = null;
//	  
//	  OTPType matchOtpType = otpTypeService.getActiveOTPTypeByName(otpType, tenantID);
//	  if (matchOtpType == null) return null; // TODO Raise Error ?
//	  
//	  
//	  
//	  // TODO retrieve OTP from customer
//	  for (OTPInstance otpinst : profile.getOTPInstances()) {
//		  
//		  if (otpinst.getOTPTypeDisplayName().equals(otpType)) {
//			  if (otpinst.getLatestError() != null && 
//					  now.before(DateUtils.addSeconds(otpinst.getLatestError(), matchOtpType.getTimeWindow())) ) {
//				  ttlerrorsinwindow += otpinst.getErrorCount();
//			  }
//			  if (latestRelevant == null) latestRelevant=otpinst;
//			  else if (previousRelevant == null) previousRelevant=otpinst;
//					  
//					  
//			  relevantOTP.add(otpinst);
//		  } else {
//			  nonRelevantOTP.add(otpinst);
//		  }
//	  }
//	  
//	  
//	    schemaBuilder.field("otpType", Schema.STRING_SCHEMA);
//	    schemaBuilder.field("otpStatus", Schema.STRING_SCHEMA);
//	    schemaBuilder.field("otpValue", Schema.STRING_SCHEMA);
//	    schemaBuilder.field("checksCount", Schema.INT32_SCHEMA);
//	    schemaBuilder.field("errorCount", Schema.INT32_SCHEMA);
//	    schemaBuilder.field("creationDate", Timestamp.builder().optional().schema());
//	    schemaBuilder.field("latestUpdate", Timestamp.builder().optional().schema());
//	    schemaBuilder.field("latestSuccess", Timestamp.builder().optional().schema());
//	    schemaBuilder.field("latestError", Timestamp.builder().optional().schema());
//	    schemaBuilder.field("expirationDate", Timestamp.builder().optional().schema());

//	  private Integer maxWrongCheckAttemptsByInstance;
//	  private Integer maxWrongCheckAttemptsByTimeWindow;
//	  private Integer maxConcurrentWithinTimeWindow;
//	  private Integer timeWindow;
//	  private Integer banPeriod;  // int32 seconds allows >60years
//	  private Integer instanceExpirationDelay;

	 
//	  
//	  
//	  OTPInstance otpBase = retrieveDummyOTPInstance();
//	  
//	  // TODO retrieve the type content
//	  // OTPType otpType = otpTypeService.getStoredOTPType(otpBase.getOTPTypeID());
//	  //OTPType otpType = retrieveDummyOTPType();
//	  
//	  
//	  Date mynow = new Date();
//	  OTPInstance otpUpdate = otpBase.copy();
//	  
//	  // TBD retrieve all other of same kind
//
//
//	  // TODO check latest(status=Banned) + banPeriod" < now
//	  // if (false) {
//	  // otpUpdate.setErrorCount(otpUpdate.getErrorCount() + 1);
//	  // otpUpdate.setOTPStatus(OTPStatus.RaisedBan);
//	  // flagNewError = true;
//	  // }
//	  
//	  // check if expired
//	  if ( !flagNewError && mynow.after(otpBase.getExpirationDate())) {
//		  otpUpdate.setOTPStatus(OTPStatus.Expired);
//		  otpUpdate.setErrorCount(otpUpdate.getErrorCount() + 1);
//		  flagNewError = true;
//	  } 
//	
//	  if (!flagNewError ) {
//	  // not expired
//		  
//		  if (otpBase.getOTPValue().equals(otpValue)) {
//			  otpUpdate.setChecksCount(otpUpdate.getChecksCount() + 1);
//			  otpUpdate.setOTPStatus(OTPStatus.ChecksSuccess);
//		  } else {
//			  otpUpdate.setErrorCount(otpUpdate.getErrorCount() + 1);
//			  otpUpdate.setOTPStatus(OTPStatus.ChecksError);
//			  flagNewError = true;
//		  }
//	  }
//		  
//		  // error or not, check if this is already too many errors for this one
//		  if (otpUpdate.getErrorCount() >= otpType.getMaxWrongCheckAttemptsByInstance()){
//			  otpUpdate.setOTPStatus(OTPStatus.Burnt);
//			  flagNewError = true;
//		  }
//		  
//		  if (flagNewError) {
//		  // TODO does this triggers global max check attempts
//		  // todo count( status in error ChecksError,Burnt,RaisedBan + date > now-timeWindow ) > maxWrongCheckAttemptsByTimeWindow
//		  //then  
//		  //		otpUpdate.setOTPStatus(OTPStatus.RaisedBan);
//		  }
//
//	  // global update
//	  otpUpdate.setLatestUpdate(mynow);
//	   
//	  // TODO push updates to topic
//	  // TODO SYNC ?
//	  
//	  return otpUpdate; // somehow status 
	  return null;
  }

  
//  public static void dummy1 (String moduleID, String featureID, String origin, String resellerID, KafkaProducer<byte[],byte[]> kafkaProducer, int tenantID) throws ThirdPartyManagerException
//  {
//    DeliveryManagerDeclaration deliveryManagerDeclaration = Deployment.getDeliveryManagers().get(PURCHASE_FULFILLMENT_MANAGER_TYPE);
//    if (deliveryManagerDeclaration == null)
//      {
//        log.error("Internal error, cannot find a deliveryManager with a RequestClassName as com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest");
//        throw new ThirdPartyManagerException(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
//      }
//
//    Serializer<StringKey> keySerializer = StringKey.serde().serializer();
//    Serializer<PurchaseFulfillmentRequest> valueSerializer = ((ConnectSerde<PurchaseFulfillmentRequest>) deliveryManagerDeclaration.getRequestSerde()).serializer();
//    
//    String deliveryRequestID = zuks.getStringKey();
//    // Build a json doc to create the PurchaseFulfillmentRequest
//    HashMap<String,Object> request = new HashMap<String,Object>();
//    request.put("subscriberID", subscriberID);
//    request.put("offerID", offerID);
//    request.put("quantity", quantity);
//    request.put("salesChannelID", salesChannelID); 
//    request.put("deliveryRequestID", deliveryRequestID);
//    request.put("eventID", "event from " + Module.fromExternalRepresentation(moduleID).toString()); // No event here
//    request.put("moduleID", moduleID);
//    request.put("featureID", featureID);
//    request.put("origin", origin);
//    request.put("resellerID", resellerID);
//    request.put("deliveryType", deliveryManagerDeclaration.getDeliveryType());
//    JSONObject valueRes = JSONUtilities.encodeObject(request);
//    
//    PurchaseFulfillmentRequest purchaseRequest = new PurchaseFulfillmentRequest(subscriberProfile,subscriberGroupEpochReader,valueRes, deliveryManagerDeclaration, offerService, paymentMeanService, resellerService, productService, supplierService, voucherService, SystemTime.getCurrentTime(), tenantID);
//    purchaseRequest.forceDeliveryPriority(DELIVERY_REQUEST_PRIORITY);
//    String topic = deliveryManagerDeclaration.getRequestTopic(purchaseRequest.getDeliveryPriority());
//
//    Future<PurchaseFulfillmentRequest> waitingResponse=null;
//    if(sync){
//      waitingResponse = purchaseResponseListenerService.addWithOnValueFilter(purchaseResponse->!purchaseResponse.isPending()&&purchaseResponse.getDeliveryRequestID().equals(deliveryRequestID));
//    }
//
//    kafkaProducer.send(new ProducerRecord<byte[],byte[]>(
//        topic,
//        keySerializer.serialize(topic, new StringKey(deliveryRequestID)),
//        valueSerializer.serialize(topic, purchaseRequest)
//        ));
//    keySerializer.close(); valueSerializer.close(); // to make Eclipse happy
//    if (sync) {
//      PurchaseFulfillmentRequest result =  handleWaitingResponse(waitingResponse);
//        if (result != null)
//          {
//            if (result.getStatus().getReturnCode() == ((PurchaseFulfillmentStatus.PURCHASED).getReturnCode()))
//              {
//                return handleWaitingResponse(waitingResponse);
//              }
//            else
//              {
//                int returnCode = result.getStatus().getReturnCode();
//                String returnMessage = result.getStatus().name();
//                JSONObject additionalInformation = new JSONObject();
//                additionalInformation.put("deliveryRequestID", result.getDeliveryRequestID());
//                throw new ThirdPartyManagerException(returnMessage, returnCode, additionalInformation);
//              }
//          }
//    }
//    return purchaseRequest;
//
//  }
  
  

  
  
  // OTP Creation
  public OTPInstanceChangeEvent generateOTP(OTPInstanceChangeEvent otpRequest, SubscriberProfile profile, OTPTypeService otpTypeService, int tenantID) {
	  OTPType baseOtpType = otpTypeService.getActiveOTPTypeByName(otpRequest.getOTPTypeName(),tenantID);
	  if (baseOtpType == null) return null; // TODO raise error
	  
	  
	  // TODO check not banned not possible if not attached to customer hence attached profile
	  if (profile != null ) {
		  // TODO checks for already banned or maxed instances
		  
		 // TODO check1 latest ==ban + now < "banPeriod"

		 // TODO check2 count(  now - "timeWindow" ) < "maxConcurrentWithinTimeWindow"
		  
	  }
	  
	  String regex = baseOtpType.getValueGenerationRegex();
	  int nbdigits = baseOtpType.getValueGenerationDigits();
	  
	  String otpValue = null;
	  if (regex != null) {
	  	  otpValue = generateFromRegex(regex);
	  } else if (nbdigits>0) {
		  otpValue = generateNonZeroLeadingOTPValue(nbdigits);
	  } else {
		  // TODO raise error
		  return null;
  	  }
	  Date now = new Date();			 
	  OTPInstance otpInstance = new OTPInstance(otpRequest.getOTPTypeName(),OTPStatus.New,otpValue,0,0,now,now,null, null,DateUtils.addSeconds(now,baseOtpType.getInstanceExpirationDelay()));
	  
	  // TODO subscrbiber Profile to be updated
	  otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.SUCCESS);
	  return otpRequest;
	  
  }


  
  
  
  // sample code generators 
  private static String generateNonZeroLeadingOTPValue(int length)
  {
	  if (length>1) {
		  return generateFromRegex("[123456789][0123456789]{"+String.valueOf(length-1)+"}");
	  } else if (length==1){
		  return generateFromRegex("[123456789]");
      }else {
		  return null;
	  }
  }
  private static String generateDigitsOTPValue(int length)
  {
	  if (length>0) {
		  StringBuilder result= new StringBuilder();
		  generateRandomString(new StringBuilder("0123456789"), length, result);
		  return result.toString();
	  } else {
		  return null;
	  }
  }
  
 //  (See if this can be mutualized with TokenUtils class ... )
  /*****************************************
  *
  *    Finite-state machine State
  *
  *****************************************/

 private enum State
 {
   REGULAR_CHAR,
   REGULAR_CHAR_AFTER_BRACKET,
   LEFT_BRACKET,
   LEFT_BRACE,
   ERROR;
 }

  private static void generateRandomString(StringBuilder possibleValues, int length, StringBuilder result)
  {
	String values=possibleValues.toString();
    for (int i=0; i<length; i++)
      {
    	int index = random.nextInt(values.length());
        result.append( values.substring(index, index+1) );
      }
  }

  // Duplicate code taken from TokenUtils :
  /*****************************************
  *
  *  generateFromRegex
  *
  *****************************************/
 /**
  * Generate a string that matches the regex passed in parameter.
  * Code supports simple patterns, such as :
  *   [list of chars]{numbers}      ( "{numbers}" can be omitted and defaults to 1)
  * There can be as many of these patterns as needed, mixed with regular chars that are not interpreted.
  * Example of valid regex :
  *   gl-[0123456789abcdef]{5}-[01234]
  *   [ACDEFGHJKMNPQRTWXY34679]{5}
  *   tc-[ 123456789][0123456789]{7}-tc
  * @param regex regex to generate from.
  * @return generated string, or an error message if regex has an invalid syntax.
  */
 public static String generateFromRegex(String regex)
 {
   StringBuilder output = new StringBuilder();
   StringBuilder chooseFrom = new StringBuilder();
   StringBuilder numOccurences = new StringBuilder();
   State currentState = State.REGULAR_CHAR;

   for (char c : regex.toCharArray())
     {
       switch (currentState)
       {

         case REGULAR_CHAR:
           if (c == '[')
             {
               currentState = State.LEFT_BRACKET;
             }
           else if (c == '{')
             {
               currentState = State.LEFT_BRACE;
             } 
           else
             {
               output.append(String.valueOf(c)); // Regular char goes to output string
             }
           break;

         case REGULAR_CHAR_AFTER_BRACKET:
           if (c == '{')
             {
               currentState = State.LEFT_BRACE;
             } 
           else
             {
               generateRandomString(chooseFrom, 1, output); // No braces after [...] means 1 occurrence
               chooseFrom = new StringBuilder();
               if (c == '[')
                 {
                   currentState = State.LEFT_BRACKET;
                 } 
               else
                 {
                   output.append(String.valueOf(c));
                   currentState = State.REGULAR_CHAR;
                 }
             }
           break;

         case LEFT_BRACKET:
           if (c == ']')
             {
               currentState = State.REGULAR_CHAR_AFTER_BRACKET;
             } 
           else if (c == '{')
             {
               output.append("-INVALID cannot have '{' inside brackets");
               currentState = State.ERROR;
             } 
           else
             {
               chooseFrom.append(String.valueOf(c));
             }
           break;

         case LEFT_BRACE:
           if (c == '}')
             {
               if (numOccurences.length() == 0)
                 { 
                   output.append("-INVALID cannot have '{}'");
                   currentState = State.ERROR;
                 } 
               else if (chooseFrom.length() == 0)
                 {
                   output.append("-INVALID cannot have []{n}");
                   currentState = State.ERROR;
                 }
               else
                 {
                   int numberOfOccurrences = Integer.parseInt(numOccurences.toString());
                   generateRandomString(chooseFrom, numberOfOccurrences, output);
                   chooseFrom = new StringBuilder();
                   numOccurences = new StringBuilder();
                   currentState = State.REGULAR_CHAR;
                 }
             } 
           else if (c == '[')
             {
               output.append("-INVALID cannot have '[' inside braces");
               currentState = State.ERROR;
             } 
           else
             {
               numOccurences.append(String.valueOf(c));
             }
           break;

         case ERROR:
           return output.toString();
       }
     }

   //
   // Check final state (after processing all input)
   //
   switch (currentState)
   {
     case REGULAR_CHAR_AFTER_BRACKET:
       generateRandomString(chooseFrom, 1, output); // No braces after [...] means 1 occurrence
       break;
     case LEFT_BRACKET:
     case LEFT_BRACE:
       output.append("-INVALID cannot end with pending '{' or '['");
       break;
     case REGULAR_CHAR:
     case ERROR:
       break;
   }

   return output.toString();
 }
 
  
//  public static void setComplexObjectValue(SubscriberProfile profile, String complexTypeName, String elementID, String subfieldName, Object value) throws ComplexObjectException
//  {
//    Collection<ComplexObjectType> types = complexObjectTypeService.getActiveComplexObjectTypes(SystemTime.getCurrentTime(), profile.getTenantID());
//    ComplexObjectType type = null;
//    for(ComplexObjectType current : types) { if(current.getComplexObjectTypeName().equals(complexTypeName)) { type = current; break; } }
//    if(type == null) { throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_COMPLEX_TYPE, "Unknown " + complexTypeName); }
//    
//    // retrieve the subfield declaration
//    ComplexObjectTypeSubfield subfieldType = null;
//    for(ComplexObjectTypeSubfield currentField : type.getSubfields().values()){ if(currentField.getSubfieldName().equals(subfieldName)){ subfieldType = currentField; break; }} 
//    if(subfieldType == null) { throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_SUBFIELD, "Unknown " + subfieldName + " into " + complexTypeName);}
//    
//    // ensure the value's type is ok
//    if(value != null) {
//      switch (subfieldType.getCriterionDataType())
//        {
//        case IntegerCriterion:
//          // must be an Integer or a Long
//          if(!(value instanceof Integer) && !(value instanceof Long)){ throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_COMPLEX_TYPE.BAD_SUBFIELD_TYPE, "Provided value " + value + " should be of type Integer or Long"); }
//          break;
//          
//        case DateCriterion:
//          // must be of type Date
//          if(!(value instanceof Date)){ throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_COMPLEX_TYPE.BAD_SUBFIELD_TYPE, "Provided value " + value + " should be of type Date"); }
//          break;
//          
//        case StringCriterion:
//          // must be a String
//          if(!(value instanceof String)){ throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_COMPLEX_TYPE.BAD_SUBFIELD_TYPE, "Provided value " + value + " should be of type String"); }
//          
//          break;
//        case BooleanCriterion:
//          // must be a Boolean
//          if(!(value instanceof Boolean)){ throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_COMPLEX_TYPE.BAD_SUBFIELD_TYPE, "Provided value " + value + " should be of type Boolean"); }
//          
//          break;
//        case StringSetCriterion:
//          // must be a List<String>
//          if(!(value instanceof List)){ throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_COMPLEX_TYPE.BAD_SUBFIELD_TYPE, "Provided value " + value + " should be of type List<String>"); }
//        default:
//          break;
//        }
//    }
//    
//    // check if the element ID exists
//    if(!type.getAvailableElements().contains(elementID)){ throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_ELEMENT, "Unknown " + elementID + " into " + complexTypeName);}
//    
//    // check if the profile already has an instance for this type / element
//    List<ComplexObjectInstance> instances = profile.getComplexObjectInstances();
//    if(instances == null) { instances = new ArrayList<>(); profile.setComplexObjectInstances(instances);}
//    
//    ComplexObjectInstance instance = null;
//    for(ComplexObjectInstance current : instances) { if(current.getComplexObjectTypeID().equals(type.getComplexObjectTypeID()) && current.getElementID().equals(elementID)) { instance = current; break; }}
//    if(instance == null)
//      {
//        instance = new ComplexObjectInstance(type.getComplexObjectTypeID(), elementID);
//        instances.add(instance);        
//      }
//    
//    Map<String, DataModelFieldValue> valueSubFields = instance.getFieldValues();
//    if(value == null)
//      {
//        valueSubFields.remove(subfieldType.getSubfieldName());
//      }
//    else 
//      {
//        DataModelFieldValue valueSubField = new DataModelFieldValue(subfieldType.getSubfieldName(), subfieldType.getPrivateID(), value);
//        if(valueSubFields == null) { valueSubFields = new HashMap<>(); instance.setFieldValues(valueSubFields); }
//        valueSubFields.put(subfieldType.getSubfieldName(), valueSubField);
//      }
//  }
  
//  public static String getOTPString(SubscriberProfile profile, String otpTypeName, String elementID, String subfieldName) throws ComplexObjectException
//  {
//    Object value = getComplexObjectValue(profile, complexTypeName, elementID, subfieldName);
//    if(value != null && !(value instanceof String)) { throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.BAD_SUBFIELD_TYPE, "complexTypeName:" + complexTypeName + " elementID:" + elementID + " subfieldName:" + subfieldName + " is not a String but " + value.getClass().getName());}
//    return (String)value;
//	  return "Unimplemented";
//  }
  
//  public static Boolean getComplexObjectBoolean(SubscriberProfile profile, String complexTypeName, String elementID, String subfieldName) throws ComplexObjectException
//  {
//    Object value = getComplexObjectValue(profile, complexTypeName, elementID, subfieldName);
//    if(value == null) { return null; }
//    if(value != null && !(value instanceof Boolean)) { throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.BAD_SUBFIELD_TYPE, "complexTypeName:" + complexTypeName + " elementID:" + elementID + " subfieldName:" + subfieldName + " is not a boolean but " + value.getClass().getName());}
//    return ((Boolean)value).booleanValue();
//  }
  
//  public static Date getComplexObjectDate(SubscriberProfile profile, String complexTypeName, String elementID, String subfieldName) throws ComplexObjectException
//  {
//    Object value = getComplexObjectValue(profile, complexTypeName, elementID, subfieldName);
//    if(value != null && !(value instanceof Date)) { throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.BAD_SUBFIELD_TYPE, "complexTypeName:" + complexTypeName + " elementID:" + elementID + " subfieldName:" + subfieldName + " is not a Date but " + value.getClass().getName());}
//    return (Date)value;
//  }
//
//  public static Long getComplexObjectLong(SubscriberProfile profile, String complexTypeName, String elementID, String subfieldName) throws ComplexObjectException
//  {
//    Object value = getComplexObjectValue(profile, complexTypeName, elementID, subfieldName);
//    if(value == null) { return null; }
//    if(value != null && !(value instanceof Integer) && !(value instanceof Long)) { throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.BAD_SUBFIELD_TYPE, "complexTypeName:" + complexTypeName + " elementID:" + elementID + " subfieldName:" + subfieldName + " is not a long but " + value.getClass().getName());}
//    if(value instanceof Long) { return ((Long)value); }
//    if(value instanceof Integer) { return new Long((int)((Long)value).longValue()); }
//    return -1L; // should not happen...    
//  }
//  
//  public static Integer getComplexObjectInteger(SubscriberProfile profile, String complexTypeName, String elementID, String subfieldName) throws ComplexObjectException
//  {
//    Object value = getComplexObjectValue(profile, complexTypeName, elementID, subfieldName);
//    if(value == null) { return null; }
//    if(value != null && !(value instanceof Integer) && !(value instanceof Long)) { throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.BAD_SUBFIELD_TYPE, "complexTypeName:" + complexTypeName + " elementID:" + elementID + " subfieldName:" + subfieldName + " is not a long but " + value.getClass().getName());}
//    if(value instanceof Integer) { return ((Integer)value); }
//    if(value instanceof Long) { return (int)((Long)value).longValue(); }
//    return -1; // should not happen... 
//  }
//  
//  public static List<String> getComplexObjectStringSet(SubscriberProfile profile, String complexTypeName, String elementID, String subfieldName) throws ComplexObjectException
//  {
//    Object value = getComplexObjectValue(profile, complexTypeName, elementID, subfieldName);
//    if(value != null && !(value instanceof ArrayList)) { throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.BAD_SUBFIELD_TYPE, "complexTypeName:" + complexTypeName + " elementID:" + elementID + " subfieldName:" + subfieldName + " is not a StringSet but " + value.getClass().getName());}
//    return (List<String>) value; 
//  }
//  
//  private static Object getComplexObjectValue(SubscriberProfile profile, String complexTypeName, String elementID, String subfieldName) throws ComplexObjectException
//  {
//    Collection<ComplexObjectType> types = complexObjectTypeService.getActiveComplexObjectTypes(SystemTime.getCurrentTime(), profile.getTenantID());
//    ComplexObjectType type = null;
//    for(ComplexObjectType current : types) { if(current.getComplexObjectTypeName().equals(complexTypeName)) { type = current; break; } }
//    if(type == null) { throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_COMPLEX_TYPE, "Unknown " + complexTypeName); }
//
//    // retrieve the subfield declaration
//    ComplexObjectTypeSubfield subfieldType = null;
//    for(ComplexObjectTypeSubfield currentField : type.getSubfields().values()){ if(currentField.getSubfieldName().equals(subfieldName)){ subfieldType = currentField; break; }} 
//    if(subfieldType == null) { return ComplexObjectUtilsReturnCodes.UNKNOWN_SUBFIELD; }
//
//    // check if the element ID exists
//    if(!type.getAvailableElements().contains(elementID)){ throw new ComplexObjectException(ComplexObjectUtilsReturnCodes.UNKNOWN_ELEMENT, "Unknown " + elementID + " into " + complexTypeName);}
//    
//    // check if the profile already has an instance for this type / element
//    List<ComplexObjectInstance> instances = profile.getComplexObjectInstances();
//    if(instances == null) { return null;}
//    ComplexObjectInstance instance = null;
//    for(ComplexObjectInstance current : instances) { if(current.getElementID().equals(elementID)) { instance = current; break; }}
//    if(instance == null) { return null; }
//    Map<String, DataModelFieldValue> values = instance.getFieldValues();
//    if(values == null) { return null; }
//    DataModelFieldValue value = values.get(subfieldName);
//    if(value == null) { return null; }    
//    return value.getValue();
//  }

}
