package com.evolving.nglm.evolution.otp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.commons.lang3.time.DateUtils;
// import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.EvolutionUtilities;
import com.evolving.nglm.evolution.RESTAPIGenericReturnCodes;
import com.evolving.nglm.evolution.SourceAddressService;
import com.evolving.nglm.evolution.SubscriberEvaluationRequest;
import com.evolving.nglm.evolution.SubscriberMessageTemplateService;
import com.evolving.nglm.evolution.SubscriberProfile;
import com.evolving.nglm.evolution.SubscriberState;
import com.evolving.nglm.evolution.ContactPolicyCommunicationChannels.ContactType;
import com.evolving.nglm.evolution.DialogTemplate;
import com.evolving.nglm.evolution.otp.OTPInstance.OTPStatus;
import com.rii.utilities.SystemTime;

public class OTPUtils
{


	  /**************************************************************
	   *
	   * STATIC VARIABLES/ENUMS OF CLASS
	   *
	   **************************************************************/
	
  private static Random random = new Random();
  private static final Logger log = LoggerFactory.getLogger(OTPUtils.class);

  private static OTPTypeService otpTypeService;
  static
  {
    otpTypeService = new OTPTypeService(System.getProperty("broker.servers"), "otpinstance-otptypeservice", Deployment.getOTPTypeTopic(), false);
    otpTypeService.start();
  }

  private static List<OTPStatus> statusListValidToInvalidate = new ArrayList<OTPStatus>();
  static
  {
	  statusListValidToInvalidate.add(OTPStatus.ChecksError);
	  statusListValidToInvalidate.add(OTPStatus.ChecksSuccess);
	  statusListValidToInvalidate.add(OTPStatus.New);
  }

  // local comparator used later to extract the active/latest instance
  private static class OTPCreationDateComparator implements Comparator<OTPInstance>
  {
    @Override
    public int compare(OTPInstance o1, OTPInstance o2)
    {
      return o1.getCreationDate().compareTo(o2.getCreationDate());
    }
  }



  /**************************************************************
   *
   * Evolution Engine message handling SECTION
   *	basically ee calls handleOTPEvent with an OTPInstanceChangeEvent request and expects an OTPInstanceChangeEvent response and depending on the Action this reroutes to either
   *	generateOTP : generates a new otp and invalidate any previous otp
   *	checkOTP : tries to validate a code on the active otp
   **************************************************************/

  
  // called by EvolutionEngine when receiving an event from thirdparty message
  public static OTPInstanceChangeEvent handleOTPEvent(OTPInstanceChangeEvent otpRequest, SubscriberState subscriberState, OTPTypeService otpTypeService, SubscriberMessageTemplateService subscriberMessageTemplateService, SourceAddressService sourceAddressService, SubscriberEvaluationRequest subscriberEvaluationRequest, EvolutionEventContext evolutionEventContextint,int tenantID)
  {
    switch (otpRequest.getAction())
      {
      case Check:
        return checkOTP(otpRequest, subscriberState.getSubscriberProfile(), otpTypeService, tenantID);
      case Generate:
        return generateOTP(otpRequest, subscriberState, otpTypeService, subscriberMessageTemplateService, sourceAddressService, subscriberEvaluationRequest, evolutionEventContextint, tenantID);
      case Burn:
        // pending story but will come soon ...
        return invalidateAllOTPsFromProfile(otpRequest, subscriberState.getSubscriberProfile(), otpTypeService, tenantID);
      default:
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
        return otpRequest;
      }
  }


  
  // no known call yet but meant to force invalidate all otp when a security issue asked to invalidate them
  public static OTPInstanceChangeEvent invalidateAllOTPsFromProfile(OTPInstanceChangeEvent otpRequest, SubscriberProfile profile, OTPTypeService otpTypeService, int tenantID)
  {
    Date now = new Date();

    List<OTPInstance> candidates = profile.getOTPInstances().stream().filter(c -> statusListValidToInvalidate.contains(c.getOTPStatus())).collect(Collectors.toList());
    List<OTPInstance> nonCandidates = profile.getOTPInstances().stream().filter(c -> !statusListValidToInvalidate.contains(c.getOTPStatus())).collect(Collectors.toList());

    for (OTPInstance updateOtp : candidates)
      {
        updateOtp.setOTPStatus(OTPStatus.Expired);
        updateOtp.setLatestUpdate(now);
      }

    List<OTPInstance> otpToStore = new ArrayList<OTPInstance>();
    otpToStore.addAll(candidates);
    otpToStore.addAll(nonCandidates);
    profile.setOTPInstances(otpToStore);
    otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.SUCCESS);
    return otpRequest;
  }

  private static void markAsError(OTPInstance instanceToError, OTPStatus errorToUseForInstance, boolean allowEscalateInstanceStatus, 
		  OTPInstanceChangeEvent requestToError, RESTAPIGenericReturnCodes errorToUseForRequest, boolean allowEscalateRequestStatus,
		  OTPType otptype, List<OTPInstance> sameTypeInstancesList, Date updateDate
		  ) {
      // error on the check : update the counters
	  instanceToError.setOTPStatus(errorToUseForInstance);
	  instanceToError.setErrorCount(instanceToError.getErrorCount() + 1);
	  instanceToError.setLatestError(updateDate);
	  instanceToError.setLatestUpdate(updateDate);
	  requestToError.setReturnStatus(errorToUseForRequest);

	  // collecting counters and checking for higher error:
      int remainingAttempts = otptype.getMaxWrongCheckAttemptsByInstance() - instanceToError.getErrorCount();
      requestToError.setRemainingAttempts(remainingAttempts);
      int globalerrorstimewindow = sameTypeInstancesList.stream().filter(c -> (c.getLatestError() != null && DateUtils.addSeconds(c.getLatestError(), otptype.getTimeWindow()).after(updateDate))).mapToInt(o -> o.getErrorCount()).sum();
      requestToError.setGlobalErrorCounts(globalerrorstimewindow);
      
      // check maxed this instance 
      //
      //
      if ( allowEscalateInstanceStatus && remainingAttempts <= 0 ) {
    	  requestToError.setRemainingAttempts(0); // should be 0 with standard workflow but let's have a clean return message 
    	  instanceToError.setOTPStatus(OTPStatus.RaisedBan);
      }
      if ( allowEscalateRequestStatus  && remainingAttempts <= 0 ) requestToError.setReturnStatus(RESTAPIGenericReturnCodes.MAX_NB_OF_ATTEMPT_REACHED);
      
      // check global counter for all candidates of the type
      if ( allowEscalateInstanceStatus && globalerrorstimewindow >= otptype.getMaxWrongCheckAttemptsByTimeWindow() ) {
    	  requestToError.setRemainingAttempts(0); // maybe more attemps left before but since we ban lets tell nore are left
	  instanceToError.setOTPStatus(OTPStatus.RaisedBan);
      }
      if ( allowEscalateRequestStatus  && globalerrorstimewindow >= otptype.getMaxWrongCheckAttemptsByTimeWindow() ) requestToError.setReturnStatus(RESTAPIGenericReturnCodes.MAX_NB_OF_ATTEMPT_REACHED);   
  }
  // 
  public static OTPInstanceChangeEvent checkOTP(OTPInstanceChangeEvent otpRequest, SubscriberProfile profile, OTPTypeService otpTypeService, int tenantID)
  {
    try {
	// make sure we'll have a return status since "future" filters on not "New"
	otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
		  
	if (profile == null)
      {
        // probably already raised before it reaches this section but for safety reasons
        // lets reject
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
        return otpRequest;
      }
    Date now = new Date();

    // check and retrieve otpType
    OTPType otptype = otpTypeService.getActiveOTPTypeByName(otpRequest.getOTPTypeName(), tenantID);
    if (otptype == null)
      {
<<<<<<< HEAD
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.INVALID_OTP);
        // TODO decide of the correct error to return, ELEMENT_NOT_FOUND was used for generation
=======
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.ELEMENT_NOT_FOUND);
        // TODO decide of the correct error to return
>>>>>>> fd29423d16f129e251131457f3a7d56bfc45654c
        return otpRequest;
      }

    // maybe extent checks to many more empty-like values ...
    if (otpRequest.getOTPCheckValue() == null)
      {
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.MISSING_PARAMETERS);
        return otpRequest;
      }

    List<OTPInstance> candidates = profile.getOTPInstances().stream().filter(c -> c.getOTPTypeDisplayName().equals(otpRequest.getOTPTypeName())).collect(Collectors.toList());

    // GLOBAL CHECKS
    // Check 01 at least one of the requested type
    if ( candidates == null || candidates.isEmpty()) {
        // candidates is empty : no generate called probably hence raising INVALID
        log.debug("No max candidate for the given otptype. Maybe check without generate (or already cleaned).");
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.INVALID_OTP);// or maybe MISSING_PARAMETERS);
        return otpRequest;
    }
    
    OTPInstance tomatch;
    tomatch = Collections.max(candidates, new OTPCreationDateComparator());

    // Check 02 : not during a ban issue
    // testing only the latest should be enough
    if (tomatch.getOTPStatus().equals(OTPStatus.RaisedBan) && DateUtils.addSeconds(tomatch.getLatestUpdate(), otptype.getBanPeriod()).after(now))
          {
            otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.CUSTOMER_NOT_ALLOWED);
            return otpRequest;
          }

    // Check 03 : check if expired
    if (tomatch.getExpirationDate() == null || tomatch.getExpirationDate().before(now))
      {
    	// not seen as an error just leave everything as it is and reply expired.
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.OTP_EXPIRED);
        return otpRequest;
      }
    
    // Check 04 : already maxed out instance
    if (tomatch.getOTPStatus().equals(OTPStatus.MaxNbReached))
    {
      // leaves as it, does not increment error count
      otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.MAX_NB_OF_ATTEMPT_REACHED);
      return otpRequest;
    }
    
    // Check 05 : check if already burnt with success
    if (tomatch.getOTPStatus().equals(OTPStatus.Burnt))
      {
    	// keep burnt but count the error and possibly ban since this could be seen as invalid
    	markAsError(tomatch, OTPStatus.Burnt, false, 
    			otpRequest, RESTAPIGenericReturnCodes.OTP_BURNT, true, 
    			otptype, candidates, now);
        return otpRequest;
      }

    // Check 06 : finally check the value
    //
    // for information but not explictery checked :
    // expected remaining status : New (no attempt yet), ChecksError (latest attempt
    // was a failure but still allowed to try), ChecksSuccess (latest attempt was a
    // success but still alowed to recheck)
    if (tomatch.getOTPValue().equals(otpRequest.getOTPCheckValue()))
      {
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.SUCCESS);
        tomatch.setChecksCount(tomatch.getChecksCount() + 1);
        tomatch.setLatestSuccess(now);
        tomatch.setLatestUpdate(now);
        tomatch.setOTPStatus(OTPStatus.ChecksSuccess); // for now may allow re-checks
        
        // mark as Burnt if requested (will disable future checks)
        if (otpRequest.getForceBurn() != null && otpRequest.getForceBurn()) tomatch.setOTPStatus(OTPStatus.Burnt);
      }
    else 
      {
    	// Mismatch => mark as error with possible escalation
    	markAsError(tomatch, OTPStatus.ChecksError, true,
    			otpRequest, RESTAPIGenericReturnCodes.INVALID_OTP, true,
    			otptype,candidates, now);
      }
    }
    catch (Exception e) {
    	otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
    }
    return otpRequest;
  }

  // OTP clearance for daily customer polling
  // TODO : check this is included in a workflow that updates the profile afterwards
  public static void clearOldOTPs(SubscriberProfile profile, OTPTypeService otpTypeService, int tenantID)
  {
    Date now = new Date();
    List<OTPInstance> otps = profile.getOTPInstances();
    List<OTPInstance> newOtps = new ArrayList<OTPInstance>();

    // if profile has nothing, then nothing to do
    if (otps.isEmpty()) return;

    // not empty, lets check for elements that are older than the timewindow or banperiod
    for (OTPInstance oldOtp : otps)
      {
    	try {
        OTPType otpType = otpTypeService.getActiveOTPTypeByName(oldOtp.getOTPTypeDisplayName(), tenantID);
        // otp with a no longer active type ... can be deleted/skipped
        if (otpType == null) continue;
        // clean/skip malformed with no creation date
        if ( oldOtp.getCreationDate() == null ) continue;
        
        // otp outside relevant time windows can be deleted/skipped
        // (and not banned since ban has its own window) 
        if ( DateUtils.addSeconds(oldOtp.getCreationDate(), otpType.getTimeWindow()).before(now) && oldOtp.getExpirationDate().before(now) && oldOtp.getOTPStatus() != OTPStatus.RaisedBan) continue;

        // otp banned but banperiod outlonged ... can be deleted
        if (oldOtp.getOTPStatus() == OTPStatus.RaisedBan && DateUtils.addSeconds(oldOtp.getLatestUpdate(), otpType.getBanPeriod()).before(now)) continue;
        
        // not in a "terminal" status but passed the expiration, keep for global counts
        // but set it to expired
        if (oldOtp.getOTPStatus() != OTPStatus.Expired && oldOtp.getOTPStatus() != OTPStatus.Burnt && oldOtp.getOTPStatus() != OTPStatus.RaisedBan && oldOtp.getExpirationDate().before(now))
          {
            oldOtp.setLatestUpdate(now);
            oldOtp.setOTPStatus(OTPStatus.Expired);
          }
        
        // all other cases :
        // no update just keep it unmodified
    	} catch (Exception e) {
    		log.debug("Exception while trying to flag OTPInstance for cleaning. Keeping as is.");
    	}
    	
        // passed the criterias to be kept and updates already done, let's store :
        newOtps.add(oldOtp);
      }
    profile.setOTPInstances(newOtps);
  }

  // OTP Creation
  public static OTPInstanceChangeEvent generateOTP(OTPInstanceChangeEvent otpRequest, SubscriberState subscriberState, OTPTypeService otpTypeService, SubscriberMessageTemplateService subscriberMessageTemplateService, SourceAddressService sourceAddressService, SubscriberEvaluationRequest subscriberEvaluationRequest, EvolutionEventContext evolutionEventContext, int tenantID)
  {
try {
    if (subscriberState == null)
      {
        // probably already raised before it reaches this section but for safety reasons
        // lets reject
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
        return otpRequest;
      }
    SubscriberProfile profile = subscriberState.getSubscriberProfile();
    Date now = SystemTime.getCurrentTime();

    // check and retrieve otpType
    OTPType otptype = otpTypeService.getActiveOTPTypeByName(otpRequest.getOTPTypeName(), tenantID);
    if (otptype == null)
      {
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.ELEMENT_NOT_FOUND);
        return otpRequest;
      }

    	// check for previous elements of the same type to invalidate them or forbid current action :
		// testing only the latest should be enough
        List<OTPInstance> initialOtpList = profile.getOTPInstances().stream().filter(c -> c.getOTPTypeDisplayName().equals(otpRequest.getOTPTypeName())).collect(Collectors.toList());

        if ( initialOtpList != null && !initialOtpList.isEmpty() ) {
    		OTPInstance mostRecentOtp = Collections.max(initialOtpList, new OTPCreationDateComparator());

    		// Check 01 : not during a ban issue
    		if (mostRecentOtp.getOTPStatus().equals(OTPStatus.RaisedBan) && DateUtils.addSeconds(mostRecentOtp.getLatestUpdate(), otptype.getBanPeriod()).after(now))
    		{
    			otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.CUSTOMER_NOT_ALLOWED);
    			return otpRequest;
    		}


    // Check 02 : not have asked too many elements of the given type within the timewindow
    if (otptype.getMaxConcurrentWithinTimeWindow() <= initialOtpList.stream().filter(c -> c.getOTPTypeDisplayName().equals(otptype.getOTPTypeName()) && DateUtils.addSeconds(c.getCreationDate(), otptype.getTimeWindow()).after(now)).count())
      {
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.CUSTOMER_NOT_ALLOWED);
        return otpRequest;
      }

    // Invalidate all PREVIOUS INSTANCES that may still be active
    for (OTPInstance previous : initialOtpList)
      {
        if (statusListValidToInvalidate.contains(previous.getOTPStatus()))
          {
            previous.setOTPStatus(OTPStatus.Expired);
            previous.setLatestUpdate(now);
          }
      }
    }
    // OK to proceed
         
    // generating an otp VALUE STring according to type content :
    String otpValue = null;
    if (otptype.getValueGenerationRegex() != null)
      {
        otpValue = generateFromRegex(otptype.getValueGenerationRegex());
      }
    else if (otptype.getValueGenerationDigits() != null && otptype.getValueGenerationDigits() > 0)
      {
        otpValue = generateNonZeroLeadingOTPValue(otptype.getValueGenerationDigits());
      }
    else
      {
        log.debug("Impossible to generate a code for otp : no generation method filled in GUI OTPType object content.");
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
        return otpRequest;
      }
    OTPInstance otpInstance = new OTPInstance(otptype.getDisplay(), OTPStatus.New, otpValue, 0, 0, now, now, null, null, DateUtils.addSeconds(now, otptype.getInstanceExpirationDelay()));

    // put the relevant content of this instance in the returning event
    List<OTPInstance> existingInstances = profile.getOTPInstances();
    // ( force profile.setOTPInstances since it could have been null pointer and not an existing List )
    if (existingInstances == null) existingInstances = new ArrayList<OTPInstance>();
    existingInstances.add(otpInstance);
    profile.setOTPInstances(existingInstances);

    // send the notification ( sms )
    Map<String, String> tags = new HashMap<>();
    tags.put("otpCode", otpInstance.getOTPValue());
    List<Pair<DialogTemplate, String>> templates = EvolutionUtilities.getNotificationTemplateForAreaAvailability("oneTimePassword", subscriberMessageTemplateService, sourceAddressService, tenantID);
    for(Pair<DialogTemplate, String> template : templates)
      {
        EvolutionUtilities.sendMessage(evolutionEventContext, tags, template.getFirstElement().getDialogTemplateID(), ContactType.ActionNotification, template.getSecondElement(), subscriberEvaluationRequest, subscriberState, otpRequest.getFeatureID(), otpRequest.getModuleID());
      }
    
    // prepare response
    otpRequest.setRemainingAttempts(otptype.getMaxWrongCheckAttemptsByInstance());
    otpRequest.setValidityDuration(otptype.getInstanceExpirationDelay());
    otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.SUCCESS);

    otpRequest.setOTPCheckValue(otpValue); // at least for debug now, but should not be returned to the customer...    

} catch (Exception e ){
	log.debug("Exception During : "+e.getMessage());
	otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
}

	return otpRequest;
  }

  /**************************************************************
   *
   * TOOLS for instance's otp codes generation
   * (See if this can be mutualized with TokenUtils class ... )
   *
   **************************************************************/


  // sample code generators
  private static String generateNonZeroLeadingOTPValue(int length)
  {
    if (length > 1)
      {
        return generateFromRegex("[123456789][0123456789]{" + String.valueOf(length - 1) + "}");
      }
    else if (length == 1)
      {
        return generateFromRegex("[123456789]");
      }
    else
      {
        return null;
      }
  }

  private static String generateDigitsOTPValue(int length)
  {
    if (length > 0)
      {
        StringBuilder result = new StringBuilder();
        generateRandomString(new StringBuilder("0123456789"), length, result);
        return result.toString();
      }
    else
      {
        return null;
      }
  }

  // (See if this can be mutualized with TokenUtils class ... )
  /*****************************************
   *
   * Finite-state machine State
   *
   *****************************************/

  private enum State
  {
    REGULAR_CHAR, REGULAR_CHAR_AFTER_BRACKET, LEFT_BRACKET, LEFT_BRACE, ERROR;
  }

  private static void generateRandomString(StringBuilder possibleValues, int length, StringBuilder result)
  {
    String values = possibleValues.toString();
    for (int i = 0; i < length; i++)
      {
        int index = random.nextInt(values.length());
        result.append(values.substring(index, index + 1));
      }
  }

  // Duplicate code taken from TokenUtils :
  /*****************************************
   *
   * generateFromRegex
   *
   *****************************************/
  /**
   * Generate a string that matches the regex passed in parameter. Code supports
   * simple patterns, such as : [list of chars]{numbers} ( "{numbers}" can be
   * omitted and defaults to 1) There can be as many of these patterns as needed,
   * mixed with regular chars that are not interpreted. Example of valid regex :
   * gl-[0123456789abcdef]{5}-[01234] [ACDEFGHJKMNPQRTWXY34679]{5} tc-[
   * 123456789][0123456789]{7}-tc
   * 
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

}
