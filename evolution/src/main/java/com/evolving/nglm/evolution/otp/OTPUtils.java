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

  private static Random random = new Random();
  private static final Logger log = LoggerFactory.getLogger(OTPUtils.class);

  private static OTPTypeService otpTypeService;

  static
    {
      otpTypeService = new OTPTypeService(System.getProperty("broker.servers"), "otpinstance-otptypeservice", Deployment.getOTPTypeTopic(), false);
      otpTypeService.start();
    }

  private static class OTPCreationDateComparator implements Comparator<OTPInstance>
  {
    @Override
    public int compare(OTPInstance o1, OTPInstance o2)
    {
      return o1.getCreationDate().compareTo(o2.getCreationDate());
    }
  }

//    // tests only :
//    private static OTPInstance retrieveDummyOTPInstance() {
//        Date now = new Date();
//        return new OTPInstance("Dummy", OTPStatus.New, "DUMMY", 0, 0, now, now, null, null, DateUtils.addYears(now, 1));
//    }
//
//    private static List<OTPInstance> retrieveDummyOTPInstancesList() {
//        ArrayList<OTPInstance> list = new ArrayList<OTPInstance>();
//        list.add(retrieveDummyOTPInstance());
//        list.add(retrieveDummyOTPInstance());
//        return list;
//    }
//
//    private static OTPType retrieveDummyOTPType() {
//        JSONObject fieldTypeJSON = new JSONObject();
//        fieldTypeJSON.put("id", "666");
//        fieldTypeJSON.put("name", "typename0");
//        fieldTypeJSON.put("displayName", "DUMMY");
//        fieldTypeJSON.put("active", true);
//        fieldTypeJSON.put("maxWrongCheckAttemptsByInstance", 1);
//        fieldTypeJSON.put("maxWrongCheckAttemptsByTimeWindow", 10);
//        fieldTypeJSON.put("maxConcurrentWithinTimeWindow", 1);
//        fieldTypeJSON.put("timeWindow", 600);
//        fieldTypeJSON.put("banPeriod", 10800);
//        fieldTypeJSON.put("instanceExpirationDelay", 600);
//        fieldTypeJSON.put("valueGenerationDigits", 6);
//        fieldTypeJSON.put("valueGenerationRegex", "[123456789][0123456789]{5}");
//        OTPType returnedOTPType = new OTPType(fieldTypeJSON, System.currentTimeMillis(), 1);
//        return returnedOTPType;
//    }

  public static OTPInstanceChangeEvent burnAllOTPsFromProfile(OTPInstanceChangeEvent otpRequest, SubscriberProfile profile, OTPTypeService otpTypeService, int tenantID)
  {
    Date now = new Date();

    // TODO make this list static
    List<OTPInstance.OTPStatus> statusListValidToBurn = new ArrayList<OTPInstance.OTPStatus>();
    statusListValidToBurn.add(OTPInstance.OTPStatus.ChecksError);
    statusListValidToBurn.add(OTPInstance.OTPStatus.ChecksSuccess);
    statusListValidToBurn.add(OTPInstance.OTPStatus.New);

    List<OTPInstance> candidates = profile.getOTPInstances().stream().filter(c -> statusListValidToBurn.contains(c.getOTPStatus())).collect(Collectors.toList());
    List<OTPInstance> nonCandidates = profile.getOTPInstances().stream().filter(c -> !statusListValidToBurn.contains(c.getOTPStatus())).collect(Collectors.toList());

    for (OTPInstance updateOtp : profile.getOTPInstances().stream().filter(c -> statusListValidToBurn.contains(c.getOTPStatus())).collect(Collectors.toList()))
      {
        updateOtp.setOTPStatus(OTPInstance.OTPStatus.Burnt);
        updateOtp.setLatestUpdate(now);
      }

    List<OTPInstance> otpToStore = new ArrayList<OTPInstance>();
    otpToStore.addAll(candidates);
    otpToStore.addAll(nonCandidates);
    profile.setOTPInstances(otpToStore);
    otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.SUCCESS);
    return otpRequest;
  }

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
        return burnOTPsFromGivenType(otpRequest, subscriberState.getSubscriberProfile(), otpTypeService, tenantID);
      default:
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.SYSTEM_ERROR);
        return otpRequest;
      }
  }

  public static OTPInstanceChangeEvent burnOTPsFromGivenType(OTPInstanceChangeEvent otpRequest, SubscriberProfile profile, OTPTypeService otpTypeService, int tenantID)
  {

    Date now = new Date();

    List<OTPInstance.OTPStatus> statusListValidToBurn = new ArrayList<OTPInstance.OTPStatus>();
    statusListValidToBurn.add(OTPInstance.OTPStatus.ChecksError);
    statusListValidToBurn.add(OTPInstance.OTPStatus.ChecksSuccess);
    statusListValidToBurn.add(OTPInstance.OTPStatus.New);

    List<OTPInstance> candidates = profile.getOTPInstances().stream().filter(c -> c.getOTPTypeDisplayName().equals(otpRequest.getOTPTypeName()) & statusListValidToBurn.contains(c.getOTPStatus())).collect(Collectors.toList());
    List<OTPInstance> nonCandidates = profile.getOTPInstances().stream().filter(c -> (!c.getOTPTypeDisplayName().equals(otpRequest.getOTPTypeName())) | (!statusListValidToBurn.contains(c.getOTPStatus()))).collect(Collectors.toList());

    for (OTPInstance updateOtp : profile.getOTPInstances().stream().filter(c -> statusListValidToBurn.contains(c.getOTPStatus())).collect(Collectors.toList()))
      {
        updateOtp.setOTPStatus(OTPInstance.OTPStatus.Burnt);
        updateOtp.setLatestUpdate(now);
      }

    List<OTPInstance> otpToStore = new ArrayList<OTPInstance>();
    otpToStore.addAll(candidates);
    otpToStore.addAll(nonCandidates);
    profile.setOTPInstances(otpToStore);
    otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.SUCCESS);
    return otpRequest;
  }

  public static OTPInstanceChangeEvent checkOTP(OTPInstanceChangeEvent otpRequest, SubscriberProfile profile, OTPTypeService otpTypeService, int tenantID)
  {

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
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.MISSING_PARAMETERS);
        // maybe RELATIONSHIP_NOT_FOUND would have been better ...
        return otpRequest;
      }

    // maybe extent check to many more empty-like values ...
    if (otpRequest.getOTPCheckValue() == null)
      {
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.MISSING_PARAMETERS);
        // maybe RELATIONSHIP_NOT_FOUND would have been better ...
        return otpRequest;
      }

    List<OTPInstance> candidates = profile.getOTPInstances().stream().filter(c -> c.getOTPTypeDisplayName().equals(otpRequest.getOTPTypeName())).collect(Collectors.toList());

    OTPInstance tomatch;

    // GLOBAL CHECKS
    // Check 01 : not during a ban issue
    // testing only the latest should be enough
    try
      {
        tomatch = Collections.max(candidates, new OTPCreationDateComparator());

        if (tomatch.getOTPStatus().equals(OTPInstance.OTPStatus.RaisedBan) && DateUtils.addSeconds(tomatch.getLatestUpdate(), otptype.getBanPeriod()).after(now))
          {
            // maybe add some other return values ??
            otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.CUSTOMER_NOT_ALLOWED);
            return otpRequest;
          }
      }
    catch (NoSuchElementException e)
      {
        // Check 02 at least one of the requested type
        // candidates is empty : none of the type hence raising INVALID
        log.info("debug check no max candidate");
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.INVALID_OTP);// or maybe MISSING_PARAMETERS);
        return otpRequest;
      }

    // Check 03 : already global errors for this type without even checking
    if (otptype.getMaxWrongCheckAttemptsByInstance() <= candidates.stream().filter(c -> DateUtils.addDays(c.getCreationDate(), otptype.getTimeWindow()).after(now)).mapToInt(o -> o.getErrorCount().intValue()).sum())
      {
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.CUSTOMER_NOT_ALLOWED);// or maybe
                                                                                   // MAX_NB_OF_ATTEMPT_REACHED);
        return otpRequest;
      }

    // Check 04 : check if expired
    if (tomatch.getExpirationDate().before(now))
      {
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.OTP_EXPIRED);
        // maybe add updates of the status (or just wait for daily event update
        // if (!tomatch.getOTPStatus().equals(OTPInstance.OTPStatus.Expired) &&
        // !tomatch.getOTPStatus().equals(OTPInstance.OTPStatus.RaisedBan)){
        // tomatch.setLatestUpdate(now);
        // tomatch.setOTPStatus(OTPInstance.OTPStatus.Expired);}
        return otpRequest;
      }

    // Check 05 : check if already burnt
    if (tomatch.getOTPStatus().equals(OTPInstance.OTPStatus.Burnt))
      {
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.OTP_EXPIRED);// could be INVALID_OTP, incoming status OTP_BURNT;
        // maybe add updates of the status (or just wait for daily event update
        // TODO decide if this should count as an additional error or not, so far yes
        tomatch.setErrorCount(tomatch.getErrorCount() + 1);
        tomatch.setLatestError(now);
        tomatch.setLatestUpdate(now);
        return otpRequest;
      }

    // Check 06 : finally check the value
    // expected remaining status : New (no attempt yet), ChecksError (latest attempt
    // was a failure but still allowed to try), ChecksSuccess (latest attempt was a
    // success but still alowed to recheck)
    if (tomatch.getOTPValue().equals(otpRequest.getOTPCheckValue()))
      {
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.SUCCESS);
        tomatch.setChecksCount(tomatch.getChecksCount() + 1);
        // BURN OR mark AS SUCCESS depending on forceBurn (SUCCESS means it will allow more rechecks)
        tomatch.setOTPStatus(OTPInstance.OTPStatus.ChecksSuccess);
        if (otpRequest.getForceBurn() != null && otpRequest.getForceBurn()) tomatch.setOTPStatus(OTPInstance.OTPStatus.Burnt);

        tomatch.setLatestSuccess(now);
        tomatch.setLatestUpdate(now);
      }
    else
      {
        // error on the check : update the counters
        tomatch.setOTPStatus(OTPInstance.OTPStatus.ChecksError); // for now simply an error
        tomatch.setErrorCount(tomatch.getErrorCount() + 1);
        tomatch.setLatestError(now);
        tomatch.setLatestUpdate(now);
        // and check for higher ban while collecting counters:
        // check maxed this instance
        int remainingAttempts = otptype.getMaxWrongCheckAttemptsByInstance() - tomatch.getErrorCount();
        otpRequest.setRemainingAttempts(remainingAttempts);
        if (remainingAttempts <= 0)
          {
            tomatch.setOTPStatus(OTPInstance.OTPStatus.Burnt);
            otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.MAX_NB_OF_ATTEMPT_REACHED);
          }
        // check global counter for all candidates of the type
        int globalerrorstimewindow = candidates.stream().filter(c -> (c.getLatestError() != null && DateUtils.addSeconds(c.getLatestError(), otptype.getTimeWindow()).after(now))).mapToInt(o -> o.getErrorCount()).sum();
        otpRequest.setGlobalErrorCounts(globalerrorstimewindow);
        if (globalerrorstimewindow >= otptype.getMaxWrongCheckAttemptsByTimeWindow())
          {
            tomatch.setOTPStatus(OTPInstance.OTPStatus.RaisedBan);
            otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.MAX_NB_OF_ATTEMPT_REACHED);
          }
      }
    return otpRequest;
  }

  // OTP clearance for daily customer polling
  public static void clearOldOTPs(SubscriberProfile profile, OTPTypeService otpTypeService, int tenantID)
  {

    Date now = new Date();
    List<OTPInstance> otps = profile.getOTPInstances();
    List<OTPInstance> newOtps = new ArrayList<OTPInstance>();

    if (otps.isEmpty()) return;

    // not empty, lets check for element that are passed the timewindow or ban
    // period
    for (OTPInstance oldOtp : otps)
      {
        OTPType otpType = otpTypeService.getActiveOTPTypeByName(oldOtp.getOTPTypeDisplayName(), tenantID);
        // otp with a no longer active type ... can be deleted/skipped
        if (otpType == null) continue;
        // otp outside relevant time windows (and not banned since ban has its own
        // window) ... can be deleted/skipped
        if (DateUtils.addSeconds(oldOtp.getCreationDate(), otpType.getTimeWindow()).before(now) && oldOtp.getExpirationDate().before(now) && oldOtp.getOTPStatus() != OTPInstance.OTPStatus.RaisedBan) continue;
        // otp banned but bannedperiod outlonged ... can be deleted
        if (oldOtp.getOTPStatus() == OTPInstance.OTPStatus.RaisedBan && DateUtils.addSeconds(oldOtp.getLatestUpdate(), otpType.getBanPeriod()).before(now)) continue;
        // not in a "terminal" status but passed the expiration, keep for global counts
        // but set it to expired
        if (oldOtp.getOTPStatus() != OTPInstance.OTPStatus.Expired && oldOtp.getOTPStatus() != OTPInstance.OTPStatus.Burnt && oldOtp.getOTPStatus() != OTPInstance.OTPStatus.RaisedBan && oldOtp.getExpirationDate().before(now))
          {
            oldOtp.setLatestUpdate(now);
            oldOtp.setOTPStatus(OTPInstance.OTPStatus.Expired);
            // ok to go on with those changes
          }
        // all other cases :
        // no update just keep it

        // passed the criterias to be kept and updates already done, let's store
        newOtps.add(oldOtp);
      }
    profile.setOTPInstances(newOtps);
  }

  // OTP Creation
  public static OTPInstanceChangeEvent generateOTP(OTPInstanceChangeEvent otpRequest, SubscriberState subscriberState, OTPTypeService otpTypeService, SubscriberMessageTemplateService subscriberMessageTemplateService, SourceAddressService sourceAddressService, SubscriberEvaluationRequest subscriberEvaluationRequest, EvolutionEventContext evolutionEventContext, int tenantID)
  {

    if (subscriberState == null)
      {
        // probably already raised before it reaches this section but for safety reasons
        // lets reject
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.CUSTOMER_NOT_FOUND);
        return otpRequest;
      }
    SubscriberProfile profile = subscriberState.getSubscriberProfile();
    Date now = SystemTime.getCurrentTime();
    List<OTPInstance> initialOtpList = profile.getOTPInstances().stream().filter(c -> c.getOTPTypeDisplayName().equals(otpRequest.getOTPTypeName())).collect(Collectors.toList());

    // check and retrieve otpType
    OTPType otptype = otpTypeService.getActiveOTPTypeByName(otpRequest.getOTPTypeName(), tenantID);
    if (otptype == null)
      {
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.MISSING_PARAMETERS);
        // maybe RELATIONSHIP_NOT_FOUND would have been better ...
        return otpRequest;
      }

    try
      {
        OTPInstance mostRecentOtp = Collections.max(initialOtpList, new OTPCreationDateComparator());

        // Check 01 : not during a ban issue
        // testing only the latest should be enough
        if (mostRecentOtp.getOTPStatus().equals(OTPInstance.OTPStatus.RaisedBan) && DateUtils.addSeconds(mostRecentOtp.getLatestUpdate(), otptype.getBanPeriod()).after(now))
          {
            // maybe add some other return values ??
            otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.CUSTOMER_NOT_ALLOWED);
            return otpRequest;
          }

      }
    catch (NoSuchElementException e)
      {
        // ok no prior means can go on
      }

    // Check 02 : not have asked too many elements of the given type within the
    // timewindow
    if (otptype.getMaxConcurrentWithinTimeWindow() <= initialOtpList.stream().filter(c -> c.getOTPTypeDisplayName().equals(otptype.getOTPTypeName()) && DateUtils.addDays(c.getCreationDate(), otptype.getTimeWindow()).after(now)).count())
      {
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.CUSTOMER_NOT_ALLOWED);
        return otpRequest;
      }

    // OK to proceed
    // BURN ALL PREVIOUS ELEMENTS
    List<OTPStatus> statusToBurn = new ArrayList<OTPStatus>();
    statusToBurn.add(OTPStatus.New);
    statusToBurn.add(OTPStatus.ChecksError);
    statusToBurn.add(OTPStatus.ChecksSuccess);

    for (OTPInstance previous : initialOtpList)
      {
        if (statusToBurn.contains(previous.getOTPStatus()))
          {
            previous.setOTPStatus(OTPStatus.Burnt);
            previous.setLatestUpdate(now);
          }
      }

    // generating an otp VALUE STring according to type content :
    String otpValue = null;
    if (otptype.getValueGenerationRegex() != null)
      {
        otpValue = generateFromRegex(otptype.getValueGenerationRegex());
      }
    else if (otptype.getValueGenerationDigits() > 0)
      {
        otpValue = generateNonZeroLeadingOTPValue(otptype.getValueGenerationDigits());
      }
    else
      {
        // maybe system error is better to indicate the error is in the otpType settings
        otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.MISSING_PARAMETERS);
        return otpRequest;
      }
    OTPInstance otpInstance = new OTPInstance(otptype.getDisplay(), OTPStatus.New, otpValue, 0, 0, now, now, null, null, DateUtils.addSeconds(now, otptype.getInstanceExpirationDelay()));

    // put the relevant content of this instance in the returning event
    List<OTPInstance> existingInstances = profile.getOTPInstances();
    if (existingInstances == null) existingInstances = new ArrayList<OTPInstance>();
    existingInstances.add(otpInstance);
    profile.setOTPInstances(existingInstances);

    // send the notification
    Map<String, String> tags = new HashMap<>();
    tags.put("otpCode", otpInstance.getOTPValue());
    
    List<Pair<DialogTemplate, String>> templates = EvolutionUtilities.getNotificationTemplateForAreaAvailability("oneTimePassword", subscriberMessageTemplateService, sourceAddressService, tenantID);
    for(Pair<DialogTemplate, String> template : templates)
      {
        EvolutionUtilities.sendMessage(evolutionEventContext, tags, template.getFirstElement().getDialogTemplateID(), ContactType.ActionNotification, template.getSecondElement(), subscriberEvaluationRequest, subscriberState, otpRequest.getFeatureID(), otpRequest.getModuleID());
      }
    
    otpRequest.setOTPCheckValue(otpValue); // at least for debug now, but should not be returned to the customer...
    otpRequest.setRemainingAttempts(otptype.getMaxWrongCheckAttemptsByInstance());
    otpRequest.setValidityDuration(otptype.getInstanceExpirationDelay());
    otpRequest.setReturnStatus(RESTAPIGenericReturnCodes.SUCCESS);

    return otpRequest;
  }

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
