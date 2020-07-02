/*****************************************************************************
*
*  RESTAPIGenericReturnCodes.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

public enum RESTAPIGenericReturnCodes
{
  SUCCESS(0, "SUCCESS", "Successful operation"),
  AUTHENTICATION_FAILURE(1, "AUTHENTICATION_FAILURE", "User authentication failed"),
  INSUFFICIENT_USER_RIGHTS(2, "INSUFFICIENT_USER_RIGHTS", "Insufficient user rights"),
  MALFORMED_REQUEST(3, "MALFORMED_REQUEST", "Malformed request"),
  MISSING_PARAMETERS(4, "MISSING_PARAMETERS", "Missing mandatory parameters"),
  BAD_FIELD_VALUE(5, "BAD_FIELD_VALUE", "Bad parameter value"),
  INCONSISTENCY_ERROR(6, "INCONSISTENCY_ERROR", "Parameters inconsistency error"),
  LICENSE_RESTRICTION(8, "LICENSE_RESTRICTION", "License restriction"),
  CUSTOMER_NOT_FOUND(20, "CUSTOMER_NOT_FOUND", "Customer not found"),
  SYSTEM_ERROR(21, "SYSTEM_ERROR", "System error"),
  TIMEOUT(22, "TIMEOUT", "Timeout"),
  THROTTLING(23, "THROTTLING", "Throttling threshold exceeded"),
  THIRD_PARTY_ERROR(24, "THIRD_PARTY_ERROR", "Third-party error"),
  LOCK_ERROR(25, "LOCK_ERROR", "Lock error"),
  CUSTOMER_ALREADY_EXISTS(50, "CUSTOMER_ALREADY_EXISTS", "Customer already exists"),
  CUSTOMER_NOT_ELIGIBLE(51, "CUSTOMER_NOT_ELIGIBLE", "Customer not eligible"),
  RELATIONSHIP_NOT_FOUND(60, "RELATIONSHIP_NOT_FOUND", "Relationship not found"),
  ELEMENT_NOT_FOUND(80, "ELEMENT_NOT_FOUND", "Element not found"),
  ALREADY_APPROVED(81, "ALREADY_APPROVED", "Request already approved"),
  APPROBATION_EXPIRED(82, "APPROBATION_EXPIRED", "Approbation request expired"),
  ALREADY_DISAPPROVED(83, "ALREADY_DISAPPROVED", "Request already disapproved"),
  APPROBATION_CANCELED(84, "APPROBATION_CANCELED", "Approbation request canceled"),
  ALREADY_REQUESTED(85, "ALREADY_REQUESTED", "Approval already requested"),
  OBSOLETE_APPROBATION(86, "OBSOLETE_APPROBATION", "Approbation request obsolete"),
  BONUS_NOT_FOUND(100, "BONUS_NOT_FOUND", "Bonus not found"),
  INSUFFICIENT_BONUS_BALANCE(101, "INSUFFICIENT_BONUS_BALANCE", "Insufficient bonus balance"),
  CAMPAIGN_NOT_FOUND(200, "CAMPAIGN_NOT_FOUND", "Campaign not found"),
  CAMPAIGN_BAD_STATUS(201, "CAMPAIGN_BAD_STATUS", "Campaign bad status"),
  CUSTOMER_ALREADY_IN_CAMPAIGN(202, "CUSTOMER_ALREADY_IN_CAMPAIGN", "Customer already in campaign"),
  LOYALTY_PROJECT_NOT_FOUND(300, "LOYALTY_PROJECT_NOT_FOUND", "Loyalty project not found"),
  CUSTOMER_NOT_IN_PROJECT(301, "CUSTOMER_NOT_IN_PROJECT", "Customer not in project"),
  LOYALTY_CLASS_NOT_IN_PROJECT(302, "LOYALTY_CLASS_NOT_IN_PROJECT", "Loyalty class not found in project"),
  LOYALTY_TYPE_NOT_FOUND(303, "LOYALTY_TYPE_NOT_FOUND", "Loyalty type not found"),
  OFFER_NOT_FOUND(400, "OFFER_NOT_FOUND", "Offer not found"),
  PRODUCT_NOT_FOUND(401, "PRODUCT_NOT_FOUND", "Offer content items not found"),
  INVALID_PRODUCT(402, "INVALID_PRODUCT", "Product not available"),
  OFFER_NOT_APPLICABLE(403, "OFFER_NOT_APPLICABLE", "Offer criteria not met"),
  INSUFFICIENT_STOCK(404, "INSUFFICIENT_STOCK", "Lack of offer stock"),
  INSUFFICIENT_BALANCE(405, "INSUFFICIENT_BALANCE", "Customer insufficient balance"),
  BAD_OFFER_STATUS(406, "BAD_OFFER_STATUS", "Offer status not allowing the purchase"),
  PRICE_NOT_APPLICABLE(407, "PRICE_NOT_APPLICABLE", "Offer price criteria not met"),
  NO_VOUCHER_CODE_AVAILABLE(408, "NO_VOUCHER_CODE_AVAILABLE", "No voucher code available"),
  CHANNEL_DEACTIVATED(409, "CHANNEL_DEACTIVATED", "Sales channel not activated"),
  CUSTOMER_OFFER_LIMIT_REACHED(410, "CUSTOMER_OFFER_LIMIT_REACHED", "Customer exceeding offer limit"),
  BAD_OFFER_DATES(411, "BAD_OFFER_DATES", "Offer dates not allowing the purchase"),
  CHANNEL_NOT_FOUND(412, "CHANNEL_NOT_FOUND", "Channel not found"),
  VOUCHER_CODE_NOT_FOUND(413, "VOUCHER_CODE_NOT_FOUND", "Voucher code not found or not associated to msisdn"),
  PARTNER_NOT_FOUND(414, "PARTNER_NOT_FOUND", "Partner not found"),
  VOUCHER_ALREADY_REDEEMED(415, "VOUCHER_ALREADY_REDEEMED", "Voucher already redeemed"),
  VOUCHER_EXPIRED(416, "VOUCHER_EXPIRED", "Voucher expired"),
  VOUCHER_NOT_ASSIGNED(417, "VOUCHER_NOT_ASSIGNED", "Voucher not assigned"),
  VOUCHER_NON_REDEEMABLE(418, "VOUCHER_NON_REDEEMABLE", "Voucher non redeemable"),
  EVENT_NAME_UNKNOWN(419, "EVENT_NAME_UNKNOWN", "Event Name Unknown"),
  BAD_3RD_PARTY_EVENT_CLASS_DEFINITION(420, "BAD_3RD_PARTY_EVENT_CLASS_DEFINITION", "3rd Party event class does follow standard"),
  INACTIVE_RESELLER(421, "INACTIVE_RESELLER", "The reseller is inactive"),
  RESELLER_WITHOUT_SALESCHANNEL(422, "RESELLER_WITHOUT_SALESCHANNEL", "Reseller is not in any saleschannel"),
  SALESCHANNEL_RESELLER_MISMATCH(423, "SALESCHANNEL_RESELLER_MISMATCH", "product sales channel and reseller sales channel mismatch"),
  INVALID_TOKEN_CODE(500, "INVALID_TOKEN_CODE", "The token-code is not valid"),
  MSISDN_TOKEN_CODE_NOT_COMPATIBLE(501, "MSISDN_TOKEN_CODE_NOT_COMPATIBLE", "The token-code is not associated to the current MSISDN"),
  CONCURRENT_ALLOCATION(502, "CONCURRENT_ALLOCATION", "The pair <MSISDN, token-code> is already in allocation phase"),
  CONCURRENT_ACCEPT(503, "CONCURRENT_ACCEPT", "The pair <MSISDN, token-code> is already in accept phase"),
  NO_OFFER_ALLOCATED(504, "NO_OFFER_ALLOCATED", "Unable to allocate any offer for the pair <MSISDN, token-code>"),
  OFFER_NOT_PRESENTED(505, "OFFER_NOT_PRESENTED", "Unable to accept the offers for the pair <MSISDN, token-code>"),
  NO_OFFER_REFUSED(506, "NO_OFFER_REFUSED", "Offer Optimizer is not able to refuse the offers for the pair <MSISDN, token-code>"),
  NO_TOKENS_RETURNED(507, "NO_TOKENS_RETURNED", "Unable to return the list of tokens for current MSISDN"),
  NO_OFFERS_RETURNED(508, "NO_OFFERS_RETURNED", "Unable to return the list of offers for the pair <MSISDN, token-code>"),
  TOO_MANY_ACCEPTED_OFFERS(509, "TOO_MANY_ACCEPTED_OFFERS", "You tried to accept more than one offer for the pair <MSISDN, token-code>"),
  TOKEN_RESEND_NO_ACTIVE_TOKENS(510, "TOKEN_RESEND_NO_ACTIVE_TOKENS", "No active tokens available"),
  TOKEN_RESEND_MAX_NUMBER_OF_RESEND_FOR_TOKEN(511, "TOKEN_RESEND_MAX_NUMBER_OF_RESEND_FOR_TOKEN", "No tokens sent due to max number of token resend reached"),
  TOKEN_RESEND_MESSAGE_SENDING_ERROR(512, "TOKEN_RESEND_MESSAGE_SENDING_ERROR", "Not able to resend any tokens due to message notification error"),
  CHANNEL_WITH_ALLOCATED_OFFERS(513, "CHANNEL_WITH_ALLOCATED_OFFERS", "Channel with allocated offers: cannot be deactivate"),
  TOKEN_BAD_TYPE(515, "TOKEN_BAD_TYPE", "Token is not DNBO type."),
  INVALID_TOKEN_TYPE(516, "INVALID_TOKEN_TYPE", "Unknown Token Type."),
  CANNOT_GENERATE_TOKEN_CODE(517, "UNABLE TO GENERATE TOKEN CODE", "Unable to generate token code, check the pattern."),
  INVALID_STRATEGY(518, "INVALID_STRATEGY", "Invalid Strategy found for scoring offers"),
  PREDICTION_NOT_AVAILABLE(600, "PREDICTION_NOT_AVAILABLE", "Prediction not available"),
  SENT(700, "SENT", "Sent"),
  NO_CUSTOMER_LANGUAGE(701, "NO_CUSTOMER_LANGUAGE", "Missing customer language for this message"),
  NO_CUSTOMER_CHANNEL(702, "NO_CUSTOMER_CHANNEL", "Missing customer communication channel for this message"),
  UNDELIVERABLE(703, "UNDELIVERABLE", "Undeliverable message"),
  INVALID(704, "INVALID", "Invalid message"),
  QUEUE_FULL(705, "QUEUE_FULL", "Message queue full"),
  EXPIRED(707, "EXPIRED", "Expired"),
  PENDING(708, "PENDING", "Pending"),
  RESCHEDULE(709, "RESCHEDULE", "Reschedule"),
  BLOCKED_BY_CONTACT_POLICY(710,"BLOCKED_BY_CONTACT_POLICY","Message is blocked by contact policy rules"),
  CONTACT_POLICY_EVALUATION_ERROR(711,"CONTACT_POLICY_EVALUATION_ERROR","Contact policy rules validation failed"),
  UNKNOWN(-1, "UNKNOWN", "UNKNOWN");
  
  private int genericResponseCode;
  private String genericResponseMessage;
  private String genericDescription;
  private RESTAPIGenericReturnCodes(int genericResponseCode, String genericResponseMessage, String genericDescription) { this.genericResponseCode = genericResponseCode; this.genericResponseMessage = genericResponseMessage; this.genericDescription = genericDescription; }
  public int getGenericResponseCode() { return genericResponseCode; }
  public String getGenericResponseMessage() { return genericResponseMessage; }
  public String getGenericDescription() { return genericDescription; }
  public static RESTAPIGenericReturnCodes fromGenericResponseCode(int genericResponseCode) 
  { 
    for (RESTAPIGenericReturnCodes enumeratedValue : RESTAPIGenericReturnCodes.values()) 
      { 
        if (enumeratedValue.getGenericResponseCode() == genericResponseCode) return enumeratedValue; 
      } 
    return UNKNOWN; 
  }
  public static RESTAPIGenericReturnCodes fromGenericResponseMessage(String genericResponseMessage)
  {
    for (RESTAPIGenericReturnCodes enumeratedValue : RESTAPIGenericReturnCodes.values())
    {
      if (enumeratedValue.getGenericResponseMessage().equals(genericResponseMessage)) return enumeratedValue;
    }
    return UNKNOWN;
  }

}
