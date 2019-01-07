package com.lumatagroup.expression.driver.SMTP;

public enum NotificationStatusCode {

    
    // Email success or error codes
              
    DELIVERED  (250),     
    AUTHENTICATION_FAILED  (503), 
    ADDRESS_FAILED  (550),        
    UNKNOWN  (500),    
    CAN_NOT_OPEN_CONNECTION  (101), //Cannot open connection
    CONNECTION_REFUSED  (111),	//-Connection refused    
    SERVER_NOT_AVAILABLE  (421),	//-Service not available 
    MAILBOX_FULL  (422),	//-Recipient mailbox full
    DISK_FULL  (431),	//-Recipient disk full   
    CONNECTION_DROPPED  (442),	//-Connection was dropped
    OUTGOING_MESSAGE_TIMEOUT  (447),	//-Outgoing message timed out
    ROUTING_ERROR  (449),	//-Routing error         
    REQUESTED_ACTION_NOT_TAKEN  (450),	//-Requested action not taken
    PAGE_UNAVAILABLE  (465),	//-Code page unavailable 
    BAD_EMAIL_ADDRESS  (510),	//-Bad email address     
    BAD_EMAIL_ADDRESS_1  (511),	//-Bad email address     
    DNS_ERROR  (512),	//-DNS error             
    ADDRESS_TYPE_INCORRECT  (513),	//-Adress type incorrect 
    TOO_BIG_MESSAGE  (523),	//-Too big message       
    AUTHENTICATION_IS_REQUIRED  (530),	//-Anthentication is required
    RECIPIENT_ADDRESS_REJECTED  (541),	//-Recipient adress rejected 
    BLACK_LIST  (571),	//-Black listes 
    
    EMAIL_ACCEPTED   (100),
    RECIPIENT_NOT_VERIFIED  (354),
    BLANK_EMAIL_SUBJECT  (553),
    ILLEGAL_SENDER  (555),
    
    EMAIL_SENT   (1);
    
    private Integer returncode;
    private NotificationStatusCode(Integer returncode) { this.returncode = returncode; }
    public Integer getReturnCode() { return returncode; }
    public static NotificationStatusCode fromReturnCode(Integer externalRepresentation) { for (NotificationStatusCode enumeratedValue : NotificationStatusCode.values()) { if (enumeratedValue.getReturnCode().equals(externalRepresentation)) return enumeratedValue; } return UNKNOWN; }
}