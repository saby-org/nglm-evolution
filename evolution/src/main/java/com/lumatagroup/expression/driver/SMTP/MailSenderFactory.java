package com.lumatagroup.expression.driver.SMTP;

import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.Logger;

import com.evolving.nglm.evolution.MailNotificationManager;
import com.lumatagroup.expression.driver.SMTP.constants.SMTPConstants;
import com.lumatagroup.expression.driver.SMTP.util.SMTPUtil;


/**
 * 
 * @author Bhavishya
 *
 */
public class MailSenderFactory {
	private static Logger logger = Logger.getLogger(MailSenderFactory.class);
    private final Map<String, Object> smtpDriverConfigurationMap;
    private final ArrayList<SimpleEmailSender> senders = new ArrayList<SimpleEmailSender>();
	
    /**
     * MailSenderFactory constructor
     * @param smtpDriverConfigurationMap
     */
    public MailSenderFactory(Map<String, Object> smtpDriverConfigurationMap) {
        logger.debug("Inside MailSenderFactory.constructor.");
        this.smtpDriverConfigurationMap = smtpDriverConfigurationMap;
    }

    /**
     * init() method used to create SMTP driver connection.
     * @param SMTPDriver3rdParty driver
     */
	public void init(MailNotificationManager mailNotificationManager) {
		logger.debug("MailSenderFactory.init() method execution started...");
        
        if (smtpDriverConfigurationMap == null) {
        	logger.info("MailSenderFactory.init(): no configuration");
        	throw new NullPointerException("SMTP Driver Configuration Map is null.");
		} 
        else {
				logger.debug("MailSenderFactory.init(): handle "+smtpDriverConfigurationMap);
				try {
                    SMTPConnection conn =  new SMTPConnection(mailNotificationManager,
							SMTPUtil.convertString(smtpDriverConfigurationMap.get(SMTPConstants.MAIL_SMTP_HOST)),
							SMTPUtil.convertString(smtpDriverConfigurationMap.get(SMTPConstants.MAIL_SMTP_SOCKETFACTORY_PORT)),
							SMTPUtil.convertString(smtpDriverConfigurationMap.get(SMTPConstants.MAIL_SMTP_SOCKETFACTORY_CLASS)),
							SMTPUtil.convertString(smtpDriverConfigurationMap.get(SMTPConstants.MAIL_SMTP_AUTH)),
							SMTPUtil.convertString(smtpDriverConfigurationMap.get(SMTPConstants.USER_NAME)),
							SMTPUtil.convertString(smtpDriverConfigurationMap.get(SMTPConstants.PASSWORD)),
							SMTPUtil.convertString(smtpDriverConfigurationMap.get(SMTPConstants.SMTP_PROTOCOL)),
							SMTPUtil.convertString(smtpDriverConfigurationMap.get(SMTPConstants.DRIVER_SESSION_DEBUG_FLAG)),
							SMTPUtil.convertString(smtpDriverConfigurationMap.get(SMTPConstants.SENDER_EMAIL_ADDRESS)),
							SMTPUtil.convertString(smtpDriverConfigurationMap.get(SMTPConstants.CONNECTION_TIMEOUT_VAL)),
							SMTPUtil.convertInt(smtpDriverConfigurationMap.get(SMTPConstants.SMTP_DRIVER_CONNECTION_CHECKING_TIME)) == null
									? SMTPConstants.DEFAULT_SMTP_DRIVER_CONNECTION_CHECKING_TIME_VAL
									: SMTPUtil.convertInt(smtpDriverConfigurationMap.get(SMTPConstants.SMTP_DRIVER_CONNECTION_CHECKING_TIME)).intValue(),
							SMTPUtil.convertString(smtpDriverConfigurationMap.get(SMTPConstants.HTML_CONTENT_CHARSET)),
							SMTPUtil.convertString(smtpDriverConfigurationMap.get(SMTPConstants.SUBJECT_CHARSET))
					);
                    String driverName = SMTPUtil.convertString(smtpDriverConfigurationMap.get("name"));
                    String replyTo = SMTPUtil.convertString(smtpDriverConfigurationMap.get(SMTPConstants.REPLY_TO_EMAIL_ADDRESS));
            		String fromEmail = SMTPUtil.convertString(smtpDriverConfigurationMap.get(SMTPConstants.SENDER_EMAIL_ADDRESS));
            		String forceSubject = SMTPUtil.convertString(smtpDriverConfigurationMap.get(SMTPConstants.EMAIL_SUBJECT));
            		
                    if (logger.isInfoEnabled()) 
                    	logger.info("MailSenderFactory.init() successfuly initialized SMTPConnection for "+SMTPUtil.convertString(smtpDriverConfigurationMap.get("name")));		
                    // Code review- Need to remove configuration map param - 
                    SimpleEmailSender sender = new SimpleEmailSender(mailNotificationManager, driverName, conn, replyTo, fromEmail, forceSubject);

                    senders.add(sender);
				}				
				catch (Exception e) {
					logger.error("Exception occured in MailSenderFactory.init(): failed to initialize SMTPConnection from "+SMTPUtil.convertString(smtpDriverConfigurationMap.get("name"))+" due to "+e);
					e.printStackTrace();
				}
		}
	}
	
	public SimpleEmailSender[] getEmailSenders() {
		return senders.toArray(new SimpleEmailSender[]{});
	}
}
