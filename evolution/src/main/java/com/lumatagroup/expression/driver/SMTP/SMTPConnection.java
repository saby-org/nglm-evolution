package com.lumatagroup.expression.driver.SMTP;

import java.io.ByteArrayOutputStream;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Date;
import java.util.Properties;

import javax.mail.AuthenticationFailedException;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.SendFailedException;
import javax.mail.Session;
import javax.mail.URLName;
import javax.mail.internet.InternetAddress;
import javax.security.sasl.AuthenticationException;

import org.apache.log4j.Logger;

import com.evolving.nglm.evolution.MailNotificationManager;
import com.lumatagroup.expression.driver.SMTP.constants.SMTPConstants;
import com.sun.mail.smtp.SMTPMessage;
import com.sun.mail.smtp.SMTPTransport;

/**
 * 
 * @author Bhavishya
 *
 */
public class SMTPConnection {
	private static Logger logger = Logger.getLogger(SMTPConnection.class);
	
	private String host;
	private String port;
	private String socketFactoryClass;
	private boolean authFlag;
	private String userName;
	private String password;
	private String protocol;
	private boolean sessionDebugFlag;
	private String fromEmail;
	public String connectionTimoutVal; 
	private String contentType;
	private String subjectCharset;
	
	private Properties props;
	private SMTPTransport smtpTransport = null;
	private Session session;
	ByteArrayOutputStream os = new ByteArrayOutputStream();
	PrintStream ps = new PrintStream(os);
	private boolean started = false; // not yet
	public long lastActiveTime;
	private static int driverConnectionTimeCheck;
	public  boolean isSNMPConnectionStopAlarmSend = false;

	/**
	 * SMTPConnection constructor to initialize the configuration parameters and
	 * SMTP properties.
	 * 
	 * @param driverName
	 * @param host
	 * @param port
	 * @param socketFactoryClass
	 * @param smtpAuthFlag
	 * @param userName
	 * @param password
	 * @param protocol
	 * @throws MessagingException 
	 */
	// Code Review- Need to remove the config parameter if not required in
	// future - Done.
	public SMTPConnection(MailNotificationManager mailNotificationManager, String host, String port, String socketFactoryClass, 
									String smtpAuthFlag, final String userName, 
									final String password, String protocol, String debugFlag, String frmEmail, String connectionTimeout, int connectionCheckTime, String htmlContentCharset, String subjectCharset) throws Exception{
		if (host == null || port == null || socketFactoryClass == null
				|| protocol == null) {
			logger.info("SMTP connection parameters should not be null..");
			throw new NullPointerException(
					"Missing argument for SMTPConnection constructor.");
		}
		if (Boolean.parseBoolean(smtpAuthFlag) == false || smtpAuthFlag == null
				|| smtpAuthFlag.isEmpty()) {
			this.authFlag = Boolean.FALSE;
			this.userName = null;
			this.password = null;

		} else {
			this.authFlag = Boolean.parseBoolean(smtpAuthFlag);
			this.userName = userName;
			this.password = password;
		}
		this.host = host;
		this.port = port;
		this.socketFactoryClass = socketFactoryClass;
		this.protocol = protocol;
		this.fromEmail = frmEmail;
		this.sessionDebugFlag = Boolean.parseBoolean(debugFlag);
		this.props = setSMTPProperties();
		this.session = createSession(this.authFlag, this.props);
		this.connectionTimoutVal = connectionTimeout;
		driverConnectionTimeCheck = connectionCheckTime;

		if (htmlContentCharset!=null && !htmlContentCharset.trim().isEmpty()){
			this.contentType = "text/html; charset=\""+htmlContentCharset.trim()+"\"";
			logger.info(SMTPConstants.HTML_CONTENT_CHARSET+" conf set, override default 'Content-type = text/html' with 'Content-type = "+this.contentType+"'");
		} else {
			this.contentType = "text/html";
			logger.info("using default 'Content-type = "+this.contentType+"'");
		}
		if (subjectCharset!=null && !subjectCharset.trim().isEmpty()){
		    this.subjectCharset = subjectCharset.trim();
		} else {
		    this.subjectCharset = null;
		}

		if (this.session == null) {
			logger.info("Invalid session..." + session);
			throw new NullPointerException("Session null, problem in creating session.");
		}
		boolean tryToReconnect = true;
		while (!started && tryToReconnect){
			try{
			smtpTransport = connectSMTPTransport();
			lastActiveTime = System.currentTimeMillis();
			}catch(AuthenticationFailedException authF){
				logger.error("AuthenticationFailedException occured in SMTPConnection.constructor. "+authF.getMessage());
				tryToReconnect = false; //break the loop ,since credentials are incorrect, do not want to retry
			}catch (Exception e) {
				if(smtpTransport == null || !smtpTransport.isConnected()){
					tryToReconnect = true; //connection retry
//					this.driver.sendConnectionStatusTrap(false,"SMTPConnection to 3rdParty SMTPServer not Loaded Successfully. Trying to reconnect.... ");
					logger.error("SMTP failed to load due to "+e.getMessage() +" Trying to reconnect......");
				}
				try {
					Thread.sleep(driverConnectionTimeCheck);
				} catch (InterruptedException e1) {
					logger.error("InterruptedException occured in SMTPConnection.constructor. "+e1.getMessage());
				}
			}
		}
		
		if(smtpTransport ==null && !started){
			throw new AuthenticationFailedException("Authentication Failed");
		}

		//sendMail();
	}

	/**
	 * This method is used to set the SMTP properties to the
	 * @return java.util.Properties object
	 */
	public Properties setSMTPProperties() throws MessagingException, AuthenticationFailedException {
		logger.debug("START: execution of SMTPConnection.setSMTPProperties() method. ");
		Properties props = new Properties();
		props.put(SMTPConstants.MAIL_SMTP_HOST, host);
		props.put(SMTPConstants.MAIL_SMTP_SOCKETFACTORY_PORT, port);
		props.put(SMTPConstants.MAIL_SMTP_SOCKETFACTORY_CLASS,socketFactoryClass);
		if (authFlag) {
			props.put(SMTPConstants.MAIL_SMTP_AUTH, authFlag);
		} else {
			props.put(SMTPConstants.MAIL_SMTP_AUTH, false);
			authFlag = false;
		}
		switch (protocol) {
		case "SMTPS":
			props.put("mail.smtp.ssl.enable", true);
			break;
		case "SMTP":
			props.put("mail.smtp.ssl.enable", false);
//			props.put("mail.transport.protocol", "smtp");
			break;
		default:
			logger.info("Invalid protocol...");
			break;
		}
		logger.debug("END: execution of SMTPConnection.setSMTPProperties() method. ");
		return props;
	}

	/**
	 * This method is used to create SMTP mail message
	 * 
	 * @param session
	 * @param configMap
	 * @param toEmailAddress
	 * @return
	 */
	public SMTPMessage createSMTPMessage(String fromEmail, String subject, String emailText, String toEmailAddress, String replyTo) 
										throws MessagingException, SendFailedException {
		logger.debug("START: execution of SMTPConnection.createSMTPMessage() method. ");
		SMTPMessage smtpMsg = new SMTPMessage(session);
		try {
			smtpMsg.setFrom(new InternetAddress(fromEmail));
			smtpMsg.addRecipient(Message.RecipientType.TO, new InternetAddress(toEmailAddress));
			
			if ( !subject.isEmpty() ) {
				smtpMsg.setSubject(subject, subjectCharset/*can be null*/);
			}

			smtpMsg.setContent(emailText, contentType);
			InternetAddress[] replyToAddress = new InternetAddress[1];
			if(replyTo != null && !replyTo.isEmpty()){
				replyToAddress[0] = new InternetAddress(replyTo);
				smtpMsg.setReplyTo(replyToAddress);
			}
		} catch (MessagingException mex) {
			logger.error("Exception occured in SMTPConnection.createSMTPMessage(): "+ mex.getMessage());
			mex.printStackTrace();
		}
		logger.debug("END: execution of SMTPConnection.createSMTPMessage() method. ");
		return smtpMsg;
	}

	/**
	 * This method is used to get the javax.mail.Session object after SMTP
	 * authentication and setting the SMTP properties.
	 * 
	 * @param authFlag
	 * @return
	 * @throws MessagingException 
	 * @throws AuthenticationException
	 */
	public Session createSession(boolean authFlag, Properties props) throws MessagingException, AuthenticationFailedException{
		logger.debug("START: SMTPConnection.createSession() method.");
		logger.debug("authFlag:" + authFlag);
		session = null;
		if (authFlag) {
			logger.debug("Going to authenticate using credentials username:"+ userName + "	| password:" + password);
			SMTPAuthenticator authenticator = new SMTPAuthenticator(userName, password);
			//session = Session.getDefaultInstance(props, authenticator);
			session = Session.getInstance(props, authenticator);
		} else {
			session = Session.getInstance(props, null);
			authFlag = false;
		}
		logger.info("Props: "+session.getProperties());
		// Flag is mentioned in the YAML file, used to debug only, generally keep it false to avoid un-necessary logs of SMTP library.
		//session.setDebug(sessionDebugFlag);
		if (sessionDebugFlag) {
	        logger.info("JAVAMAIL debug mode is ON");
	        session.setDebug(true);
	        session.setDebugOut(ps);
	     }
		logger.debug("END: SMTPConnection.createSession() method execution.");
		return session;
	}

	/**
	 * This method is to connect the SMTP server using credentials authentication or without authentication
	 * @return SMTPTransport smtpTransport
	 */
	public SMTPTransport connectSMTPTransport() throws MessagingException,Exception {
		logger.debug("START: SMTPConnection.getSMTPTransport() method execution");
		SMTPTransport smtpTransport = new SMTPTransport(session, new URLName(this.host));
		smtpTransport.setUseRset(true);
		if (sessionDebugFlag) { 
			logger.info(os); 
		}
		try {
			if ((userName != null && !userName.isEmpty()) && (password != null && !password.isEmpty())) {
				logger.debug("Going to connect SMTP Transport through credentials using host:{"+this.host+"}"+", port:{"+this.port+"}"+", userName:{"+userName+"}"+", password:{"+password+"}");
				smtpTransport.setStartTLS(true);
				smtpTransport.connect(this.host, Integer.parseInt(this.port),userName, password);
				this.lastActiveTime = System.currentTimeMillis();
				logger.debug("Inside SMTPConnection.connectSMTPTransport() last active time: "+new Date(this.lastActiveTime));
				started = true;
			}else{
				logger.debug("Going to connect SMTP Transport without credentials using host:{"+this.host+"}"+", port:{"+this.port+"}");
				smtpTransport.connect(this.host, Integer.parseInt(this.port), null, null);
				this.lastActiveTime = System.currentTimeMillis();
				logger.debug("Inside SMTPConnection.connectSMTPTransport() last active time: "+this.lastActiveTime);
				setTransportObject(smtpTransport);
				started = true;
			}
			if(isSNMPConnectionStopAlarmSend){
				isSNMPConnectionStopAlarmSend = false;
//				this.driver.sendConnectionStatusTrap(true, null);
			}
			logger.info("SMTP Transport connected successfully.");
			if (sessionDebugFlag) { 
				logger.info(os); 
			}
		}catch (AuthenticationFailedException mEx) {
			smtpTransport = null;
			logger.error("AuthenticationFailedException occured in SMTPConnection.getSMTPTransport(): "+ mEx);
			started = false;
			throw new AuthenticationFailedException("Authentication Failed");
		}
		catch (MessagingException mEx) {
			smtpTransport = null;
			logger.error("MessagingException occured in SMTPConnection.getSMTPTransport(): "+ mEx);
			started = false;
			throw new MessagingException(mEx.getMessage());
		} catch(Exception ex){
			smtpTransport = null;
			started = false;
			logger.error("Exception occured in SMTPConnection.getSMTPTransport(): "+ ex);
			throw new Exception(ex.getMessage());
		}
		finally{
			try {
				ps.close();
				os.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		/*if(connectionTimoutVal != null && !connectionTimoutVal.isEmpty()){
			logger.debug("Connection timeout param value is: "+connectionTimoutVal +". So we are initiating a thread to avoid connection timeout.");
			Thread t = new ConnectionChecker(smtpTransport, Long.parseLong(connectionTimoutVal.trim()));
			t.start();
		}*/
		logger.debug("END: SMTPConnection.getSMTPTransport() method execution.");
		return smtpTransport;
	}
	
	
	public boolean isTimeOut(long connectionTimeOutVal){
		long  currentTime = System.currentTimeMillis();
		logger.debug("Timeout value : "+new Date(connectionTimeOutVal)+" CurrentTime : "+new Date(currentTime));
		if(currentTime >= connectionTimeOutVal ){
			return true;
		}else{
			return false;
		}
	}
	
	public SMTPTransport reconnect() throws MessagingException, Exception{
		logger.debug("Disconnecting from SMTP Server");
		disconnect();
		logger.debug("Connecting with SMTP Server");
		return connectSMTPTransport();
	}
	
	public void disconnect(){
		try {
			if(smtpTransport != null){
				smtpTransport.close();
			}
		} catch (MessagingException mEx) {
			logger.error("Exception occured in SMTPConnection.disconnect() while disconnecting the server: "+mEx);
		}
	}

	public SMTPTransport getTransportObject(){
		return smtpTransport;
	}
	public void setTransportObject(SMTPTransport transObj){
		smtpTransport = transObj;
	}
	
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}
	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}
	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	public boolean isAuthFlag() {
		return authFlag;
	}

	public void setAuthFlag(boolean authFlag) {
		this.authFlag = authFlag;
	}
	
	public String getConnectionTimoutVal() {
		return connectionTimoutVal;
	}

	public void setConnectionTimoutVal(String connectionTimoutVal) {
		this.connectionTimoutVal = connectionTimoutVal;
	}

	public long getLastActiveTime() {
		return lastActiveTime;
	}

	public void setLastActiveTime(long lastActiveTime) {
		this.lastActiveTime = lastActiveTime;
	}
}
