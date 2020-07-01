package com.lumatagroup.expression.driver.SMTP;

import javax.mail.PasswordAuthentication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Bhavishya
 * This class is used to authentication by user name and password.
 *
 */
public class SMTPAuthenticator extends javax.mail.Authenticator {
	private static Logger logger = LoggerFactory.getLogger(SMTPAuthenticator.class);
    private String smtpAuthUserName;
    private String smtpAuthPassword;
    private PasswordAuthentication authentication;
    /**
     * Constructor used to set the user name and password.
     * @param userName
     * @param password
     */
    public SMTPAuthenticator(String userName, String password){
    	this.smtpAuthUserName = userName;
    	this.smtpAuthPassword = password;
    	authentication = new PasswordAuthentication(userName, password);
    }
    
    /**
     * @return PasswordAuthentication object.
     */
    protected PasswordAuthentication getPasswordAuthentication() {    	
        return authentication;
     }
 }