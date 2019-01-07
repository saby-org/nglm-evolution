package com.lumatagroup.expression.driver.SMPP;

import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lumatagroup.expression.driver.SMPP.configuration.SMSC;
import com.lumatagroup.expression.driver.SMPP.Util.SMPPUtil;
import com.lumatagroup.expression.driver.SMPP.Util.SMPPUtil.SMPP_CONFIGS;

public class SMPPConnectionManager {
	
    private static final Logger logger = LoggerFactory.getLogger(SMPPConnectionManager.class);
	
	private static SMPPConnectionManager instance = new SMPPConnectionManager();
	
	private HashMap<String, SMPPConnection> connections = new HashMap<>(); // name / connection, i.e. 2 connections with the same name will refer the same connection object
	
	private SMPPConnectionManager(){
		
	}
	
	public static SMPPConnectionManager getInstance(){
		return instance;
	}
	
	public synchronized SMPPConnection getSMPPConnection(SMSC smppDriverConfigurationMap){
		if(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.name) == null){
			logger.warn("SMPPConnectionManager.getSMPPConnection No name was provided for SMPPConnection " + smppDriverConfigurationMap );

		}
		SMPPConnection connToReturn = connections.get(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.name));
		if(connToReturn == null){
			if (logger.isInfoEnabled()) {
				logger.info("SMPPConnectionManager.getSMPPConnection try to create in connect the connection " + smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.name));
			}
			connToReturn = new SMPPConnection(SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.name)), 
		    		  SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.address)), 
		    		  SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.port)),
		    		  SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.connection_type)),
		    		  SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.system_id)), 
		    		  SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.password)),
		    		  SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.system_type))/* can be null */, true,
		    		  SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.time_between_reconnection)),
		    		  SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.time_between_connection_check)),
		    		  SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.smpp_receiver_thread_number)),
		    		  SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.magic_number_configuration)));
			connections.put(SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.name)), connToReturn);
			
			if (logger.isDebugEnabled()) logger.info("SMPPConnectionManager attempt to start SMPPConnection for "+smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.name));
			if (connToReturn.start()) {
				if (logger.isInfoEnabled()) logger.info("SMPPConnectionManager successfuly started SMPPConnection for "+smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.name));
			} else {
				logger.warn("SMPPConnectionManager failed to start SMPPConnection for "+smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.name));
			}
		}
		if (logger.isInfoEnabled()) {
			logger.info("SMPPConnectionManager.getSMPPConnection return connection " + connToReturn);
		}
		return connToReturn;
	}

}
