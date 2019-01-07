package com.lumatagroup.expression.driver.SMPP;

import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.SMSNotificationManager;
import com.lumatagroup.expression.driver.SMPP.configuration.SMSC;
import com.lumatagroup.expression.driver.SMPP.Util.SMPPUtil;
import com.lumatagroup.expression.driver.SMPP.Util.SMPPUtil.SMPP_CONFIGS;


/**
 * Responsible for making up the SMSSender objects (and their related SMPPConnection).
 * <p>If there are several configured SMPPConnection, that will make almost
 * round-robin paradigm. Actually the quicker SMSC will receive more SMS,
 * cos the SMSWorker will be more available. 
 */
public class SMSSenderFactory {
    private static final Logger logger = LoggerFactory.getLogger(SMSSenderFactory.class);
    private final SMSC smppDriverConfigurationMap;
	private final ArrayList<SimpleSMSSender> senders = new ArrayList<SimpleSMSSender>();
	
    public SMSSenderFactory(SMSC smppDriverConfigurationMap) {
        this.smppDriverConfigurationMap = smppDriverConfigurationMap;
        if (logger.isInfoEnabled()) logger.info("SMSSenderFactory.ctor");
    }

	public void init(SMSNotificationManager smsNotificationManager) {
		if (logger.isDebugEnabled()) logger.debug("SMSSenderFactory.init");
        
        if (smppDriverConfigurationMap == null || smppDriverConfigurationMap.getSize() == 0) {
			logger.error("SMSSenderFactory.init: no configuration");
		} else {
            
				if (logger.isDebugEnabled()) logger.debug("SMSSenderFactory.init handle "+smppDriverConfigurationMap);
				try {
					SMPPConnection conn = SMPPConnectionManager.getInstance().getSMPPConnection(smppDriverConfigurationMap);

					if (logger.isInfoEnabled()) logger.info("SMSSenderFactory.init successfuly initialized SMPPConnection for "+smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.name));                   

					//@formatter:off
					SimpleSMSSender sender = new SimpleSMSSender(smsNotificationManager,
							SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.name)),
					                                             conn,
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.source_addr)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.source_addr_ton)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.source_addr_npi)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.dest_addr_ton)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.dest_addr_npi)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.data_coding)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.load_HP_roman8_as_data_coding0)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.load_UTF16_as_data_coding8)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.data_packing)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.encoding_charset)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.expiration_period)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.delay_on_queue_full)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.max_per_sec)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.interval_retry)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.support_sar)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.support_udh)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.handle_submit_sm_response_in_multi_part)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.dest_addr_subunit)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.service_type)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.delivery_receipt_decoding_hex_dec)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.is_send_to_payload)),
					                                             SMPPUtil.convertString(smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.midnight_expiry_smooth_hours)));
					
					
					//@formatter:on					
					senders.add(sender);
					if (logger.isInfoEnabled()) logger.info("SMSSenderFactory.init successfuly initialized SMSSender for "+smppDriverConfigurationMap.getProperty(SMPP_CONFIGS.name));
				} catch (Exception e) {
					logger.error("SMSSenderFactory.init failed to initialize SMSSender from "+smppDriverConfigurationMap+" due to "+e,e);
					e.printStackTrace();
				}
		}
	}

	
	public SimpleSMSSender[] getSMSSenders() {
		return senders.toArray(new SimpleSMSSender[]{});
	}
}
