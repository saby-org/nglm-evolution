package com.lumatagroup.expression.driver.dyn;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.simple.JSONObject;

import com.evolving.nglm.evolution.MailNotificationManager;
import com.lumatagroup.expression.driver.SMTP.constants.SMTPFeedbackType;
import com.lumatagroup.expression.driver.SMTP.model.ClickedUrlBean;
import com.lumatagroup.expression.driver.SMTP.model.SMTPFeedbackMsg;
import com.lumatagroup.expression.driver.SMTP.util.Conf;
import com.lumatagroup.expression.driver.SMTP.util.HttpUtil;
import com.lumatagroup.expression.driver.SMTP.util.ParseFeedbackResponse;
import com.lumatagroup.expression.driver.SMTP.util.SMTPUtil;
import com.evolving.nglm.core.JSONUtilities;

public class PollFeedback {

	private static Logger logger = LoggerFactory.getLogger(PollFeedback.class);
	
	public static void checkDelivered(SMTPFeedbackMsg feedbackMsg, String deliveryUrl, int httpRequestTimeoutVal){
		ParseFeedbackResponse parseResponse = new ParseFeedbackResponse();
		try{
			String respString = HttpUtil.doGet(deliveryUrl,"PollFeedback.checkDelivered()", httpRequestTimeoutVal);

			if(respString != null ){
				Response responseObj = parseResponse.parse(respString);
				if(responseObj != null){
					logger.debug("response message: "+responseObj.getMessage()+"	|	response status: "+responseObj.getStatus());
					if(responseObj.getMessage().equalsIgnoreCase("OK") && responseObj.getStatus() == 200){

						if(responseObj.getData()!=null && responseObj.getData().getDelivered()!=null){ 
							Delivered[] deliveredsList = responseObj.getData().getDelivered();
							boolean isDelivered = false;
							for(int i=0;i<deliveredsList.length;i++){
									Delivered deliver = deliveredsList[i];
									if(deliver!=null && feedbackMsg.getMessageId().equalsIgnoreCase(deliver.getXheaders().getXmessageId())){
										feedbackMsg.setFeedbackType(SMTPFeedbackType.DELIVERED);
										feedbackMsg.getFeedbackType().setStatus(true);
										feedbackMsg.setDescription("ACCEPTED");
										isDelivered = true;
										break;
									}
							}
							if(!isDelivered){
								feedbackMsg.setFeedbackType(SMTPFeedbackType.DELIVERED);
								feedbackMsg.getFeedbackType().setStatus(false);
								feedbackMsg.setDescription("NOT ACCEPTED");
							}
						}else{
							feedbackMsg.setFeedbackType(SMTPFeedbackType.DELIVERED);
							feedbackMsg.getFeedbackType().setStatus(false);
							feedbackMsg.setDescription("ERROR IN DELIVER API : 0 Delivered objects");
						}
					}else{
						feedbackMsg.setFeedbackType(SMTPFeedbackType.ERROR);
						feedbackMsg.getFeedbackType().setStatus(false);
						feedbackMsg.setDescription("ERROR IN DELIVER API : "+responseObj.getStatus());
					}
				}else{
					logger.error("Response object is null while parsing the DELIVER API response for message id: "+feedbackMsg.getMessageId());
					feedbackMsg.setFeedbackType(SMTPFeedbackType.ERROR);
					feedbackMsg.getFeedbackType().setStatus(false);
					feedbackMsg.setDescription("ERROR IN PARSING DELIVER API RESPONSE");
				}
			}else{
				logger.error("HTTP REQUEST CONNECTION problem in DELIVER URL API for message id: "+feedbackMsg.getMessageId());
				feedbackMsg.setDescription("HTTP REQUEST CONNECTION PROBLEM IN DELIVER API");
			}
		} catch (Exception e) {
			feedbackMsg.setFeedbackType(SMTPFeedbackType.ERROR);
			feedbackMsg.getFeedbackType().setStatus(false);
			feedbackMsg.setDescription("ERROR IN DELIVER API : "+e.getMessage());
			logger.error("Exception occured in PollFeedback.checkDelivered(): "+e);
			e.printStackTrace();
		}
	}

	public static void checkOpen(SMTPFeedbackMsg feedbackMsg, String openUrl, int httpRequestTimeoutVal){
		
		ParseFeedbackResponse parseResponse = new ParseFeedbackResponse();
		try{
			String respString = HttpUtil.doGet(openUrl,"PollFeedback.checkOpen()", httpRequestTimeoutVal);
			
			if(respString != null ){
				Response responseObj = parseResponse.parse(respString);
				if(responseObj != null){
					logger.debug("response message: "+responseObj.getMessage()+"	|	response status: "+responseObj.getStatus());
					if(responseObj.getMessage().equalsIgnoreCase("OK") && responseObj.getStatus() == 200){

						if(responseObj.getData()!=null && responseObj.getData().getOpens()!=null){ 
							Opens[] opensList = responseObj.getData().getOpens();
							boolean isDelivered = false;
							for(int i=0;i<opensList.length;i++){
									Opens opens = opensList[i];
									if(opens!=null && feedbackMsg.getMessageId().equalsIgnoreCase(opens.getXheaders().getXmessageId()))
									{
										feedbackMsg.setFeedbackType(SMTPFeedbackType.OPEN);
										feedbackMsg.getFeedbackType().setStatus(true);
										feedbackMsg.setDescription("DELIVERED");
										isDelivered = true;
										break;
									}
							}
							if(!isDelivered){
								feedbackMsg.setFeedbackType(SMTPFeedbackType.OPEN);
								feedbackMsg.getFeedbackType().setStatus(false);
								feedbackMsg.setDescription("NOT OPENED");
							}
						}else{
							feedbackMsg.setFeedbackType(SMTPFeedbackType.OPEN);
							feedbackMsg.getFeedbackType().setStatus(false);
							feedbackMsg.setDescription("ERROR IN OPEN EMAIL API : 0 Delivered objects");
						}
					}else{
						feedbackMsg.setFeedbackType(SMTPFeedbackType.ERROR);
						feedbackMsg.getFeedbackType().setStatus(false);
						feedbackMsg.setDescription("ERROR IN OPEN EMAIL API : "+responseObj.getStatus());
					}
				}else{
					logger.error("Response object is null while parsing the OPEN EMAIL API response for message id: "+feedbackMsg.getMessageId());
					feedbackMsg.setFeedbackType(SMTPFeedbackType.ERROR);
					feedbackMsg.getFeedbackType().setStatus(false);
					feedbackMsg.setDescription("ERROR IN PARSING OPEN EMAIL API RESPONSE");
				}
			}else{
				logger.error("HTTP REQUEST CONNECTION problem in OPEN EMAIL API for message id: "+feedbackMsg.getMessageId());
				feedbackMsg.setDescription("HTTP REQUEST CONNECTION PROBLEM IN OPEN EMAIL API");
			}
		} catch (Exception e) {
			feedbackMsg.setFeedbackType(SMTPFeedbackType.ERROR);
			feedbackMsg.getFeedbackType().setStatus(false);
			feedbackMsg.setDescription("ERROR IN OPEN EMAIL API : "+e.getMessage());
			logger.error("Exception occured in PollFeedback.checkOpen(): "+e);
			e.printStackTrace();
		}
	}
	
	public static void checkOpen2(Date from, Date to, MailNotificationManager mailNotificationManager) {
		logger.info("PollFeedback.checkOpen2() start " + from + " " + to);
		try {
			String respString = HttpUtil.doGet(Conf.getUrlEmailOpened(),"PollFeedback.checkOpen2()", from, to);
			/* Here is a JSon sample that we receive from Dyn 
	/opens :
	{
    	"response": {
        	"status": 200,
        	"message": "OK",
        	"data": {
            	"opens": [
                	{
                    	"xheaders": {
                        	"X-messageId": "80919059952947854"
                    	},
                    	"emailaddress": "kowalska77@icloud.com",
                    	"ip": "86.136.179.133",
                    	"stage": "read",
                    	"date": "2017-06-15T13:02:13+00:00"
                	},...
     /opens/unique :
     "opens": [
                {
                    "xheaders": {
                        "X-messageId": "1498219036554_10"
                    },
                    "emailaddress": "test@gmail.com",
                    "date": "2017-06-23T11:57:22+00:00",
                    "count": "1"
                },....
			 */
			if (respString != null ) {
				Response responseObj = new ParseFeedbackResponse().parse(respString);
				if (responseObj != null) {
					logger.info("response message: "+responseObj.getMessage()+" / "+responseObj.getStatus());
					if (responseObj.getMessage().equalsIgnoreCase("OK") && responseObj.getStatus() == 200) {
						if (responseObj.getData()!=null && responseObj.getData().getOpens()!=null) {
							Opens[] opensList = responseObj.getData().getOpens();
							logger.info("Got "+opensList.length+" opens");
							for (int i=0; i<opensList.length; i++) {
								Opens opens = opensList[i];
								logger.debug("opens["+i+"] = "+opens);
								sendFeedback(opens.getXheaders().getXmessageId(), responseObj.getStatus(), mailNotificationManager);
								
								// When we connect to "/opens/unique" , we get "count" info.
								// We always return "DELIVERED"								
								/*
								// When we connect to "/opens" , we get "stage" info. We can check the detailed status (skimmed, read, seen)
								NotificationStatus ns = NotificationStatus.SENT;
								String stage = opens.getStage();
								if ("seen".equalsIgnoreCase(stage) || "skimmed".equalsIgnoreCase(stage))
									ns = NotificationStatus.SENT;
								else if ("read".equalsIgnoreCase(stage))
									ns = NotificationStatus.DELIVERED;
								else {
									if (logger.isInfoEnabled()) logger.info("Unexpected response: "+stage);
								}
								*/    
								
								
							}
						} else {
							if (logger.isDebugEnabled()) logger.debug(PollFeedback.class.getName() + ".checkOpen2 : empty response received.");
						}
					} else {
						if (logger.isTraceEnabled())
							logger.trace(PollFeedback.class.getName() + ".checkOpen2 : REST error : " + responseObj.getMessage() + " " + responseObj.getStatus());
					}
				} else {
					logger.info(PollFeedback.class.getName() + ".checkOpen2 : REST API returned empty string");
				}
			} else {
			  logger.info(PollFeedback.class.getName() + ".checkOpen2 : REST API returned null string");
			}
		} catch (Exception e) {
		  logger.error(PollFeedback.class.getName() + ".checkOpen2 : REST API exception : " + e);
		}
		if (logger.isDebugEnabled()) logger.debug("PollFeedback.checkOpen2() end ");
	}


	public static void isBounceBack(SMTPFeedbackMsg feedbackMsg, String openUrl, int httpRequestTimeoutVal){
		ParseFeedbackResponse parseResponse = new ParseFeedbackResponse();
		try{
			String respString = HttpUtil.doGet(openUrl,"PollFeedback.isBounceBack()", httpRequestTimeoutVal);
			
			if(respString != null ){
				Response responseObj = parseResponse.parse(respString);
				if(responseObj != null){
					logger.debug("response message: "+responseObj.getMessage()+"	|	response status: "+responseObj.getStatus());
					if(responseObj.getMessage().equalsIgnoreCase("OK") && responseObj.getStatus() == 200){
						if(responseObj.getData()!=null && responseObj.getData().getBounces()!=null){ 
							Bounces[] bounceList = responseObj.getData().getBounces();
							boolean isbounce = false;
							for(int i=0;i<responseObj.getData().getBounces().length;i++){
								Bounces bounce = bounceList[i];
								if(bounce!=null && feedbackMsg.getMessageId().equalsIgnoreCase(bounce.getXmessageId())){
									feedbackMsg.setFeedbackType(SMTPFeedbackType.BOUNCED);
									feedbackMsg.getFeedbackType().setStatus(true);
									isbounce = true;
									break;
								}
							}
							if(!isbounce){
								feedbackMsg.setFeedbackType(SMTPFeedbackType.BOUNCED);
								feedbackMsg.getFeedbackType().setStatus(false);
							}
						}else{
							feedbackMsg.setFeedbackType(SMTPFeedbackType.BOUNCED);
							feedbackMsg.getFeedbackType().setStatus(false);
						}
					}else{
						feedbackMsg.setFeedbackType(SMTPFeedbackType.ERROR);
						feedbackMsg.getFeedbackType().setStatus(false);
					}
				}else{
					logger.error("Response object is null while parsing the BOUNCE BACK API response for message id: "+feedbackMsg.getMessageId());
					feedbackMsg.setFeedbackType(SMTPFeedbackType.ERROR);
					feedbackMsg.getFeedbackType().setStatus(false);
				}
			}else{
				logger.error("HTTP REQUEST CONNECTION problem in BOUNCE BACK API for message id: "+feedbackMsg.getMessageId());
			}
		} catch (Exception e) {
			feedbackMsg.setFeedbackType(SMTPFeedbackType.ERROR);
			feedbackMsg.getFeedbackType().setStatus(false);
			logger.error("Exception occured in PollFeedback.isBounceBack("+feedbackMsg.getMessageId()+"): "+e.getMessage());
		}
	}

	public static void isClickedURL(SMTPFeedbackMsg feedbackMsg, String clickedUrl, String msgId, String dateFormat, int httpRequestTimeoutVal){
		ParseFeedbackResponse parseResponse = new ParseFeedbackResponse();
		Date pollDateTime = new Date();
		ClickedUrlBean beanObj = null;
		try{
			beanObj = getClickedUrlBeanObject(feedbackMsg.getCriteMap(), msgId, SMTPUtil.convertDateAsPattern(pollDateTime, dateFormat));
			String respString = HttpUtil.doGet(clickedUrl,"PollFeedback.isClickedURL()", httpRequestTimeoutVal);
			if(respString != null ){
				Response responseObj = parseResponse.parse(respString);
				if(responseObj != null){
					logger.debug("response message: "+responseObj.getMessage()+"	|	response status: "+responseObj.getStatus());
					if(responseObj.getMessage().equalsIgnoreCase("OK") && responseObj.getStatus() == 200){
						if(responseObj.getData()!=null && responseObj.getData().getClicks()!=null){ 
							StringBuffer buf = new StringBuffer();
							Clicks[] clicksList = responseObj.getData().getClicks();
							boolean isClicked = false;
							for(int i=0;i<clicksList.length;i++){
									Clicks clicks = clicksList[i];
									if(clicks!=null && clicks.getClicklink() != null && !clicks.getClicklink().isEmpty()){
										if(i==0){
											buf.append(clicks.getClicklink());
											feedbackMsg.setFeedbackType(SMTPFeedbackType.CLICKED);
											feedbackMsg.getFeedbackType().setStatus(true);
											feedbackMsg.setDescription("CLICKED");
											isClicked = true;
										}else{
											buf.append(" | "+clicks.getClicklink());
										}
										//break;
									}
							}
							beanObj.setUrlClicked(buf.toString());
							if(!isClicked){
								feedbackMsg.setFeedbackType(SMTPFeedbackType.CLICKED);
								feedbackMsg.getFeedbackType().setStatus(false);
								feedbackMsg.setDescription("NOT CLICKED");
							}
						}else{
							feedbackMsg.setFeedbackType(SMTPFeedbackType.CLICKED);
							feedbackMsg.getFeedbackType().setStatus(false);
							feedbackMsg.setDescription("NOT CLICKED");
						}
					}else{
						feedbackMsg.setFeedbackType(SMTPFeedbackType.ERROR);
						feedbackMsg.getFeedbackType().setStatus(false);
						feedbackMsg.setDescription("ERROR- IN CLICKED URL API WITH RESPONSE STATUS: "+responseObj.getStatus());
					}
				}else{
					logger.error("Response object is null while parsing the CLICKED URL API response for message id: "+feedbackMsg.getMessageId());
					feedbackMsg.setFeedbackType(SMTPFeedbackType.ERROR);
					feedbackMsg.getFeedbackType().setStatus(false);
					feedbackMsg.setDescription("ERROR- IN PARSING CLICKED URL API RESPONSE");
				}
			}else{
				logger.error("HTTP REQUEST CONNECTION problem in CLICKED URL API for message id: "+feedbackMsg.getMessageId());
				feedbackMsg.setDescription("ERROR- HTTP REQUEST CONNECTION PROBLEM IN CLICKED URL API");
			}
			logger.info(beanObj.getMessageId()+","+beanObj.getMsisdn()+","+beanObj.getModuleId()+","+beanObj.getFeatureId()+","+beanObj.getPollingDateTime()
					+","+feedbackMsg.getDescription()+","+beanObj.getUrlClicked());
			
		} catch (Exception e) {
			feedbackMsg.setFeedbackType(SMTPFeedbackType.ERROR);
			feedbackMsg.getFeedbackType().setStatus(false);
			feedbackMsg.setDescription("ERROR- EXCEPTION OCCURED "+e.getMessage());
			logger.error("Exception occured in PollFeedback.isClickedURL("+feedbackMsg.getMessageId()+"): "+e.getMessage());
			beanObj = getClickedUrlBeanObject(feedbackMsg.getCriteMap(), msgId, SMTPUtil.convertDateAsPattern(pollDateTime, dateFormat));
			logger.info(beanObj.getMessageId()+","+beanObj.getMsisdn()+","+beanObj.getModuleId()+","+beanObj.getFeatureId()+","+beanObj.getPollingDateTime()
					+","+feedbackMsg.getDescription()+","+beanObj.getUrlClicked());
		}
	}
	
	public static ClickedUrlBean getClickedUrlBeanObject(Map<String, String> map, String messageId, String pollingDateTime){
		ClickedUrlBean beanObj = new ClickedUrlBean();
		if(map != null && map.size() > 0){
			beanObj.setModuleId(map.get("DMM_CI_MODULE_ID"));
			beanObj.setFeatureId(map.get("DMM_CI_FEATURE_ID"));
			beanObj.setMsisdn(map.get("DMM_CI_CUSTOMER_ID"));
			beanObj.setMessageId(messageId);
			beanObj.setPollingDateTime(pollingDateTime);
		}
		return beanObj;
	}
	
	  private static void sendFeedback(String messageId, int status, MailNotificationManager mailNotificationManager){
	    logger.info("PollFeedback.sendFeedback("+messageId+", "+status+") got a response, processing feedback");
	    HashMap<String,Object> correlatorUpdateRecord = new HashMap<String,Object>();
	    correlatorUpdateRecord.put("result", status);
//	    correlatorUpdateRecord.put("deliveryTime", packet.getDeliveryTime().toString());
//	    correlatorUpdateRecord.put("destination", packet.getDestination());
//	    correlatorUpdateRecord.put("sequenceNumber", packet.getSequenceNum());
	    JSONObject correlatorUpdate = JSONUtilities.encodeObject(correlatorUpdateRecord);
	    mailNotificationManager.submitCorrelatorUpdateDeliveryRequest(messageId, correlatorUpdate);
	  }
}
