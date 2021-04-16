package com.lumatagroup.expression.driver.SMTP.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lumatagroup.expression.driver.SMTP.FeedbackThread;

/**
 * 
 * @author Bhavishya
 *
 */
public class SMTPUtil {
	private static Logger logger = LoggerFactory.getLogger(SMTPUtil.class);
	public static  Date  getFeedBackDate(){
		return new Date();
	}
	public static String convertString(Object obj){
		String returnValue=null;
		if(obj!=null){
			returnValue=String.valueOf(obj);
		}
		return returnValue;
	}
	
	public static Integer convertInt(Object obj){
		String returnValue=null;
		if(obj!=null){
			returnValue=String.valueOf(obj);
			if(returnValue!=null){
				try{
					return Integer.parseInt(returnValue);
				}catch(NumberFormatException e){}
			}
		}
		
		return null;
	}
	
	public static Long convertLong(Object obj){
		String returnValue = null;
		if (obj != null) {
			returnValue = String.valueOf(obj);
			if (returnValue!=null) {
				try {
					return Long.parseLong(returnValue);
				} catch (NumberFormatException e){}
			}
		}
		return null;
	}

	public static String convertDateAsPattern(Date date, String dateFormat){
		if (date == null) {
	          throw new IllegalArgumentException("The date must not be null");
	    }
		SimpleDateFormat ft = new SimpleDateFormat (dateFormat); // WARNING, deprecated usage of SimpleDateFormat - See RLMDateUtils
		String tz = Conf.getTimeZone();
		if (logger.isDebugEnabled()) logger.debug("convertDateAsPattern tz = " + tz);
		ft.setTimeZone(TimeZone.getTimeZone(tz));  // Dates in Dyn are expressed in UTC (tz should be "GMT")
		return ft.format(date);
	}
	
	public static String convertFromDateWithPattern(Date date, String dateFormat){
		if (logger.isDebugEnabled()) logger.debug("convertFromDateWithPattern START " + date + " " + dateFormat);
		String res = null;
		if (date == null) {
	          throw new IllegalArgumentException("The date must not be null");
	    }
		SimpleDateFormat ft = new SimpleDateFormat (dateFormat); // WARNING, deprecated usage of SimpleDateFormat - See RLMDateUtils
		res = ft.format(date);
		if (logger.isDebugEnabled()) logger.debug("convertFromDateWithPattern END " + res);
		return res;
	}
	
	public static Date convertFromStringWithPattern(String date, String dateFormat){
		if (logger.isDebugEnabled()) logger.debug("convertDateFromPattern START " + date + " " + dateFormat);
		Date res = null;
		if (date == null) {
	          throw new IllegalArgumentException("The date must not be null");
	    }
		SimpleDateFormat ft = new SimpleDateFormat (dateFormat); // WARNING, deprecated usage of SimpleDateFormat - See RLMDateUtils
		try {
			res = ft.parse(date);
		} catch (ParseException e) {
			if (logger.isDebugEnabled()) logger.debug("convertDateFromPattern exception " + e.getLocalizedMessage());
		}
		if (logger.isDebugEnabled()) logger.debug("convertDateFromPattern END " + res);
		return res;
	}

	/*
	 From Dyn documentation https://help.dyn.com/date-formatting-for-the-api/
	It is important to have the proper format for the date field in all API requests. We use basic ISO 8601 format for mixed date-time:
		yyyy-MM-DDThh:mm:ss
	
		    yyyy is the 4-digit year
		    MM is the 2-digit month (zero-padded)
		    DD is the 2-digit day (zero-padded)
		    T is a constant to indicate that the time follows
		    hh is the 2-digit hour (zero-padded)
		    mm is the 2-digit minute (zero-padded)
		    ss is the 2-digit second (zero-padded)
	
			Note: All dates and times are stored and displayed in UTC (Greenwich Mean Time) timezone.
			
		Javadoc : https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
			Letter 	Date or Time Component 	Presentation 	Examples
			y 	Year 	Year 	1996; 96
			M 	Month in year 	Month 	July; Jul; 07
			d 	Day in month 	Number 	10
			H 	Hour in day (0-23) 	Number 	0
			m 	Minute in hour 	Number 	30
			s 	Second in minute 	Number 	55
	*/
	public static String convertDateToDynFormat(Date date) {
		String conversionFmt = Conf.getThirdPartyRequestApiDateFormat();
		String res = convertDateAsPattern(date, conversionFmt);
		if (logger.isDebugEnabled()) logger.debug("convertDateToDynFormat " + date + " " + conversionFmt + " = " + res);
		return res;
	}
	
	public static Date getStartTime() {
		return new Date();
	}
	
	public static Date add(Date date, int calendarField, int pollingTimeAmount) {
	      if (date == null) {
	          throw new IllegalArgumentException("The date must not be null");
	      }
	      Calendar c = Calendar.getInstance();
	      c.setTime(date);
	      c.add(calendarField, pollingTimeAmount);
	      return c.getTime();
	}
	
	public static Date calculateEndTime(Date date, int amount) {
	      return add(date, Calendar.SECOND, (amount/1000));
	}
	
	public static String createRequestUrl(String requestUrl, Date startDate, Date endDate) {
		String startTxt = convertDateToDynFormat(startDate);
		String endTxt = convertDateToDynFormat(endDate);
		return createRequestUrl2(requestUrl, startTxt, endTxt);
	}
	
//	public static String createRequestUrl(String requestUrl, String messageId, String startTimeStr, String endTimeStr, String apiKey){
//		StringBuffer url = new StringBuffer(requestUrl);
//		if(startTimeStr != null && endTimeStr != null){
//			url.append("?apikey="+apiKey).append("&starttime="+startTimeStr).append("&endtime="+endTimeStr).append("&X-messageId="+messageId);
//		}else{
//			url.append("?apikey="+apiKey).append("&X-messageId="+messageId);
//		}
//		return url.toString();
//	}
	
	public static String createRequestUrl2(String requestUrl, String startTimeStr, String endTimeStr){
		String res;
		String apiKey = Conf.getUrlApiKey();
		StringBuffer url = new StringBuffer(requestUrl).append("?apikey="+apiKey);
		if (startTimeStr != null) {
			url.append("&starttime="+startTimeStr);
		}
		if (endTimeStr != null) {
			url.append("&endtime="+endTimeStr);
		}
		res = url.toString();
		if (logger.isDebugEnabled()) logger.debug(">>>>> createRequestUrl2 res = " + res);
		return res;
	}
	
}
