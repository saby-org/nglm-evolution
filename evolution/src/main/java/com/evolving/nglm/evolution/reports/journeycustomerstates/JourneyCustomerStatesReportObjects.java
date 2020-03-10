package com.evolving.nglm.evolution.reports.journeycustomerstates;

import java.util.Arrays;
import java.util.List;

/**
 * This is the central point for data related to Journey topic.
 * It holds definitions for Elastic Search fields, and the key used in the topics.
 *
 */
public class JourneyCustomerStatesReportObjects {
	
	public static final String SUBSCRIBER_ES_INDEX = "subscriberprofile";
	public static final String JOURNEY_ES_INDEX = "journeystatistic*";
	
	public static final String TOPIC_PREFIX = ""; // Topic is JourneyReport-20181214_132459-[12]

	public static final int INDEX_CUSTOMER = 0;
	public static final int INDEX_JOURNEY = 1;
	
	public static final String APPLICATION_ID_PREFIX = "1-evol-appid-";
	public static final String CLIENTID_PREFIX = "client-journeys-";

	public static final String KEY_STR = "subscriberID"; // Must exist in schemas of both indexes.
	
	public static final String CUSTOMER_CUSTOMER_ID    = "subscriberID";
	public static final String CUSTOMER_PROFILE_ID     = "ratePlan";
	public static final String CUSTOMER_GENDER         = "accountTypeID";
	public static final String CUSTOMER_AGE            = "region";
	
	public static final String JOURNEY_CUSTOMER_ID     = "subscriberID";
	public static final String JOURNEY_JOURNEY_ID      = "journeyInstanceID";
	public static final String JOURNEY_CUSTOMER_STATUS = "toNodeID";
	public static final String JOURNEY_EVENT_DATE      = "transitionDate";
	public static final String JOURNEY_TRANSITION_DATE = "nodeID/transitionDate";
	
	public static final List<String> journeyCustomerStatesHeadings = Arrays.asList(CUSTOMER_CUSTOMER_ID, CUSTOMER_PROFILE_ID, CUSTOMER_AGE, CUSTOMER_GENDER, JOURNEY_TRANSITION_DATE);
	
	static public class StatusEntry {
		public final static String CUSTOMER_STATUS = "customer_status"; // Must be name of field in Java object 
		public final static String EVENT_DATE = "event_date"; // Must be name of field in Java object 
		@Override
		public String toString() {
			return "("+customer_status + ";" + event_date + ")";
		}
		public StatusEntry() {
			super();
		}
		public StatusEntry(String customer_status, String event_date) {
			this();
			this.customer_status = customer_status;
			this.event_date = event_date;
		}
		public String customer_status;
		public String event_date;
	}

	
}
