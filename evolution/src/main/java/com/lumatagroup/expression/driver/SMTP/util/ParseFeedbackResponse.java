package com.lumatagroup.expression.driver.SMTP.util;

import org.apache.log4j.Logger;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.lumatagroup.expression.driver.dyn.Response;

public class ParseFeedbackResponse {
	
	private static Logger logger = Logger.getLogger(ParseFeedbackResponse.class);
	
	public Response parse(String responseString){
		Response responseObj = null;
		try {
			JSONParser jsonParser = new JSONParser();
			JSONObject jsonObject = (JSONObject) jsonParser.parse(responseString);
			Gson gson = new GsonBuilder().setPrettyPrinting().create();
			responseObj = gson.fromJson(jsonObject.get("response").toString(), Response.class);
		} catch (ParseException e) {
			logger.error("Exception occured in ParseFeedbackResponse.parse(): "+e.getMessage());
			e.printStackTrace();
		}
		return responseObj;
	}
	
	public static void main(String[] resp){
		ParseFeedbackResponse parse = new ParseFeedbackResponse();
//		String respString = "{\"response\":{\"status\":200,\"message\":\"OK\",\"data\":{\"bounces\":[]}}}";
		String respString = "{\"response\":{\"status\":200,\"message\":\"OK\",\"data\":{\"bounces\":[{\"bouncetype\":\"hard\",\"bouncerule\":\"emaildoesntexist\",\"bounceemail\":\"email@domain.org\",\"bouncemessage\":\"MESSAGE_HERE\",\"bouncetime\":\"2004-02-12T15:19:21+00:00\",\"notified\":\"\",\"X-messageId\":\"486\"}]}}}";
		Response respObj  = parse.parse(respString);
		System.out.println(respObj);
	}
}
