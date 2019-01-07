package com.lumatagroup.expression.driver.SMTP.util;

import org.apache.log4j.Logger;

import com.lumatagroup.expression.driver.SMTP.model.SMTPFeedbackMsg;

public class E4OUpdater {
	private static Logger LOG = Logger.getLogger(E4OUpdater.class);

	public void UpdateFeedback(SMTPFeedbackMsg fdbkmsg){
		LOG.info("Inside E4OUpdater.UpdateFeedback() for messageId : "+fdbkmsg.getMessageId());
	}


	public int getDelay(final int numOfConstantTry,
			final int basicDelay,
			final int currentTry) {
		//@formatter:on

		if (currentTry <= numOfConstantTry) {
			return basicDelay;
		}

		return basicDelay * (currentTry - numOfConstantTry + 1);
	}

	/*public static void main(String[] st){
		E4OUpdater ed = new E4OUpdater();
		System.out.println(ed.getDelay(20,100,21));
	}*/

}
