package com.lumatagroup.expression.driver.SMTP;

import java.util.Date;
import com.evolving.nglm.evolution.MailNotificationManager;
import com.lumatagroup.expression.driver.SMTP.util.Conf;
import com.lumatagroup.expression.driver.dyn.PollFeedback;

public class FeedbackThread extends Thread {
	private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(FeedbackThread.class);
	private boolean mustStop = false;
	private Date from;
	private String messageIdentifierMagic;
	private MailNotificationManager mailNotificationManager;
	
	public FeedbackThread(MailNotificationManager mailNotificationManager) {
		logger.debug("FeedbackThread constructor");
		if (logger.isDebugEnabled()) logger.debug("FeedbackThread messageIdentifierMagic = "+messageIdentifierMagic);
		long now = new Date().getTime();
		int dayInSeconds = 60*60*24;
		from = new Date(now-1000L*dayInSeconds*Conf.getInitialDurationDays());
		if (logger.isDebugEnabled()) logger.debug("FeedbackThread now = "+now+" et "+from.getTime());
		this.mailNotificationManager = mailNotificationManager;
	}

	public void run() {
		while (!mustStop) {
			if (getLock()) {
				// only one driver will get here
				processResponsesFromDyn();
				releaseLock(true);
			}
			long pollingInterval = Conf.getPollingInterval();
			if (logger.isDebugEnabled()) logger.debug("FeedbackThread going to sleep for "+pollingInterval+" seconds at "+new Date());
			try {
				sleep(pollingInterval*1000L);
			} catch (InterruptedException e) {
				if (logger.isDebugEnabled()) logger.debug("FeedbackThread early wakeup2 : "+e.getLocalizedMessage() + " with mustStop="+mustStop);
			}
		}
		logger.info("SMTPDriver3rdPartyNDM.run asked to shutdown");
	}

	private boolean getLock() {
		String thread = Thread.currentThread().getId()+"";
//		if (logger.isDebugEnabled()) logger.debug(".................. FeedbackThread.getLock START th="+thread);
//		boolean res = false;
//		
//		// If lock was last taken a long time ago (now - limit), it may be because of a crash -> get it anyway
//		Date limit = new Date(System.currentTimeMillis() - Conf.getLockGracePeriod() * 60 * 1000);
//		String limitSQL = convertDateToMySQLFormat(limit);
//		String sqlLock = "UPDATE " + Conf.getDateTableName() + " SET locked=TRUE, date_locked=NOW() "
//				+ "WHERE id=" + Conf.getDateId() + " AND ( locked=FALSE OR date_locked <= '" + limitSQL + "')";
//        if (logger.isDebugEnabled()) logger.debug("FeedbackThread.getLock executing request : "+sqlLock);
//       	int updateCount = DBSQLRequest.update(sqlLock, null, pool);
//   		logger.trace("FeedbackThread.getLock th="+thread+" updateCount="+updateCount);
//   		if (updateCount >= SQLUtils.DB_QUERY_OK) {
//   			res = (updateCount == 1);
//   		} else {
//   			logger.warn("FeedbackThread.getLock failed");
//        }
//		if (logger.isDebugEnabled()) logger.debug(".................. FeedbackThread.getLock END " + res + " th=" + thread);
//		return res;
		return true;
	}

	private void releaseLock(boolean shouldHaveLock) {
		String thread = Thread.currentThread().getId()+"";
//		if (logger.isDebugEnabled()) logger.debug(":::::::::::::::::::: FeedbackThread.releaseLock START th="+thread);
//		String sqlLock = "UPDATE " + Conf.getDateTableName() + " SET locked=FALSE, date_locked=NULL WHERE id=" + Conf.getDateId() + " AND locked=TRUE";
//        if (logger.isDebugEnabled()) logger.debug("FeedbackThread.releaseLock executing request : "+sqlLock);
//       	int updateCount = DBSQLRequest.update(sqlLock, null, pool);
//   		logger.trace("FeedbackThread.releaseLock th="+thread+" updateCount="+updateCount);
//   		if (updateCount >= SQLUtils.DB_QUERY_OK) {
//   			if (shouldHaveLock && (updateCount == 0)) {
//   				logger.info("************************ FeedbackThread.releaseLock error : thread "+thread+" did not have the lock");
//   			}
//   		} else {
//   			logger.warn("FeedbackThread.releaseLock failed");
//        }
//		if (logger.isDebugEnabled()) logger.debug("::::::::::::::::::::: FeedbackThread.releaseLock END th=" + thread);
	}

	/*
	 * We hold the DB lock
	 */
	private void processResponsesFromDyn() {
		logger.info("FeedbackThread.processResponsesFromDyn START "+Thread.currentThread().getId());
		Date to = new Date();
		// gather responses from Dyn REST API
		PollFeedback.checkOpen2(from, to, mailNotificationManager);
		logger.info("FeedbackThread.processResponsesFromDyn END "+Thread.currentThread().getId());	
	}

	/*
	 * Can be called by multiple drivers in multiple NDM instances on multiple nodes.
	 * If lock if free, exactly one driver will get the lock (return true). All others immediately get false.
	 * Implem inspired by https://www.xaprb.com/blog/2006/07/26/how-to-coordinate-distributed-work-with-mysqls-get_lock
	 */
//	private boolean getLock2() {
//		if (logger.isDebugEnabled()) logger.debug(".................. FeedbackThread.getLock START th="+Thread.currentThread().getId());
//		boolean res = false;
//		String sqlLock = "select COALESCE(GET_LOCK('" + Conf.getLockName() + "', 0), 0)";
//        if (logger.isDebugEnabled()) logger.debug("FeedbackThread.getLock executing request : "+sqlLock);
//        DBSQLRequest sqlR = new DBSQLRequest();
//        ResultSet rs = sqlR.select(sqlLock, null, pool);
//        try {
//            if (rs != null) {
//                if (rs.next()) {
//                    String status = rs.getString(1); // get 1st column
//            		logger.trace("FeedbackThread.getLock th="+Thread.currentThread().getId()+" status="+status);
//                    res = "1".equals(status);
//                } else {
//                	if (logger.isDebugEnabled()) logger.debug("FeedbackThread.getLock received an empty response");
//                }
//            } else {
//            	if (logger.isDebugEnabled()) logger.debug("FeedbackThread.getLock received a null response");
//            }
//        } catch (SQLException e) {
//            logger.warn("FeedbackThread.getLock failed due to " + e.getLocalizedMessage());
//        } finally {
//        	sqlR.close_request();
//        }
//		if (logger.isDebugEnabled()) logger.debug(".................. FeedbackThread.getLock END " + res + " th=" + Thread.currentThread().getId());
//		return res;
//	}
//
//	private String computeUniqID() {
//		String thread = Thread.currentThread().getId()+"";
//		String myIP = "";
//	    try {
//			myIP = InetAddress.getLocalHost().getHostAddress();
//		} catch (UnknownHostException e1) {
//	        logger.error("FeedbackThread.getLock cannot resolve localhost "+e1);
//		}
//	    String uniqId = myIP + "_" + thread;
//		return uniqId;
//	}
	
//	private boolean getNonblockingLockDebug() {
//		return true;
//	}
//	private void releaseLockDebug() {}

//	private void releaseLock2() {
//		if (logger.isDebugEnabled()) logger.debug(":::::::::::::::::::: FeedbackThread.releaseLock START th="+Thread.currentThread().getId());
//		String sqlRelease = "select COALESCE(RELEASE_LOCK('" + Conf.getLockName() + "'), 0)";
//        if (logger.isDebugEnabled()) logger.debug("FeedbackThread.releaseLock executing request : "+sqlRelease);
//        DBSQLRequest sqlR = new DBSQLRequest();
//        ResultSet rs = sqlR.select(sqlRelease, null, pool);
//        try {
//            if (rs != null) {
//                if (rs.next()) {
//                    String status = rs.getString(1); // get 1st column
//            		logger.trace("FeedbackThread.releaseLock th="+Thread.currentThread().getId()+" status="+status);
//                    if ("0".equals(status)) {
//                   		logger.info("FeedbackThread.releaseLock error : thread "+Thread.currentThread().getId()+" did not have the lock");
//                    }
//                } else {
//                	if (logger.isDebugEnabled()) logger.info("FeedbackThread.releaseLock received an empty response");
//                }
//            } else {
//            	if (logger.isDebugEnabled()) logger.info("FeedbackThread.releaseLock received a null response");
//            }
//        } catch (SQLException e) {
//            logger.warn("FeedbackThread.releaseLock failed due to " + e.getLocalizedMessage());
//        } finally {
//        	sqlR.close_request();
//        }
//		if (logger.isDebugEnabled()) logger.debug(":::::::::::::::::::: FeedbackThread.releaseLock END th="+Thread.currentThread().getId());
//	}
	
	
}
