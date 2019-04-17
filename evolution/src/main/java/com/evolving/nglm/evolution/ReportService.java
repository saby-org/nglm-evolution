/****************************************************************************
*
*  ReportService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.reports.ReportManager;

public class ReportService extends GUIService
{
/*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(ReportService.class);

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ReportService(String bootstrapServers, String groupID, String reportTopic, boolean masterService, ReportListener reportListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "ReportService", groupID, reportTopic, masterService, getSuperListener(reportListener), "putReport", "removeReport", notifyOnSignificantChange);
  }

  //
  //  constructor
  //
  
  public ReportService(String bootstrapServers, String groupID, String reportTopic, boolean masterService, ReportListener reportListener)
  {
    this(bootstrapServers, groupID, reportTopic, masterService, reportListener, true);
  }

  //
  //  constructor
  //

  public ReportService(String bootstrapServers, String groupID, String reportTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, reportTopic, masterService, (ReportListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(ReportListener reportListener)
  {
    GUIManagedObjectListener superListener = null;
    if (reportListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { reportListener.reportActivated((Report) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { reportListener.reportDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getSummaryJSONRepresentation
  *
  *****************************************/

  @Override protected JSONObject getSummaryJSONRepresentation(GUIManagedObject guiManagedObject)
  {
    JSONObject result = super.getSummaryJSONRepresentation(guiManagedObject);
    result.put(Report.EFFECTIVE_SCHEDULING, guiManagedObject.getJSONRepresentation().get(Report.EFFECTIVE_SCHEDULING));
    return result;
  }
  
  /*****************************************
  *
  *  getReports
  *
  *****************************************/

  public String generateReportID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredReport(String reportID) { return getStoredGUIManagedObject(reportID); }
  public Collection<GUIManagedObject> getStoredReports() { return getStoredGUIManagedObjects(); }
  public boolean isActiveReport(GUIManagedObject reportUnchecked, Date date) { return isActiveGUIManagedObject(reportUnchecked, date); }
  public Report getActiveReport(String reportID, Date date) { return (Report) getActiveGUIManagedObject(reportID, date); }
  public Collection<Report> getActiveReports(Date date) { return (Collection<Report>) getActiveGUIManagedObjects(date); }
  
  /*****************************************
  *
  *  putReport
  *
  *****************************************/

  public void putReport(Report report, boolean newObject, String userID) throws GUIManagerException
  {
    Date now = SystemTime.getCurrentTime();
    putGUIManagedObject(report, now, newObject, userID);
  }

  /*****************************************
  *
  *  interface ReportListener
  *
  *****************************************/

  public interface ReportListener
  {
	  public void reportActivated(Report report);
	  public void reportDeactivated(String guiManagedObjectID);
  }

  private ZooKeeper zk = null;
  private static final int NB_TIMES_TO_TRY = 10;

  /*
   * Called by GuiManager to trigger a one-time report
   */
  public void launchReport(String reportName) {
	  log.trace("launchReport : " + reportName);
	  String znode = ReportManager.getControlDir() + File.separator + "launchReport-" + reportName + "-";
	  if (getZKConnection()) {
		  log.debug("Trying to create ephemeral znode " + znode + " for " + reportName);
		  try {
			  // Create new file in control dir with reportName inside, to trigger report generation
			  zk.create(znode, reportName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		  } catch (KeeperException e) {
			  log.info("Got "+e.getLocalizedMessage());
		  } catch (InterruptedException e) {
			  log.info("Got "+e.getLocalizedMessage());
		  }
	  } else {
		  log.info("There was a major issue connecting to zookeeper");
	  }
  }

  private boolean isConnectionValid(ZooKeeper zookeeper) {
	  return (zookeeper != null) && (zookeeper.getState() == States.CONNECTED);
  }
  
  public boolean isReportRunning(String reportName) {
    String znode = ReportManager.getLockDir() + File.separator + "launchReport-" + reportName + "-";
    Stat stat = null;
    // 
    // return false if anything goes wrong
    //
    boolean result = false;
    try
      { 
        if (getZKConnection()) {
          stat = zk.exists(znode, null);
          result = (stat != null);
        }else {
          log.info("There was a major issue connecting to zookeeper");
        }
        
      } catch (KeeperException e)
      {
        e.printStackTrace();
      } catch (InterruptedException e)
      {
        e.printStackTrace();
      }
    
    return result;
    
    
  }
  
  private boolean getZKConnection()  {
	  if (!isConnectionValid(zk)) {
		  log.debug("Trying to acquire ZooKeeper connection to "+Deployment.getZookeeperConnect());
		  int nbLoop = 0;
		  do {
			  try {
				  zk = new ZooKeeper(Deployment.getZookeeperConnect(), 3000, new Watcher() { @Override public void process(WatchedEvent event) {} }, false);
				  if (!isConnectionValid(zk)) {
					  log.info("Could ot get a zookeeper connection, waiting..."+(NB_TIMES_TO_TRY-nbLoop));
					  try { Thread.sleep(5*1000); } catch (InterruptedException e) {}
				  }
			  } catch (IOException e) {
				  log.info("could not create zookeeper client using {}", Deployment.getZookeeperConnect());
			  }
		  } while (!isConnectionValid(zk) && (nbLoop++ < NB_TIMES_TO_TRY));
	  }
	  return isConnectionValid(zk);
  }

/*****************************************
 *
 *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  reportListener
    //

	  ReportListener reportListener = new ReportListener()
    {
      @Override public void reportActivated(Report report) {
    	  log.trace("report activated: " + report.getReportID());
      }
      @Override public void reportDeactivated(String guiManagedObjectID) { log.trace("report deactivated: " + guiManagedObjectID); }
    };

    //
    //  reportService
    //

    ReportService reportService = new ReportService(Deployment.getBrokerServers(), "example-001", Deployment.getReportTopic(), false, reportListener);
    reportService.start();

    //
    //  sleep forever
    //

    while (true)
      {
        try
          {
            Thread.sleep(Long.MAX_VALUE);
          }
        catch (InterruptedException e)
          {
            //
            //  ignore
            //
          }
      }
  }

}
