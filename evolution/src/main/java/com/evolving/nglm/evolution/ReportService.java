/****************************************************************************
*
*  ReportService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.reports.ReportDriver;
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
  public GUIManagedObject getStoredReport(String reportID, boolean includeArchived) { return getStoredGUIManagedObject(reportID, includeArchived); }
  public Collection<GUIManagedObject> getStoredReports() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredReports(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
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
  public void launchReport(String reportName)
  {
    log.trace("launchReport : " + reportName);
    String znode = ReportManager.getControlDir() + File.separator + "launchReport-" + reportName + "-";
    if (getZKConnection())
      {
        log.debug("Trying to create ephemeral znode " + znode + " for " + reportName);
        try
          {
            // Create new file in control dir with reportName inside, to trigger
            // report generation
            zk.create(znode, reportName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
          }
        catch (KeeperException e)
          {
            log.info("Got " + e.getLocalizedMessage());
          }
        catch (InterruptedException e)
          {
            log.info("Got " + e.getLocalizedMessage());
          }
      }
    else
      {
        log.info("There was a major issue connecting to zookeeper");
      }
  }

  private boolean isConnectionValid(ZooKeeper zookeeper)
  {
	  return (zookeeper != null) && (zookeeper.getState() == States.CONNECTED);
  }
  
  public boolean isReportRunning(String reportName) {
    try
      { 
        if (getZKConnection()) 
          {
            List<String> children = zk.getChildren(ReportManager.getControlDir(), false);
            for(String child : children) 
              {
                if(child.contains(reportName)) 
                  {
                    String znode = ReportManager.getControlDir() + File.separator+child;
                    return (zk.exists(znode, false) != null);
                  }
              }
          }
        else
          {
            log.info("There was a major issue connecting to zookeeper");
          } 
      }
    catch (KeeperException e)
    {
      log.info(e.getLocalizedMessage());
    }
    catch (InterruptedException e) { }
    return false;
  }
  
  private boolean getZKConnection()  {
	  if (!isConnectionValid(zk)) {
		  log.debug("Trying to acquire ZooKeeper connection to "+Deployment.getZookeeperConnect());
		  int nbLoop = 0;
		  do
		    {
		      try 
		      {
		        zk = new ZooKeeper(Deployment.getZookeeperConnect(), 3000, new Watcher() { @Override public void process(WatchedEvent event) {} }, false);
		        try { Thread.sleep(1*1000); } catch (InterruptedException e) {}
		        if (!isConnectionValid(zk))
		          {
		            log.info("Could not get a zookeeper connection, waiting... ("+(NB_TIMES_TO_TRY-nbLoop)+" more times to go)");
		            try { Thread.sleep(5*1000); } catch (InterruptedException e) {}
		          }
		      }
		      catch (IOException e) 
		      {
		        log.info("could not create zookeeper client using {}", Deployment.getZookeeperConnect());
		      }
		    }
		  while (!isConnectionValid(zk) && (nbLoop++ < NB_TIMES_TO_TRY));
	  }
	  return isConnectionValid(zk);
  }
  
  //==
  public JSONObject generateResponseJSON(GUIManagedObject guiManagedObject, boolean fullDetails, Date date)
  {
	  JSONObject responseJSON = super.generateResponseJSON(guiManagedObject, fullDetails, date);

	  if (guiManagedObject instanceof Report)
	  {
		  Report report = (Report) guiManagedObject;
		  try
		  {
			  Class<ReportDriver> reportClass = (Class<ReportDriver>) Class.forName(report.getReportClass());
			  Constructor<ReportDriver> cons = reportClass.getConstructor();
			  ReportDriver rd = cons.newInstance((Object[]) null);
			  JSONArray json = rd.reportFilters();
			  responseJSON.put("filters", json);
		  }
		  catch (Exception e)
		  {
			  // handle any kind of exception that can happen during generating the report, and do not crash the container
			  e.printStackTrace();
		  }
	  }
	  else
	  {
		  log.error("Should never happen!");
	  }
	  return responseJSON;
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
      @Override public void reportActivated(Report report)
      {
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
