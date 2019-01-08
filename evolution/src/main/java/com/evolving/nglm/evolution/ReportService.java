/****************************************************************************
*
*  ReportService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.Date;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

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

  private ReportListener reportListener = null;
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ReportService(String bootstrapServers, String groupID, String reportTopic, boolean masterService, ReportListener reportListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "ReportService", groupID, reportTopic, masterService, getSuperListener(reportListener), "putReport", "removeReport", notifyOnSignificantChange);
    // TODO : should get rid of this
    this.reportListener = reportListener; 
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
  *  launchReport
  *
  *****************************************/

  public void launchReport(GUIManagedObject report, String userID) throws GUIManagerException
  {
	  log.trace("in launchReport with "+report);
	  reportListener.reportActivated((Report) report);
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
