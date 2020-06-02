/****************************************************************************
*
*  ReportService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.CronFormat;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.core.utilities.UtilitiesException;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Report.SchedulingInterval;
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
  public static final DateFormat TIMESTAMP_PRINT_FORMAT;
  static
  {
    TIMESTAMP_PRINT_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    TIMESTAMP_PRINT_FORMAT.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
  }
  
  private File reportDirectory;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ReportService(String bootstrapServers, String groupID, String reportTopic, boolean masterService, ReportListener reportListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "ReportService", groupID, reportTopic, masterService, getSuperListener(reportListener), "putReport", "removeReport", notifyOnSignificantChange);
    this.reportDirectory = validateAndgetReportDirectory();
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
  
  /***************************
   * 
   * launchReport GUI
   * 
   ***************************/
  
  public void launchReport(String reportName, Boolean backendSimulator)
  {
    if (!backendSimulator)
      {
        Date now = SystemTime.getCurrentTime(); 
        launchReport(reportName, now);
      }
    else
      {
        Collection<Report> activeReports = getActiveReports(SystemTime.getCurrentTime());
        Report report = null;
        for (Report activeReport : activeReports)
          {
            if (activeReport.getName().equals(reportName) && !activeReport.getEffectiveScheduling().isEmpty()) report = activeReport;
          }
        if (report == null )
          {
            log.info("RAJ K no active report found with scheduling for name {}", reportName);
          }
        else
          {
            launchReport(report);
          }
      }
  }
  
  /***************************
   * 
   * launchReport back end
   * 
   ***************************/
  
  public void launchReport(Report report)
  {
    //
    //  now
    //
    
    Date now = SystemTime.getCurrentTime();
    
    //
    //  getPendingReportsForDates
    //
    
    Set<Date> pendingReportsForDates = getPendingReportsForDates(report);
    pendingReportsForDates.add(now);
    
    StringBuilder dateRAJKString = new StringBuilder();
    pendingReportsForDates.forEach(dt -> dateRAJKString.append(" " + printDate(dt)));
    log.info("RAJ K generating reoports for dates {}", dateRAJKString);
    
    for (Date date : pendingReportsForDates)
      {
        launchReport(report.getName(), date);
      }
  }
  
  /***************************
   * 
   * launchReport
   * 
   ***************************/
  
  public void launchReport(String reportName, final Date reportGenerationDate)
  {
    //
    //  znode
    //
    
    String znode = ReportManager.getControlDir() + File.separator + "launchReport-" + reportName + "-";
    
    //
    //  data
    //
    
    JSONObject zkJSON = new JSONObject();
    zkJSON.put("reportName", reportName);
    zkJSON.put("reportGenerationDate", reportGenerationDate.getTime());
    byte[] zkData = zkJSON.toJSONString().getBytes(StandardCharsets.UTF_8);
    
    if (getZKConnection())
      {
        try
          {
            //
            // Create new file in control dir with reportName inside, to trigger report generation
            //
            
            //zk.create(znode, reportName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            zk.create(znode, zkData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
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
  
  /***************************
   * 
   * getPendingReportsForDates
   * 
   ***************************/
  
  private Set<Date> getPendingReportsForDates(Report report)
  {
    Set<Date> pendingDates = new HashSet<Date>();
    
    //
    //  filter by name
    //
    
    FileFilter filter = new FileFilter() { public boolean accept(File file) { return file.getName().startsWith(report.getName()); } };
    
    //
    //  generatedReports
    //
    
    Set<Date> generatedDates = new HashSet<Date>();
    int doLs = 0;
    while(doLs < 3)
      {
        File[] generatedReports = this.reportDirectory.listFiles(filter);
        if (generatedReports == null)
          {
            log.warn("unable to listFiles in {} - may be an I/O error - will retry", Deployment.getReportManagerOutputPath());
            
            //
            //  re validate
            //
            
            this.reportDirectory = validateAndgetReportDirectory();
          }
        else
          {
            for (File generatedReportFile : generatedReports)
              {
                String fileName = generatedReportFile.getName();
                Date reportDate = getReportDate(fileName, report.getName());
                if (reportDate != null) generatedDates.add(reportDate);
              }
          }
        doLs ++;
      }
    
    //
    //  pendingDates
    //
    
    StringBuilder generatedDatesRAJKString = new StringBuilder();
    generatedDates.forEach(dt -> generatedDatesRAJKString.append(" " + printDate(dt)));
    log.info("RAJ K getPendingReportsForDates generatedDates {}", generatedDatesRAJKString);
    
    pendingDates = compareAndGetDates(report, generatedDates);
    
    StringBuilder pendingDatesDatesRAJKString = new StringBuilder();
    pendingDates.forEach(dt -> pendingDatesDatesRAJKString.append(" " + printDate(dt)));
    log.info("RAJ K getPendingReportsForDates pendingDates {}", pendingDatesDatesRAJKString);
    
    //
    //  filterIfUpdated
    //
    
    //pendingDates = pendingDates.stream().filter(pendingDadate -> pendingDadate.after(report.getUpdatedDate())).collect(Collectors.toSet());
    
    //
    //  return
    //
    
    return pendingDates;
  }

  /***************************
   * 
   * compareAndGetDates
   * 
   ***************************/
  
  private Set<Date> compareAndGetDates(Report report, Set<Date> generatedDates)
  {
    Set<Date> result = new HashSet<Date>();
    final Date now = RLMDateUtils.truncate(SystemTime.getCurrentTime(), Calendar.DATE, Deployment.getBaseTimeZone());
    for (SchedulingInterval schedulingInterval : report.getEffectiveScheduling())
      {
        log.info("RAJ K compareAndGetDates schedulingInterval {}", schedulingInterval);
        
        //
        //  ignore HOURLY reports
        //
        
        if (schedulingInterval == SchedulingInterval.HOURLY) continue;
        
        Calendar c = SystemTime.getCalendar();
        c.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
        c.setTime(now);
        Date start = null;
        Date end = null;
        
        //
        //  datesToCheck
        //
        
        Set<Date> datesToCheck = new LinkedHashSet<Date>();
        switch (schedulingInterval)
        {
          case DAILY:
            end = RLMDateUtils.addDays(now, -1, Deployment.getBaseTimeZone());
            start = RLMDateUtils.addDays(now, -6, Deployment.getBaseTimeZone());
            break;
            
          case WEEKLY:
            int i = c.get(Calendar.DAY_OF_WEEK) - c.getFirstDayOfWeek();
            c.add(Calendar.DATE, -i - 7);
            start = c.getTime();
            c.add(Calendar.DATE, 6);
            end = c.getTime();
            break;
            
          case MONTHLY:
            int dayOfMonth = c.get(Calendar.DAY_OF_MONTH);
            Date previousMonth = RLMDateUtils.addMonths(now, -1, Deployment.getBaseTimeZone());
            start = RLMDateUtils.addDays(previousMonth, -dayOfMonth + 1, Deployment.getBaseTimeZone());
            c.setTime(start);
            int endDay = c.getActualMaximum(Calendar.DAY_OF_MONTH);
            end = RLMDateUtils.addDays(start, endDay -1, Deployment.getBaseTimeZone());
            break;

          default:
            break;
        }
        while (start.before(end) || start.equals(end))
          {
            datesToCheck.add(start);
            start = RLMDateUtils.addDays(start, 1, Deployment.getBaseTimeZone());
          }
        
        StringBuilder datesToCheckRAJKString = new StringBuilder();
        datesToCheck.forEach(dt -> datesToCheckRAJKString.append(" " + printDate(dt)));
        log.info("RAJ K compareAndGetDates datesToCheck {}", datesToCheckRAJKString);
        //
        // compare
        //
        
        switch (schedulingInterval)
        {
          case DAILY:
            for (Date dateToCheck : datesToCheck)
              {
                if (!generatedDates.contains(dateToCheck)) 
                  {
                    result.add(dateToCheck);
                  }
              }
            break;
            
          case WEEKLY:
            boolean generatedLastWeek = false;
            for (Date dateToCheck : datesToCheck)
              {
                if (generatedDates.contains(dateToCheck))
                  {
                    generatedLastWeek = true;
                    break;
                  }
              }
            if (!generatedLastWeek)
              {
                Date lastWeekReportDate = getPreviousReportDate(Deployment.getWeeklyReportCronEntryString(), now);
                if(lastWeekReportDate == null) lastWeekReportDate = RLMDateUtils.addWeeks(now, -1, Deployment.getBaseTimeZone());
                lastWeekReportDate = RLMDateUtils.truncate(lastWeekReportDate, Calendar.DATE, Deployment.getBaseTimeZone());
                result.add(lastWeekReportDate);
              }
            break;
            
          case MONTHLY:
            boolean generatedLastMonth = false;
            for (Date dateToCheck : datesToCheck)
              {
                if (generatedDates.contains(dateToCheck))
                  {
                    generatedLastMonth = true;
                    break;
                  }
              }
            if (!generatedLastMonth)
              {
                Date lastMonthReportDate = getPreviousReportDate(Deployment.getMonthlyReportCronEntryString(), now);
                if (lastMonthReportDate != null)
                  {
                    int thisMonth = RLMDateUtils.getField(now, Calendar.MONTH, Deployment.getBaseTimeZone());
                    int lastMonthReportsMonth = RLMDateUtils.getField(lastMonthReportDate, Calendar.MONTH, Deployment.getBaseTimeZone());
                    if (thisMonth == lastMonthReportsMonth) lastMonthReportDate = RLMDateUtils.addMonths(lastMonthReportDate, -1, Deployment.getBaseTimeZone());
                  }
                else
                  {
                    lastMonthReportDate = RLMDateUtils.addMonths(now, -1, Deployment.getBaseTimeZone());
                  }
                lastMonthReportDate = RLMDateUtils.truncate(lastMonthReportDate, Calendar.DATE, Deployment.getBaseTimeZone());
                result.add(lastMonthReportDate);
              }
            break;

          default:
            break;
        }
      }
    
    //
    //  return
    //
    
    return result;
  }

  /***************************
   * 
   * getPreviousReportDate
   * 
   ***************************/
  
  private Date getPreviousReportDate(String cron, Date now)
  {
    Date result = null;
    try
      {
        CronFormat cronFormat = new CronFormat(cron, TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
        result = cronFormat.previous(now);
      } 
    catch (UtilitiesException e)
      {
        log.error("can't parse cron entry {}", cron);
      }
    return result;
  }

  /***************************
   * 
   * getReportDate
   * 
   ***************************/
  
  private Date getReportDate(String reportFileName, String fileNameInitial)
  {
    Date result = null;
    String reportDateString = reportFileName.split(fileNameInitial + "_")[1].split("." + Deployment.getReportManagerFileExtension())[0];
    try
      {
        SimpleDateFormat sdf = new SimpleDateFormat(Deployment.getReportManagerDateFormat());
        sdf.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
        result = sdf.parse(reportDateString);
        result = RLMDateUtils.truncate(result, Calendar.DATE, Deployment.getBaseTimeZone());
      } 
    catch (ParseException e)
      {
        log.error(e.getMessage());
      }
    return result;
  }
  
  /***************************
   * 
   * validateAndgetReportDirectory
   * 
   ***************************/
  
  private File validateAndgetReportDirectory()
  {
    //
    //  create
    //
    
    File reportDirectoryFile = new File(Deployment.getReportManagerOutputPath());
    
    //
    // validate
    //
    
    if (! reportDirectoryFile.isAbsolute())throw new ServerRuntimeException("reportDirectory must specify absolute path");
    
    //
    // can ls
    //
    
    if (reportDirectoryFile.listFiles() == null) throw new ServerRuntimeException("unable to ls reportDirectory " + reportDirectoryFile.getAbsolutePath());
    
    //
    // return
    //
    
    return reportDirectoryFile;
  }
  
  public String printDate(Date date)
  {
    return TIMESTAMP_PRINT_FORMAT.format(date);
  }
  

}
