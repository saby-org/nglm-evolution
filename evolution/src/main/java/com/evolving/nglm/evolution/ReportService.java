/****************************************************************************
*
*  ReportService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.CronFormat;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.core.utilities.UtilitiesException;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Report.SchedulingInterval;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportGenerationDateComperator;
import com.evolving.nglm.evolution.reports.ReportManager;
import com.evolving.nglm.evolution.tenancy.Tenant;

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
    TIMESTAMP_PRINT_FORMAT = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
    TIMESTAMP_PRINT_FORMAT.setTimeZone(TimeZone.getTimeZone(Deployment.getDefault().getTimeZone())); // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct or should it be per tenant ???
  }
  
  private Map<Integer, File> reportDirectoryPerTenant = new HashMap<Integer, File>();

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ReportService(String bootstrapServers, String groupID, String reportTopic, boolean masterService, ReportListener reportListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "ReportService", groupID, reportTopic, masterService, getSuperListener(reportListener), "putReport", "removeReport", notifyOnSignificantChange);
    for (Tenant tenant : Deployment.getTenants()) {
      int tenantID = tenant.getTenantID();
      String fullPath = getReportOutputPath(tenantID);
      Path path = Paths.get(fullPath);
      try
        {
          Files.createDirectories(path);
        }
      catch (IOException e)
        {
          log.error("Cannot create directory to store reports for tenant " + tenantID + " : " + fullPath + " : " + e.getLocalizedMessage());
        }
    }

    for(Tenant tenant : Deployment.getTenants())
      {
        int tenantID = tenant.getTenantID();
        File f = validateAndgetReportDirectory(tenantID);
        reportDirectoryPerTenant.put(tenantID, f);
      }
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
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { reportListener.reportDeactivated(guiManagedObjectID); }
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
  public Collection<GUIManagedObject> getStoredReports(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredReports(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveReport(GUIManagedObject reportUnchecked, Date date) { return isActiveGUIManagedObject(reportUnchecked, date); }
  public Report getActiveReport(String reportID, Date date) { return (Report) getActiveGUIManagedObject(reportID, date); }
  public Collection<Report> getActiveReports(Date date, int tenantID) { return (Collection<Report>) getActiveGUIManagedObjects(date, tenantID); }
  
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
  
  public void launchReport(String reportName, Boolean backendSimulator, int tenantID)
  {
    if (!backendSimulator)
      {
        Date now = SystemTime.getCurrentTime(); 
        launchReport(reportName, now, tenantID);
      }
    else
      {
        Collection<Report> activeReports = getActiveReports(SystemTime.getCurrentTime(), tenantID);
        Report report = null;
        for (Report activeReport : activeReports)
          {
            if (activeReport.getName().equals(reportName) && !activeReport.getEffectiveScheduling().isEmpty()) report = activeReport;
          }
        if (report == null )
          {
            if(log.isDebugEnabled()) log.debug("no active report found with scheduling for name {}", reportName);
          }
        else
          {
            launchReport(report, tenantID, false);
            launchReport(report, tenantID, true);
          }
      }
  }
  
  /***************************
   * 
   * launchReport back end
   * 
   ***************************/
  
  public void launchReport(Report report, int tenantID, boolean launchPendingReports)
  {
    //
    //  now
    //
    
    Date now = SystemTime.getCurrentTime();
    
    //
    //  getPendingReportsForDates
    //
    
    Set<Date> pendingReportDates = new HashSet<Date>();
    if (launchPendingReports && report.getMissingReportArearCount() > 0)
      {
        pendingReportDates = getPendingReportsForDates(report, tenantID);
      }
    
    if (!launchPendingReports) pendingReportDates.add(now);
    
    
    //
    //  set to list
    //
    
    List<Date> pendingReportsForDates = pendingReportDates.stream().collect(Collectors.toList());
    
    //
    //  sort
    //
    
    Collections.sort(pendingReportsForDates, Collections.reverseOrder());
    if (launchPendingReports) pendingReportsForDates = pendingReportsForDates.stream().limit(Long.valueOf(report.getMissingReportArearCount())).collect(Collectors.toList());
    
    //
    //  log
    //
    
    if(log.isInfoEnabled())
      {
        StringBuilder dateRAJKString = new StringBuilder();
        pendingReportsForDates.forEach(dt -> dateRAJKString.append("," + printDate(dt)));
        log.info("generating reoports of {} for dates {}", report.getName(), dateRAJKString);
      }
    
    //
    //  launchReport
    //
    
    for (Date date : pendingReportsForDates)
      {
        launchReport(report.getName(), date, tenantID);
      }
  }
  
  public static String buildControlNodename(String reportName, int tenantID, Date reportGenerationDate, String timeZone) {
    return buildControlNodeNameInitial(reportName, tenantID) + "_" + RLMDateUtils.formatDateDay(reportGenerationDate, timeZone) + "_";
  }
  
  public static String buildControlNodeNameInitial(String reportName, int tenantID) {
    return "launchReport-" + reportName + "-" + tenantID;
  }
  
  public static String buildFullControlNodename(String reportName, int tenantID, Date reportGenerationDate, String timeZone) {
    return ReportManager.getControlDir() + File.separator + buildControlNodename(reportName, tenantID, reportGenerationDate, timeZone) + "-"; // "-" at the end because a random int will be appended (sequential node)
  }
  
  public static String getReportOutputPath(int tenantID) {
    return Deployment.getDeployment(tenantID).getReportManagerOutputPath()+File.separator+tenantID+File.separator;
  }
  
  /***************************
   * 
   * launchReport
   * 
   ***************************/
  
  public void launchReport(String reportName, final Date reportGenerationDate, int tenantID)
  {
    String tz = Deployment.getDeployment(tenantID).getTimeZone();
    
    //
    //  znode
    //
    
    String znode = buildFullControlNodename(reportName, tenantID, reportGenerationDate, tz); // this is decomposed in ReportGenerationDateComperator
    
    //
    //  data
    //
    
    JSONObject zkJSON = new JSONObject();
    zkJSON.put("reportName", reportName);
    zkJSON.put("reportGenerationDate", reportGenerationDate.getTime());
    zkJSON.put("tenantID", tenantID);
    byte[] zkData = zkJSON.toJSONString().getBytes(StandardCharsets.UTF_8);
    
    if (getZKConnection())
      {
        try
          {
            //
            // Create new file in control dir with reportName inside, to trigger report generation
            //
            
            zk.create(znode, zkData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL); //zk.create(znode, reportName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
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
  
  public boolean isReportRunning(String reportName, int tenantID) {
    try
      { 
        if (getZKConnection()) 
          {
            List<String> children = zk.getChildren(ReportManager.getControlDir(), false);
            for(String child : children) 
              {
                if(child.contains(buildControlNodeNameInitial(reportName,tenantID))) 
                  {
                    String znode = ReportManager.getControlDir() + File.separator + child; // child contains sequential unknown part
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
  
  public JSONObject generateResponseJSON(GUIManagedObject guiManagedObject, boolean fullDetails, Date date)
  {
	  JSONObject responseJSON = super.generateResponseJSON(guiManagedObject, fullDetails, date);
	  
	  responseJSON.put("tenantID", guiManagedObject.getTenantID());

	  if (guiManagedObject instanceof Report)
	  {
		  Report report = (Report) guiManagedObject;
		  try
		  {
			  Class<ReportDriver> reportClass = (Class<ReportDriver>) Class.forName(report.getReportClass());
			  Constructor<ReportDriver> cons = reportClass.getConstructor();
			  ReportDriver rd = cons.newInstance((Object[]) null);
			  
			  List<FilterObject> filters = rd.reportFilters();
			  JSONArray jsonArray = new JSONArray();

			  if(filters != null && !filters.isEmpty()) 
			  {
				  for (FilterObject filter : filters)
				  {
					  JSONObject filterJSON, argumentJSON;
					  filterJSON = new JSONObject();
					  filterJSON.put("criterionField", filter.getColumnName());
					  argumentJSON = new JSONObject();
					  argumentJSON.put("valueType", filter.getColumnType().getExternalRepresentation());
					  StringBuffer expression = new StringBuffer();
					  JSONArray valueJSON = new JSONArray();
					  for (Object value : filter.getValues())
					  {
						  valueJSON.add(value);
						  expression.append("'").append(value.toString()).append("',");
					  }
					  argumentJSON.put("value", valueJSON);
					  argumentJSON.put("expression", expression.length()>0 ? "[" + expression.subSequence(0, expression.length()-2) + "]" : "[]");
					  argumentJSON.put("timeUnit", null);
					  filterJSON.put("argument", argumentJSON);
					  jsonArray.add(filterJSON);
				  }
				  responseJSON.put("filters", jsonArray);
			  }
			  
        List<String> headers = rd.reportHeader();
        jsonArray = new JSONArray();
        if (headers != null) 
        {
          for (String header : headers)
          {
            jsonArray.add(header);
          }
        }
        responseJSON.put("header", jsonArray);

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
  
  /***************************
   * 
   * getPendingReportsForDates
   * 
   ***************************/
  
  private Set<Date> getPendingReportsForDates(Report report,int tenantID)
  {
    Set<Date> pendingDates = new HashSet<Date>();
    
    //
    //  filter by name
    //
    
    //FileFilter filter = new FileFilter() { public boolean accept(File file) { return file.getName().startsWith(report.getName()); } };
    DirectoryStream.Filter<Path> filter = new DirectoryStream.Filter<Path>() { @Override public boolean accept(Path entry) throws IOException { return entry.getFileName().toString().startsWith(report.getName()); } };
    
    //
    //  generatedReports
    //
    
    Set<Date> generatedDates = new HashSet<Date>();
    final Path dir = Paths.get(getReportOutputPath(tenantID));
    try
      {
        final DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir, filter);
        Iterator<Path> iterator = dirStream.iterator();
        while (iterator.hasNext())
          {
            Path generatedReportFilePath = iterator.next();
            String fileName = generatedReportFilePath.getFileName().toString();
            Date reportDate = getReportDate(fileName, report.getName(), tenantID);
            if (reportDate != null) generatedDates.add(reportDate);
          }
        dirStream.close();
      } 
    catch (IOException e) { e.printStackTrace(); }
    
    
    
    
    /*   listFiles is not believable
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
      }*/
    
    //
    //  pendingDates
    //
    
    StringBuilder generatedDatesRAJKString = new StringBuilder();
    generatedDates.forEach(dt -> generatedDatesRAJKString.append("," + printDate(dt)));
    if(log.isErrorEnabled()) log.error("{} already generatedDates {}", report.getName(), generatedDatesRAJKString);
    
    pendingDates = compareAndGetDates(report, generatedDates, report.getTenantID());
    
    StringBuilder pendingDatesDatesRAJKString = new StringBuilder();
    pendingDates.forEach(dt -> pendingDatesDatesRAJKString.append("," + printDate(dt)));
    if(log.isErrorEnabled()) log.error("{} has pendingDates {} ", report.getName(), pendingDatesDatesRAJKString);
    
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
  
  private Set<Date> compareAndGetDates(Report report, Set<Date> generatedDates, int tenantID)
  {
    Set<Date> result = new HashSet<Date>();
    final Date now = RLMDateUtils.truncate(SystemTime.getCurrentTime(), Calendar.DATE, Deployment.getDeployment(tenantID).getTimeZone());
    for (SchedulingInterval schedulingInterval : report.getEffectiveScheduling())
      {
        if(log.isDebugEnabled()) log.debug("compareAndGetDates schedulingInterval {}", schedulingInterval);
        
        //
        //  ignore HOURLY reports
        //
        
        if (schedulingInterval == SchedulingInterval.HOURLY) continue;
        
        Calendar c = SystemTime.getCalendar();
        c.setTimeZone(TimeZone.getTimeZone(Deployment.getDeployment(tenantID).getTimeZone()));
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
            end = RLMDateUtils.addDays(now, -1, Deployment.getDeployment(tenantID).getTimeZone());
            start = RLMDateUtils.addDays(now, -6, Deployment.getDeployment(tenantID).getTimeZone());
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
            Date previousMonth = RLMDateUtils.addMonths(now, -1, Deployment.getDeployment(tenantID).getTimeZone());
            start = RLMDateUtils.addDays(previousMonth, -dayOfMonth + 1, Deployment.getDeployment(tenantID).getTimeZone());
            c.setTime(start);
            int endDay = c.getActualMaximum(Calendar.DAY_OF_MONTH);
            end = RLMDateUtils.addDays(start, endDay -1, Deployment.getDeployment(tenantID).getTimeZone());
            break;

          default:
            break;
        }
        while (start.before(end) || start.equals(end))
          {
            datesToCheck.add(start);
            start = RLMDateUtils.addDays(start, 1, Deployment.getDeployment(tenantID).getTimeZone());
          }
        
        StringBuilder datesToCheckRAJKString = new StringBuilder();
        datesToCheck.forEach(dt -> datesToCheckRAJKString.append("," + printDate(dt)));
        if(log.isDebugEnabled()) log.debug("compareAndGetDates datesToCheck {}", datesToCheckRAJKString);
        
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
                Date lastWeekReportDate = getPreviousReportDate(Deployment.getDeployment(tenantID).getWeeklyReportCronEntryString(), now, tenantID);
                if (lastWeekReportDate != null)
                  {
                    int thisWK = RLMDateUtils.getField(now, Calendar.WEEK_OF_YEAR, Deployment.getDeployment(tenantID).getTimeZone());
                    int lastWKReportsWK = RLMDateUtils.getField(lastWeekReportDate, Calendar.WEEK_OF_YEAR, Deployment.getDeployment(tenantID).getTimeZone());
                    if (thisWK == lastWKReportsWK) lastWeekReportDate = RLMDateUtils.addWeeks(lastWeekReportDate, -1, Deployment.getDeployment(tenantID).getTimeZone());
                  }
                else
                  {
                    lastWeekReportDate = RLMDateUtils.addWeeks(now, -1, Deployment.getDeployment(tenantID).getTimeZone());
                  }
                lastWeekReportDate = RLMDateUtils.truncate(lastWeekReportDate, Calendar.DATE, Deployment.getDeployment(tenantID).getTimeZone());
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
                Date lastMonthReportDate = getPreviousReportDate(Deployment.getDeployment(tenantID).getMonthlyReportCronEntryString(), now, tenantID);
                if (lastMonthReportDate != null)
                  {
                    int thisMonth = RLMDateUtils.getField(now, Calendar.MONTH, Deployment.getDeployment(tenantID).getTimeZone());
                    int lastMonthReportsMonth = RLMDateUtils.getField(lastMonthReportDate, Calendar.MONTH, Deployment.getDeployment(tenantID).getTimeZone());
                    if (thisMonth == lastMonthReportsMonth) lastMonthReportDate = RLMDateUtils.addMonths(lastMonthReportDate, -1, Deployment.getDeployment(tenantID).getTimeZone());
                  }
                else
                  {
                    lastMonthReportDate = RLMDateUtils.addMonths(now, -1, Deployment.getDeployment(tenantID).getTimeZone());
                  }
                lastMonthReportDate = RLMDateUtils.truncate(lastMonthReportDate, Calendar.DATE, Deployment.getDeployment(tenantID).getTimeZone());
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
  
  private Date getPreviousReportDate(String cron, Date now, int tenantID)
  {
    Date result = null;
    try
      {
        CronFormat cronFormat = new CronFormat(cron, TimeZone.getTimeZone(Deployment.getDeployment(tenantID).getTimeZone()));
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
  
  private Date getReportDate(String reportFileName, String fileNameInitial, int tenantID)
  {
    Date result = null;
    try
      {
        String reportDateString = reportFileName.split(fileNameInitial + "_")[1].split("." + Deployment.getDeployment(tenantID).getReportManagerFileExtension())[0];
        SimpleDateFormat sdf = new SimpleDateFormat(Deployment.getDeployment(tenantID).getReportManagerFileDateFormat());   // TODO EVPRO-99
        sdf.setTimeZone(TimeZone.getTimeZone(Deployment.getDeployment(tenantID).getTimeZone()));
        result = sdf.parse(reportDateString);
        result = RLMDateUtils.truncate(result, Calendar.DATE, Deployment.getDeployment(tenantID).getTimeZone());
      } 
    catch (Exception e)
      {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error(stackTraceWriter.toString());
        log.error("skipping as badfile {}", reportFileName);
      }
    return result;
  }
  
  /***************************
   * 
   * validateAndgetReportDirectory
   * 
   ***************************/
  
  private File validateAndgetReportDirectory(int tenantID)
  {
    //
    //  create
    //
    
    File reportDirectoryFile = new File(getReportOutputPath(tenantID));
    
    //
    // validate
    //
    
    if (! reportDirectoryFile.isAbsolute())
      {
        if (log.isErrorEnabled()) log.error("reportDirectory must specify absolute path");
        throw new ServerRuntimeException("reportDirectory must specify absolute path");
      }
    
    //
    // can ls
    //
    
    if (reportDirectoryFile.listFiles() == null)
      {
        if (log.isErrorEnabled()) log.error("unable to ls reportDirectory {}", reportDirectoryFile.getAbsolutePath());
        throw new ServerRuntimeException("unable to ls reportDirectory " + reportDirectoryFile.getAbsolutePath());
      }
    
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
