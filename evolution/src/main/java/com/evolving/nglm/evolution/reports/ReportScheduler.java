package com.evolving.nglm.evolution.reports;

import java.io.File;
import java.util.Collection;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.UniqueKeyServer;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.ReportConfiguration;
import com.evolving.nglm.evolution.ReportService;
import com.evolving.nglm.evolution.ReportService.ReportListener;

public class ReportScheduler implements Watcher {
	private static final int sessionTimeout = 10*1000; // 60 seconds
	private String controlDir = null;
	private static ZooKeeper zk = null;
	private Map<String,ReportConfiguration> reportsConfig = null;
	private static String zkHostList;
	private static final Logger log = LoggerFactory.getLogger(ReportScheduler.class);
	private ReportService reportService;

	public ReportScheduler() throws Exception {
		String topDir = Deployment.getReportManagerZookeeperDir();
		controlDir = topDir + File.separator + ReportManager.CONTROL_SUBDIR;
		log.debug("controlDir = "+controlDir);
		reportsConfig = Deployment.getReportsConfiguration();
		if (reportsConfig == null) {
			log.error("There is no 'reportsConfiguration' config in deployment.json, no report is configured, exiting");
			System.exit(1);
		}
		log.debug("reportsConfig = "+reportsConfig);
		zk  = new ZooKeeper(zkHostList, sessionTimeout, this);
		log.debug("ZK client created : "+zk);
		listenToGui();
		populateWithReports();
	}

	@SuppressWarnings("unchecked")
	private void populateWithReports() {
		
		// DEBUG
		Collection<GUIManagedObject> reports = reportService.getStoredReports();
		log.trace("reports before : "+reports);
		for (GUIManagedObject report : reports) {
			log.trace("report before : "+report);
		}
		// DEBUG
		
		JSONArray reportsConfigValues = Deployment.getReportsConfigJSon();
    	log.trace("Got from config : "+reportsConfigValues);
        for (int i=0; i<reportsConfigValues.size(); i++) {
        	JSONObject json = (JSONObject) reportsConfigValues.get(i);
        	log.trace("element : "+json.toJSONString());

        	json.put(Report.EFFECTIVE_SCHEDULING, Report.SchedulingInterval.NONE.name());
        	String reportID = reportService.generateReportID();
            json.put("id", reportID);
            String guiManagedObjectName = "objectName-"+json.get(Report.REPORT_NAME); // TODO : check this
            json.put("name", guiManagedObjectName);
            json.put("readOnly", Boolean.FALSE);
            json.put("active", Boolean.TRUE);
            json.put("apiVersion", 1);
            json.remove("reportClass");
            
        	log.trace("element2 : "+json.toJSONString());
        	long epoch = new UniqueKeyServer().getEpoch();
			try {
				Report report = new Report(json, epoch, null);
	        	log.trace("report for kafka : "+report);
	        	boolean newObject = true;
	        	String userID = "Administrator"; // TODO : might need to fix this
	        	// Send report to Kafka to GUI to consume
				reportService.putReport(report, newObject, userID );
			} catch (GUIManagerException e) {
				log.debug("Issue with Gui : "+e.getLocalizedMessage());
			}
        }
        
		// DEBUG
		reports = reportService.getStoredReports();
		log.trace("reports after : "+reports);
		for (GUIManagedObject report : reports) {
			log.trace("report after : "+report);
		}
		// DEBUG

	}
	
	public void listenToGui() {
		ReportListener reportListener = new ReportListener() {
			@Override public void reportActivated(Report report) {
				log.trace("report activated : " + report);
			}
			@Override public void reportDeactivated(String guiManagedObjectID) {
				log.trace("report deactivated: " + guiManagedObjectID);
			}
		};
		log.trace("Creating ReportService");
		reportService = new ReportService(
				Deployment.getBrokerServers(),
				"reportscheduler-reportservice-001",
				Deployment.getReportTopic(),
				true, // master because we need to create initial list of reports with 'putReport'
				reportListener);
		reportService.start();
		log.trace("ReportService started");
	}
	
	@Override
	public void process(WatchedEvent event) {
		log.trace("Got event, ignore it... "+event);
	}

	public static void main(String[] args) {
		zkHostList  = Deployment.getZookeeperConnect();
		try {
			ReportScheduler rs = new ReportScheduler();
			log.debug("ZK client created");
			while (true) { //  sleep forever
		        try {
		            Thread.sleep(Long.MAX_VALUE);
		          } catch (InterruptedException ignore) {}
		      }
		} catch (Exception e) {
			log.info("Issue : "+e.getLocalizedMessage());
			e.printStackTrace();
		}
	}

}
