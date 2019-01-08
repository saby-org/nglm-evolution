package com.evolving.nglm.evolution.reports;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.UniqueKeyServer;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.Report.SchedulingInterval;
import com.evolving.nglm.evolution.ReportConfiguration;
import com.evolving.nglm.evolution.ReportService;
import com.evolving.nglm.evolution.ReportService.ReportListener;

public class ReportScheduler implements Watcher {
	private static final String CONTROL_SUBDIR = "control";
	private static final int sessionTimeout = 10*1000; // 60 seconds
	private String controlDir = null;
	private ZooKeeper zk = null;
	private Map<String,ReportConfiguration> reportsConfig = null;
	private static String zkHostList;
	private static String kafkaNode;
	private static final Logger log = LoggerFactory.getLogger(ReportScheduler.class);
	private ReportService reportService;

	public ReportScheduler() throws Exception {
		String topDir = Deployment.getReportManagerZookeeperDir();
		controlDir = topDir + File.separator + CONTROL_SUBDIR;
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
            String guiManagedObjectName = "objectName-"+json.get(Report.REPORT_NAME); // TODO : fix this
            json.put("name", guiManagedObjectName);
            json.put("readOnly", Boolean.FALSE);
            json.put("valid", Boolean.TRUE);
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

	private void createControlZNode(String reportName) {
		String znode;
		do { // Wait for ReportManager to be initialized
			znode = ReportManager.getControlDir();
			try {
	            Thread.sleep(5); // Time for ReportManager to finish initializing
	          } catch (InterruptedException ignore) {}
		} while (znode == null);
		znode += File.separator + "launchReport-";
		log.debug("Trying to create ephemeral znode " + znode);
		try {
			// Create new file in control dir with reportName inside, to trigger report generation
			zk.create(znode, reportName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		} catch (KeeperException e) {
			log.info("Got "+e.getLocalizedMessage());
		} catch (InterruptedException e) {
			log.info("Got "+e.getLocalizedMessage());
		}
	}
	
	public void listenToGui() {
		ReportListener reportListener = new ReportListener() {
			@Override public void reportActivated(Report report) {
				log.trace("report activated : " + report);
				String name = report.getName();
				createControlZNode(name);
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
				false, // masterService
				reportListener);
		reportService.start();
		log.trace("ReportService started");
	}
	
	@Override
	public void process(WatchedEvent event) {
		log.trace("Got event, ignore it... "+event);
	}

	public static void main(String[] args) {
		log.info("ReportScheduler: received " + args.length + " args");
		for(String arg : args){
			log.info("ReportScheduler main : arg " + arg);
		}
		if (args.length < 2) {
			log.error("Usage : ReportScheduler ZKhostList KafkaNode");
			System.exit(1);
		}
		zkHostList  = args[0];
		kafkaNode   = args[1];
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
