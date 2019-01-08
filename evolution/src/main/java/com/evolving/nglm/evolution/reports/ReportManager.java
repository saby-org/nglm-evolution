package com.evolving.nglm.evolution.reports;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.ReportConfiguration;
import com.evolving.nglm.evolution.ReportService;
import com.evolving.nglm.evolution.ReportService.ReportListener;

public class ReportManager implements Watcher{

	private static final String CONTROL_SUBDIR = "control";
	private static final String LOCK_SUBDIR = "lock";
	private static final int sessionTimeout = 10*1000; // 60 seconds

	private static String controlDir = null;
	private String lockDir = null;
	private ZooKeeper zk = null;
	private Map<String,ReportConfiguration> reportsConfig = null;
	private static String zkHostList;
	private static String kafkaNode;
	private static String esNode;
	private DateFormat dfrm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z z");
	private static final Logger log = LoggerFactory.getLogger(ReportManager.class);

	/*
	 * Used by ReportScheduler to launch reports
	 */
	public static String getControlDir() {
		return controlDir;
	}
	
	public ReportManager() throws Exception {
		String topDir = Deployment.getReportManagerZookeeperDir();
		controlDir = topDir + File.separator + CONTROL_SUBDIR;
		lockDir = topDir + File.separator + LOCK_SUBDIR;
		log.debug("controlDir = "+controlDir+" , lockDir = "+lockDir);
		reportsConfig = Deployment.getReportsConfiguration();
		if (reportsConfig == null) {
			log.error("There is no 'reportsConfiguration' config in deployment.json, no report is configured, exiting");
			System.exit(1);
		}
		log.debug("reportsConfig = "+reportsConfig);
		zk  = new ZooKeeper(zkHostList, sessionTimeout, this);
		log.debug("ZK client created : "+zk);
		// TODO next 3 lines could be done once for all in nglm-evolution/.../evolution-setup-zookeeper.sh
		createZKNode(topDir, true);
		createZKNode(controlDir, true);
		createZKNode(lockDir, true);
		List<String> children = zk.getChildren(controlDir, this); // sets watch
	}

	private void createZKNode(String znode, boolean canExist) {
		log.debug("Trying to create znode "	+ znode
				+ " (" + (canExist?"may":"must not")+" already exist)");
		try {
			zk.create(znode, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			if (canExist && (e.code() == KeeperException.Code.NODEEXISTS)) {
				log.trace(znode+" already exists, this is OK");
			} else {
				log.info("Got "+e.getLocalizedMessage());
			}
		} catch (InterruptedException e) {
			log.info("Got "+e.getLocalizedMessage());
		}
	}

	@Override
	public void process(WatchedEvent event) {
		log.trace("Got event : "+event);
		try {
			if (event.getType().equals(EventType.NodeChildrenChanged)) {
				List<String> children = zk.getChildren(controlDir, this); // get the children and renew watch
				Collections.sort(children); // we are getting an unsorted list
				for (String child : children) {
					String controlFile = controlDir + File.separator + child;
					String lockFile = lockDir + File.separator + child;
					log.trace("Processing entry "+child+" with znodes "+controlFile+" and "+lockFile);
					if (zk.exists(lockFile, false) == null) {
						try {
							log.trace("Trying to create lock file "+lockFile);
							zk.create(
									lockFile,
									dfrm.format(new Date()).getBytes(), 
									Ids.OPEN_ACL_UNSAFE,
									CreateMode.EPHEMERAL);
							try {
								log.trace("Lock file "+lockFile+" successfully created");
								Stat stat = null;
								Charset utf8Charset = Charset.forName("UTF-8");
								byte[] d = zk.getData(controlFile, false, stat);
								String data = new String(d, utf8Charset);
								log.info("Got data "+data);
								Scanner s = new Scanner(data+"\n"); // Make sure s.nextLine() will work
								String reportName = s.next().trim();
								String restOfLine = s.nextLine().trim();
								s.close();
								ReportConfiguration reportConfig = reportsConfig.get(reportName);
								if (reportConfig == null) {
									log.error("Report does not exist : "+reportName);
								} else {
									log.debug("reportConfig = "+reportConfig);
									handleReport(reportName, reportConfig, restOfLine);
								}
							} catch (KeeperException | InterruptedException e) {
								log.error("Issue while reading from control node "+e.getLocalizedMessage());
							} catch (IllegalCharsetNameException e) {
								log.error("Unexpected issue, UTF-8 does not seem to exist "+e.getLocalizedMessage());
							} finally {
								try {
									log.trace("Deleting control file "+controlFile);
									zk.delete(controlFile, -1);
								} catch (KeeperException | InterruptedException e) {
									log.trace("Issue deleting control file : "+e.getLocalizedMessage());
								}
								try {
									log.trace("Deleting lock file "+lockFile);
									zk.delete(lockFile, -1);
									log.trace("Both files deleted");
								} catch (KeeperException | InterruptedException e) {
									log.trace("Issue deleting lock file : "+e.getLocalizedMessage());
								}
							}
						} catch (KeeperException | InterruptedException ignore) {
							// even so we check the existence of a lock,
							// it could have been created in the mean time
							// making create fail. We catch and ignore it.
							log.trace("Failed to create lock file, this is OK "
									+lockFile+ ":"+ignore.getLocalizedMessage());
						} 
					}
				}
			}
		} catch (Exception e){
			log.error("Error processing report", e);
		}
	}

	private void handleReport(String reportName, ReportConfiguration reportConfig, String restOfLine) {
		log.trace("Processing "+reportName+" "+restOfLine);
		String[] params = null;
		if (!"".equals(restOfLine)) {
			params = restOfLine.split("\\s+"); // split with spaces
		}
		if (params != null) {
			for (String param : params) {
				log.debug("  param : " + param);
			}
		}
		try {
			String outputPath = Deployment.getReportManagerOutputPath();
			log.trace("outputPath = "+outputPath);
			String dateFormat = Deployment.getReportManagerDateFormat();
			log.trace("dateFormat = "+dateFormat);
			String fileExtension = Deployment.getReportManagerFileExtension();
			log.trace("dateFormat = "+fileExtension);

			SimpleDateFormat sdf;
			try {
				sdf = new SimpleDateFormat(dateFormat);
			} catch (IllegalArgumentException e) {
				log.error("Config error : date format "+dateFormat+" is invalid, using default"+e.getLocalizedMessage());
				sdf = new SimpleDateFormat(); // Default format, might not be valid in a filename, sigh...
			}
			String fileSuffix = sdf.format(new Date());
			String csvFilename = "" 
					+ outputPath 
					+ File.separator
					+ reportName
					+ "_"
					+ fileSuffix
					+ "."
					+ fileExtension;
			log.trace("csvFilename = " + csvFilename);

			@SuppressWarnings("unchecked")
			Class<ReportDriver> report = (Class<ReportDriver>) Class.forName(reportConfig.getReportClass());
			Constructor<ReportDriver> cons = report.getConstructor();
			ReportDriver rd = cons.newInstance((Object[]) null);
			rd.produceReport(zkHostList, kafkaNode, esNode, csvFilename, reportConfig, params);
		} catch (ClassNotFoundException e) {
			log.error("Undefined class name "+e.getLocalizedMessage());
		} catch (NoSuchMethodException e) {
			log.error("Undefined method "+e.getLocalizedMessage());
		} catch (SecurityException|InstantiationException|IllegalAccessException|
				IllegalArgumentException|InvocationTargetException e) {
			log.error("Error : "+e.getLocalizedMessage());
		}
	}

	public static void main(String[] args) {
		log.info("ReportManager: received " + args.length + " args");
		for(String arg : args){
			log.info("ReportManager main : arg " + arg);
		}
		if (args.length < 3) {
			log.error("Usage : ReportManager ZKhostList KafkaNode ESNode");
			System.exit(1);
		}
		zkHostList  = args[0];
		kafkaNode   = args[1];
		esNode      = args[2];
		try {
			ReportManager rm = new ReportManager();
			log.debug("ZK client created");
			while (true) { //  sleep forever
		        try {
		            Thread.sleep(Long.MAX_VALUE);
		          } catch (InterruptedException ignore) {}
		      }
		} catch (Exception e) {
			log.info("Issue in Zookeeper : "+e.getLocalizedMessage());
		}
	}

}
