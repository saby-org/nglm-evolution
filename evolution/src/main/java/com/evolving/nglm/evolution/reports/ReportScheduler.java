/*****************************************************************************
 *
 *  ReportScheduler.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import java.io.File;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.ReportService;
import com.evolving.nglm.evolution.ReportService.ReportListener;

/**
 * This class handles the automatic launching of reports, based on the cron-like configuration.
 * NOTE : this is not yet fully implemented.
 *
 */
public class ReportScheduler implements Watcher {
  private static final int sessionTimeout = 10*1000; // 60 seconds
  private String controlDir = null;
  private static ZooKeeper zk = null;
  private static String zkHostList;
  private static final Logger log = LoggerFactory.getLogger(ReportScheduler.class);
  private ReportService reportService;

  public ReportScheduler() throws Exception {
    String topDir = Deployment.getReportManagerZookeeperDir();
    controlDir = topDir + File.separator + ReportManager.CONTROL_SUBDIR;
    log.debug("controlDir = "+controlDir);
    zk  = new ZooKeeper(zkHostList, sessionTimeout, this);
    log.debug("ZK client created : "+zk);
    listenToGui();
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
    // ReportService will be used to gather information about reports to schedule them regularly (not yet implemented) 
    log.trace("Creating ReportService");
    reportService = new ReportService(
        Deployment.getBrokerServers(),
        "reportscheduler-reportservice-001",
        Deployment.getReportTopic(),
        false,
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
