package com.evolving.nglm.evolution.extracts;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.NoSuchElementException;

public class ExtractLauncher implements Runnable
{

  private static final Logger log = LoggerFactory.getLogger(ExtractLauncher.class);

  private String zkHostList;
  private String brokerServers;
  private String esNode;
  private ExtractManagerStatistics extractManagerStatistics;
  private String zkNodeChild;
  private ZooKeeper zk;
  private String controlDir;
  private String lockDir;
  private DateFormat dfrm;

  private Thread t;
  private String threadName;

  public ExtractLauncher(ZooKeeper zk,String controlDir,String lockDir, String zkNodeChild, String zkHostList, String brokerServers, String esNode,DateFormat dfrm,ExtractManagerStatistics extractManagerStatistics)
  {
    this.zk = zk;
    this.controlDir = controlDir;
    this.lockDir = lockDir;
    this.zkNodeChild = zkNodeChild;
    this.zkHostList = zkHostList;
    this.brokerServers = brokerServers;
    this.esNode = esNode;
    this.dfrm = dfrm;
    this.extractManagerStatistics = extractManagerStatistics;
    this.threadName = zkNodeChild;
  }

  @Override
  public void run()
  {
    try
      {
        String controlFile = controlDir + File.separator + zkNodeChild;
        String lockFile = lockDir + File.separator + zkNodeChild;
        log.trace("Checking if lock exists : " + lockFile);
        if (zk.exists(lockFile, false) == null)
          {
            log.trace("Processing entry " + zkNodeChild + " with znodes " + controlFile + " and " + lockFile);
            try
              {
                log.trace("Trying to create lock " + lockFile);
                zk.create(lockFile, dfrm.format(SystemTime.getCurrentTime()).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                try
                  {
                    log.trace("Lock " + lockFile + " successfully created");
                    Stat stat = null;
                    Charset utf8Charset = Charset.forName("UTF-8");
                    byte[] d = zk.getData(controlFile, false, stat);
                    //the logical flow is based on  JSON
                    String data = new String(d, utf8Charset);
                    log.info("Got data " + data);
                    JSONObject extractItemJSON = (JSONObject) (new JSONParser()).parse(data);
                    handleExtract(new ExtractItem(extractItemJSON));
                    extractManagerStatistics.incrementExtractCount();
                  }
                catch (KeeperException | InterruptedException | NoSuchElementException e)
                  {
                    log.error("Issue while reading from control node " + e.getLocalizedMessage(), e);
                    extractManagerStatistics.incrementFailureCount();
                  }
                catch (IllegalCharsetNameException e)
                  {
                    log.error("Unexpected issue, UTF-8 does not seem to exist " + e.getLocalizedMessage(), e);
                    extractManagerStatistics.incrementFailureCount();
                  }
                catch (Exception e) // this is OK because we trace the root cause, and we'll fix it
                  {
                    log.error("Unexpected issue " + e.getLocalizedMessage(), e);
                    extractManagerStatistics.incrementFailureCount();
                  }
                finally
                  {
                    log.info("Deleting control " + controlFile);
                    try
                      {
                        zk.delete(controlFile, -1);
                      }
                    catch (KeeperException | InterruptedException e)
                      {
                        log.info("Issue deleting control : " + e.getLocalizedMessage(), e);
                      }
                    finally
                      {
                        log.info("Deleting lock " + lockFile);
                        try
                          {
                            zk.delete(lockFile, -1);
                            log.info("Both files deleted");
                          }
                        catch (KeeperException | InterruptedException e)
                          {
                            log.info("Issue deleting lock : " + e.getLocalizedMessage(), e);
                          }
                      }
                  }
              }
            catch (KeeperException | InterruptedException ignore)
              {
                // even so we check the existence of a lock, it could have been created in the mean time making create fail. We catch and ignore it.
                log.trace("Failed to create lock file, this is OK " + lockFile + ":" + ignore.getLocalizedMessage(), ignore);
              }
          } else
          {
            log.trace("--> This extract is already processed by another ReportManager instance");
          }
      }
    catch (KeeperException | InterruptedException e)
      {
        log.error("Error processing extract", e);
      }
  }

  /*****************************************
   *
   *  handleReport
   *
   *****************************************/

  private void handleExtract(ExtractItem extractItem)
  {
    log.trace("---> Starting extract "+extractItem.getJSONObjectAsString());
    try
      {
        String outputPath = Deployment.getExtractManagerOutputPath();
        log.trace("outputPath = "+outputPath);
        String dateFormat = Deployment.getExtractManagerDateFormat();
        log.trace("dateFormat = "+dateFormat);
        String fileExtension = Deployment.getExtractManagerFileExtension();
        log.trace("dateFormat = "+fileExtension);

        SimpleDateFormat sdf;
        try {
          sdf = new SimpleDateFormat(dateFormat);
        } catch (IllegalArgumentException e) {
          log.error("Config error : date format "+dateFormat+" is invalid, using default"+e.getLocalizedMessage(), e);
          sdf = new SimpleDateFormat(); // Default format, might not be valid in a filename, sigh...
        }
        String fileSuffix = sdf.format(SystemTime.getCurrentTime());
        String csvFilename = ""
                + outputPath
                + File.separator
                + extractItem.getUserId()
                +"_"
                + extractItem.getExtractName()
                + "_"
                + fileSuffix
                + "."
                + fileExtension;
        log.trace("csvFilename = " + csvFilename);

        ExtractDriver ed = new ExtractDriver();
        try
          {
            ed.produceExtract(extractItem, zkHostList, brokerServers, esNode, csvFilename);
          }
        catch (Exception e)
          {
            // handle any kind of exception that can happen during generating the extract, and do not crash the container
            log.error("Exception processing extract " + extractItem.getExtractName() + " : " + e);
          }
        log.trace("---> Finished extract "+extractItem.getExtractName());
      }
    catch (SecurityException|IllegalArgumentException e)
      {
        log.error("Error : "+e.getLocalizedMessage(), e);
        extractManagerStatistics.incrementFailureCount();
      }
  }

  public void start()
  {
    if (t == null) {
      t = new Thread (this, threadName);
      t.start ();
    }
  }
}
