/*****************************************************************************
*
*  LicenseChecker.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rii.utilities.JSONUtilities.JSONUtilitiesException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LicenseChecker
{
  /*****************************************
  *
  *  constants
  *
  *****************************************/

  private static final int RESTAPIVersion = 1;
  private static final int DefaultCheckFrequency = 15;

  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(LicenseChecker.class);

  /*****************************************
  *
  *  zookeeper
  *
  *****************************************/
  
  private String zookeeperConnect = null;
  private String licenseRoot = null;

  /*****************************************
  *
  *  state
  *
  *****************************************/

  private String componentID;
  private String nodeID;
  private int licenseCheckFrequency;
  private int locationCheckFrequency;
  Set<Pair<String,Integer>> locations = null;
  boolean stopChecking = false;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public LicenseChecker(String componentID, String nodeID, String zookeeperRoot, String zookeeperConnect)
  {
    this(componentID, nodeID, zookeeperRoot, zookeeperConnect, DefaultCheckFrequency);
  }

  public LicenseChecker(String componentID, String nodeID, String zookeeperRoot, String zookeeperConnect, int checkFrequency)
  {
    //
    //  set
    //

    this.componentID = componentID;
    this.nodeID = nodeID;
    this.licenseRoot = zookeeperRoot + "/licensemanager";
    this.zookeeperConnect = zookeeperConnect;
    this.licenseCheckFrequency = checkFrequency;
    this.locationCheckFrequency = checkFrequency*2;

    //
    //  start
    //

    start();
  }

  /****************************************
  *
  *  start
  *
  *****************************************/

  private void start()
  {
    //
    //  get license location
    //

    Runnable licenseLocationReader = new Runnable() { @Override public void run() { readLocation(); } };
    Thread licenseLocationReaderThread = new Thread(licenseLocationReader);
    licenseLocationReaderThread.start();

    //
    //  wait for first location read
    //

    ensureLocationsRead();

    //
    //  start license manager poll
    //

    Runnable licenseManagerPoller = new Runnable() { @Override public void run() { pollLicenseManager(); } };
    Thread licenseManagerPollerThread = new Thread(licenseManagerPoller);
    licenseManagerPollerThread.start();

  }

  /*****************************************
  *
  *  stopChecking
  *
  *****************************************/
  
  public synchronized void stop()
  {
    stopChecking = true;
    notifyAll();
  }

  /***********************************************************************
  *
  *  license manager location 
  *
  ***********************************************************************/
  
  /*****************************************
  *
  *  setLocations
  *
  *****************************************/

  private synchronized void setLocations(Set<Pair<String,Integer>> newLocations)
  {
    this.locations = newLocations;
    log.debug("setlocations: {}", this.locations);
    notifyAll();
  }

  /*****************************************
  *
  *  getLocations
  *
  *****************************************/

  private synchronized Set<Pair<String,Integer>> getLocations()
  {
    return new HashSet<Pair<String, Integer>> (this.locations);
  }
  
  /*****************************************
  *
  * readLicenseManagerLocation
  *
  *****************************************/

  private void readLicenseManagerLocation()
  {
  }

  /*****************************************
  *
  *  ensureLocationsRead
  *
  *****************************************/
  
  private synchronized void ensureLocationsRead()
  {
    while (this.locations == null)
      {
        synchronized (this)
          {
            try
              {
                wait(500);
              }
            catch (InterruptedException e)
              {
                // nothing
              }
          }
      }
  }

  /*****************************************
  *
  *  readLocation
  *
  *****************************************/
  
  private void readLocation()
  {
    do
      {
        /*****************************************
        *
        *  read from zookeeper
        *
        *****************************************/
        
        //
        //  create a client
        // 

        ZooKeeper zookeeper = null;
        while (zookeeper == null)
          {
            try
              {
                zookeeper = new ZooKeeper(this.zookeeperConnect, 3000, new Watcher() { @Override public void process(WatchedEvent event) {} }, true);
              }
            catch (IOException e)
              {
                // ignore
              }
          }
    
        //
        //  ensure connected
        //
        
        while (zookeeper.getState().isAlive() && ! zookeeper.getState().isConnected())
          {
            try { Thread.currentThread().sleep(200); } catch (InterruptedException ie) { }
          }

        //
        //  read data
        //
        
        List<String> locationList = null;
        try
          {
            locationList = zookeeper.getChildren(this.licenseRoot + "/sessions", true, null);
          }
        catch (KeeperException|InterruptedException e)
          {
            log.error("Could not read license locations.");
          }

        //
        //  close
        //

        try
          {
            zookeeper.close();
          }
        catch (InterruptedException e)
          {
            // ignore
          }

        /*****************************************
        *
        *  update list of license manager locations
        *
        *****************************************/

        if ((locationList != null) && locationList.size() > 0)
          {
            Set<Pair<String, Integer>> updatedLocations = new HashSet<Pair<String,Integer>>();

            for (String location : locationList)
              {
                Pair<String,Integer> locationPair = null;
                try
                  {
                    String [] fields = location.split(":");
                    if (fields.length == 2)
                      {
                        locationPair = new Pair(fields[0], new Integer(fields[1]));
                      }
                  }
                catch (NumberFormatException nfe)
                  {
                    log.error("Error parsing licensemanager location {}", location);
                  }

                //
                //  add
                // 

                if (locationPair != null)
                  {
                    updatedLocations.add(locationPair);
                  }
              }

            //
            //  set locations
            //

            setLocations(updatedLocations);
          }
        
        /*****************************************
        *
        *  sleep
        *
        *****************************************/

        synchronized (this)
          {
            try
              {
                wait(locationCheckFrequency*1000);
              }
            catch (InterruptedException ie)
              {
                // nothing
              }
          }
        
      }
    while (!stopChecking);
  }

  /***********************************************************************
  *
  *  license checking
  *
  ***********************************************************************/

  private volatile LicenseState currentLicenseState = null;
  
  /*****************************************
  *
  *  pollLicenseManager
  *
  *****************************************/

  private void pollLicenseManager()
  {
    do
      {
        /*****************************************
        *
        *  force license check
        *
        *****************************************/

        checkLicense(true);
        
        /*****************************************
        *
        *  sleep
        *
        *****************************************/

        synchronized (this)
          {
            try
              {
                wait(licenseCheckFrequency*1000);
              }
            catch (InterruptedException ie)
              {
                // nothing
              }
          }
      }
    while (!stopChecking);

  }

  /*****************************************
  *
  *  getLicenseState
  *
  *****************************************/

  private synchronized LicenseState getLicenseState()
  {
    return currentLicenseState;
  }
  
  /*****************************************
  *
  *  setLicenseState
  *
  *****************************************/

  private synchronized void setLicenseState(LicenseState newLicenseState)
  {
    this.currentLicenseState = newLicenseState;
  }

  /*****************************************
  *
  *  checkLicense
  *
  *****************************************/

  public LicenseState checkLicense()
  {
    return checkLicense(false);
  }

  public LicenseState checkLicense(boolean force)
  {
    if (force)
      {
        checkLicense_internal();
      }
    return getLicenseState();
  }

  /*****************************************
  *
  *  checkLicense_internal
  *
  *****************************************/

  private void checkLicense_internal()
  {
    /*****************************************
    *
    *  cycle through all LM locations until we contact one successfully
    *
    *****************************************/
    
    Set<Pair<String,Integer>> locations = getLocations();
    LicenseState newLicenseState = null;
    for (Pair<String,Integer> location : locations)
      {
        String host = location.getFirstElement();
        Integer port = location.getSecondElement();
        try
          {
            newLicenseState = new LicenseState(sendCheckLicenseRequest(host, port));
          }
        catch(IOException ioe)
          {
            log.error("checkLicense: Could not contact License manager at {}:{}", host, port);
          }

        //
        //  break if successful
        //

        if (newLicenseState != null) break;
      }

    //
    // set license state
    //

    setLicenseState(newLicenseState != null ? newLicenseState : noLicenseManager());
    log.debug("Refreshed license state: {}", getLicenseState());
  }

  /*****************************************
  *
  *  sendCheckLicenseRequest
  *
  *****************************************/
  
  private String sendCheckLicenseRequest(String host, int port) throws IOException
  {
    //
    //  connection
    //

    URL url = new URL("http://" + host + ":" + port + "/nglm-license/checkLicense");
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();

    //
    //  set method and request headers
    //

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    //
    // request Body
    //

    String requestBody = formatCheckLicenseRequest(this.componentID, this.nodeID);
    DataOutputStream writer = new DataOutputStream(connection.getOutputStream());
    writer.writeBytes(requestBody);
    writer.flush();
    writer.close();
    log.trace("LicenseRequest: {} ", requestBody);

    //
    //  response
    //
    
    InputStream in  = connection.getInputStream();
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    StringBuilder response = new StringBuilder();
    String line;
    while((line = reader.readLine()) != null) {
      response.append(line);
    }
    in.close();
    String result = response.toString();
    log.trace("LicenseResponse: {} ", result);

    //
    //  return
    // 

    return result;
  }

  /*****************************************
  *
  *  formatCheckLicenseRequest
  *
  *****************************************/

  String formatCheckLicenseRequest(String componentID, String nodeID)
  {
    JSONObject jsonRequest = new JSONObject();

    jsonRequest.put("apiVersion", RESTAPIVersion);
    jsonRequest.put("componentID", componentID);
    jsonRequest.put("nodeID", nodeID);

    return jsonRequest.toString();
  }

  /***********************************************************************
  *
  *  class LicenseState
  *
  ***********************************************************************/

  static LicenseState noLicenseManager()
  {
    return new LicenseState(false, "no license manager", LicenseManager.Outcome.NoLicenseManager);
  }
        
  static public class LicenseState
  {
    boolean valid;
    String responseText;
    LicenseManager.Outcome outcome = null;
    List<Alarm> alarms;

    /*****************************************
    *
    *  constructor - json
    *
    *****************************************/
    
    LicenseState(String jsonString) throws JSONUtilitiesException
    {
      //
      //  get json root
      //

      JSONObject jsonRoot = null;
      try
        {
          jsonRoot = (JSONObject) (new JSONParser()).parse(jsonString);
        }
      catch (org.json.simple.parser.ParseException pe)
        {
          throw new JSONUtilitiesException ("license state parse error", pe);
        }

      //
      //  check version
      //
      
      int apiVersion = JSONUtilities.decodeInteger(jsonRoot, "apiVersion", true);
      if (apiVersion > RESTAPIVersion) throw new RuntimeException ("Unsupported api version");

      //
      //  responseCode
      //

      String responseCode = JSONUtilities.decodeString(jsonRoot, "responseCode", true);
      this.valid = responseCode.equalsIgnoreCase("ok");

      //
      //  response
      //

      this.responseText = JSONUtilities.decodeString(jsonRoot, "response", true);

      //
      //  alarms
      //

      this.alarms = new ArrayList<Alarm>();
      JSONArray jsonAlarms = JSONUtilities.decodeJSONArray(jsonRoot, "alarms", true);
      for (int i=0; i<jsonAlarms.size(); i++)
        {
          JSONObject alarmObject = (JSONObject) jsonAlarms.get(i);
          this.alarms.add(new Alarm(alarmObject));
        }

      //
      //  outcome
      //
        
      this.outcome = LicenseManager.Outcome.valueOf(JSONUtilities.decodeString(jsonRoot, "outcome", true));
    }

    /*****************************************
    *
    *  constructor - basic
    *
    *****************************************/

    LicenseState(boolean valid, String responseText, LicenseManager.Outcome outcome)
    {
      this.valid = valid;
      this.responseText = responseText;
      this.outcome = outcome;
      this.alarms = new ArrayList<Alarm>();
    }

    //
    //  getters
    //

    public boolean isValid () { return valid; }
    public String getResponseText() { return responseText; }
    public LicenseManager.Outcome getOutcome() { return outcome; }
    public List<Alarm> getAlarms() { return alarms; }

    /*****************************************
    *
    *  getHighestAlarm
    *
    *****************************************/
    
    public Alarm getHighestAlarm()
    {
      //
      //  Note: prefer AlarmType.License_TimeLimit
      //
      
      Alarm highestAlarm = null;

      for (Alarm alarm : alarms)
        {
          if (highestAlarm == null)
            {
              highestAlarm = alarm;
            }
          else if ((alarm.getLevel() == highestAlarm.getLevel()) && (alarm.getType() == Alarm.AlarmType.License_TimeLimit))
            {
              highestAlarm = alarm;
            }
          else if (alarm.getLevel().getExternalRepresentation() > highestAlarm.getLevel().getExternalRepresentation())
            {
              highestAlarm = alarm;
            }
        }

      //
      //  return
      //

      return highestAlarm;
    }

    /*****************************************
    *
    *  anyAlarm
    *
    *****************************************/

    public boolean anyAlarm() { return getHighestAlarm().getLevel().getExternalRepresentation() > 0; }
    
    /*****************************************
    *
    *  toString
    *
    *****************************************/

    public String toString()
    {
      StringBuilder buf = new StringBuilder();

      buf.append("LicenseState:valid=" + (valid ? "yes" : "no"));
      buf.append(",responseText=" + responseText);
      buf.append(",alarms=");
      for (int i=0; i<alarms.size(); i++)
        {
          buf.append(((i==0) ? "[" : "") + alarms.get(i).getJSONRepresentation() + ((i<(alarms.size()-1)) ?  "," : "]"));
        }
      buf.append("outcome=" + outcome);

      return buf.toString();
    }
  }
}
