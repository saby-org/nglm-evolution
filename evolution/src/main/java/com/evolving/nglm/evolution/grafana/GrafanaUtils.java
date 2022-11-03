package com.evolving.nglm.evolution.grafana;

import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.evolution.TokenUtils;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;
import com.evolving.nglm.evolution.tenancy.Tenant;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;


public class GrafanaUtils
{

  
  //
  // logger
  //

  private static final Logger log = LoggerFactory.getLogger(GrafanaUtils.class);

  
  public static boolean prepareGrafanaForTenants(ElasticsearchClientAPI elasticsearch)
  {
    // check if MAG is deployed on the client side
    boolean magDeploy = false;
    String magDeployedString = System.getenv("MAG_DEPLOYED");
    log.info("== MAG status ==" + magDeployedString);
    if(magDeployedString != null && magDeployedString.trim().toLowerCase().equals("true")) 
    { 
      log.info("MAG status is: " + magDeployedString + " ==> MAG is deployed on the client side");
      magDeploy = true;
    }
    else
    {
      log.info("MAG status is: " + magDeployedString + " ==> MAG is NOT deployed on the client side");
    }
    
    try
      {
        // prepare the curls
        Set<Tenant> tenants = Deployment.getTenants();
        
       

        HashMap<String, Integer> existingOrgs = getExistingGrafanaOrgs();
        log.info("existingOrgs: " + existingOrgs);
        
        // Iterate the existingOrgs and delete those that are neither Main Org.nor t<tenantID>
        for (Map.Entry<String, Integer> orgUnderStudy : existingOrgs.entrySet()) 
        {
          log.info("orgUnderStudy: " + orgUnderStudy.getKey());
          if(!orgUnderStudy.getKey().equals("Main Org.")) 
          {
            boolean isOrgNameTtenantID = false; 
            for (Tenant tenant : tenants)
            {
              int tenantID = tenant.getTenantID();
              log.info("tenantID under-study: " +tenantID);
              if (orgUnderStudy.getKey().equals("t"+tenantID))
                {
                isOrgNameTtenantID = true;
                break;
                }
            }
            
            if (!isOrgNameTtenantID) 
            {
              log.info(orgUnderStudy.getKey() + " should be deleted");
              HttpResponse response = sendGrafanaCurl(null, "/api/orgs/" + orgUnderStudy.getValue(), "DELETE");
              if (response == null) {
                log.warn("Could not get a non null response while loading organization " + orgUnderStudy.getKey() + " with OrgID "  + orgUnderStudy.getValue());
              }
              if (response.getStatusLine().getStatusCode() != 200) {
                log.warn("Problem while deleting org " + orgUnderStudy.getKey() + " for orgID, " + orgUnderStudy.getValue() + " error code " + response.getStatusLine().getStatusCode() + " response message " + response.getStatusLine().getReasonPhrase());
              }
              if(response.getStatusLine().getStatusCode() == 200) {
                log.info(orgUnderStudy.getKey() + " is deleted");
              }
            }
          }
        }
        for (Tenant tenant : tenants)
          {
            int tenantID = tenant.getTenantID();
            
            // check if organization exists
//            String orgName = tenant.getDisplay();
            String orgName = "t" + tenantID;
            if(tenantID == 0) 
            { 
              orgName = "Main Org."; 
            }
            if (!existingOrgs.containsKey(orgName))
            {
              // create this org.
              try 
              {
                Integer orgID = createGrafanaOrg(orgName);
                if (orgID != null)
                {
                  existingOrgs.put(orgName, orgID);
                }
              }catch (Exception e) 
              {
                log.warn("Problem while creating the organization " + orgName, e);
              }
            }
              
            if (existingOrgs.containsKey(orgName))
              {
                // Switch to the org
                int orgID = existingOrgs.get(orgName);

                // switch using the optained orgId
                boolean switchOK = switchToGrafanaOrg(orgID);

                if (switchOK)
                  {
                    // -----------Prepare Data sources-----------------
                    // check which datasource already exists in this org
                    HashMap<String, Integer> exisitingDatasources = getExistingGrafanaDatasourceForOrg(orgID);

                    // retrieve all datasource configuration that must exist at the end
                    Reflections reflections = new Reflections(new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage("config", ClasspathHelper.contextClassLoader(), ClasspathHelper.staticClassLoader())).setScanners(new ResourcesScanner()));
                    Set<String> fileNames = reflections.getResources(x -> x.startsWith("grafana-datasource"));
                    
                    for (String currentFileName : fileNames)
                      {
                        if (!currentFileName.endsWith(".json")) continue;
                        // check if the datasource exists
                        log.info("GrafanaUtils.prepareGrafanaForTenants Handle datasource file " + currentFileName);

                        InputStream is = GrafanaUtils.class.getResourceAsStream("/" + currentFileName);
                        java.util.Scanner scanner = new java.util.Scanner(is).useDelimiter("\\A");
                        String s = scanner.hasNext() ? scanner.next() : "";
                        scanner.close();

                        // replace variables if needed
                        int index = 0;
                        HashMap<String, String> toReplace = new HashMap<>();
                        while (s.substring(index, s.length()).contains("<_"))
                          {
                            String currentString = s.substring(index, s.length());
                            // let's extract this variable...
                            String varName = currentString.substring(currentString.indexOf("<_") + 2, currentString.indexOf("_>"));
                            String varValue = System.getenv().get(varName);
                            if (varValue == null)
                              {
                                log.warn("GrafanaUtils.prepareGrafanaForTenants Can't retrieve environment variable " + varName + " for datasource in file " + currentFileName);
                                break;
                              }
                            else
                              {
                                toReplace.put(varName, varValue);
                                index = index + currentString.indexOf("_>") + 2;
                              }
                          }
                        for (Map.Entry<String, String> replace : toReplace.entrySet())
                          {
                            s = s.replace("<_" + replace.getKey() + "_>", replace.getValue());
                          }
                        System.out.println("OK file " + currentFileName + " " + s);

                        log.debug("GrafanaUtils.prepareGrafanaForTenants ===parsing a datasource====");
                        try
                          {
                            JSONObject datasourcesDef = (JSONObject) (new JSONParser()).parse(s);

                            JSONArray datasourcesArray = (JSONArray) datasourcesDef.get("datasources");
                            for (int i = 0; i < datasourcesArray.size(); i++)
                              {
                                JSONObject datasourceDef = (JSONObject) datasourcesArray.get(i);
                                if (datasourceDef.get("database") != null)
                                  {
                                    String currentdatabase = (String) datasourceDef.get("database");
                                    currentdatabase = currentdatabase.replace("t*", "t" + tenantID);
                                    datasourceDef.put("database", currentdatabase);
                                  }
                                String expectedName = (String) datasourceDef.get("name");
                                if (!exisitingDatasources.containsKey(expectedName))
                                  {
                                    // create the datasource
                                    Pair<String, Integer> db = createGrafanaDatasourceForOrg(orgID, datasourceDef);
                                    if (db != null && db.getFirstElement() != null && db.getSecondElement() != null)
                                      {
                                        log.debug("GrafanaUtils.prepareGrafanaForTenants Datasource " + db.getFirstElement() + " " + db.getSecondElement() + " well created for org " + orgID);
                                      }
                                    else
                                      {
                                        log.warn("GrafanaUtils.prepareGrafanaForTenants Problem while creating Datasource " + db + " for orgID " + orgID + " for datasource file name " + currentFileName);
                                      }
                                  }
                                else
                                  {
                                    log.info("contains...");
                                  }
                              }
                          }
                        catch (Exception e)
                          {
                            log.warn("GrafanaUtils.prepareGrafanaForTenants Exception " + e.getClass().getName() + " while handling datasource " + currentFileName + " for orgId " + orgID, e);
                          }
                      }

                    // -----------Prepare Dashboards-----------------
                    // check which dashboard already exists in this org and extract its uid
                    HashMap<String, String> exisitingDashBoards = getExistingGrafanaDashboardForOrg(orgID);
                    
                    // retrieve all dashboards's configuration that must exist at the end
                    reflections = new Reflections(new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage("com.evolving.nglm.evolution", ClasspathHelper.contextClassLoader(), ClasspathHelper.staticClassLoader())).setScanners(new ResourcesScanner()));
                    Set<String> dbFileNames = reflections.getResources(x -> x.startsWith("grafana-gui"));
                    Set<String> nonT0FileNames = new LinkedHashSet<String>();
                    Set<String> t0FileNames = new LinkedHashSet<String>();
                    Set<String> magFileNames = new LinkedHashSet<String>();
                    for(String fileName : dbFileNames)
                    {
                      if(tenantID == 0 && fileName.startsWith("config/grafana-gui-t0"))
                      {
                        t0FileNames.add(fileName);
                      }
                      if(tenantID != 0 && !fileName.startsWith("config/grafana-gui-t0"))
                      {
                        magFileNames.add(fileName);
                      }
                      if(tenantID != 0 && !fileName.startsWith("config/grafana-gui-t0") && !fileName.startsWith("config/grafana-gui-mag"))
                      {
                        nonT0FileNames.add(fileName);
                      }
                    }
                    if(tenantID == 0)
                    {
                      createDashboardForOrg(elasticsearch, existingOrgs, tenantID, orgID, t0FileNames, exisitingDashBoards);
                    }
                    else if(tenantID != 0){
                      if(magDeploy == true) {
                        createDashboardForOrg(elasticsearch, existingOrgs, tenantID, orgID, magFileNames, exisitingDashBoards);
                      }
                      else {
                        createDashboardForOrg(elasticsearch, existingOrgs, tenantID, orgID, nonT0FileNames, exisitingDashBoards);
                      }
                    }
                  }
                else
                  {
                    log.warn("GrafanaUtils.prepareGrafanaForTenants: Can't switch to org " + orgID);
                    return false;
                  }
              }
          }
        return true;
      }
      catch(Exception e)
      {
        log.warn("Exception " + e.getClass().getName() + " while creating grafana configuration", e);
        return false;
      }
  }

  private static void createDashboardForOrg(ElasticsearchClientAPI elasticsearch, HashMap<String, Integer> existingOrgs,
      int tenantID, int orgID, Set<String> fileNames, HashMap<String, String> exisitingDashBoards) {
                    for (String currentFileName : fileNames)
                    {
                      try {
                        // check if the dashboard already exists
                        InputStream is = GrafanaUtils.class.getResourceAsStream("/" + currentFileName);
                        java.util.Scanner scanner = new java.util.Scanner(is).useDelimiter("\\A");
                        String s = scanner.hasNext() ? scanner.next() : "";
                        s = s.replace("tenantID:camptenantID", "tenantID:" + tenantID);
                        s = s.replace("replaceWithTenantID", "" + tenantID );
                        
                        log.trace("GrafanaUtils.prepareGrafanaForTenants ===parsing a Dashboard==== " + currentFileName + "\n" + s);
                        JSONObject fulldashboardDef = (JSONObject) (new JSONParser()).parse(s);
                        JSONObject dashboardDef = (JSONObject) fulldashboardDef.get("dashboard");
                        String expectedTitle = (String) dashboardDef.get("title");
                        String existingUID = exisitingDashBoards.get(expectedTitle);
                        String regex = "[ACDEFGHJKMNPQRTWXYacdefghjkmnpqrtwx34679]{15}";
//                        HashMap<String,Object> mapDbEs=new HashMap<String,Object>();
//                        UpdateRequest request = new UpdateRequest();
                        
                        // setting the tenant timeZone in the dashboard
                        Deployment deployment = Deployment.getDeployment(tenantID);
                        String tz = deployment.getTimeZone();
                        
                        log.info("The dashboard under-study is: === " + expectedTitle + " ===");
                        
                        // 1- The dashboard already exists but its uid doesn't start with t<tenantID>-
                        if (exisitingDashBoards.containsKey(expectedTitle)&& existingUID.substring(0, 3).equals("t" + tenantID + "-") == false)
                        {
                          log.debug("GrafanaUtils.prepareGrafanaForTenants: Dashboard " + expectedTitle + " already exists for orgID " + orgID + " for dashboard file name " + currentFileName + " and it'll be deleted and recreated.");
                          // Delete it using its exisitng uid
                          HttpResponse response = sendGrafanaCurl(null, "/api/dashboards/uid/" + existingUID, "DELETE");
                          log.info("Dashboard titled " + expectedTitle + " with uid " + existingUID + " is deleted");
                          if (response == null) {
                            log.warn("Could not get a non null response while loading dashboard " + expectedTitle + " for organisation "  + orgID);
                          }
                          if (response.getStatusLine().getStatusCode() != 200) {
                            log.warn("Problem while loading dashboard " + expectedTitle + " for organisation orgID, " + orgID + " error code " + response.getStatusLine().getStatusCode() + " response message " + response.getStatusLine().getReasonPhrase());
                          }
                          // Then recreate it using a unique uid that starts with t<tenandID>-
                          String newUID = "t"+ tenantID + "-" + TokenUtils.generateFromRegex(regex);
                          s= s.replace("replaceWithTenantTimeZone", tz);
                          s = s.replace("replaceWithUniqueID", newUID );
                          fulldashboardDef = (JSONObject) (new JSONParser()).parse(s);
//                          mapDbEs.put("name",expectedTitle);
//                          mapDbEs.put("reportID",newUID);
//                          request = new UpdateRequest("dashboard_links",newUID);
                          log.info("The new uid of the already existing Dashboard: " + expectedTitle + " is " + newUID);
                        }
                        // 2- The dashboard already exists and its uid starts with t<tenantID>-
                        else if (exisitingDashBoards.containsKey(expectedTitle) && existingUID.substring(0, 3).equals("t" + tenantID + "-") == true)
                        {
                          // Delete the dashboard if it is the "New General Dashboard" or the "Business - Campaign Dashboard"
                          // TODO -this verification will be no longer needed when all clients are up-to-date-
                          for (Map.Entry<String, String> dbUnderStudy : exisitingDashBoards.entrySet()) 
                          {
                            if(dbUnderStudy.getKey().equals("New General Dashboard") || dbUnderStudy.getKey().equals("Business - Campaign Dashboard")) 
                            {
                              log.info("Dashboard titled " + dbUnderStudy.getKey() + " with uid " + dbUnderStudy.getValue() + " is one of the two dashboards to be deleted");
                              HttpResponse response = sendGrafanaCurl(null, "/api/dashboards/uid/" + dbUnderStudy.getValue(), "DELETE");
                              if (response == null) {
                                log.warn("Could not get a non null response while loading dashboard " + dbUnderStudy.getKey() + " for organisation "  + orgID);
                              }
                              if (response.getStatusLine().getStatusCode() != 200) {
                                log.warn("Problem while deleting dashboard " + dbUnderStudy.getKey() + " for organisation orgID, " + orgID + " error code " + response.getStatusLine().getStatusCode() + " response message " + response.getStatusLine().getReasonPhrase());
                              }
                            } 
                            else
                            {
                              // overwrite it using the same existing uid
                              log.debug("GrafanaUtils.prepareGrafanaForTenants: Dashboard " + expectedTitle + " already exists for orgID " + orgID + " for dashboard file name " + currentFileName + " and it'll be overwritten.");
                              s= s.replace("replaceWithTenantTimeZone", tz);
                              s= s.replace("replaceWithUniqueID", existingUID);
                              fulldashboardDef = (JSONObject) (new JSONParser()).parse(s);
                              //                          mapDbEs.put("name",expectedTitle);
                              //                          mapDbEs.put("reportID",existingUID);
                              //                          request = new UpdateRequest("dashboard_links",existingUID);
                              log.info("The uid of the already existing Dashboard: " + expectedTitle + " is " + existingUID);
                            }
                          }
                        }
                        // 3- The dashboard doesn't exist already
                        else 
                        {
                          log.debug("GrafanaUtils.prepareGrafanaForTenants: Dashboard " + expectedTitle + " doesn't exist for orgID " + orgID + " for dashboard file name " + currentFileName + " and it'll be created.");
                          // Create it using a unique uid that starts with t<tenandID>-
                          String newUID = "t"+ tenantID + "-" + TokenUtils.generateFromRegex(regex);
                          s= s.replace("replaceWithTenantTimeZone", tz);
                          s = s.replace("replaceWithUniqueID", newUID );
                          fulldashboardDef = (JSONObject) (new JSONParser()).parse(s);
//                          mapDbEs.put("name",expectedTitle);
//                          mapDbEs.put("reportID",newUID);
//                          request = new UpdateRequest("dashboard_links",newUID);
                          log.info("The uid of the newly created Dashboard: " + expectedTitle + " is " + newUID );
                        }
                        scanner.close();
                        // Overwrite / create the dashboard
                        Pair<String, Integer> db = createGrafanaDashBoardForOrg(orgID, fulldashboardDef);
                        if (db != null && db.getFirstElement() != null && db.getSecondElement() != null)
                        {
                          log.debug("GrafanaUtils.prepareGrafanaForTenants: Dashboard " + db.getFirstElement() + " " + db.getSecondElement() + " is well loaded");
                        }
                        else
                        {
                          log.warn("GrafanaUtils.prepareGrafanaForTenants: Problem while loading Dashboard " + db.getFirstElement() + " for orgID " + orgID + " for dashboard file name " + currentFileName);
                        }
                        
//                        mapDbEs.put("webguiname","IAR"); 
//                        mapDbEs.put("type","GRAFANA"); 
//                        mapDbEs.put("icon",""); 
//                        mapDbEs.put("iframewidth",1800); 
//                        mapDbEs.put("iframeheight",1600); 
//                        mapDbEs.put("webpath",""); 
//                        mapDbEs.put("reportGroup",""); 
//                        mapDbEs.put("permissionKey",""); 
//                        mapDbEs.put("tenantid",tenantID);  
//                        request.doc(mapDbEs);
//                        request.docAsUpsert(true);
//                        request.retryOnConflict(4);
//
//                        try {
//                          elasticsearch.update(request, RequestOptions.DEFAULT);
//                        } catch (Exception e) {
//                          StringWriter stackTraceWriter = new StringWriter();
//                          e.printStackTrace(new PrintWriter(stackTraceWriter, true));
//                          log.error("Pushing failed: "+stackTraceWriter.toString()+"");
//                        }
                      }
                      catch(Exception e)
                      {
                        log.warn("Exception " + e.getClass().getName() + " while loading dasboard from file " + currentFileName + " for tenant " + tenantID, e);
                      }

                    }
                  }
  
  private static HttpResponse sendGrafanaCurl(JSONObject body, String uri, String httpMethod) {

    String grafanaHost = System.getenv("GRAFANA_HOST");
    String grafanaPort = System.getenv("GRAFANA_FOR_GUI_PORT");
    String grafanaUser = System.getenv("GRAFANA_USER_GUI");
    String grafanaPassword = System.getenv("GRAFANA_PASSWORD_GUI");

    CloseableHttpResponse response = null;
    try {

      HttpRequestBase request = null;
      switch (httpMethod) {
      case "POST":
        request = new HttpPost("http://" + grafanaHost + ":" + grafanaPort + uri);
        break;

      case "GET":
        request = new HttpGet("http://" + grafanaHost + ":" + grafanaPort + uri);
        break;
        
      case "DELETE":
        request = new HttpDelete("http://" + grafanaHost + ":" + grafanaPort + uri);
        break;

      default:
        break;
      }

      if (request instanceof HttpPost && body != null) {
        ((HttpPost) request).setEntity(new StringEntity(body.toJSONString()));
      }

      request.setHeader("Content-type", "application/json");
      String encoding = Base64.getEncoder().encodeToString(new String(grafanaUser + ":" + grafanaPassword).getBytes());
      request.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoding);

      int httpTimeout = 10000;
      RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(httpTimeout).setSocketTimeout(httpTimeout)
          .setConnectionRequestTimeout(httpTimeout).build();

      CloseableHttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
      response = httpClient.execute(request);
      return response;
    } catch (Exception e) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants Exception " + e.getClass().getName() + " while configuring grafana", e);
      return null;
    }
  }
  
  private static HashMap<String, Integer> getExistingGrafanaOrgs()
  {
    // get all the existing organizations
    HttpResponse response = sendGrafanaCurl(null, "/api/orgs", "GET");

    if (response == null) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Could not get a non null response of grafana orgs, maybe grafana is not fully started yet");
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    }
    if (response.getStatusLine().getStatusCode() != 200) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Could not get list of grafana orgs, error code " + response.getStatusLine().getStatusCode()+ " response message " + response.getStatusLine().getReasonPhrase());
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    }

    // if we are here, then the status code is 200
    // parse the entity response
    try 
    {
      JSONArray responseJson = (JSONArray) (new JSONParser())
          .parse(EntityUtils.toString(response.getEntity(), "UTF-8"));
      HashMap<String, Integer> existingOrgs = new HashMap<>();
      for (int i = 0; i < responseJson.size(); i++) {
        JSONObject currentOrg = (JSONObject) responseJson.get(i);
        int orgID = JSONUtilities.decodeInteger(currentOrg, "id");
        String orgName = JSONUtilities.decodeString(currentOrg, "name");
        existingOrgs.put(orgName, orgID);
      }
      return existingOrgs;
    }
    catch(Exception e) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Exception " + e.getClass().getName() + " while getting all grafana orgs", e);
      return null;
    }
  }
  
  private static Integer createGrafanaOrg(String orgName)
  {
    JSONObject orgDef = new JSONObject();
    orgDef.put("name", orgName);
    HttpResponse response = sendGrafanaCurl(orgDef, "/api/orgs", "POST");

    if (response == null) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Could not get a non null response of grafana orgs creation ");
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    }
    if (response.getStatusLine().getStatusCode() != 200) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Could not get list of grafana orgs creation, error code " + response.getStatusLine().getStatusCode()+ " response message " + response.getStatusLine().getReasonPhrase());
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    }
    
    try 
    {
      JSONObject responseJson = (JSONObject) (new JSONParser())
          .parse(EntityUtils.toString(response.getEntity(), "UTF-8"));

      Long orgID = (Long) responseJson.get("orgId");
      if(orgID != null)
        {
          return (int)orgID.longValue();
        }
      else 
        {
          return null;
        }
      }
    catch(Exception e) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Exception " + e.getClass().getName() + " while creating grafana orgs " + orgName, e);
      return null;
    }
  }
  
  private static boolean switchToGrafanaOrg(int orgID)
  {
    // switch using the optained orgId
    HttpResponse response = sendGrafanaCurl(null, "/api/user/using/" + orgID, "POST");
    
    if (response == null) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Could not get a non null response of grafana orgs switch " + orgID);
      return false;
    }
    if (response.getStatusLine().getStatusCode() != 200) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Could not switch to grafana org " + orgID + ", error code " + response.getStatusLine().getStatusCode()+ " response message " + response.getStatusLine().getReasonPhrase());
      return false;
    }
    
    try 
    {
      JSONObject responseJson = (JSONObject) (new JSONParser())
          .parse(EntityUtils.toString(response.getEntity(), "UTF-8"));
      if(responseJson.get("message") != null && responseJson.get("message").equals("Active organization changed"))
        {
          return true;
        }
      else 
        {
          log.warn("GrafanaUtils.prepareGrafanaForTenants: Wrong message while switching org " + orgID + " " + responseJson.get("message"));
          return false;
        }
    }
    catch(Exception e) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Exception " + e.getClass().getName() + " while switching to grafana orgs " + orgID, e);
      return false;
    }
  }
  
  private static HashMap<String, String> getExistingGrafanaDashboardForOrg(int orgID)
  {
    HttpResponse response = sendGrafanaCurl(null, "/api/search", "GET");
    if (response == null) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Could not get a non null response when getting list of dashboard for orgID " + orgID);
      return null;
    }
    if (response.getStatusLine().getStatusCode() != 200) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Could not get list of dashboards for org " + orgID + ", error code " + response.getStatusLine().getStatusCode()+ " response message " + response.getStatusLine().getReasonPhrase());
      return null;
    }
    
    // if we are here, then the status code is 200
    // parse the entity response
    try 
    {
      JSONArray responseJson = (JSONArray) (new JSONParser())
          .parse(EntityUtils.toString(response.getEntity(), "UTF-8"));
      HashMap<String, String> existingDashboard = new HashMap<>();
      for (int i = 0; i < responseJson.size(); i++) {
        JSONObject currentDashboard = (JSONObject) responseJson.get(i);
        String dbID = JSONUtilities.decodeString(currentDashboard, "uid");
        String titleDb = JSONUtilities.decodeString(currentDashboard, "title");        
        existingDashboard.put(titleDb, dbID);
      }
      return existingDashboard;
    }
    catch(Exception e) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Exception " + e.getClass().getName() + " while getting all grafana dashboard for orgs " + orgID, e);
      return null;
    }
  }
  
  private static Pair<String, Integer> createGrafanaDashBoardForOrg(int orgID, JSONObject dbDefinition)
  {
    HttpResponse response = sendGrafanaCurl(dbDefinition, "/api/dashboards/db", "POST");
    
    if (response == null) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Could not get a non null response while loading dashboard " + dbDefinition.get("title") + " for organisation "  + orgID);
      return null;
    }
    if (response.getStatusLine().getStatusCode() != 200) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Problem while loading dashboard " + dbDefinition.get("title") + " for organisation orgID, " + orgID + " error code " + response.getStatusLine().getStatusCode() + " response message " + response.getStatusLine().getReasonPhrase());
      return null;
    }
    
    try 
    {
      JSONObject responseJson = (JSONObject) (new JSONParser()).parse(EntityUtils.toString(response.getEntity(), "UTF-8"));
      String title = (String) responseJson.get("slug");
      Long id = (Long) responseJson.get("id");
      return new Pair<String, Integer>(title, id != null ? (int)id.longValue() : null);
    }
    catch(Exception e) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Exception " + e.getClass().getName() + " while loading dashboard " + dbDefinition.get("title") + " for organisation orgID " + orgID, e);
      return null;
    }
  }
  
  
  private static HashMap<String, Integer> getExistingGrafanaDatasourceForOrg(int orgID)
  {
    HttpResponse response = sendGrafanaCurl(null, "/api/datasources", "GET");
    if (response == null) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Could not get a non null response when getting list of datasources for orgID " + orgID);
      return null;
    }
    if (response.getStatusLine().getStatusCode() != 200) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Could not get list of datasources for org " + orgID + ", error code " + response.getStatusLine().getStatusCode());
      return null;
    }
    
    // if we are here, then the status code is 200
    // parse the entity response
    try 
    {
      JSONArray responseJson = (JSONArray) (new JSONParser())
          .parse(EntityUtils.toString(response.getEntity(), "UTF-8"));
      HashMap<String, Integer> existingDatasources = new HashMap<>();
      for (int i = 0; i < responseJson.size(); i++) {
        JSONObject currentDashboard = (JSONObject) responseJson.get(i);
        int id = JSONUtilities.decodeInteger(currentDashboard, "id");
        String name = JSONUtilities.decodeString(currentDashboard, "name");        
        existingDatasources.put(name, id);
      }
      return existingDatasources;
    }
    catch(Exception e) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Exception " + e.getClass().getName() + " while getting all grafana datasource for orgs " + orgID, e);
      return null;
    }
  }
  
  private static Pair<String, Integer> createGrafanaDatasourceForOrg(int orgID, JSONObject dsDefinition)
  {
    HttpResponse response = sendGrafanaCurl(dsDefinition, "/api/datasources", "POST");
    
    if (response == null) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Could not get a non null response while creating datasource " + dsDefinition.get("title") + " for organisation "  + orgID);
      return null;
    }
    if (response.getStatusLine().getStatusCode() != 200) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Problem while creating datasource " + dsDefinition.get("title") + " for organisation orgID, " + orgID + " error code " + response.getStatusLine().getStatusCode()+ " response message " + response.getStatusLine().getReasonPhrase());
      return null;
    }
    
    try 
    {
      JSONObject responseJson = (JSONObject) (new JSONParser()).parse(EntityUtils.toString(response.getEntity(), "UTF-8"));
      String name = (String) responseJson.get("name");
      Long id = (Long) responseJson.get("id");
      return new Pair<String, Integer>(name, id != null ? (int)id.longValue() : null);
    }
    catch(Exception e) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants: Exception " + e.getClass().getName() + " while creating datasource " + dsDefinition.get("title") + " for organisation orgID " + orgID, e);
      return null;
    }
  }
}
