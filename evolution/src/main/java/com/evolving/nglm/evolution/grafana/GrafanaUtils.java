package com.evolving.nglm.evolution.grafana;

import java.io.InputStream;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
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
import com.evolving.nglm.evolution.tenancy.Tenant;


public class GrafanaUtils
{

  
  //
  // logger
  //

  private static final Logger log = LoggerFactory.getLogger(GrafanaUtils.class);

  
  public static boolean prepareGrafanaForTenants()
  {
    try
      {
        // prepare the curls
        Set<Tenant> tenants = Deployment.getTenants();

        HashMap<String, Integer> existingOrgs = getExistingGrafanaOrgs();

        for (Tenant tenant : tenants)
          {
            int tenantID = tenant.getTenantID();
            if (tenantID == 0)
              {
                continue;
              }

            // check if organization exists
            String orgName = tenant.getDisplay();
            if (!existingOrgs.containsKey(orgName))
              {
                // create this org.
                Integer orgID = createGrafanaOrg(orgName);
                if (orgID != null)
                  {
                    existingOrgs.put(orgName, orgID);
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
                    // Prepare Data sources
                    // check which dashboard already exist in this org
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
                            // let extract this variable...
                            String varName = currentString.substring(currentString.indexOf("<_") + 2, currentString.indexOf("_>"));
                            String varValue = System.getenv().get(varName);
                            if (varValue == null)
                              {
                                log.warn("GrafanaUtils.prepareGrafanaForTenants Can't retrieve environment variable " + varName + " for datasource in file " + currentFileName);
                                continue;
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

                        log.info("GrafanaUtils.prepareGrafanaForTenants ===parsing a datasource====");
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
                                    currentdatabase = currentdatabase.replace("*", "" + tenantID);
                                    datasourceDef.put("database", currentdatabase);
                                  }
                                String expectedName = (String) datasourceDef.get("name");
                                if (!exisitingDatasources.containsKey(expectedName))
                                  {
                                    // create the datasource
                                    Pair<String, Integer> db = createGrafanaDatasourceForOrg(orgID, datasourceDef);
                                    if (db != null && db.getFirstElement() != null && db.getSecondElement() != null)
                                      {
                                        log.info("GrafanaUtils.prepareGrafanaForTenants Datasource " + db.getFirstElement() + " " + db.getSecondElement() + " well created for org " + orgID);
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

                    // check which dashboard already exist in this org
                    HashMap<String, Integer> exisitingDashBoards = getExistingGrafanaDashboardForOrg(orgID);

                    // retrieve all dashboards's configuration that must exist at the end
                    reflections = new Reflections(new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage("com.evolving.nglm.evolution", ClasspathHelper.contextClassLoader(), ClasspathHelper.staticClassLoader())).setScanners(new ResourcesScanner()));
                    fileNames = reflections.getResources(x -> x.startsWith("grafana-gui"));

                    for (String currentFileName : fileNames)
                      {
                        try {
                          // check if the dashboard exists
                          InputStream is = GrafanaUtils.class.getResourceAsStream("/" + currentFileName);
                          java.util.Scanner scanner = new java.util.Scanner(is).useDelimiter("\\A");
                          String s = scanner.hasNext() ? scanner.next() : "";
                          scanner.close();
  
                          log.info("GrafanaUtils.prepareGrafanaForTenants ===parsing a Dashboard==== " + currentFileName + "\n" + s);
                          JSONObject fullDashbaordDef = (JSONObject) (new JSONParser()).parse(s);
                          JSONObject dashbaordDef = (JSONObject) fullDashbaordDef.get("dashboard");
                          String expectedTitle = (String) dashbaordDef.get("title");
                          if (!exisitingDashBoards.containsKey(expectedTitle))
                            {
                              // create the dashboard
                              Pair<String, Integer> db = createGrafanaDashBoardForOrg(orgID, fullDashbaordDef);
                              if (db != null && db.getFirstElement() != null && db.getSecondElement() != null)
                                {
                                  log.info("GrafanaUtils.prepareGrafanaForTenants Dashboard " + db.getFirstElement() + " " + db.getSecondElement() + " well created");
                                }
                              else
                                {
                                  log.warn("GrafanaUtils.prepareGrafanaForTenants Problem while creating Dashboard " + db + " for orgID " + orgID + " for dashboard file name " + currentFileName);
                                }
                            }
                        }
                        catch(Exception e)
                        {
                          log.warn("Exception " + e.getClass().getName() + " while creating dasboard from file " + currentFileName + " for tenant " + tenantID, e);
                        }
                        
                      }
                  }
                else
                  {
                    log.warn("GrafanaUtils.prepareGrafanaForTenants Can't switch to org " + orgID);
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
  
  private static HttpResponse sendGrafanaCurl(JSONObject body, String uri, String httpMethod) {

    String grafanaHost = System.getenv("GRAFANA_HOST");
    String grafanaPort = System.getenv("GRAFANA_FOR_GUI_PORT");
    String grafanaUser = System.getenv("GRAFANA_USER");
    String grafanaPassword = System.getenv("GRAFANA_PASSWORD");

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
      log.warn("GrafanaUtils.prepareGrafanaForTenants Could not get a non null response of grafana orgs, maybe grafana is not fully started yet");
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    }
    if (response.getStatusLine().getStatusCode() != 200) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants Could not get list of grafana orgs, error code " + response.getStatusLine().getStatusCode()+ " response message " + response.getStatusLine().getReasonPhrase());
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
      log.warn("GrafanaUtils.prepareGrafanaForTenants Exception " + e.getClass().getName() + " while getting all grafana orgs", e);
      return null;
    }
  }
  
  private static Integer createGrafanaOrg(String orgName)
  {
    JSONObject orgDef = new JSONObject();
    orgDef.put("name", orgName);
    HttpResponse response = sendGrafanaCurl(orgDef, "/api/orgs", "POST");

    if (response == null) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants Could not get a non null response of grafana orgs creation ");
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    }
    if (response.getStatusLine().getStatusCode() != 200) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants Could not get list of grafana orgs creation, error code " + response.getStatusLine().getStatusCode()+ " response message " + response.getStatusLine().getReasonPhrase());
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
      log.warn("GrafanaUtils.prepareGrafanaForTenants Exception " + e.getClass().getName() + " while creating grafana orgs " + orgName, e);
      return null;
    }
  }
  
  private static boolean switchToGrafanaOrg(int orgID)
  {
    // switch using the optained orgId
    HttpResponse response = sendGrafanaCurl(null, "/api/user/using/" + orgID, "POST");
    
    if (response == null) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants Could not get a non null response of grafana orgs switch " + orgID);
      return false;
    }
    if (response.getStatusLine().getStatusCode() != 200) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants Could not switch to grafana org " + orgID + ", error code " + response.getStatusLine().getStatusCode()+ " response message " + response.getStatusLine().getReasonPhrase());
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
          log.warn("GrafanaUtils.prepareGrafanaForTenants Wrong message while switching org " + orgID + " " + responseJson.get("message"));
          return false;
        }
    }
    catch(Exception e) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants Exception " + e.getClass().getName() + " while switching to grafana orgs " + orgID, e);
      return false;
    }
  }
  
  private static HashMap<String, Integer> getExistingGrafanaDashboardForOrg(int orgID)
  {
    HttpResponse response = sendGrafanaCurl(null, "/api/search", "GET");
    if (response == null) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants Could not get a non null response when getting list of dashboard for orgID " + orgID);
      return null;
    }
    if (response.getStatusLine().getStatusCode() != 200) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants Could not get list of dashboards for org " + orgID + ", error code " + response.getStatusLine().getStatusCode()+ " response message " + response.getStatusLine().getReasonPhrase());
      return null;
    }
    
    // if we are here, then the status code is 200
    // parse the entity response
    try 
    {
      JSONArray responseJson = (JSONArray) (new JSONParser())
          .parse(EntityUtils.toString(response.getEntity(), "UTF-8"));
      HashMap<String, Integer> existingDashboard = new HashMap<>();
      for (int i = 0; i < responseJson.size(); i++) {
        JSONObject currentDashboard = (JSONObject) responseJson.get(i);
        int dbID = JSONUtilities.decodeInteger(currentDashboard, "id");
        String titleDb = JSONUtilities.decodeString(currentDashboard, "title");        
        existingDashboard.put(titleDb, dbID);
      }
      return existingDashboard;
    }
    catch(Exception e) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants Exception " + e.getClass().getName() + " while getting all grafana dashboard for orgs " + orgID, e);
      return null;
    }
  }
  
  private static Pair<String, Integer> createGrafanaDashBoardForOrg(int orgID, JSONObject dbDefinition)
  {
    HttpResponse response = sendGrafanaCurl(dbDefinition, "/api/dashboards/db", "POST");
    
    if (response == null) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants Could not get a non null response while creating dashboard " + dbDefinition.get("title") + " for organisation "  + orgID);
      return null;
    }
    if (response.getStatusLine().getStatusCode() != 200) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants Could not while creating dashboard " + dbDefinition.get("title") + " for organisation orgID, error code " + response.getStatusLine().getStatusCode() + " response message " + response.getStatusLine().getReasonPhrase());
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
      log.warn("GrafanaUtils.prepareGrafanaForTenants Exception " + e.getClass().getName() + " while creating dashboard " + dbDefinition.get("title") + " for organisation orgID " + orgID, e);
      return null;
    }
  }
  
  
  private static HashMap<String, Integer> getExistingGrafanaDatasourceForOrg(int orgID)
  {
    HttpResponse response = sendGrafanaCurl(null, "/api/datasources", "GET");
    if (response == null) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants Could not get a non null response when getting list of datasources for orgID " + orgID);
      return null;
    }
    if (response.getStatusLine().getStatusCode() != 200) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants Could not get list of datasources for org " + orgID + ", error code " + response.getStatusLine().getStatusCode());
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
      log.warn("GrafanaUtils.prepareGrafanaForTenants Exception " + e.getClass().getName() + " while getting all grafana datasource for orgs " + orgID, e);
      return null;
    }
  }
  
  private static Pair<String, Integer> createGrafanaDatasourceForOrg(int orgID, JSONObject dsDefinition)
  {
    HttpResponse response = sendGrafanaCurl(dsDefinition, "/api/datasources", "POST");
    
    if (response == null) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants Could not get a non null response while creating datasource " + dsDefinition.get("title") + " for organisation "  + orgID);
      return null;
    }
    if (response.getStatusLine().getStatusCode() != 200) {
      log.warn("GrafanaUtils.prepareGrafanaForTenants Could not while creating datasource " + dsDefinition.get("title") + " for organisation orgID, error code " + response.getStatusLine().getStatusCode()+ " response message " + response.getStatusLine().getReasonPhrase());
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
      log.warn("GrafanaUtils.prepareGrafanaForTenants Exception " + e.getClass().getName() + " while creating datasource " + dsDefinition.get("title") + " for organisation orgID " + orgID, e);
      return null;
    }
  }
}
