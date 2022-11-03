  /*****************************************
  *
  *  StockRecurrenceAndNotificationJob
  *
  *****************************************/

package com.evolving.nglm.evolution;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.ThirdPartyManager.AuthenticatedResponse;
import com.evolving.nglm.evolution.ThirdPartyManager.ThirdPartyManagerException;

public class StockRecurrenceAndNotificationJob  extends ScheduledJob 
{
  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  private OfferService offerService;
  private ProductService productService;
  private VoucherService voucherService;
  private CallingChannelService callingChannelService;
  private CatalogCharacteristicService catalogCharacteristicService;
  private SalesChannelService salesChannelService;
  private SupplierService supplierService;
  private String fwkServer;
  private JSONObject fromJSON;
  private JSONObject credentialJson;
  int httpTimeout = 50000;
  RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(httpTimeout).setSocketTimeout(httpTimeout).setConnectionRequestTimeout(httpTimeout).build();
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public StockRecurrenceAndNotificationJob(String jobName, String periodicGenerationCronEntry, String baseTimeZone, boolean scheduleAtStart, OfferService offerService, ProductService productService, VoucherService voucherService, CallingChannelService callingChannelService, CatalogCharacteristicService catalogCharacteristicService, SalesChannelService salesChannelService, SupplierService supplierService, String fwkServer, String fwkEmailSMTPUserName)
  {
    super(jobName, periodicGenerationCronEntry, baseTimeZone, scheduleAtStart); 
    this.offerService = offerService;
    this.productService = productService;
    this.voucherService = voucherService;
    this.callingChannelService = callingChannelService;
    this.catalogCharacteristicService = catalogCharacteristicService;
    this.salesChannelService = salesChannelService;
    this.supplierService = supplierService;
    this.fwkServer = fwkServer;
    Map<String, Object> fromJson = new LinkedHashMap<String, Object>();
    fromJson.put("SenderEmail", fwkEmailSMTPUserName);
    fromJson.put("Sender", "Admin");
    this.fromJSON = JSONUtilities.encodeObject(fromJson);
    Map<String, Object> credentialJsonRepresentation = new LinkedHashMap<String, Object>();
    credentialJsonRepresentation.put("LoginName", Deployment.getStockAlertLoginCredentail());
    credentialJsonRepresentation.put("Password", Deployment.getStockAlertPasswordCredentail());
    this.credentialJson = JSONUtilities.encodeObject(credentialJsonRepresentation);
  }

  /*****************************************
  *
  *  run
  *
  *****************************************/
  
  @Override protected void run()
  {
    Date now = SystemTime.getCurrentTime();
    Collection<Offer> activeOffers = offerService.getActiveOffers(now, 0);
    for (Offer offer : activeOffers)
      {
        Integer remainingStock = offer.getApproximateRemainingStock();
        boolean stockThersoldBreached = remainingStock != null && (remainingStock <= offer.getStockAlertThreshold());
        if (stockThersoldBreached)
          {
            //
            //  send notification
            //
            
            if (offer.getStockAlert())
              {
                log.info("ready to send alert notification for offer {}", offer.getGUIManagedObjectDisplay());
                sendNotification(offer, remainingStock);
              }
            
            //
            // auto increment stock (EVPRO-1600)
            //
            
            if (offer.getStockRecurrence())
              {
                JSONObject offerJson = offer.getJSONRepresentation();
                offerJson.replace("presentationStock", offer.getStock() + offer.getStockRecurrenceBatch());
                try
                  {
                    Offer newOffer = new Offer(offerJson, GUIManager.epochServer.getKey(), offer, catalogCharacteristicService, offer.getTenantID());
                    offerService.putOffer(newOffer, callingChannelService, salesChannelService, productService, voucherService, (offer == null), "StockRecurrenceAndNotificationJob");
                  } 
                catch (GUIManagerException e)
                  {
                    e.printStackTrace();
                  }
              } 
            else
              {
                log.debug("stock recurrence scheduling not required for offer[{}]-- remaingin stock[{}], thresold limit[{}]", offer.getOfferID(), offer.getApproximateRemainingStock(), offer.getStockAlertThreshold());
              }
          }
      }
    
    Collection<Product> activeProducts = productService.getActiveProducts(now, 0);
    for (Product product : activeProducts)
      {
        Integer remainingStock = product.getApproximateRemainingStock();
        boolean stockThersoldBreached = remainingStock != null && (remainingStock <= product.getStockAlertThreshold());
        if (stockThersoldBreached)
          {
            //
            //  send notification
            //
            
            if (product.getStockAlert())
              {
                log.info("ready to send alert notification for product {}", product.getGUIManagedObjectDisplay());
                sendNotification(product, remainingStock);
              }
          }
      }
    
    Collection<Voucher> activeVouchers = voucherService.getActiveVouchers(now, 0);
    for (Voucher voucher : activeVouchers)
      {
        Voucher voucherwithStock = (Voucher) voucherService.getStoredVoucherWithCurrentStocks(voucher.getGUIManagedObjectID(), false);
        Integer remainingStock = JSONUtilities.decodeInteger(voucherwithStock.getJSONRepresentation(), "remainingStock", false);
        if (remainingStock != null)
          {
            boolean stockThersoldBreached = remainingStock <= voucher.getStockAlertThreshold();
            if (stockThersoldBreached)
              {
                //
                //  send notification
                //
                
                if (voucher.getStockAlert())
                  {
                    log.info("ready to send alert notification for voucher {}", voucher.getGUIManagedObjectDisplay());
                    sendNotification(voucherwithStock, remainingStock);
                  }
              }
          }
        
      }
  }
  
  /*****************************************
  *
  *  sendNotification - FWK API Call
  *
  *****************************************/
  
  public void sendNotification(GUIManagedObject guiManagedObject, final Integer remainingStock)
  {
    CloseableHttpResponse httpResponse = null;
    try (CloseableHttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build())
    {
      JSONObject loginJSON = getLoginToken();
      String token =  JSONUtilities.decodeString(loginJSON, "Token", true);
      Integer userID = JSONUtilities.decodeInteger(loginJSON, "UserId", true);
      if (token == null || token.trim().isEmpty())
        {
          log.warn("bad token - skip sending stockAlert notification");
          return;
        }
      String subject = null;
      String body = null;
      List<JSONObject> receipientList = new ArrayList<JSONObject>();
      Map<String, Object> communicationMap = new HashMap<String, Object>();
      if (guiManagedObject instanceof Offer)
        {
          Offer offer = (Offer) guiManagedObject;
          receipientList = getRecipientList(offer.getNotificationEmails());
          if (receipientList.isEmpty())
            {
              log.warn("unable to send stock alert notification as RecipientList is empty for {}", guiManagedObject.getGUIManagedObjectDisplay());
              return;
            }
          Object[] bodyTags = {"offer", offer.getGUIManagedObjectDisplay(), remainingStock};
          Object[] subjectTags = {"offer", offer.getGUIManagedObjectDisplay()};
          subject = resolveTags(Deployment.getStockAlertEmailSubject(), subjectTags);
          body = resolveTags(Deployment.getStockAlertEmailBody(), bodyTags);
          
        }
      else if (guiManagedObject instanceof Product)
        {
          Product product = (Product) guiManagedObject;
          receipientList = getRecipientList(product.getNotificationEmails());
          if (receipientList.isEmpty())
            {
              log.warn("unable to send stock alert notification as RecipientList is empty for {}", guiManagedObject.getGUIManagedObjectDisplay());
              return;
            }
          Object[] bodyTags = {"product", product.getGUIManagedObjectDisplay(), remainingStock};
          Object[] subjectTags = {"product", product.getGUIManagedObjectDisplay()};
          subject = resolveTags(Deployment.getStockAlertEmailSubject(), subjectTags);
          body = resolveTags(Deployment.getStockAlertEmailBody(), bodyTags);
        }
      else if (guiManagedObject instanceof Voucher)
        {
          Voucher voucher = (Voucher) guiManagedObject;
          receipientList = getRecipientList(voucher.getNotificationEmails());
          if (receipientList.isEmpty())
            {
              log.warn("unable to send stock alert notification as RecipientList is empty for {}", guiManagedObject.getGUIManagedObjectDisplay());
              return;
            }
          Object[] bodyTags = {"voucher", voucher.getGUIManagedObjectDisplay(), remainingStock};
          Object[] subjectTags = {"voucher", voucher.getGUIManagedObjectDisplay()};
          subject = resolveTags(Deployment.getStockAlertEmailSubject(), subjectTags);
          body = resolveTags(Deployment.getStockAlertEmailBody(), bodyTags);
        }
      communicationMap.put("UserId", userID);
      communicationMap.put("From", fromJSON);
      communicationMap.put("To", JSONUtilities.encodeArray(receipientList));
      communicationMap.put("Cc", new JSONArray());
      communicationMap.put("Subject", subject);
      communicationMap.put("Body", body);
      communicationMap.put("ObjectShortDescription", "");
      communicationMap.put("IsBodyHtml", true);
      communicationMap.put("AreRecepientsApprovalManagers", false);
      communicationMap.put("AreRecepientsAllUsersWithPermission", false);
      communicationMap.put("PermissionKeyWhichRecipientsMustHave", "");
      communicationMap.put("AreMacrosAvailable", true);
      communicationMap.put("Macros", new JSONArray());
      communicationMap.put("GenerateTokenForEachRecepeint", true);
      communicationMap.put("AreFirstTwoMacrosFromBodyTokens", true);
      communicationMap.put("SendSeparateEmailForEachRecipient", true);
      communicationMap.put("ApplicationKey", "");
      communicationMap.put("ObjectId", "");
      communicationMap.put("CallBackURL", "");
      communicationMap.put("LoginName", Deployment.getStockAlertLoginCredentail());
      
      String payload = JSONUtilities.encodeObject(communicationMap).toJSONString();
      log.debug("sendNotification - FWK API Call payload {}", payload);
      
      //
      // create request
      //

      StringEntity stringEntity = new StringEntity(payload, ContentType.create("application/json"));
      HttpPost httpPost = new HttpPost("http://" + fwkServer + "/api/communication/email");
      httpPost.addHeader("token", token);
      httpPost.addHeader("Content-Type", "application/json");
      httpPost.setEntity(stringEntity);

      //
      // submit request
      //

      httpResponse = httpClient.execute(httpPost);

      //
      // process response
      //

      if (httpResponse != null && httpResponse.getStatusLine() != null && httpResponse.getStatusLine().getStatusCode() == 200)
        {
        }
      else if (httpResponse != null && httpResponse.getStatusLine() != null && httpResponse.getStatusLine().getStatusCode() == 401)
        {
          log.error("FWK communication server HTTP reponse code {} ", httpResponse.getStatusLine().getStatusCode());
        }
      else if (httpResponse != null && httpResponse.getStatusLine() != null)
        {
          log.error("FWK communication server HTTP reponse code is invalid {}", httpResponse.getStatusLine().getStatusCode());
        }
      else
        {
          log.error("FWK communication server error httpResponse or httpResponse.getStatusLine() is null {}", httpResponse, httpResponse.getStatusLine());
        }
    }
    catch(IOException e) 
    {
      log.error("failed FWK server on sending notification");
      log.error("IOException: {}", e.getMessage());
    }
    finally
    {
      if (httpResponse != null)
        try
          {
            httpResponse.close();
          } 
      catch (IOException e)
          {
            e.printStackTrace();
          }
    }
  
  }
  
  /*****************************************
  *
  *  getLoginToken - FWK API Call
  *
  *****************************************/
  
  public JSONObject getLoginToken()
  {
    JSONObject result = new JSONObject();
    CloseableHttpResponse httpResponse = null;
    try (CloseableHttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build())
    {
      //
      // create request
      //

      StringEntity stringEntity = new StringEntity(credentialJson.toJSONString(), ContentType.create("application/json"));
      HttpPost httpPost = new HttpPost("http://" + fwkServer + "/api/account/login");
      httpPost.setEntity(stringEntity);

      //
      // submit request
      //

      httpResponse = httpClient.execute(httpPost);

      //
      // process response
      //

      if (httpResponse != null && httpResponse.getStatusLine() != null && httpResponse.getStatusLine().getStatusCode() == 200)
        {
          String jsonResponse = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
          //
          // parse JSON response from FWK
          //

          result = (JSONObject) (new JSONParser()).parse(jsonResponse);

        }
      else if (httpResponse != null && httpResponse.getStatusLine() != null && httpResponse.getStatusLine().getStatusCode() == 401)
        {
          log.error("FWK server HTTP reponse code {} message {} ", httpResponse.getStatusLine().getStatusCode(), EntityUtils.toString(httpResponse.getEntity(), "UTF-8"), JSONUtilities.decodeString(credentialJson, "LoginName", true), " : user is reset in the cache" );
        }
      else if (httpResponse != null && httpResponse.getStatusLine() != null)
        {
          log.error("FWK server HTTP reponse code is invalid {}", httpResponse.getStatusLine().getStatusCode(), JSONUtilities.decodeString(credentialJson, "LoginName", true), " : user is reset in the cache");
        }
      else
        {
          log.error("FWK server error httpResponse or httpResponse.getStatusLine() is null {} {} ", httpResponse, httpResponse.getStatusLine(), JSONUtilities.decodeString(credentialJson, "LoginName", true), " : user is reset in the cache");
        }
    }
    catch(ParseException pe) 
    {
      log.error("failed to Parse ParseException {} ", pe.getMessage());
    }
    catch(IOException e) 
    {
      log.error("failed to authenticate in FWK server");
      log.error("IOException: {}", e.getMessage());
    }
    finally
    {
      if (httpResponse != null)
        try
          {
            httpResponse.close();
          } 
      catch (IOException e)
          {
            e.printStackTrace();
          }
    }
    return result;
  }

  private List<JSONObject> getRecipientList(List<String> stockAlertEmailToList)
  {
    List<JSONObject> result = new ArrayList<JSONObject>();
    for (String recipientEmail : stockAlertEmailToList)
      {
        Map<String, Object> recipientMap = new LinkedHashMap<String, Object>();
        recipientMap.put("RecipientEmail", recipientEmail);
        recipientMap.put("Recipient", "TEst");
        recipientMap.put("Macros", new JSONArray());
        result.add(JSONUtilities.encodeObject(recipientMap));
      }
    return result;
  }

  private String resolveTags(final String unformattedText, Object[] tagArgs)
  {
    MessageFormat form = new MessageFormat(unformattedText);
    return form.format(tagArgs);
  }
}
