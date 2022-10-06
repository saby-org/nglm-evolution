  /*****************************************
  *
  *  StockRecurrenceAndNotificationJob
  *
  *****************************************/

package com.evolving.nglm.evolution;

import java.io.IOException;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.stream.Collectors;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.RLMDateUtils.DatePattern;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

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
  private String fwkEmailSMTPUserName;
  int httpTimeout = 10000;
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
    this.fwkEmailSMTPUserName = fwkEmailSMTPUserName;
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
          }
        
        //
        // auto increment stock (EVPRO-1600)
        //
        
        if (offer.getStockRecurrence())
          {
            String datePattern = DatePattern.LOCAL_DAY.get();
            String tz = Deployment.getDeployment(offer.getTenantID()).getTimeZone();
            Date time = RLMDateUtils.truncate(SystemTime.getCurrentTime(), Calendar.DATE, tz);
            Date formattedTime = formattedDate(time, datePattern);
            List<Date> stockReplanishDates = getExpectedStockReplanishDates(offer, datePattern);
            
            log.info("[PRJT] offer[{}] Last Stock Replanish Date: {}", offer.getOfferID(), offer.getLastStockRecurrenceDate());
            if(stockReplanishDates.contains(formattedTime) && formattedDate(offer.getLastStockRecurrenceDate(), datePattern).compareTo(formattedTime) < 0)
              {
                log.info("[PRJT] offer[{}] next stock replanish date[{}] is today[{}]", offer.getOfferID(), stockReplanishDates.stream().filter(date -> date.compareTo(formattedTime) >= 0).findFirst(), formattedTime);
                JSONObject offerJson = offer.getJSONRepresentation();
                offerJson.replace("presentationStock", offer.getStock() + offer.getStockRecurrenceBatch());
                offerJson.put("lastStockRecurrenceDate", new SimpleDateFormat(DatePattern.REST_UNIVERSAL_TIMESTAMP_DEFAULT.get()).format(time)); // string
                try
                  {
                    //offer.setLastStockRecurrenceDate(time);
                    Offer newOffer = new Offer(offerJson, GUIManager.epochServer.getKey(), offer, catalogCharacteristicService, offer.getTenantID());
                    offerService.putOffer(newOffer, callingChannelService, salesChannelService, productService, voucherService, (offer == null), "StockRecurrenceAndNotificationJob");
                  } 
                catch (GUIManagerException e)
                  {
                    log.error("Stock Recurrence Exception: {}", e.getMessage());
                  }
              }
            else
              {
                log.info("[PRJT] offer[{}] next stock replanish date[{}] is NOT today[{}]", offer.getOfferID(), stockReplanishDates.stream().filter(date -> date.compareTo(formattedTime) > 0).findFirst(), formattedTime);
              }
          } 
        else
          {
            log.info("[PRJT] stock recurrence scheduling not required for offer[{}]-- remaingin stock[{}], thresold limit[{}]", offer.getOfferID(), offer.getApproximateRemainingStock(), offer.getStockAlertThreshold());
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
  
  public Date formattedDate(Date date, String pattern)
  {
    DateFormat formatter = new SimpleDateFormat(pattern);
    try
      {
        return formatter.parse(formatter.format(date));
      } 
    catch (java.text.ParseException e)
      {
        e.printStackTrace();
      }
    return date;
  }
  
  private List<Date> getExpectedStockReplanishDates(Offer offer, String datePattern)
  {
    Date now = SystemTime.getCurrentTime();
    String tz = Deployment.getDeployment(offer.getTenantID()).getTimeZone();
    
    Date offerStartDate = offer.getEffectiveStartDate();
    //Date offerEndDate = offer.getEffectiveEndDate();
    int stockReplanishDaysRange = Deployment.getStockReplanishDaysRange();
    Date filterStartDate = RLMDateUtils.addDays(now, -1*stockReplanishDaysRange, tz);
    Date filterEndDate = RLMDateUtils.addDays(now, stockReplanishDaysRange, tz);
    
    JourneyScheduler stockScheduler = offer.getStockScheduler();
    
    String scheduling = stockScheduler.getRunEveryUnit().toLowerCase();
    Integer scheduligInterval = stockScheduler.getRunEveryDuration();
    List<Date> tmpJourneyCreationDates = new ArrayList<Date>();
    if ("week".equalsIgnoreCase(scheduling))
      {
        Date lastDateOfThisWk = getLastDate(now, Calendar.DAY_OF_WEEK, offer.getTenantID());
        Date firstDateOfStartDateWk = getFirstDate(offerStartDate, Calendar.DAY_OF_WEEK, offer.getTenantID());
        Date lastDateOfStartDateWk = getLastDate(offerStartDate, Calendar.DAY_OF_WEEK, offer.getTenantID());
        while(lastDateOfThisWk.compareTo(lastDateOfStartDateWk) >= 0)
          {
            tmpJourneyCreationDates.addAll(getExpectedCreationDates(firstDateOfStartDateWk, lastDateOfStartDateWk, scheduling, stockScheduler.getRunEveryWeekDay(), offer.getTenantID()));
            offerStartDate = RLMDateUtils.addWeeks(offerStartDate, scheduligInterval, tz);
            lastDateOfStartDateWk = getLastDate(offerStartDate, Calendar.DAY_OF_WEEK, offer.getTenantID());
            firstDateOfStartDateWk = getFirstDate(offerStartDate, Calendar.DAY_OF_WEEK, offer.getTenantID());
          }
        
        //
        // handle the edge (if start day of next wk)
        //
        
        tmpJourneyCreationDates.addAll(getExpectedCreationDates(firstDateOfStartDateWk, lastDateOfStartDateWk, scheduling, stockScheduler.getRunEveryWeekDay(), offer.getTenantID()));
      } 
    else if ("month".equalsIgnoreCase(scheduling))
      {
        Date lastDateOfThisMonth = getLastDate(now, Calendar.DAY_OF_MONTH, offer.getTenantID());
        Date firstDateOfStartDateMonth = getFirstDate(offerStartDate, Calendar.DAY_OF_MONTH, offer.getTenantID());
        Date lastDateOfStartDateMonth = getLastDate(offerStartDate, Calendar.DAY_OF_MONTH, offer.getTenantID());
        while(lastDateOfThisMonth.compareTo(lastDateOfStartDateMonth) >= 0)
          {
            tmpJourneyCreationDates.addAll(getExpectedCreationDates(firstDateOfStartDateMonth, lastDateOfStartDateMonth, scheduling, stockScheduler.getRunEveryMonthDay(), offer.getTenantID()));
            offerStartDate = RLMDateUtils.addMonths(offerStartDate, scheduligInterval, tz);
            firstDateOfStartDateMonth = getFirstDate(offerStartDate, Calendar.DAY_OF_MONTH, offer.getTenantID());
            lastDateOfStartDateMonth = getLastDate(offerStartDate, Calendar.DAY_OF_MONTH, offer.getTenantID());
          }
        
        //
        // handle the edge (if 1st day of next month)
        //
        
        tmpJourneyCreationDates.addAll(getExpectedCreationDates(firstDateOfStartDateMonth, lastDateOfStartDateMonth, scheduling, stockScheduler.getRunEveryMonthDay(), offer.getTenantID()));
      }
    else if ("day".equalsIgnoreCase(scheduling))
      {
        Date lastDate = filterEndDate;
        while(lastDate.compareTo(offerStartDate) >= 0)
          {
            tmpJourneyCreationDates.add(new Date(offerStartDate.getTime()));
            offerStartDate = RLMDateUtils.addDays(offerStartDate, scheduligInterval, tz);
          }
      }
    else
      {
        if (log.isInfoEnabled()) log.info("invalid scheduling {}", scheduling);
      }
    
    //
    // filter out if before start date and recurrentCampaignCreationDaysRange (before / after)
    //

    //if(log.isInfoEnabled()) log.info("[PRJT] before filter tmpJourneyCreationDates {}", tmpJourneyCreationDates);
    tmpJourneyCreationDates = tmpJourneyCreationDates.stream().filter(date -> date.after(offer.getEffectiveStartDate()) && date.compareTo(filterStartDate) >= 0 && filterEndDate.compareTo(date) >= 0 ).collect(Collectors.toList());
    if(log.isInfoEnabled()) log.info("[PRJT] after filter tmpJourneyCreationDates {}", tmpJourneyCreationDates);

    //
    // return with format
    //
    return tmpJourneyCreationDates.stream().map(date -> formattedDate(date, datePattern)).collect(Collectors.toList());
  }
  
  private List<Date> getExpectedCreationDates(Date firstDate, Date lastDate, String scheduling, List<String> runEveryDay, int tenantID)
  {
    List<Date> result = new ArrayList<Date>();
    while (firstDate.before(lastDate) || firstDate.compareTo(lastDate) == 0)
      {
        int day = -1;
        switch (scheduling)
          {
            case "week":
              day = RLMDateUtils.getField(firstDate, Calendar.DAY_OF_WEEK, Deployment.getDeployment(tenantID).getTimeZone());
              break;

            case "month":
              day = RLMDateUtils.getField(firstDate, Calendar.DAY_OF_MONTH, Deployment.getDeployment(tenantID).getTimeZone());
              break;

            default:
              break;
        }
        String dayOf = String.valueOf(day);
        if (runEveryDay.contains(dayOf)) result.add(new Date(firstDate.getTime()));
        firstDate = RLMDateUtils.addDays(firstDate, 1, Deployment.getDeployment(tenantID).getTimeZone());
      }

    //
    //  handle last date of month
    //

    if ("month".equalsIgnoreCase(scheduling))
      {
        int lastDayOfMonth = RLMDateUtils.getField(lastDate, Calendar.DAY_OF_MONTH, Deployment.getDeployment(tenantID).getTimeZone());
        for (String day : runEveryDay)
          {
            if (Integer.parseInt(day) > lastDayOfMonth) result.add(new Date(lastDate.getTime()));
          }
      }
    
    log.info("[PRJT] getExpectedCreationDates(): {}", result);
    return result;
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
      List<String> receipientList = new ArrayList<String>();
      String subject = null;
      String body = null;
      Map<String, Object> communicationMap = new HashMap<String, Object>();
      if (guiManagedObject instanceof Offer)
        {
          Offer offer = (Offer) guiManagedObject;
          if (!offer.getNotificationEmails().isEmpty()) receipientList.addAll(offer.getNotificationEmails());
          Object[] bodyTags = {"offer", offer.getGUIManagedObjectDisplay(), remainingStock};
          Object[] subjectTags = {"offer", offer.getGUIManagedObjectDisplay()};
          subject = resolveTags(Deployment.getStockAlertEmailSubject(), subjectTags);
          body = resolveTags(Deployment.getStockAlertEmailBody(), bodyTags);
          
        }
      else if (guiManagedObject instanceof Product)
        {
          Product product = (Product) guiManagedObject;
          if (!product.getNotificationEmails().isEmpty()) receipientList.addAll(product.getNotificationEmails());
          Object[] bodyTags = {"product", product.getGUIManagedObjectDisplay(), remainingStock};
          Object[] subjectTags = {"product", product.getGUIManagedObjectDisplay()};
          subject = resolveTags(Deployment.getStockAlertEmailSubject(), subjectTags);
          body = resolveTags(Deployment.getStockAlertEmailBody(), bodyTags);
        }
      else if (guiManagedObject instanceof Voucher)
        {
          Voucher voucher = (Voucher) guiManagedObject;
          if (!voucher.getNotificationEmails().isEmpty()) receipientList.addAll(voucher.getNotificationEmails());
          Object[] bodyTags = {"voucher", voucher.getGUIManagedObjectDisplay(), remainingStock};
          Object[] subjectTags = {"voucher", voucher.getGUIManagedObjectDisplay()};
          subject = resolveTags(Deployment.getStockAlertEmailSubject(), subjectTags);
          body = resolveTags(Deployment.getStockAlertEmailBody(), bodyTags);
        }
      communicationMap.put("UserId", "");
      communicationMap.put("From", fwkEmailSMTPUserName);
      communicationMap.put("To", JSONUtilities.encodeArray(receipientList));
      communicationMap.put("Cc", "");
      communicationMap.put("Subject", subject);
      communicationMap.put("Body", body);
      communicationMap.put("ObjectShortDescription", "");
      communicationMap.put("IsBodyHtml", true);
      communicationMap.put("AreRecepientsApprovalManagers", false);
      communicationMap.put("AreRecepientsAllUsersWithPermission", false);
      communicationMap.put("PermissionKeyWhichRecipientsMustHave", "");
      communicationMap.put("AreMacrosAvailable", true);
      communicationMap.put("Macros", "");
      communicationMap.put("GenerateTokenForEachRecepeint", true);
      communicationMap.put("AreFirstTwoMacrosFromBodyTokens", true);
      communicationMap.put("SendSeparateEmailForEachRecipient", true);
      communicationMap.put("ApplicationKey", "");
      communicationMap.put("ObjectId", "");
      communicationMap.put("CallBackURL", "");
      String payload = JSONUtilities.encodeObject(communicationMap).toJSONString();
      log.debug("sendNotification - FWK API Call payload {}", payload);
      
      //
      // create request
      //

      StringEntity stringEntity = new StringEntity(payload, ContentType.create("application/json"));
      HttpPost httpPost = new HttpPost("http://" + fwkServer + "/fwkapi/api/communication/email");
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
          log.info("FWK communication raw response : {}", jsonResponse);

          //
          // parse JSON response from FWK
          //

          JSONObject jsonRoot = (JSONObject) (new JSONParser()).parse(jsonResponse);
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
  
  }

  private String resolveTags(final String unformattedText, Object[] tagArgs)
  {
    MessageFormat form = new MessageFormat(unformattedText);
    System.out.println(form.format(tagArgs));
    return form.format(tagArgs);
  }
  
  //
  //  getFirstDate
  //

  private Date getFirstDate(Date now, int dayOf, int tenantID)
  {
    if (Calendar.DAY_OF_WEEK == dayOf)
      {
        Date firstDateOfNext = RLMDateUtils.ceiling(now, dayOf, Deployment.getDeployment(tenantID).getTimeZone());
        return RLMDateUtils.addDays(firstDateOfNext, -7, Deployment.getDeployment(tenantID).getTimeZone());
      }
    else
      {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone(Deployment.getDeployment(tenantID).getTimeZone()));
        c.setTime(now);
        int dayOfMonth = RLMDateUtils.getField(now, Calendar.DAY_OF_MONTH, Deployment.getDeployment(tenantID).getTimeZone());
        Date firstDate = RLMDateUtils.addDays(now, -dayOfMonth+1, Deployment.getDeployment(tenantID).getTimeZone());
        return firstDate;
      }
  }

  //
  //  getLastDate
  //

  private Date getLastDate(Date now, int dayOf, int tenantID)
  {
    Date firstDateOfNext = RLMDateUtils.ceiling(now, dayOf, Deployment.getDeployment(tenantID).getTimeZone());
    if (Calendar.DAY_OF_WEEK == dayOf)
      {
        Date firstDateOfthisWk = RLMDateUtils.addDays(firstDateOfNext, -7, Deployment.getDeployment(tenantID).getTimeZone());
        return RLMDateUtils.addDays(firstDateOfthisWk, 6, Deployment.getDeployment(tenantID).getTimeZone());
      }
    else
      {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone(Deployment.getDeployment(tenantID).getTimeZone()));
        c.setTime(now);
        int toalNoOfDays = c.getActualMaximum(Calendar.DAY_OF_MONTH);
        int dayOfMonth = RLMDateUtils.getField(now, Calendar.DAY_OF_MONTH, Deployment.getDeployment(tenantID).getTimeZone());
        Date firstDate = RLMDateUtils.addDays(now, -dayOfMonth+1, Deployment.getDeployment(tenantID).getTimeZone());
        Date lastDate = RLMDateUtils.addDays(firstDate, toalNoOfDays-1, Deployment.getDeployment(tenantID).getTimeZone());
        return lastDate;
      }
  }
}
