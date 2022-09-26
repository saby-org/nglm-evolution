package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.Date;

import org.json.simple.JSONObject;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class StockRecurrenceAndNotificationJob  extends ScheduledJob 
{
  private OfferService offerService;
  private ProductService productService;
  private VoucherService voucherService;
  private CallingChannelService callingChannelService;
  private CatalogCharacteristicService catalogCharacteristicService;
  private SalesChannelService salesChannelService;
  
  public StockRecurrenceAndNotificationJob(String jobName, String periodicGenerationCronEntry, String baseTimeZone, boolean scheduleAtStart, OfferService offerService, ProductService productService, VoucherService voucherService, CallingChannelService callingChannelService, CatalogCharacteristicService catalogCharacteristicService, SalesChannelService salesChannelService)
  {
    super(jobName, periodicGenerationCronEntry, baseTimeZone, scheduleAtStart); 
    this.offerService = offerService;
    this.productService = productService;
    this.voucherService = voucherService;
    this.callingChannelService = callingChannelService;
    this.catalogCharacteristicService = catalogCharacteristicService;
    this.salesChannelService = salesChannelService;
  }

  @Override
  protected void run()
  {
    Date now = SystemTime.getCurrentTime();
    Collection<Offer> activeOffers = offerService.getActiveOffers(now, 0);
    for (Offer offer : activeOffers)
      {
        boolean stockThersoldBreached = offer.getApproximateRemainingStock() != null && (offer.getApproximateRemainingStock() <= offer.getStockAlertThreshold());
        if (stockThersoldBreached)
          {
            //
            //  send notification
            //
            
            if (offer.getStockAlert())
              {
                log.info("RAJ K ready to send alert notification for offer {}", offer.getGUIManagedObjectDisplay());
                // send stock notification RAJ K (EVPRO-1601)
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
        boolean stockThersoldBreached = product.getApproximateRemainingStock() != null && (product.getApproximateRemainingStock() <= product.getStockAlertThreshold());
        if (stockThersoldBreached)
          {
            //
            //  send notification
            //
            
            if (product.getStockAlert())
              {
                log.info("RAJ K ready to send alert notification for product {}", product.getGUIManagedObjectDisplay());
                // send stock notification RAJ K (EVPRO-1601)
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
                    log.info("RAJ K ready to send alert notification for voucherShared {}", voucher.getGUIManagedObjectDisplay());
                    // send stock notification RAJ K (EVPRO-1601)
                  }
              }
          }
        
      }
  }
}
