package com.evolving.nglm.evolution.datacubes.odr;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite.ParsedBucket;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;

import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.PaymentMeanService;
import com.evolving.nglm.evolution.SalesChannelService;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.DeliverablesMap;
import com.evolving.nglm.evolution.datacubes.mapping.JourneysMap;
import com.evolving.nglm.evolution.datacubes.mapping.LoyaltyProgramsMap;
import com.evolving.nglm.evolution.datacubes.mapping.ModulesMap;
import com.evolving.nglm.evolution.datacubes.mapping.OffersMap;
import com.evolving.nglm.evolution.datacubes.mapping.PaymentMeansMap;
import com.evolving.nglm.evolution.datacubes.mapping.SalesChannelsMap;

public class ODRDatacubeGenerator extends DatacubeGenerator
{
  private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
  private static final String DATACUBE_ES_INDEX = "datacube_odr";
  private static final String DATA_ES_INDEX_PREFIX = "detailedrecords_offers-";
  private static final String DATA_TOTAL_AMOUNT = "totalAmount";

  private List<String> filterFields;
  private List<AggregationBuilder> dataAggregations;
  private OffersMap offersMap;
  private ModulesMap modulesMap;
  private SalesChannelsMap salesChannelsMap;
  private PaymentMeansMap paymentMeansMap;
  private LoyaltyProgramsMap loyaltyProgramsMap;
  private DeliverablesMap deliverablesMap;
  private JourneysMap journeysMap;
  
  private String targetDate;
  
  public ODRDatacubeGenerator(String datacubeName, RestHighLevelClient elasticsearch, OfferService offerService, SalesChannelService salesChannelService, PaymentMeanService paymentMeanService, LoyaltyProgramService loyaltyProgramService, JourneyService journeyService)  
  {
    super(datacubeName, elasticsearch);

    this.offersMap = new OffersMap(offerService);
    this.modulesMap = new ModulesMap();
    this.salesChannelsMap = new SalesChannelsMap(salesChannelService);
    this.paymentMeansMap = new PaymentMeansMap(paymentMeanService);
    this.loyaltyProgramsMap = new LoyaltyProgramsMap(loyaltyProgramService);
    this.deliverablesMap = new DeliverablesMap();
    this.journeysMap = new JourneysMap(journeyService);
    
    //
    // Filter fields
    //
    
    this.filterFields = new ArrayList<String>();
    this.filterFields.add("offerID");
    this.filterFields.add("moduleID");
    this.filterFields.add("featureID");
    this.filterFields.add("salesChannelID");
    this.filterFields.add("meanOfPayment");
    
    //
    // Data Aggregations
    // - totalAmount
    //
    
    this.dataAggregations = new ArrayList<AggregationBuilder>();
    
    AggregationBuilder totalAmount = AggregationBuilders.sum(DATA_TOTAL_AMOUNT)
            .script(new Script(ScriptType.INLINE, "painless", "doc['offerPrice'].value * doc['offerQty'].value", Collections.emptyMap()));
    dataAggregations.add(totalAmount);
  }

  @Override protected String getDatacubeESIndex() { return DATACUBE_ES_INDEX; }
  @Override protected String getDataESIndex() { return (DATA_ES_INDEX_PREFIX+targetDate); }
  @Override protected List<String> getFilterFields() { return filterFields; }
  @Override protected List<CompositeValuesSourceBuilder<?>> getFilterComplexSources() { return Collections.emptyList(); }
  @Override protected List<AggregationBuilder> getDataAggregations() { return this.dataAggregations; }
    
  @Override
  protected boolean runPreGenerationPhase() throws ElasticsearchException, IOException, ClassCastException
  {
    offersMap.update();
    modulesMap.updateFromElasticsearch(elasticsearch);
    salesChannelsMap.update();
    paymentMeansMap.update();
    loyaltyProgramsMap.update();
    deliverablesMap.updateFromElasticsearch(elasticsearch);
    journeysMap.update();
    
    return true;
  }

  @Override
  protected void addStaticFilters(Map<String, Object> filters)
  {
    filters.put("dataDate", targetDate);
  }

  @Override
  protected void embellishFilters(Map<String, Object> filters)
  {
    String offerID = (String) filters.remove("offerID");
    filters.put("offer.id", offerID);
    filters.put("offer.display", offersMap.getDisplay(offerID, "offer"));

    String moduleID = (String) filters.remove("moduleID");
    filters.put("module.id", moduleID);
    filters.put("module.display", modulesMap.getDisplay(moduleID, "module"));

    String featureID = (String) filters.remove("featureID");
    filters.put("feature.id", featureID);
    
    String featureDisplay = featureID; // default
    switch(modulesMap.getFeature(moduleID, "feature"))
      {
        case JourneyID:
          featureDisplay = journeysMap.getDisplay(featureID, "feature");
          break;
        case LoyaltyProgramID:
          featureDisplay = loyaltyProgramsMap.getDisplay(featureID, "feature");
          break;
        case OfferID:
          featureDisplay = offersMap.getDisplay(featureID, "feature");
          break;
        case DeliverableID:
          featureDisplay = deliverablesMap.getDisplay(featureID, "feature");
          break;
        default:
          // For None feature we let the featureID as a display (it is usually a string)
          break;
      }
    filters.put("feature.display", featureDisplay);

    String salesChannelID = (String) filters.remove("salesChannelID");
    filters.put("salesChannel.id", salesChannelID);
    filters.put("salesChannel.display", salesChannelsMap.getDisplay(salesChannelID, "salesChannel"));

    String meanOfPayment = (String) filters.remove("meanOfPayment");
    filters.put("meanOfPayment.id", meanOfPayment);
    filters.put("meanOfPayment.display", paymentMeansMap.getDisplay(meanOfPayment, "meanOfPayment"));
    filters.put("meanOfPayment.paymentProviderID", paymentMeansMap.getProviderID(meanOfPayment, "meanOfPayment"));
  }
  
  @Override
  protected Map<String, Object> extractData(ParsedBucket compositeBucket, Map<String, Object> contextFilters) throws ClassCastException
  {
    HashMap<String, Object> data = new HashMap<String,Object>();
    if (compositeBucket.getAggregations() == null) {
      log.error("Unable to extract data, aggregation is missing.");
      return data;
    }
    
    ParsedSum dataTotalAmountBucket = compositeBucket.getAggregations().get(DATA_TOTAL_AMOUNT);
    if (dataTotalAmountBucket == null) {
      log.error("Unable to extract totalAmount data, aggregation is missing.");
      return data;
    }
    data.put(DATA_TOTAL_AMOUNT, (int) dataTotalAmountBucket.getValue());
    
    return data;
  }

  /*****************************************
  *
  *  run
  *
  *****************************************/
  
  public void run(Date targetDate)
  {
    this.targetDate = DATE_FORMAT.format(targetDate);
    this.run();
  }
}
