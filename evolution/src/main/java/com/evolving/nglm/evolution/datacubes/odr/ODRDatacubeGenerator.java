package com.evolving.nglm.evolution.datacubes.odr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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

import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.DeliverableDisplayMapping;
import com.evolving.nglm.evolution.datacubes.mapping.JourneyDisplayMapping;
import com.evolving.nglm.evolution.datacubes.mapping.LoyaltyProgramDisplayMapping;
import com.evolving.nglm.evolution.datacubes.mapping.MeansOfPaymentDisplayMapping;
import com.evolving.nglm.evolution.datacubes.mapping.ModuleDisplayMapping;
import com.evolving.nglm.evolution.datacubes.mapping.OfferDisplayMapping;
import com.evolving.nglm.evolution.datacubes.mapping.SalesChannelDisplayMapping;

public class ODRDatacubeGenerator extends DatacubeGenerator
{
  private static final String dataTotalAmount = "totalAmount";
  private static final String datacubeESIndex = "datacube_odr";
  private static final String dataESIndexPrefix = "detailedrecords_offers-";
  
  private List<String> filterFields;
  private List<CompositeValuesSourceBuilder<?>> filterComplexSources;
  private List<AggregationBuilder> dataAggregations;
  private OfferDisplayMapping offerDisplayMapping;
  private ModuleDisplayMapping moduleDisplayMapping;
  private SalesChannelDisplayMapping salesChannelDisplayMapping;
  private MeansOfPaymentDisplayMapping meansOfPaymentDisplayMapping;
  private LoyaltyProgramDisplayMapping loyaltyProgramDisplayMapping;
  private DeliverableDisplayMapping deliverableDisplayMapping;
  private JourneyDisplayMapping journeyDisplayMapping;
  
  public ODRDatacubeGenerator(String datacubeName, RestHighLevelClient elasticsearch)  
  {
    super(datacubeName, elasticsearch);

    this.offerDisplayMapping = new OfferDisplayMapping();
    this.moduleDisplayMapping = new ModuleDisplayMapping();
    this.salesChannelDisplayMapping = new SalesChannelDisplayMapping();
    this.meansOfPaymentDisplayMapping = new MeansOfPaymentDisplayMapping();
    this.loyaltyProgramDisplayMapping = new LoyaltyProgramDisplayMapping();
    this.deliverableDisplayMapping = new DeliverableDisplayMapping();
    this.journeyDisplayMapping = new JourneyDisplayMapping();
    
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
    // Filter Complex Sources
    // - nothing ...
    //
    
    this.filterComplexSources = new ArrayList<CompositeValuesSourceBuilder<?>>();
    
    //
    // Data Aggregations
    // - totalAmount
    //
    
    this.dataAggregations = new ArrayList<AggregationBuilder>();
    
    AggregationBuilder totalAmount = AggregationBuilders.sum(dataTotalAmount)
            .script(new Script(ScriptType.INLINE, "painless", "doc['offerPrice'].value * doc['offerQty'].value", Collections.emptyMap()));
    dataAggregations.add(totalAmount);
  }
    
  @Override
  protected void runPreGenerationPhase(RestHighLevelClient elasticsearch) throws ElasticsearchException, IOException, ClassCastException
  {
    offerDisplayMapping.updateFromElasticsearch(elasticsearch);
    moduleDisplayMapping.updateFromElasticsearch(elasticsearch);
    salesChannelDisplayMapping.updateFromElasticsearch(elasticsearch);
    meansOfPaymentDisplayMapping.updateFromElasticsearch(elasticsearch);
    loyaltyProgramDisplayMapping.updateFromElasticsearch(elasticsearch);
    deliverableDisplayMapping.updateFromElasticsearch(elasticsearch);
    journeyDisplayMapping.updateFromElasticsearch(elasticsearch);
  }

  @Override
  protected void embellishFilters(Map<String, Object> filters)
  {
    String offerID = (String) filters.remove("offerID");
    filters.put("offer.id", offerID);
    filters.put("offer.display", offerDisplayMapping.getDisplay(offerID));

    String moduleID = (String) filters.remove("moduleID");
    filters.put("module.id", moduleID);
    filters.put("module.display", moduleDisplayMapping.getDisplay(moduleID));

    String featureID = (String) filters.remove("featureID");
    filters.put("feature.id", featureID);
    
    String featureDisplay = featureID; // default
    switch(moduleDisplayMapping.getFeature(moduleID))
      {
        case JourneyID:
          featureDisplay = journeyDisplayMapping.getDisplay(featureID);
          break;
        case LoyaltyProgramID:
          featureDisplay = loyaltyProgramDisplayMapping.getDisplay(featureID);
          break;
        case OfferID:
          featureDisplay = offerDisplayMapping.getDisplay(featureID);
          break;
        case DeliverableID:
          featureDisplay = deliverableDisplayMapping.getDisplay(featureID);
          break;
        default:
          // For None feature we let the featureID as a display (it is usually a string)
          break;
      }
    filters.put("feature.display", featureDisplay);

    String salesChannelID = (String) filters.remove("salesChannelID");
    filters.put("salesChannel.id", salesChannelID);
    filters.put("salesChannel.display", salesChannelDisplayMapping.getDisplay(salesChannelID));

    String meanOfPayment = (String) filters.remove("meanOfPayment");
    filters.put("meanOfPayment.id", meanOfPayment);
    filters.put("meanOfPayment.display", meansOfPaymentDisplayMapping.getDisplay(meanOfPayment));
    filters.put("meanOfPayment.paymentProviderID", meansOfPaymentDisplayMapping.getProviderID(meanOfPayment));
  }

  @Override
  protected List<String> getFilterFields()
  {
    return filterFields;
  }

  @Override
  protected List<CompositeValuesSourceBuilder<?>> getFilterComplexSources(String date)
  {
    return this.filterComplexSources;
  }

  @Override
  protected List<AggregationBuilder> getDataAggregations(String date)
  {
    return this.dataAggregations;
  }

  @Override
  protected Map<String, Object> extractData(ParsedBucket compositeBucket, Map<String, Object> contextFilters) throws ClassCastException
  {
    HashMap<String, Object> data = new HashMap<String,Object>();
    if (compositeBucket.getAggregations() == null) {
      log.error("Unable to extract data, aggregation is missing.");
      return data;
    }
    
    ParsedSum dataTotalAmountBucket = compositeBucket.getAggregations().get(dataTotalAmount);
    if (dataTotalAmountBucket == null) {
      log.error("Unable to extract totalAmount data, aggregation is missing.");
      return data;
    }
    data.put(dataTotalAmount, (int) dataTotalAmountBucket.getValue());
    
    return data;
  }

  @Override
  protected String getDataESIndex(String date)
  {
    return (dataESIndexPrefix+date);
  }

  @Override
  protected String getDatacubeESIndex()
  {
    return datacubeESIndex;
  }
}
