package com.evolving.nglm.evolution.datacubes;

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
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.json.simple.JSONObject;

public class ODRDatacubeGenerator extends DatacubeGenerator
{
  private List<String> filterFields;
  private List<CompositeValuesSourceBuilder<?>> filterComplexSources;
  
  private List<AggregationBuilder> dataAggregations;
  private String dataTotalAmount = "totalAmount";
  
  //
  // Elasticsearch indexes
  //
  
  private final String datacubeESIndex = "datacube_odr";
  private final String dataESIndexPrefix = "detailedrecords_offers-";
  
  public ODRDatacubeGenerator(String datacubeName) 
  {
    super(datacubeName);
    
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
  }

  @Override
  protected void embellishFilters(Map<String, Object> filters)
  {
    String offerID = (String) filters.remove("offerID");
    filters.put("offer.id", offerID);
    // TODO : extract offer.display 
    filters.put("offer.display", offerID);

    String moduleID = (String) filters.remove("moduleID");
    filters.put("module.id", moduleID);
    // TODO : extract module.display 
    filters.put("module.display", moduleID);

    String featureID = (String) filters.remove("featureID");
    filters.put("feature.id", featureID);
    // TODO : extract feature.display 
    filters.put("feature.display", featureID);

    String salesChannelID = (String) filters.remove("salesChannelID");
    filters.put("salesChannel.id", salesChannelID);
    // TODO : extract salesChannel.display 
    filters.put("salesChannel.display", salesChannelID);

    String meanOfPayment = (String) filters.remove("meanOfPayment");
    filters.put("meanOfPayment.id", meanOfPayment);
    // TODO : extract meanOfPayment.display 
    filters.put("meanOfPayment.display", meanOfPayment);
    // TODO : extract meanOfPayment.paymentProviderID (default ATM)
    filters.put("meanOfPayment.paymentProviderID", "provider_Point");
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
    return (this.dataESIndexPrefix+date);
  }

  @Override
  protected String getDatacubeESIndex()
  {
    return this.datacubeESIndex;
  }
}
