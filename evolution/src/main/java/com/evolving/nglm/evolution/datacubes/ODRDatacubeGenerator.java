package com.evolving.nglm.evolution.datacubes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite.ParsedBucket;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.json.simple.JSONObject;

public class ODRDatacubeGenerator extends DatacubeGenerator
{
  private List<String> filterFields;
  private String dataPriceMeansOfPayment = "MeanOfPayment";
  private String dataPriceTotalAmount = "TotalAmount";
  
  //
  // Elasticsearch indexes
  //
  
  private final String datacubeESIndex = "datacube_odr";
  private final String dataESIndexPrefix = "detailedrecords_offers-";
  
  public ODRDatacubeGenerator() 
  {
    super("ODR-datacube");
    
    //
    // Filter fields
    //
    
    this.filterFields = new ArrayList<String>();
    this.filterFields.add("offerID");
    this.filterFields.add("moduleID");
    this.filterFields.add("featureID");
    this.filterFields.add("salesChannelID");
  }

  @Override
  protected List<String> getFilterFields()
  {
    return filterFields;
  }

  @Override
  protected List<AggregationBuilder> getDataAggregations()
  {
    // TODO : This could be done in constructor, it does not change with the date argument
    List<AggregationBuilder> datacubeDataAggregations = new ArrayList<AggregationBuilder>();

    // 
    // Price (MeanOfPayment, TotalAmount)
    //

    AggregationBuilder dataPrice = AggregationBuilders.terms(dataPriceMeansOfPayment)
        .field("meanOfPayment")
        .subAggregation(AggregationBuilders.sum(dataPriceTotalAmount)
            .script(new Script(ScriptType.INLINE, "painless", "doc['offerPrice'].value * doc['offerQty'].value", Collections.emptyMap()))
            );
    datacubeDataAggregations.add(dataPrice);
    
    return datacubeDataAggregations;
  }

  @Override
  protected JSONObject extractData(ParsedBucket compositeBucket) throws ClassCastException
  {
    JSONObject data = new JSONObject();
    if (compositeBucket.getAggregations() == null) {
      log.error("Unable to extract price data, aggregation is missing.");
      return data;
    }
    ParsedStringTerms dataPriceBuckets = compositeBucket.getAggregations().get(dataPriceMeansOfPayment);
    List<JSONObject> dataPrice = new ArrayList<JSONObject>();
    
    for(Terms.Bucket dataPriceBucket: dataPriceBuckets.getBuckets()) {
      if (dataPriceBucket.getAggregations() == null) {
        log.error("Unable to extract price data, one bucket is empty.");
        continue;
      }
      ParsedSum dataPriceBucketAmount = dataPriceBucket.getAggregations().get(dataPriceTotalAmount);
      JSONObject dataPriceItem = new JSONObject();
      dataPriceItem.put("amountTotal", (int) dataPriceBucketAmount.getValue());
      dataPriceItem.put("meanOfPaymentID", dataPriceBucket.getKey());
      dataPrice.add(dataPriceItem);
    }
    data.put("price", dataPrice);
    
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
    return this.datacubeESIndex;
  }
}
