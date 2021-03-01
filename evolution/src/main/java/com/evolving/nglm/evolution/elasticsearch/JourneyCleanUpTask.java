package com.evolving.nglm.evolution.elasticsearch;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.JourneyStatisticESSinkConnector;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;

public class JourneyCleanUpTask
{
  protected static final Logger log = LoggerFactory.getLogger(JourneyCleanUpTask.class);
  
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private JourneyService journeyService;
  private ElasticsearchClientAPI elasticsearchClient;
  private int tenantID;
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public JourneyCleanUpTask(JourneyService journeyService, ElasticsearchClientAPI elasticsearchClient, int tenantID) 
  {
    this.journeyService = journeyService;
    this.elasticsearchClient = elasticsearchClient;
    this.tenantID = tenantID;
  }

  /*****************************************
  *
  * Start
  *
  *****************************************/
  public void start() 
  {
    Date now = SystemTime.getCurrentTime();
    // Here days are converted into hours. Therefore we do not take into account timezone. A retention of 15 days means 360 hours.
    Date journeyExpirationDate = RLMDateUtils.addHours(now, -24 * Deployment.getDeployment(this.tenantID).getElasticsearchRetentionDaysJourneys());
    Date campaignExpirationDate = RLMDateUtils.addHours(now, -24 * Deployment.getDeployment(this.tenantID).getElasticsearchRetentionDaysCampaigns());
    Date bulkCampaignExpirationDate = RLMDateUtils.addHours(now, -24 * Deployment.getDeployment(this.tenantID).getElasticsearchRetentionDaysBulkCampaigns());
    
    // Init list of indices to check 
    Set<String> lowerCaseIDs = getESLowerCaseJourneyIDs();
    // Init lower case matching map
    Map<String, String> getJourneyID = getLowerCaseMatchingIDs();
    
    for(String lowerCaseJourneyID: lowerCaseIDs) {
      String journeyID = getJourneyID.get(lowerCaseJourneyID);
      
      if(journeyID != null) { // if journeyID can't be found, it will be deleted
        GUIManagedObject object = journeyService.getStoredJourney(journeyID, true);
        
        if(object.getGUIManagedObjectType() == GUIManagedObjectType.Journey) {
          if(journeyExpirationDate.before(object.getEffectiveEndDate())) {
            continue; // Will not be deleted today
          }
        } 
        else if(object.getGUIManagedObjectType() == GUIManagedObjectType.Campaign) {
          if(campaignExpirationDate.before(object.getEffectiveEndDate())) {
            continue; // Will not be deleted today
          }
        } 
        else if(object.getGUIManagedObjectType() == GUIManagedObjectType.BulkCampaign) {
          if(bulkCampaignExpirationDate.before(object.getEffectiveEndDate())) {
            continue; // Will not be deleted today
          }
        } 
        else {
          // Workflow?, Incomplete?, Others? ... treated as a Journey for the expiration date.
          if(journeyExpirationDate.before(object.getEffectiveEndDate())) {
            continue; // Will not be deleted today
          }
        }
      }
      
      // Indices deletion
      removeAllRelatedIndices(lowerCaseJourneyID);
    }
  }

  /*****************************************
  *
  * Utils
  *
  *****************************************/
  /**
   * Build the map of <lowerCaseJourneyID, journeyID> of all active journeys 
   * in the system. The goal is to be able to retrieve the "real" journeyID 
   * from the lowerCase one extracted from Elasticsearch.
   * 
   * We do not retrieve archived journeys. Therefore, if a journey has been
   * deleted, because it will not be in this map, this will automatically call
   * a clean-up on all related ES indices.
   */
  private Map<String, String> getLowerCaseMatchingIDs()
  {
    Map<String, String> result = new HashMap<String, String>();
    
    for(GUIManagedObject object : this.journeyService.getStoredJourneys(false, 0)) { // TODO EVPRO-99 check
      result.put(JourneyStatisticESSinkConnector.journeyIDFormatterForESIndex(object.getGUIManagedObjectID()), object.getGUIManagedObjectID());
    }
    
    return result;
  }
  
  /**
   * Retrieve journeyIDs from current active Elasticsearch indexes: 
   * - journeystatistic-{journeyID}
   * - datacube_journeytraffic-{journeyID}
   * - datacube_journeyrewards-{journeyID} 
   * 
   * Those are all journeyIDs that must be checked for clean up.
   * 
   * @return Set of journeyIDs (set in order to avoid duplicates).
   */
  private Set<String> getESLowerCaseJourneyIDs() 
  {
    Set<String> lowerCaseIDs = new HashSet<String>();

    Pattern journeystatisticPattern = Pattern.compile("journeystatistic-(.*)");
    Pattern journeytrafficPattern = Pattern.compile("datacube_journeytraffic-(.*)");
    Pattern journeyrewardsPattern = Pattern.compile("datacube_journeyrewards-(.*)");
    //
    // Check for journeystatistic indexes
    //
    Set<String> journeystatisticIndices = getAllIndices("journeystatistic-*");
    for(String index : journeystatisticIndices) {
      Matcher matcher = journeystatisticPattern.matcher(index);
      if(matcher.matches()) {
        lowerCaseIDs.add(matcher.group(1));
      }
    }
    
    //
    // Check for datacube_journeytraffic indexes
    //
    Set<String> journeytrafficIndices = getAllIndices("datacube_journeytraffic-*");
    for(String index : journeytrafficIndices) {
      Matcher matcher = journeytrafficPattern.matcher(index);
      if(matcher.matches()) {
        lowerCaseIDs.add(matcher.group(1));
      }
    }
      
    //
    // Check for datacube_journeyrewards indexes
    //
    Set<String> journeyrewardsIndices = getAllIndices("datacube_journeyrewards-*");
    for(String index : journeyrewardsIndices) {
      Matcher matcher = journeyrewardsPattern.matcher(index);
      if(matcher.matches()) {
        lowerCaseIDs.add(matcher.group(1));
      }
    }
    
    return lowerCaseIDs;
  }
  
  /**
   * Remove the following indices from Elasticsearch:
   * - journeystatistic-{journeyID}
   * - datacube_journeytraffic-{journeyID}
   * - datacube_journeyrewards-{journeyID} 
   */
  private void removeAllRelatedIndices(String lowerCaseJourneyID) 
  {
    removeIndex("journeystatistic-"+lowerCaseJourneyID);
    removeIndex("datacube_journeytraffic-"+lowerCaseJourneyID);
    removeIndex("datacube_journeyrewards-"+lowerCaseJourneyID);
  }

  /*****************************************
  *
  * Elasticsearch client
  *
  *****************************************/
  private void removeIndex(String indexName) 
  {
    try
    {
      AcknowledgedResponse journeystatisticDeletion = elasticsearchClient.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
      if(!journeystatisticDeletion.isAcknowledged()) {
        log.warn("Could not remove index: " + indexName);
      } 
      else {
        log.info("Successfully cleaned up index: " + indexName);
      }
    } 
    catch (ElasticsearchStatusException e) 
    {
      // NOT FOUND: it is ok if the index does not exist. Do not raise error.
      if(e.status() != RestStatus.NOT_FOUND) {
        log.error(e.getDetailedMessage());
      }
    }
    catch (ElasticsearchException e) 
    {
      log.error(e.getDetailedMessage());
    }
    catch (IOException e) 
    {
      log.error(e.getMessage());
    }
  }
  
  private Set<String> getAllIndices(String indexPattern) 
  {
    Set<String> result = new HashSet<String>();
    try 
    {
      GetIndexResponse journeystatisticIndices = elasticsearchClient.indices().get(new GetIndexRequest(indexPattern), RequestOptions.DEFAULT);
      for(String index : journeystatisticIndices.getIndices()) {
        result.add(index);
      }
    }
    catch (ElasticsearchStatusException e) 
    {
      // NOT FOUND: it is ok if the index does not exist. Do not raise error.
      if(e.status() != RestStatus.NOT_FOUND) {
        log.error(e.getDetailedMessage());
      }
    }
    catch (ElasticsearchException e) 
    {
      log.error(e.getDetailedMessage());
    }
    catch (IOException e) 
    {
      log.error(e.getMessage());
    }
    
    return result;
  }
}
