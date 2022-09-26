package com.evolving.nglm.evolution.offeroptimizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.FileSourceConnector;
import com.evolving.nglm.core.FileSourceTask;
import com.evolving.nglm.evolution.SubscriberProfile;
import com.evolving.nglm.evolution.SubscriberProfileService;
import com.evolving.nglm.evolution.SubscriberProfileService.EngineSubscriberProfileService;
import com.evolving.nglm.evolution.SubscriberProfileService.SubscriberProfileServiceException;

public class ImportedOffersFileSource extends FileSourceConnector 
{
  private static final Logger log = LoggerFactory.getLogger(ImportedOffersFileSource.class);
  
  private static SubscriberProfileService subscriberProfileService;
  
  @Override
  public Class<? extends Task> taskClass()
  {
    return ImportedOffersFileSourceTask.class;
  }
  
  public static class ImportedOffersFileSourceTask extends FileSourceTask 
  {
    @Override
    public void start(Map<String, String> taskConfig)
    {
      super.start(taskConfig);
     
      //
      // subscriberProfileService
      //
      
      subscriberProfileService = new EngineSubscriberProfileService(com.evolving.nglm.core.Deployment.getSubscriberProfileEndpoints(), 1);
    }

    @Override
    protected List<KeyValue> processRecord(String record) throws FileSourceTaskException, InterruptedException
    {
      record = record.replaceAll("\r\n", "\n"); // DOS2UNIX
      String[] tokens = record.split("[;]", -1);

      try
      {
        //
        // ignore header
        //

        if (record.startsWith("MSISDN"))
          {
            return Collections.<KeyValue>emptyList();
          }
        
        //
        // msisdn
        //
        
        String msisdn = readString(tokens[0], record);
        if (msisdn == null)
          {
            log.debug("processRecord empty externalSubsID: {}", record);
            throw new FileSourceTaskException("empty externalSubsID");
          }
        String subscriberID = resolveSubscriberID("msisdn", msisdn);
        SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID);
        
        //
        // imported offers list
        //
        
        List<String> offerIDs = new ArrayList<>();
        for (int i=1; i< tokens.length; i++)
          {
            offerIDs.add(tokens[i]);
          }
        
        //
        // update profile directly
        //
        subscriberProfile.setImportedOffersDNBO("imported-1", offerIDs); // static imported id
        subscriberProfile.getImportedOffersDNBO().entrySet().forEach(entry -> { log.info("[PRJT] "+ entry.getKey() + " " + entry.getValue()); });
        
      }
      catch (FileSourceTaskException | RuntimeException | SubscriberProfileServiceException e)
      {
        log.error("Exception: {}", e.getMessage());
        return Collections.<KeyValue>emptyList();
      }
    
    //
    // return 
    //
    
    return Collections.<KeyValue>emptyList();
    }
    
  }

}
