package com.evolving.nglm.evolution.offeroptimizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.FileSourceConnector;
import com.evolving.nglm.core.FileSourceTask;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm;
import com.evolving.nglm.evolution.OfferOptimizationAlgorithm.OfferOptimizationAlgorithmParameter;

public class ImportedOffersFileSource extends FileSourceConnector 
{
  private static final Logger log = LoggerFactory.getLogger(ImportedOffersFileSource.class);
  
  @Override
  public Class<? extends Task> taskClass()
  {
    return ImportedOffersFileSourceTask.class;
  }
  
  public static class ImportedOffersFileSourceTask extends FileSourceTask 
  {
    @Override
    protected List<KeyValue> processRecord(String record) throws FileSourceTaskException, InterruptedException
    {
      record = record.replaceAll("\r\n", "\n"); // DOS2UNIX
      String[] tokens = record.split("[;]", -1);

      try
      {
        String currentFileName = getCurrentFilename();
        
        String importedTypeID = null;
        for (OfferOptimizationAlgorithm offerOptimizationAlgorithm : Deployment.getOfferOptimizationAlgorithms().values())
          {
            if(offerOptimizationAlgorithm.getID().startsWith("imported"))
            {
              JSONObject offerOptimizationAlgorithmJSON = offerOptimizationAlgorithm.getJSONRepresentation();
              String fileTemplate = JSONUtilities.decodeString(offerOptimizationAlgorithmJSON, "fileTemplate");
              if (currentFileName.startsWith(fileTemplate))
                {
                  importedTypeID = offerOptimizationAlgorithm.getID();
                  break;
                }
            }
          }
        if (importedTypeID == null)
          {
            log.error("[ImportedOffersFileSourceTask-{}] importedTypeID not found, invalid fileTemplate - {}", getTaskNumber(), currentFileName);
            return Collections.<KeyValue>emptyList();
          }
        
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
        
        //
        // imported offers list
        //
        
        List<String> offerIDs = new ArrayList<>();
        for (int i=1; i< tokens.length; i++)
          {
            offerIDs.add(tokens[i]);
          }
        
        if (!getStopRequested() && importedTypeID != null && subscriberID != null)
          {
            ImportedOffersScoring imported = new ImportedOffersScoring(importedTypeID, subscriberID, offerIDs);
            return Collections.<KeyValue>singletonList(new KeyValue(Deployment.getImportedOffersDNBOTopic(), Schema.STRING_SCHEMA, subscriberID, ImportedOffersScoring.schema(), ImportedOffersScoring.pack(imported)));
          }
      }
      catch (FileSourceTaskException | RuntimeException e)
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
