package com.evolving.nglm.evolution.offeroptimizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.FileSourceConnector;
import com.evolving.nglm.core.FileSourceTask;

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
        
        ImportedOffersScoring imported = new ImportedOffersScoring("imported-1", subscriberID, offerIDs);
        if (!getStopRequested())
          {
            return Collections.<KeyValue>singletonList(new KeyValue("importedoffersdnbo", Schema.STRING_SCHEMA, subscriberID, ImportedOffersScoring.schema(), ImportedOffersScoring.pack(imported)));
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
