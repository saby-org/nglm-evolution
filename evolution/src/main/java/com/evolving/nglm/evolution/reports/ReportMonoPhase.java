/*****************************************************************************
 *
 *  ReportMonoPhase.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import static com.evolving.nglm.evolution.reports.ReportUtils.d;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;

public class ReportMonoPhase
{
  private static final Logger log = LoggerFactory.getLogger(ReportMonoPhase.class);

  private String esNode;
  private LinkedHashMap<String, QueryBuilder> esIndex;
  private String elasticKey;
  private boolean onlyKeepAlternateIDs; // when we read subscriberprofile, only keep info about alternateIDs
  private boolean onlyKeepAlternateIDsExtended; // when we read subscriberprofile, only keep info about alternateIDs and a little more (for 2 specific reports)
  private ReportCsvFactory reportFactory;
  private String csvfile;
  private static RestHighLevelClient elasticsearchReaderClient;

  public ReportMonoPhase(String elasticKey, String esNode, LinkedHashMap<String, QueryBuilder> esIndex, ReportCsvFactory factory, String csvfile, boolean onlyKeepAlternateIDs, boolean onlyKeepAlternateIDsExtended)
  {
    this.elasticKey = elasticKey;
    this.esNode = esNode;
    this.onlyKeepAlternateIDs = onlyKeepAlternateIDs;
    this.onlyKeepAlternateIDsExtended = onlyKeepAlternateIDsExtended;
    // convert index names to lower case, because this is what ElasticSearch expects
    this.esIndex = new LinkedHashMap<>();
    for (Entry<String, QueryBuilder> elem : esIndex.entrySet())
      {
        this.esIndex.put(elem.getKey().toLowerCase(), elem.getValue());
      }
    log.info("Starting ES read with indexes : " + this.esIndex);
    this.reportFactory = factory;
    this.csvfile = csvfile;
    ReportsCommonCode.initializeDateFormats();

  }

  public ReportMonoPhase(String elasticKey, String esNode, LinkedHashMap<String, QueryBuilder> esIndex, ReportCsvFactory factory, String csvfile)
  {
    this(elasticKey, esNode, esIndex, factory, csvfile, false, false);
  }

  public ReportMonoPhase(String elasticKey, String esNode, LinkedHashMap<String, QueryBuilder> esIndex, ReportCsvFactory factory, String csvfile, boolean onlyKeepAlternateIDs)
  {
    this(elasticKey, esNode, esIndex, factory, csvfile, onlyKeepAlternateIDs, false);
  }

  public enum PERIOD
  {
    DAYS("DAYS"), WEEKS("WEEKS"), MONTHS("MONTHS"), UNKNOWN("UNKNOWN");

    private String externalRepresentation;

    private PERIOD(String externalRepresentation)
    {
      this.externalRepresentation = externalRepresentation;
    }

    public String getExternalRepresentation()
    {
      return externalRepresentation;
    }

    public static PERIOD fromExternalRepresentation(String externalRepresentation)
    {
      for (PERIOD enumeratedValue : PERIOD.values())
        {
          if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation))
            return enumeratedValue;
        }
      return UNKNOWN;
    }
  }

  public boolean startOneToOne()
  {

    String indexes = "";
    for (String s : esIndex.keySet())
      indexes += s + " ";
    log.info("Reading data from ES in \"" + indexes + "\" indexes");

    if (csvfile == null)
      {
        log.info("csvfile is null !");
        return false;
      }

    File file = new File(csvfile + ReportUtils.ZIP_EXTENSION);
    if (file.exists())
      {
        log.info(csvfile + " already exists, do nothing");
        return false;
      }
    FileOutputStream fos;
    ZipOutputStream writer;
    try
    {
      fos = new FileOutputStream(file);
      writer = new ZipOutputStream(fos);
      ZipEntry entry = new ZipEntry(new File(csvfile).getName());
      writer.putNextEntry(entry);

      int i = 0;
      boolean addHeader = true;

      for (Entry<String, QueryBuilder> index : esIndex.entrySet())
        {

          SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder().query(index.getValue()));

          // Read all docs from ES, on esIndex[i]
          // Write to topic, one message per document

          // ESROUTER can have two access points
          // need to cut the string to get at least one
          String node = null;
          int port = 0;
          if (esNode.contains(","))
            {
              String[] split = esNode.split(",");
              if (split[0] != null)
                {
                  Scanner s = new Scanner(split[0]);
                  s.useDelimiter(":");
                  node = s.next();
                  port = s.nextInt();
                  s.close();
                }
            } else
              {
                Scanner s = new Scanner(esNode);
                s.useDelimiter(":");
                node = s.next();
                port = s.nextInt();
                s.close();
              }

          elasticsearchReaderClient = new RestHighLevelClient(RestClient.builder(new HttpHost(node, port, "http")));
          // Search for everyone

          Scroll scroll = new Scroll(TimeValue.timeValueSeconds(10L));
          String[] indicesToRead = getIndices(index.getKey());

          //
          //  indicesToRead is blank?
          //

          if (indicesToRead == null || indicesToRead.length == 0)
            {
              i++;
              continue;
            }

          searchRequest.indices(indicesToRead);
          searchRequest.source().size(getScrollSize());
          searchRequest.scroll(scroll);
          SearchResponse searchResponse;
          searchResponse = elasticsearchReaderClient.search(searchRequest, RequestOptions.DEFAULT);

          String scrollId = searchResponse.getScrollId(); // always null
          SearchHit[] searchHits = searchResponse.getHits().getHits();
          log.trace("searchHits = " + Arrays.toString(searchHits));
          if (searchHits != null)
            {
              // log.info("searchResponse = " + searchResponse.toString());
              log.trace("getFailedShards = " + searchResponse.getFailedShards());
              log.trace("getSkippedShards = " + searchResponse.getSkippedShards());
              log.trace("getTotalShards = " + searchResponse.getTotalShards());
              log.trace("getTook = " + searchResponse.getTook());
              log.info("for " + Arrays.toString(indicesToRead) + " searchHits.length = " + searchHits.length + " totalHits = " + searchResponse.getHits().getTotalHits());
            }
          boolean alreadyTraced1 = false;
          boolean alreadyTraced2 = false;
          while (searchHits != null && searchHits.length > 0)
            {
              log.debug("got " + searchHits.length + " hits");
              for (SearchHit searchHit : searchHits)
                {
                  Map<String, Object> sourceMap = searchHit.getSourceAsMap();
                  String key;
                  Object res = sourceMap.get(elasticKey);
                  if (res == null)
                    {
                      if (!alreadyTraced1)
                        {
                          log.warn("unexpected, null key while reading " + Arrays.stream(indicesToRead).map(s -> "\""+s+"\"").collect(Collectors.toList()) + " sourceMap=" + sourceMap);
                          alreadyTraced1 = true;
                        }
                    }
                  else
                    {
                      // Need to be extended to support other types of attributes, currently only int and String
                      key = (res instanceof Integer) ? Integer.toString((Integer) res) : (String) res;

                      Map<String, Object> miniSourceMap = sourceMap;
                      if (onlyKeepAlternateIDs && (i == (esIndex.size()-1))) // subscriber index is always last
                        {
                          // size optimize : only keep what is needed for the join later
                          if (!alreadyTraced2)
                            {
                              log.info("Keeping only alternate IDs");
                              alreadyTraced2 = true;
                            }
                          miniSourceMap = new HashMap<>();
                          miniSourceMap.put(elasticKey, sourceMap.get(elasticKey));
                          for (AlternateID alternateID : Deployment.getAlternateIDs().values())
                            {
                              String name = alternateID.getName();
                              log.trace("Only keep alternateID " + name);
                              if (sourceMap.get(name) == null)
                                {
                                  log.trace("Unexpected : no value for alternateID " + name);
                                }
                              else
                                {
                                  miniSourceMap.put(name, sourceMap.get(name));
                                }
                            }
                          if (onlyKeepAlternateIDsExtended)
                            {
                              miniSourceMap.put("pointBalances", sourceMap.get("pointBalances")); // keep this (for Customer Point Details report)
                              miniSourceMap.put("loyaltyPrograms", sourceMap.get("loyaltyPrograms")); // keep this (for Loyalty Program Customer States report)
                            }
                          sourceMap = null; // to help GC do its job
                        }

                      // We have in miniSourceMap the maping for this ES line, now write it to csv
                      addHeader &= reportFactory.dumpElementToCsvMono(key, miniSourceMap, writer, addHeader);
                    }
                }
              SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
              scrollRequest.scroll(scroll);
              searchResponse = elasticsearchReaderClient.searchScroll(scrollRequest, RequestOptions.DEFAULT);
              scrollId = searchResponse.getScrollId();
              searchHits = searchResponse.getHits().getHits();
            }
          log.debug("Finished with index " + i);
          if (scrollId != null)
            {
              ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
              clearScrollRequest.addScrollId(scrollId);
              elasticsearchReaderClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            }
          i++;
        }
      writer.flush();
      writer.closeEntry();
      writer.close();
    }
    catch (IOException e1)
    {
      log.info("Error when creating " + csvfile + " : " + e1.getLocalizedMessage());
      return false;
    }
    log.info("Finished producing " + csvfile + ReportUtils.ZIP_EXTENSION);
    return true;
  }

  private String[] getIndices(String key)
  {
    StringBuilder existingIndexes = new StringBuilder();
    boolean firstEntry = true;
    
    //
    // blank key 
    //
    
    if (key == null || key.isEmpty()) return null;
    
    for (String index : key.split(","))
      {
        
        //
        //  wildcard
        //
        
        if(index.endsWith("*")) 
          {
            if (!firstEntry) existingIndexes.append(",");
            existingIndexes.append(index); 
            firstEntry = false;
            continue;
          }
        
        //
        //  indices-exists
        //
        
        GetIndexRequest request = new GetIndexRequest(index);
        request.local(false); 
        request.humanReadable(true); 
        request.includeDefaults(false); 
        try
          {
            boolean exists = elasticsearchReaderClient.indices().exists(request, RequestOptions.DEFAULT);
            if (exists) 
              {
                if (!firstEntry) existingIndexes.append(",");
                existingIndexes.append(index);
                firstEntry = false;
              }
            else
              {
                log.debug("{} index does not exists - record will not be in report", index);
              }
          } 
        catch (IOException e)
          {
            e.printStackTrace();
          }
      }
    log.debug("index to be read {}", existingIndexes.toString());
    if (firstEntry) // nothing got added
      {
        return null;
      }
    else
      {
        return existingIndexes.toString().split(",");
      }
  }

  private int getScrollSize()
  {
    int scrollSize = Deployment.getElasticSearchScrollSize();
    if (scrollSize == 0)
      {
        scrollSize = ReportUtils.DEFAULT_ELASTIC_SEARCH_SCROLL_SIZE;
      }
    log.trace("Using " + scrollSize + " as scroll size in Elastic Search");
    return scrollSize;
  }

  // NEED TO DECIDE WHAT TO DO FOR MULTIPLE FILES
  
/*  
  public final boolean produceReport(String csvfile, boolean multipleFile)
  {
    if (!multipleFile)
      {
        return produceReport(csvfile);
      }
    else
      {
        if (csvfile == null)
          {
            log.info("csvfile is null !");
            return false;
          }

        File file = new File(csvfile + ReportUtils.ZIP_EXTENSION);
        if (file.exists())
          {
            log.info(csvfile + " already exists, do nothing");
            return false;
          }
        
        //
        //  consumer
        //
        
        final Consumer<String, ReportElement> consumer = createConsumer(topicIn);
        Map<String, List<Map<String, Object>>> records = new HashMap<String, List<Map<String, Object>>>();
        
        //
        //  set
        //
        
        final long delay = 10 * 1000; // 10 seconds
        final int giveUp = 3;
        int noRecordsCount = 0;
        consumer.poll(10 * 1000); // necessary for consumer to reset to beginning of partitions
        consumer.seekToBeginning(consumer.assignment());
        showOffsets(consumer);
        int nbRecords = 1;
        boolean breakMainLoop = false;
        
        //
        //  read
        //
        
        for (int nbLoop = 0; !breakMainLoop; nbLoop++)
          {
            log.debug("Doing poll...");
            final ConsumerRecords<String, ReportElement> consumerRecords = consumer.poll(delay);
            // TODO We could count the number of markers, and stop when we have seen them all
            if (consumerRecords.count() == 0)
              {
                noRecordsCount++;
                if (noRecordsCount > giveUp)
                  break;
                else
                  continue;
              }
            if (nbLoop % nbLoopForTrace == 0) log.debug("" + SystemTime.getCurrentTime() + " got " + d(consumerRecords.count()) + " records, total " + d(nbRecords) + " free mem = " + d(rt.freeMemory()) + "/" + d(rt.totalMemory()));

            for (ConsumerRecord<String, ReportElement> record : consumerRecords)
              {
                nbRecords++;
                ReportElement re = record.value();
                log.trace("read " + re);
                if (re.type != ReportElement.MARKER && re.isComplete)
                  {
                    Map<String, List<Map<String, Object>>> splittedReportElements = reportFactory.getSplittedReportElementsForFile(record.value());
                    for (String fileKey : splittedReportElements.keySet())
                    
                    if (records.containsKey(fileKey))
                      {
                        records.get(fileKey).addAll(splittedReportElements.get(fileKey));
                      }
                    else
                      {
                        List<Map<String, Object>> elements = new ArrayList<Map<String, Object>>();
                        elements.addAll(splittedReportElements.get(fileKey));
                        records.put(fileKey, elements);
                      }
                  }
              }
            while (true)
              {
                try
                  {
                    consumer.commitSync();
                    break;
                  } 
                catch (Exception e)
                  {
                    log.info(("Got " + e.getLocalizedMessage() + " we took too long to process batch and got kicked out of the group..."));
                    break;
                  }
              }
          }
        consumer.close();
        
        try
          {
            //
            //  ZIP
            //
            
            FileOutputStream fos = new FileOutputStream(file);
            ZipOutputStream writer = new ZipOutputStream(fos);
            
            for (String key : records.keySet())
              {
                //
                // data
                //

                boolean addHeader = true;
                String dataFile[] = csvfile.split("[.]");
                String dataFileName = dataFile[0] + "_" + key;
                String zipEntryName = new File(dataFileName + "." + dataFile[1]).getName();
                ZipEntry entry = new ZipEntry(zipEntryName);
                writer.putNextEntry(entry);
                for (Map<String, Object> lineMap : records.get(key))
                  {
                    reportFactory.dumpLineToCsv(lineMap, writer, addHeader);
                    addHeader = false;
                  }
              }
            writer.flush();
            writer.closeEntry();
            writer.close();
            fos.close();
          }
        catch (Exception e) 
          {
            log.error("Error in file {} ", e);
          }
      }
    log.info("Finished producing " + csvfile + ReportUtils.ZIP_EXTENSION);
    return true;
  }
*/
  
}
