/*****************************************************************************
 *
 *  ReportMonoPhase.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.DeploymentCommon;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.Journey;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

public class ReportMonoPhase
{
  private static final Logger log = LoggerFactory.getLogger(ReportMonoPhase.class);

  private String esNode;
  private LinkedHashMap<String, QueryBuilder> esIndex;
  private boolean onlyKeepAlternateIDs; // when we read subscriberprofile, only keep info about alternateIDs
  private boolean onlyKeepAlternateIDsExtended; // when we read subscriberprofile, only keep info about alternateIDs and a little more (for 2 specific reports)
  private List<String> subscriberFields;
  private ReportCsvFactory reportFactory;
  private String csvfile;
  private ElasticsearchClientAPI elasticsearchReaderClient;
  private boolean stopReadingAndWriting = false;

  public ReportMonoPhase(String esNode, LinkedHashMap<String, QueryBuilder> esIndex, ReportCsvFactory factory, String csvfile, boolean onlyKeepAlternateIDs, boolean onlyKeepAlternateIDsExtended)
  {
    this(esNode, esIndex, factory, csvfile, onlyKeepAlternateIDs, onlyKeepAlternateIDsExtended, null);
  }

  public ReportMonoPhase(String esNode, LinkedHashMap<String, QueryBuilder> esIndex, ReportCsvFactory factory, String csvfile, boolean onlyKeepAlternateIDs, boolean onlyKeepAlternateIDsExtended, List<String> subscriberFields)
  {
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
    this.subscriberFields = subscriberFields;
    ReportsCommonCode.initializeDateFormats();
    factory.setReportCsvFactoryListener(reportCsvFactoryListener);
  }

  public ReportMonoPhase(String esNode, LinkedHashMap<String, QueryBuilder> esIndex, ReportCsvFactory factory, String csvfile)
  {
    this(esNode, esIndex, factory, csvfile, false, false);
  }

  public ReportMonoPhase(String esNode, LinkedHashMap<String, QueryBuilder> esIndex, ReportCsvFactory factory, String csvfile, boolean onlyKeepAlternateIDs)
  {
    this(esNode, esIndex, factory, csvfile, onlyKeepAlternateIDs, false);
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

  ReportCsvFactoryListener reportCsvFactoryListener = new ReportCsvFactoryListener()
  {
    @Override public void StopReadingESAndWritingToFile()
    {
      stopReadingAndWriting = true;
    }
  };

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
    FileOutputStream fos = null;
    ZipOutputStream writer = null;
    boolean res = true;

    elasticsearchReaderClient = new ElasticsearchClientAPI("ReportManager");
    long totalBatchTime = 0L;
    int nbBatch = 0;
    try {
      fos = new FileOutputStream(file);
      writer = new ZipOutputStream(fos);
      ZipEntry entry = new ZipEntry(new File(csvfile).getName());
      writer.putNextEntry(entry);
      writer.setLevel(Deflater.BEST_SPEED);

      int i = 0;
      boolean addHeader = true;
      int scroolKeepAlive = getScrollKeepAlive();
      for (Entry<String, QueryBuilder> index : esIndex.entrySet())
        {
          SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(index.getValue());
          if (subscriberFields != null && (i == (esIndex.size()-1))) // subscriber index is always last
            {
              String[] subscriberFieldsArray = subscriberFields.toArray(new String[0]);
              log.debug("Only get these fields from " + index.getKey() + " : " + Arrays.toString(subscriberFieldsArray));
              searchSourceBuilder = searchSourceBuilder.fetchSource(subscriberFieldsArray, null); // only get those fields
            }
          SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder).allowPartialSearchResults(false);

          // Read all docs from ES, on esIndex[i]
          // Write to topic, one message per document

          Scroll scroll = new Scroll(TimeValue.timeValueSeconds(scroolKeepAlive));
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
          
          long startBatch = System.currentTimeMillis();
          int nbMaxTraces = 10; // max number of traces to display at INFO level
          
          String scrollId = searchResponse.getScrollId(); // always null
          SearchHit[] searchHits = searchResponse.getHits().getHits();
          logSearchResponse(indicesToRead, searchResponse, searchHits);
          boolean alreadyTraced = false;
          while (searchHits != null && searchHits.length > 0 && !stopReadingAndWriting)
            {
              if (log.isDebugEnabled()) log.debug("got " + searchHits.length + " hits");
              for (SearchHit searchHit : searchHits)
                {
                  //get out from loop if stop reading/writing event comes
                  if(stopReadingAndWriting) break;
                  Map<String, Object> sourceMap = searchHit.getSourceAsMap();
                  Map<String, Object> miniSourceMap = sourceMap;
                  if (onlyKeepAlternateIDs && (i == (esIndex.size()-1))) // subscriber index is always last
                    {
                      // size optimize : only keep what is needed for the join later
                      if (!alreadyTraced)
                        {
                          log.info("Keeping only alternate IDs");
                          alreadyTraced = true;
                        }
                      miniSourceMap = new HashMap<>();
                      miniSourceMap.put("subscriberID", sourceMap.get("subscriberID")); // always get "subscriberID"
                      for (AlternateID alternateID : Deployment.getAlternateIDs().values())
                        {
                          String name = alternateID.getName();
                          if (log.isTraceEnabled()) log.trace("Only keep alternateID " + name);
                          if (sourceMap.get(name) == null)
                            {
                              if (log.isTraceEnabled()) log.trace("Unexpected : no value for alternateID " + name);
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

                  // We have in miniSourceMap the mapping for this ES line, now write it to csv
                  addHeader &= reportFactory.dumpElementToCsvMono(miniSourceMap, writer, addHeader);
                }
              long elapsedBatch = System.currentTimeMillis() - startBatch;
              if (nbMaxTraces > 0 && elapsedBatch > scroolKeepAlive*1000L)
                {
                  log.info("Potential problem : scroll took " + elapsedBatch/1000.0 + " seconds to process, keepAlive of " + scroolKeepAlive + " seconds exceeded");
                  nbMaxTraces--;
                }
              totalBatchTime += elapsedBatch;
              nbBatch++;
              startBatch = System.currentTimeMillis();
              // EVPRO-1080 : retry search (with same scrollId) to catch spurious "ConnectionClosedException: Connection is closed"
              int retryCountSearchScroll = 5;  
              while (true) {
                try {
                  SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId).scroll(scroll);
                  searchResponse = elasticsearchReaderClient.searchScroll(scrollRequest, RequestOptions.DEFAULT);
                  break;
                } catch (org.apache.http.ConnectionClosedException ex) {
                  if (--retryCountSearchScroll < 1) {
                    throw ex; // stop retrying, rethrow original exception
                  }
                  log.info("Got exception while doing searchScroll - wait a bit and retry " + retryCountSearchScroll + " more times : " + ex.getLocalizedMessage());
                  try { Thread.sleep(60000L); } catch (InterruptedException ie) {} // wait a bit before retry
                }
              }
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
    }
    catch (IOException e1)
    {
      StringWriter stackTraceWriter = new StringWriter();
      e1.printStackTrace(new PrintWriter(stackTraceWriter, true));
      log.info("Error when creating " + csvfile + " : " + e1.getLocalizedMessage() + " stack : " + stackTraceWriter.toString());
      res = false;
    }
    finally {
      if (writer != null)
        {
          try
          {
            writer.flush();
            writer.closeEntry();
            writer.close();
          }
          catch (IOException e)
          {
            log.info("Exception " + e.getLocalizedMessage());
          }
        }
      if (fos != null)
        {
          try
          {
            fos.close();
          }
          catch (IOException e)
          {
            log.info("Exception " + e.getLocalizedMessage());
          }
        }
      try
      {
        if (elasticsearchReaderClient != null) elasticsearchReaderClient.closeCleanly();
      }
      catch (IOException e)
      {
        log.info("Exception while closing ElasticSearch client " + e.getLocalizedMessage());
      }
    }
    log.info("Finished producing " + csvfile + ReportUtils.ZIP_EXTENSION + " average time to process batch : " + ((nbBatch==0L)?"NA":(totalBatchTime/(long)nbBatch)) + " ms");
    return res;
  }

  public final boolean startOneToOne(boolean multipleFile)
  {
    if (!multipleFile)
      {
		return startOneToOne();
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

        // holding the zip writers of tmp files
        Map<String,ZipOutputStream> tmpZipFiles = new HashMap<>();

        elasticsearchReaderClient = new ElasticsearchClientAPI("ReportManager");
        long totalBatchTime = 0L;
        int nbBatch = 0;
        try {
          int i = 0;
          int scroolKeepAlive = getScrollKeepAlive();
          for (Entry<String, QueryBuilder> index : esIndex.entrySet())
            {
              SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(index.getValue());
              if (subscriberFields != null && (i == (esIndex.size()-1))) // subscriber index is always last
                {
                  String[] subscriberFieldsArray = subscriberFields.toArray(new String[0]);
                  log.debug("Only get these fields from " + index.getKey() + " : " + Arrays.toString(subscriberFieldsArray));
                  searchSourceBuilder = searchSourceBuilder.fetchSource(subscriberFieldsArray, null); // only get those fields
                }
              SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder).allowPartialSearchResults(false);

              try
              {
                // Read all docs from ES, on esIndex[i]
                // Write to topic, one message per document

                Scroll scroll = new Scroll(TimeValue.timeValueSeconds(scroolKeepAlive));
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

                long startBatch = System.currentTimeMillis();
                int nbMaxTraces = 10; // max number of traces to display at INFO level

                String scrollId = searchResponse.getScrollId(); // always null
                SearchHit[] searchHits = searchResponse.getHits().getHits();
                logSearchResponse(indicesToRead, searchResponse, searchHits);
                boolean alreadyTraced = false;
                while (searchHits != null && searchHits.length > 0)
                  {

                    Map<String, List<Map<String, Object>>> records = new HashMap<String, List<Map<String, Object>>>();

                    if (log.isDebugEnabled()) log.debug("got " + searchHits.length + " hits");
                    for (SearchHit searchHit : searchHits)
                      {
                        Map<String, Object> sourceMap = searchHit.getSourceAsMap();
                        Map<String, Object> miniSourceMap = sourceMap;
                        if (onlyKeepAlternateIDs && (i == (esIndex.size()-1))) // subscriber index is always last
                          {
                            // size optimize : only keep what is needed for the join later
                            if (!alreadyTraced)
                              {
                                log.info("Keeping only alternate IDs");
                                alreadyTraced = true;
                              }
                            miniSourceMap = new HashMap<>();
                            miniSourceMap.put("subscriberID", sourceMap.get("subscriberID")); // always get "subscriberID"
                            for (AlternateID alternateID : Deployment.getAlternateIDs().values())
                              {
                                String name = alternateID.getName();
                                if (log.isTraceEnabled()) log.trace("Only keep alternateID " + name);
                                if (sourceMap.get(name) == null)
                                  {
                                    if (log.isTraceEnabled()) log.trace("Unexpected : no value for alternateID " + name);
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

                        String _id = searchHit.getId();

                        miniSourceMap.put("_id", _id);

                        Map<String, List<Map<String, Object>>> splittedReportElements = reportFactory.getSplittedReportElementsForFileMono(miniSourceMap);
                        for (String fileKey : splittedReportElements.keySet())
                          {
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
                    long elapsedBatch = System.currentTimeMillis() - startBatch;
                    if (nbMaxTraces > 0 && elapsedBatch > scroolKeepAlive*1000L)
                      {
                        log.info("Potential problem : scroll took " + elapsedBatch/1000.0 + " seconds to process, keepAlive of " + scroolKeepAlive + " seconds exceeded");
                        nbMaxTraces--;
                      }
                    totalBatchTime += elapsedBatch;
                    nbBatch++;
                    startBatch = System.currentTimeMillis();
                    // EVPRO-1080 : retry search (with same scrollId) to catch spurious "ConnectionClosedException: Connection is closed"
                    int retryCountSearchScroll = 5;  
                    while (true) {
                      try {
                        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId).scroll(scroll);
                        searchResponse = elasticsearchReaderClient.searchScroll(scrollRequest, RequestOptions.DEFAULT);
                        break;
                      } catch (org.apache.http.ConnectionClosedException ex) {
                        if (--retryCountSearchScroll < 1) {
                          throw ex; // stop retrying, rethrow original exception
                        }
                        log.info("Got exception while doing searchScroll - wait a bit and retry " + retryCountSearchScroll + " more times : " + ex.getLocalizedMessage());
                        try { Thread.sleep(60000L); } catch (InterruptedException ie) {} // wait a bit before retry
                      }
                    }
                    scrollId = searchResponse.getScrollId();
                    searchHits = searchResponse.getHits().getHits();

                    // write each final archive zip entry into tmp files first (will be put in 1 archive at the end
                    try
                    {
                      //
                      //  ZIP
                      //
                      
                      for (String key : records.keySet())
                        {

                          String tmpFileName=file+"."+key+".tmp";
                          boolean addHeader = false;
                          ZipOutputStream writer = tmpZipFiles.get(tmpFileName); // it is closed later
                          if(writer==null){
                            addHeader = true;
                            FileOutputStream fos = new FileOutputStream(tmpFileName);
                            writer = new ZipOutputStream(fos);
                            String dataFile[] = csvfile.split("[.]");
                            String dataFileName = dataFile[0] + "_" + key;
                            String zipEntryName = new File(dataFileName + "." + dataFile[1]).getName();
                            ZipEntry entry = new ZipEntry(zipEntryName);
                            writer.putNextEntry(entry);
                            writer.setLevel(Deflater.BEST_SPEED);
                            tmpZipFiles.put(tmpFileName,writer);
                          }
                          for (Map<String, Object> lineMap : records.get(key))
                            {
                              reportFactory.dumpLineToCsv(lineMap, writer, addHeader);
                              addHeader = false;
                            }
                        }

                    }
                    catch (Exception e)
                    {
                      log.error("Error in file {} ", e);
                    }

                  }
                log.debug("Finished with index " + i);
                if (scrollId != null)
                  {
                    ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                    clearScrollRequest.addScrollId(scrollId);
                    elasticsearchReaderClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
                  }
              } catch (IOException e) { e.printStackTrace(); }
              i++;
            }
        } finally {
          try
          {
            if (elasticsearchReaderClient != null) elasticsearchReaderClient.closeCleanly();
          }
          catch (IOException e)
          {
            log.info("Exception while closing ElasticSearch client " + e.getLocalizedMessage());
          }

          // close all open tmp writer
          for(Entry<String,ZipOutputStream> tmpWriter : tmpZipFiles.entrySet()){
            try {
              tmpWriter.getValue().flush();
              tmpWriter.getValue().close();
            } catch (IOException e) {
              log.warn("Error while closing tmp file {} ", tmpWriter.getKey());
            }
          }

          // write final file from tmp

          try {

            FileOutputStream fos = new FileOutputStream(file);
            ZipOutputStream writer = new ZipOutputStream(fos);

            for(String tmpFile:tmpZipFiles.keySet()){

              // open tmp file
              FileInputStream fis = new FileInputStream(tmpFile);
              ZipInputStream reader = new ZipInputStream(fis);

              writer.putNextEntry(reader.getNextEntry());
              writer.setLevel(Deflater.BEST_SPEED);

              // copy to final file
              int length;
              byte[] bytes = new byte[5*1024*1024];//5M buffer
              while ((length=reader.read(bytes))!=-1) writer.write(bytes,0,length);

              // close and delete tmp
              reader.closeEntry();
              reader.close();
              new File(tmpFile).delete();
            }
            writer.flush();
            writer.closeEntry();
            writer.close();

          } catch (IOException e) {
            log.error("Error while concatenating tmp files", e);
          }
        }
        log.info("Finished producing " + csvfile + ReportUtils.ZIP_EXTENSION + " average time to process batch : " + ((nbBatch==0L)?"NA":(totalBatchTime/(long)nbBatch)) + " ms");
        return true;
      }
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////

  public final boolean startOneToOneMultiThread(JourneyService journeyService, List<Journey> activeJourneys )
  {
    if (csvfile == null) {
        log.info("csvfile is null !");
        return false;
      }
    File file = new File(csvfile + ReportUtils.ZIP_EXTENSION);
    if (file.exists()) {
        log.info(csvfile + " already exists, do nothing");
        return false;
      }
    if (esIndex.entrySet().size() != 1) {
      log.error("internal error : esIndex should contain 1 element " + esIndex.entrySet().size());
      return false;
    }
    Entry<String, QueryBuilder> indexEntry = esIndex.entrySet().iterator().next();
    String indexList = indexEntry.getKey(); // this is in lowercase...
    QueryBuilder query = indexEntry.getValue();
    int maxParallelThreads = DeploymentCommon.getJourneysReportMaxParallelThreads();
    try {
      elasticsearchReaderClient = getESAPI(esNode);    // used by getIndices()
      String[] indicesToRead = getIndices(indexList);
      if (indicesToRead == null || indicesToRead.length == 0) return true;
      List<String> indicesNotWokflows = new ArrayList<>();
      for (String singleIndex : indicesToRead) { // list is in lowercase, need to find back real journeyIDs
        String exactJourneyID = null;
        String journeyIDLowerCase = singleIndex.replace("journeystatistic-", "");
        for (Journey journey : activeJourneys) { // find correct journeyID
          if (journey.getJourneyID().equalsIgnoreCase(journeyIDLowerCase)) {
            exactJourneyID = journey.getJourneyID();
            break;
          }
        }
        if (exactJourneyID != null) {
          GUIManagedObject gmo = journeyService.getStoredJourney(exactJourneyID);
          if (gmo != null && gmo instanceof Journey && !((Journey) gmo).isWorkflow()) { // ignore workflows
            indicesNotWokflows.add(singleIndex);
          }
        }
      }
      List<Thread> threads = new ArrayList<>();
      AtomicInteger activeThreads = new AtomicInteger(0);
      CountDownLatch latch = new CountDownLatch(indicesNotWokflows.size());
      List<String> tempFiles = new ArrayList<>();
      
      for (String singleIndex : indicesNotWokflows) {
        String exactJourneyID = null;
        String journeyIDLowerCase = singleIndex.replace("journeystatistic-", "");
        for (Journey journey : activeJourneys) { // find correct journeyID
          if (journey.getJourneyID().equalsIgnoreCase(journeyIDLowerCase)) {
            exactJourneyID = journey.getJourneyID();
            break;
          }
        }
        GUIManagedObject gmo = journeyService.getStoredJourney(exactJourneyID);
        Journey journey = (Journey) gmo; // cast without checking, better to crash than to wait endlessly in latch.await() later
        String tmpFileName = file + "." + singleIndex + ".tmp";
        tempFiles.add(tmpFileName);
        Thread thread = new Thread( () -> { 
          processSingleJourney(journey, tmpFileName, query, singleIndex, latch, activeThreads);
        } );
        threads.add(thread);
      }
      for (Thread thread : threads) {
        log.info("Starting subthread " + thread.getName() + " with latch " + latch.getCount());
        thread.start();
        try { Thread.sleep(100L); } catch (InterruptedException ie) {} // always wait a bit, prevents storm
        while (activeThreads.get() >= maxParallelThreads) {
          try { Thread.sleep(1000L); } catch (InterruptedException ie) {} // do not allow more than maxParallelThreads simultaneously
        }
      }
      threads = null; // for GC
      try { latch.await(); } catch (InterruptedException e) {}
      log.info("All threads finished");

      FileOutputStream fos = new FileOutputStream(file);
      ZipOutputStream writer = new ZipOutputStream(fos);
      for (String tmpFile : tempFiles) {
        if (Files.exists(FileSystems.getDefault().getPath(tmpFile))) {
          log.debug("Reading from temp file " + tmpFile);
          FileInputStream fis = new FileInputStream(tmpFile);
          ZipInputStream reader = new ZipInputStream(fis);
          writer.putNextEntry(reader.getNextEntry());
          writer.setLevel(Deflater.BEST_SPEED);
          // copy to final file
          int length;
          byte[] bytes = new byte[5*1024*1024];//5M buffer
          while ((length=reader.read(bytes))!=-1) {
            writer.write(bytes,0,length);
          }
          reader.closeEntry();
          reader.close();
          new File(tmpFile).delete();
        }
      }
      writer.flush();
      writer.closeEntry();
      writer.close();
    } catch (IOException e) {
      log.error("Error while concatenating tmp files", e);
    } finally {
      try {
        if (elasticsearchReaderClient != null) elasticsearchReaderClient.close();
      }
      catch (IOException e)
      {
        log.info("Exception while closing ElasticSearch client " + e.getLocalizedMessage());
      }
    }
    log.info("Finished producing " + csvfile + ReportUtils.ZIP_EXTENSION);
    return true;
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  
  private void processSingleJourney(Journey journey, String tmpFileName, QueryBuilder query, String singleIndex, CountDownLatch latch, AtomicInteger activeThreads)
  {
    long startThread = System.currentTimeMillis();
    activeThreads.incrementAndGet();
    ZipOutputStream writer = null;
    ElasticsearchClientAPI localESClient = null;
    long totalBatchTime = 0L;
    int nbBatch = 0;
    try {
      FileOutputStream fos = null;
      localESClient = getESAPI(esNode);
      int scroolKeepAlive = getScrollKeepAlive();
      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query);
      SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder).allowPartialSearchResults(false);
      Scroll scroll = new Scroll(TimeValue.timeValueSeconds(scroolKeepAlive));
      
      searchRequest.indices(singleIndex);
      searchRequest.source().size(getScrollSize());
      searchRequest.scroll(scroll);
      SearchResponse searchResponse;
      searchResponse = localESClient.search(searchRequest, RequestOptions.DEFAULT);
      long startBatch = System.currentTimeMillis();
      int nbMaxTraces = 10; // max number of traces to display at INFO level
      String scrollId = searchResponse.getScrollId(); // always null
      SearchHit[] searchHits = searchResponse.getHits().getHits();
      logSearchResponse(new String[] {singleIndex}, searchResponse, searchHits);
      boolean addHeader = true;
      while (searchHits != null && searchHits.length > 0) {
        List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
        if (log.isDebugEnabled()) log.debug("got " + searchHits.length + " hits");
        for (SearchHit searchHit : searchHits) {
          Map<String, Object> miniSourceMap = searchHit.getSourceAsMap();
          String _id = searchHit.getId();
          miniSourceMap.put("_id", _id);
          Map<String, List<Map<String, Object>>> splittedReportElements = reportFactory.getDataMultithread(journey, miniSourceMap);
          if (splittedReportElements.keySet().size() != 0) { // will be 0 for workflows
            if (splittedReportElements.keySet().size() == 1) {
              String journeyIDFromMethod = splittedReportElements.keySet().iterator().next();
              if (singleIndex.equalsIgnoreCase("journeystatistic-"+journeyIDFromMethod)) {
                records.addAll(splittedReportElements.get(journeyIDFromMethod));
              } else {
                log.error("Internal error : splittedReportElements returned wrong journeyID : " + journeyIDFromMethod + " " + singleIndex);
              }
            } else {
              log.error("Internal error : splittedReportElements should contain 1 mapping, not " + splittedReportElements.keySet().size());
            }
          }
        }
        long elapsedBatch = System.currentTimeMillis() - startBatch;
        if (nbMaxTraces > 0 && elapsedBatch > scroolKeepAlive*1000L) {
          log.info("Potential problem : scroll took " + elapsedBatch/1000.0 + " seconds to process, keepAlive of " + scroolKeepAlive + " seconds exceeded");
          nbMaxTraces--;
        }
        totalBatchTime += elapsedBatch;
        nbBatch++;
        startBatch = System.currentTimeMillis();
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        scrollRequest.scroll(scroll);
        searchResponse = localESClient.searchScroll(scrollRequest, RequestOptions.DEFAULT);
        scrollId = searchResponse.getScrollId();
        searchHits = searchResponse.getHits().getHits();
        try {
          for (Map<String, Object> lineMap : records)
            {
              if (addHeader) { // only create zip entry if there is actual data to write (exclude workflows for example)
                fos = new FileOutputStream(tmpFileName);
                writer = new ZipOutputStream(fos);
                String dataFile[] = csvfile.split("[.]");
                String dataFileName = dataFile[0] + "_" + singleIndex;
                String zipEntryName = new File(dataFileName + "." + dataFile[1]).getName();
                ZipEntry entry = new ZipEntry(zipEntryName);
                writer.putNextEntry(entry);
                writer.setLevel(Deflater.BEST_SPEED);
              }
              reportFactory.dumpLineToCsv(lineMap, writer, addHeader);
              addHeader = false;
            }
        } catch (Exception e) {
          log.error("Error writing tmp file " + tmpFileName + " {} ", e);
        }
      }
      if (scrollId != null) {
          ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
          clearScrollRequest.addScrollId(scrollId);
          localESClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        }
    } catch (IOException e) { e.printStackTrace(); }
    finally {
      try {
        if (writer != null) {
          writer.flush();
          writer.close();
        }
        if (localESClient != null) localESClient.close();
      }
      catch (IOException e) {
        log.info("Exception while releasing resources " + e.getLocalizedMessage());
      } finally {
        long elapsedThread = System.currentTimeMillis() - startThread;
        log.info("Finished with subthread " + Thread.currentThread().getName() + " took " + elapsedThread + " ms, decreasing latch from " + latch.getCount() + " current active threads : " + activeThreads.decrementAndGet() + " average time to process batch : " + ((nbBatch==0L)?"NA":(totalBatchTime/(long)nbBatch)) + " ms");
        latch.countDown(); // we're done
      }
    }
  }

  private ElasticsearchClientAPI getESAPI(String nodes)
  {
    int connectTimeout = Deployment.getElasticsearchConnectionSettings("ReportManager", false).getConnectTimeout();
    int queryTimeout = Deployment.getElasticsearchConnectionSettings("ReportManager", false).getQueryTimeout();
    return new ElasticsearchClientAPI("ReportManager");
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  
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
            log.info("Exception " + e.getLocalizedMessage());
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

  public static int getScrollSize()
  {
    int scrollSize = com.evolving.nglm.core.Deployment.getElasticsearchScrollSize();
    if (scrollSize == 0)
      {
        scrollSize = ReportUtils.DEFAULT_ELASTIC_SEARCH_SCROLL_SIZE;
      }
    log.trace("Using " + scrollSize + " as scroll size in Elastic Search");
    return scrollSize;
  }

  public static int getScrollKeepAlive()
  {
    int scrollKeepAlive = com.evolving.nglm.core.Deployment.getElasticSearchScrollKeepAlive();
    if (scrollKeepAlive == 0)
      {
        scrollKeepAlive = ReportUtils.DEFAULT_ELASTIC_SEARCH_SCROLL_KEEP_ALIVE;
      }
    log.trace("Using " + scrollKeepAlive + " as scroll keep alive in Elastic Search");
    return scrollKeepAlive;
  }

  private void logSearchResponse(String[] indicesToRead, SearchResponse searchResponse, SearchHit[] searchHits)
  {
    if (log.isTraceEnabled()) log.trace("searchHits = " + Arrays.toString(searchHits));
    if (searchResponse != null && searchHits != null)
      {
        if (log.isTraceEnabled()) 
          {
            log.trace("getFailedShards = " + searchResponse.getFailedShards());
            log.trace("getSkippedShards = " + searchResponse.getSkippedShards());
            log.trace("getTotalShards = " + searchResponse.getTotalShards());
            log.trace("getTook = " + searchResponse.getTook());
          }
        StringWriter sw = new StringWriter();
        TotalHits totalHits = searchResponse.getHits().getTotalHits();
        long totalHitsLong = totalHits.value;
        (new PrintWriter(sw)).printf(", totalHits = %,d%s", totalHitsLong, (totalHits.relation == Relation.EQUAL_TO)?"":"+"); // better formating, can be huge
        String logMsg = "For " + Arrays.toString(indicesToRead) + " searchHits.length = " + searchHits.length + sw;
        if (searchResponse.getFailedShards() != 0 || searchResponse.getSkippedShards() != 0)
          {
            logMsg += " getFailedShards = " + searchResponse.getFailedShards() +
                      " getSkippedShards = " + searchResponse.getSkippedShards() +
                      " getTotalShards = " + searchResponse.getTotalShards() +
                      " getTook = " + searchResponse.getTook();
          }
        log.info(logMsg);
      }
  }

  public ElasticsearchClientAPI getESClient() { return elasticsearchReaderClient; }

}
