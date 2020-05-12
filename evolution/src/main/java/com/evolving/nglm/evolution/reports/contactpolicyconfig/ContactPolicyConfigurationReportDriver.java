/*****************************************************************************
 *
 *  ContactPolicyConfigurationReportDriver.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports.contactpolicyconfig;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ContactPolicyConfigurationReportDriver extends ReportDriver 
{
  private static final Logger log = LoggerFactory.getLogger(ContactPolicyConfigurationReportDriver.class);
  private ContactPolicyService contactPolicyService;
  private SegmentContactPolicyService segmentContactPolicyService;
  private JourneyObjectiveService journeyObjectiveService;
  private SegmentationDimensionService segmentationDimensionService;
  private boolean addHeaders=true;

  @Override
  public void produceReport(Report report, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params)
  {
    log.info("Entered in to the contact policy produceReport");

    Random r = new Random();
    int apiProcessKey = r.nextInt(999);

    contactPolicyService = new ContactPolicyService(kafka, "contactPolicyConfigReportDriver-contactpolicyservice-" + apiProcessKey, Deployment.getContactPolicyTopic(), false);
    contactPolicyService.start();

    segmentContactPolicyService = new SegmentContactPolicyService(kafka, "contactPolicyConfigReportDriver-segmentcontactpolicyservice-" + apiProcessKey, Deployment.getSegmentContactPolicyTopic(), false);
    segmentContactPolicyService.start();

    journeyObjectiveService = new JourneyObjectiveService(kafka, "contactPolicyConfigReportDriver-journeyobjectiveservice-" + apiProcessKey, Deployment.getJourneyObjectiveTopic(), false);
    journeyObjectiveService.start();
    
    segmentationDimensionService = new SegmentationDimensionService(kafka, "contactPolicyConfigReportDriver-segmentationDimensionservice-" + apiProcessKey, Deployment.getSegmentationDimensionTopic(), false);
    segmentationDimensionService.start();

    File file = new File(csvFilename+".zip");
    FileOutputStream fos;
    ZipOutputStream writer = null;
    try
    {
      if(contactPolicyService.getStoredContactPolicies().size()==0)
        {
          log.info("No Contact Policies ");
          NGLMRuntime.addShutdownHook(new ShutdownHook(contactPolicyService, segmentContactPolicyService, journeyObjectiveService));
        }
      else
        {
          fos = new FileOutputStream(file);
          writer = new ZipOutputStream(fos);
          ZipEntry entry = new ZipEntry(new File(csvFilename).getName()); // do not include tree structure in zipentry, just csv filename
          writer.putNextEntry(entry);
          for (GUIManagedObject guiManagedObject : contactPolicyService.getStoredContactPolicies())
            {
              if(guiManagedObject instanceof ContactPolicy)
                {
                  ContactPolicy contactPolicy = (ContactPolicy) guiManagedObject;
                  JSONObject contactPolicyJSON = contactPolicyService.generateResponseJSON(guiManagedObject, true, SystemTime.getCurrentTime());
                  List<String> segmentIDs = contactPolicyService.getAllSegmentIDsUsingContactPolicy(contactPolicy.getContactPolicyID(), segmentContactPolicyService);
                  List<String> segmentNames = new ArrayList<>();
                  for(String segmentID :segmentIDs) {
                    Segment segment = segmentationDimensionService.getSegment(segmentID);
                    segmentNames.add(segment.getName());
                  }
                  List<String> journeyObjectives = contactPolicyService.getAllJourneyObjectiveNamesUsingContactPolicy(contactPolicy.getContactPolicyID(), journeyObjectiveService);
                  contactPolicyJSON.put("segmentNames", JSONUtilities.encodeArray(segmentNames));
                  contactPolicyJSON.put("journeyObjectiveNames", JSONUtilities.encodeArray(journeyObjectives));
                  Map<String, Object> formattedFields = formatSimpleFields(contactPolicyJSON);
                  if(contactPolicy.getContactPolicyCommunicationChannels() != null) {
                    for(ContactPolicyCommunicationChannels channel : contactPolicy.getContactPolicyCommunicationChannels()) {
                      formattedFields.put("communicationChannelName", channel.getCommunicationChannelName());
                      
                      StringBuilder sbLimits = new StringBuilder();
                      String limits = null;
                      if(channel.getMessageLimits() != null && !(channel.getMessageLimits().isEmpty())) {
                        for(MessageLimits limit : channel.getMessageLimits()) {
                          sbLimits.append(limit.getMaxMessages()).append(" per ").append(limit.getDuration()).append(" ").append(limit.getTimeUnit()).append(",");
                        }
                        limits = sbLimits.toString().substring(0, sbLimits.toString().length()-1);
                      }  
                      formattedFields.put("limits", limits);
                      
                      try
                      {
                        dumpElementToCsv(formattedFields, writer);
                      } 
                      catch (Exception e)
                      {
                        e.printStackTrace();
                      }
                      
                    }
                  }
                }
            }
        } 
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    finally 
    {
      if(writer != null) 
        {
          writeCompleted(writer);
        }
    }
  }


  private void dumpElementToCsv(Map<String, Object> allFields, ZipOutputStream writer) throws Exception
  {
    String csvSeparator = ReportUtils.getSeparator();
    
    if (addHeaders)
      {
        String headers = "";
        for (String fields : allFields.keySet())
          {
            headers += fields + csvSeparator;
          }
        headers = headers.substring(0, headers.length() - 1);
        writer.write(headers.getBytes());
        writer.write("\n".getBytes());
        addHeaders = false;
      }
    
    String line = ReportUtils.formatResult(allFields);
    writer.write(line.getBytes());
    writer.write("\n".getBytes());
  }
  
  private Map<String, Object> formatSimpleFields(JSONObject jsonObject)
  {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("policyId", jsonObject.get("id").toString());
    result.put("policyName", jsonObject.get("display").toString());
    if (jsonObject.get("description") != null)
      {
        result.put("description", jsonObject.get("description").toString());
      }
    else
      {
        result.put("description", "");
      }

    result.put("active", jsonObject.get("active").toString());
  
    
    StringBuilder sbSegments = new StringBuilder();
    String segments = null;
    if(jsonObject.get("segmentNames") != null) {
      JSONArray jsonArray = (JSONArray) jsonObject.get("segmentNames");
      if(!jsonArray.isEmpty()) {
        for(int i = 0; i<jsonArray.size(); i++) {
          sbSegments.append(jsonArray.get(i)).append(",");
        }
        segments = sbSegments.toString().substring(0, sbSegments.toString().length()-1);
      }
    }  
    
    StringBuilder sbObjectives = new StringBuilder();
    String journeyObjectives = null;
    if(jsonObject.get("journeyObjectiveNames") != null) {
      JSONArray jsonArray = (JSONArray) jsonObject.get("journeyObjectiveNames");
      if(!jsonArray.isEmpty()) {
        for(int i = 0; i<jsonArray.size(); i++) {
          sbObjectives.append(jsonArray.get(i)).append(",");
        }
        journeyObjectives = sbObjectives.toString().substring(0, sbObjectives.toString().length()-1);
      }
    }  
    
    result.put("segments", segments);
    result.put("journeyObjectives", journeyObjectives);
    
    return result;
  }
  
  private void writeCompleted(ZipOutputStream writer)
  {
    try
      {
        writer.flush();
        writer.closeEntry();
        writer.close();
      } catch (IOException e)
      {
        log.warn("ContactPolicyConfigurationReportDriver.writeCompleted(error closing BufferedWriter)", e);
      }
    log.info("csv Writer closed");
    NGLMRuntime.addShutdownHook(new ShutdownHook(contactPolicyService, segmentContactPolicyService, journeyObjectiveService));
  }
  private static class ShutdownHook implements NGLMRuntime.NGLMShutdownHook
  {
    private ContactPolicyService contactPolicyService;
    private SegmentContactPolicyService segmentContactPolicyService;
    private JourneyObjectiveService journeyObjectiveService;  

    public ShutdownHook(ContactPolicyService contactPolicyService, SegmentContactPolicyService segmentContactPolicyService, JourneyObjectiveService journeyObjectiveService)
    {
      this.contactPolicyService = contactPolicyService;
      this.segmentContactPolicyService = segmentContactPolicyService;
      this.journeyObjectiveService = journeyObjectiveService;
    }

    @Override
    public void shutdown(boolean normalShutdown)
    {
      if(contactPolicyService!=null){contactPolicyService.stop();log.trace("contactPolicyService stopped..");}
      if(segmentContactPolicyService!=null){segmentContactPolicyService.stop();log.trace("segmentContactPolicyService stopped..");}
      if(journeyObjectiveService!=null){journeyObjectiveService.stop();log.trace("journeyObjectiveService stopped..");}
    }
  }
}
