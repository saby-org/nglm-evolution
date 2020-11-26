/****************************************************************************
*
*  UploadedFileService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.Expression.ExpressionEvaluationException;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.SubscriberProfile.ValidateUpdateProfileRequestException;
import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UploadedFileService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(UploadedFileService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private UploadedFileListener uploadedFileListener = null;
  public static final String basemanagementApplicationID = "101";
  public static final String DATATYPE_VRIABLE_PATTER = "\\<(.*?)\\>";  // <String>Name, <Int>Years, <String>Gift

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public UploadedFileService(String bootstrapServers, String groupID, String uploadFileTopic, boolean masterService, UploadedFileListener uploadedFileListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "UploadedFileService", groupID, uploadFileTopic, masterService, getSuperListener(uploadedFileListener), "putUploadedFile", "deleteUploadedFile", notifyOnSignificantChange);
  }

  //
  //  constructor
  //
  
  public UploadedFileService(String bootstrapServers, String groupID, String uploadFileTopic, boolean masterService, UploadedFileListener uploadedFileListener)
  {
    this(bootstrapServers, groupID, uploadFileTopic, masterService, uploadedFileListener, true);
  }

  //
  //  constructor
  //

  public UploadedFileService(String bootstrapServers, String groupID, String uploadFileTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, uploadFileTopic, masterService, (UploadedFileListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(UploadedFileListener uploadedFileListener)
  {
    GUIManagedObjectListener superListener = null;
    if (uploadedFileListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { uploadedFileListener.fileActivated((UploadedFile) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { uploadedFileListener.fileDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getSummaryJSONRepresentation
  *
  *****************************************/

  @Override protected JSONObject getSummaryJSONRepresentation(GUIManagedObject guiManagedObject)
  {
    JSONObject result = new JSONObject();
    result.put("id", guiManagedObject.getJSONRepresentation().get("id"));
    result.put("destinationFilename", guiManagedObject.getJSONRepresentation().get("destinationFilename"));
    result.put("fileType", guiManagedObject.getJSONRepresentation().get("fileType"));
    result.put("fileSize", guiManagedObject.getJSONRepresentation().get("fileSize"));
    result.put("userID", guiManagedObject.getJSONRepresentation().get("userID"));
    result.put("accepted", guiManagedObject.getAccepted());
    result.put("valid", guiManagedObject.getAccepted());
    result.put("processing", isActiveGUIManagedObject(guiManagedObject, SystemTime.getCurrentTime()));
    result.put("readOnly", guiManagedObject.getReadOnly());
    return result;
  }

  /*****************************************
  *
  *  getUploadedFiles
  *
  *****************************************/

  public String generateFileID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredUploadedFile(String fileID) { return getStoredGUIManagedObject(fileID); }
  public GUIManagedObject getStoredUploadedFile(String fileID, boolean includeArchived) { return getStoredGUIManagedObject(fileID, includeArchived); }
  public Collection<GUIManagedObject> getStoredUploadedFiles() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredUploadedFiles(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
  public boolean isActiveUploadedFile(GUIManagedObject uploadedFileUnchecked, Date date) { return isActiveGUIManagedObject(uploadedFileUnchecked, date); }
  public UploadedFile getActiveUploadedFile(String uploadedFileID, Date date) { return (UploadedFile) getActiveGUIManagedObject(uploadedFileID, date); }
  public Collection<UploadedFile> getActiveUploadedFiles(Date date) { return (Collection<UploadedFile>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putUploadedFile
  *
  *****************************************/

  public void putUploadedFile(GUIManagedObject guiManagedObject, InputStream inputStrm, String filename, boolean newObject, String userID) throws GUIManagerException, IOException
  {
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();
    FileOutputStream destFile = null;
    try {

      //
      // store file
      //

      destFile = new FileOutputStream(new File(UploadedFile.OUTPUT_FOLDER+filename));
      byte[] bytes = new byte[1024];
      int readSize = inputStrm.read(bytes);
      while(readSize > 0) {
        byte[] finalArray = new byte[readSize];
        for(int i = 0; i < readSize ; i++) {
          finalArray[i] = bytes[i];           
        }
        destFile.write(finalArray);
        readSize = inputStrm.read(bytes);
      }
    }catch(Exception e) {
      StringWriter stackTraceWriter = new StringWriter();
      e.printStackTrace(new PrintWriter(stackTraceWriter, true));
      log.error("Exception saving file: putUploadedFile API: {}", stackTraceWriter.toString());
      removeGUIManagedObject(guiManagedObject.getGUIManagedObjectID(), now, userID);
    }finally {
      if(destFile != null) {
        destFile.flush();
        destFile.close();
      }
    }
    
    //
    // validate 
    //
    
    if (guiManagedObject instanceof UploadedFile)
      {
        UploadedFile uploadededFile = (UploadedFile) guiManagedObject;
        uploadededFile.validate();

        //
        // count segments
        //
        
        if(uploadededFile.getApplicationID().equals(basemanagementApplicationID))
          {
            Map<String, Integer> count = new HashMap<String,Integer>();
            List<String> lines = new ArrayList<String>();
            
            //
            //  read file
            //
            
            try (Stream<String> stream = Files.lines(Paths.get(UploadedFile.OUTPUT_FOLDER + filename)))
              {
                lines = stream.filter(line -> (line != null && !line.trim().isEmpty())).map(String::trim).collect(Collectors.toList());
                for (String line : lines)
                  {
                    String subscriberIDSegementName[] = line.split(Deployment.getUploadedFileSeparator());
                    if (subscriberIDSegementName.length >= 2)
                      {
                      
                        //
                        //  details
                        //
                      
                        String subscriberID = subscriberIDSegementName[0];
                        String segmentName = subscriberIDSegementName[1];
                      
                        //
                        //  count
                        //
                      
                        if (segmentName != null && !segmentName.trim().isEmpty() && subscriberID != null && !subscriberID.trim().isEmpty())
                          {
                            count.put(segmentName.trim(), count.get(segmentName.trim()) != null ? count.get(segmentName.trim()) + 1 : 1);
                          }
                      }
                    else
                      {
                        log.warn("UploadedFileService.putUploadedFile(not two values, skip. line="+line+")");
                      }
                  }
              }
            catch (IOException e)
              {
                log.warn("UploadedFileService.putUploadedFile(problem with file parsing)", e);
              }
            
            //
            // add metadata
            //
            
            ((UploadedFile) guiManagedObject).addMetaData("segmentCounts", JSONUtilities.encodeObject(count));
          }
      }

    //
    //  put
    //

    putGUIManagedObject(guiManagedObject, now, newObject, userID);   
  }
  
  /*****************************************
  *
  *  putUploadedFile
  *
  *****************************************/

  public void putUploadedFileWithVariables(GUIManagedObject guiManagedObject, InputStream inputStrm, String filename, boolean newObject, String userID) throws GUIManagerException, IOException
  {
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();
    FileOutputStream destFile = null;
    try {

      //
      // store file
      //

      destFile = new FileOutputStream(new File(UploadedFile.OUTPUT_FOLDER+filename));
      byte[] bytes = new byte[1024];
      int readSize = inputStrm.read(bytes);
      while(readSize > 0) {
        byte[] finalArray = new byte[readSize];
        for(int i = 0; i < readSize ; i++) {
          finalArray[i] = bytes[i];           
        }
        destFile.write(finalArray);
        readSize = inputStrm.read(bytes);
      }
    }catch(Exception e) {
      StringWriter stackTraceWriter = new StringWriter();
      e.printStackTrace(new PrintWriter(stackTraceWriter, true));
      log.error("Exception saving file: putUploadedFileWithVariables API: {}", stackTraceWriter.toString());
      removeGUIManagedObject(guiManagedObject.getGUIManagedObjectID(), now, userID);
    }finally {
      if(destFile != null) {
        destFile.flush();
        destFile.close();
      }
    }
    
    //
    // validate 
    //
    
    if (guiManagedObject instanceof UploadedFile)
      {
        UploadedFile uploadededFile = (UploadedFile) guiManagedObject;
        uploadededFile.validate();
        
        ArrayList<String> variables = new ArrayList<String>();
        ArrayList<JSONObject> valiablesJSON = new ArrayList<JSONObject>();
        
        //
        //  scan and validate
        //
        
        List<String> lines = new ArrayList<String>();
        
        //
        //  read file
        //
        
        try (Stream<String> stream = Files.lines(Paths.get(UploadedFile.OUTPUT_FOLDER + filename)))
          {
            lines = stream.filter(line -> (line != null && !line.trim().isEmpty())).map(String::trim).collect(Collectors.toList());
            boolean isHeader = true;
            for (String line : lines)
              {
                line = line.trim();
                if (isHeader)
                  {
                    //
                    //  validate and prepare the variables
                    //
                    
                    isHeader = false;
                    String headers[] = line.split(Deployment.getUploadedFileSeparator(), -1);
                    boolean isFirstColumn = true;
                    for (String header : headers)
                      {
                        header = header.trim();
                        if (isFirstColumn)
                          {
                            //
                            //  first column is for alternteID - validate and set alternateID
                            //
                            
                            Map<String, AlternateID> alternateIDs = Deployment.getAlternateIDs();
                            if (alternateIDs.get(header) == null) throw new GUIManagerException("invalid alternateID " + header, "supported alternateIDs are " + alternateIDs.keySet());
                            variables.add(header);
                            uploadededFile.setCustomerAlternateID(header);
                            isFirstColumn = false;
                          }
                        else
                          {
                            String dataType = getDatatype(header);
                            String variableName = getVaribaleName(header);
                            HashMap<String, String> variablesDataTypes = new LinkedHashMap<String, String>();
                            variablesDataTypes.put("variableName", variableName);
                            variablesDataTypes.put("dataType", dataType);
                            valiablesJSON.add(JSONUtilities.encodeObject(variablesDataTypes));
                            variables.add(variableName);
                          }
                      }
                  }
                else
                  {
                    //
                    //  validate the data type value
                    //
                    
                    String values[] = line.split(Deployment.getUploadedFileSeparator(), -1);
                    boolean isFirstColumn = true;
                    int index = 0;
                    for (String value : values)
                      {
                        value = value.trim();
                        if (!isFirstColumn)
                          {
                            String variableName = variables.get(index);
                            String dataType = getDataType(variableName, valiablesJSON);
                            CriterionDataType CriterionDataType = EvaluationCriterion.CriterionDataType.fromExternalRepresentation(dataType);
                            validateValue(variableName, CriterionDataType, value);
                          }
                        isFirstColumn = false;
                        index++;
                      }
                  }
              }
          } 
        catch (IOException e)
          {
            log.warn("UploadedFileService.putUploadedFileWithVariables(problem with file parsing)", e);
          }
        
        //
        // variables
        //
        
        HashMap<String, Object> fileVariables = new LinkedHashMap<String, Object>();
        fileVariables.put("fileVariables", JSONUtilities.encodeArray(valiablesJSON));
        ((UploadedFile) guiManagedObject).addMetaData("variables", JSONUtilities.encodeObject(fileVariables));
      }

    //
    //  put
    //

    putGUIManagedObject(guiManagedObject, now, newObject, userID);  
  }
  
  private String getDataType(String variableName, ArrayList<JSONObject> valiablesJSON)
  {
    String result = null;
    for (JSONObject variableJSON : valiablesJSON)
      {
        String varName = JSONUtilities.decodeString(variableJSON, "variableName", true);
        String dataType = JSONUtilities.decodeString(variableJSON, "dataType", true);
        if (variableName.equals(varName))
          {
            result = dataType;
            break;
          }
      }
    return result;
  }

  private void validateValue(String variableName, CriterionDataType criterionDataType, String rawValue) throws GUIManagerException
  {
    log.debug("validateValue {}, {}, {}", variableName, criterionDataType, rawValue);
    switch (criterionDataType)
    {
      case StringCriterion:
        break;
        
      case IntegerCriterion:
        try
          {
            Integer.parseInt(rawValue);
          }
        catch(Exception ex)
          {
            throw new GUIManagerException("bad value in " + criterionDataType, "invalid " + criterionDataType + " value " + rawValue + " for variable " + variableName + " in file line no ");
          }
        break;
        
      case DoubleCriterion:
        try
          {
            Integer.parseInt(rawValue);
          }
      catch(Exception ex)
        {
          throw new GUIManagerException("bad value in " + criterionDataType, "invalid " + criterionDataType + " value " + rawValue + " for variable " + variableName + " in file line no ");
        }
        break;
        
      case DateCriterion:
        try 
        {
          GUIManagedObject.parseDateField(rawValue);
        }
      catch(JSONUtilitiesException ex)
        {
          throw new GUIManagerException("invaid date format", "invalid dateString " + rawValue + " for variable " + variableName + " in file line no ");
        }
        break;
        
      case TimeCriterion:
        String[] args = rawValue.split(":");
        if (args.length != 3) 
          {
            throw new GUIManagerException("invaid time format", "invalid timeString " + rawValue + " for variable " + variableName + " in file line no ");
          }
        break;

      default:
        throw new GUIManagerException("datatype not supported", "invalid dataType " + criterionDataType + " for variable " + variableName + " in file line no ");
    }
  }

  private String getVaribaleName(String header)
  {
    String result = null;
    Pattern pattern = Pattern.compile(DATATYPE_VRIABLE_PATTER);
    Matcher matcher = pattern.matcher(header);
    if (matcher.find())
      {
        result = header.replaceAll(matcher.group(0), "");
      }
    return result;
  }

  private String getDatatype(String header)
  {
    String result = null;
    Pattern pattern = Pattern.compile(DATATYPE_VRIABLE_PATTER);
    Matcher matcher = pattern.matcher(header);
    if (matcher.find()) result = matcher.group(1);
    return result;
  }

  /*****************************************
  *
  *  deleteUploadedFile
  *
  *****************************************/

  public void deleteUploadedFile(String fileID, String userID, UploadedFile uploadedFile) {
    
    //
    // remove UploadedFile object
    //

    removeGUIManagedObject(fileID, SystemTime.getCurrentTime(), userID); 

    //
    // remove file
    //

    File file = new File(UploadedFile.OUTPUT_FOLDER+uploadedFile.getDestinationFilename());
    if(file.exists()) {
      if(file.delete()) {
        log.debug("UploadedFileService.deleteUploadedFile: File has been deleted successfully");
      }else {
        log.warn("UploadedFileService.deleteUploadedFile: File has not been deleted");
      }
    }else {
      log.warn("UploadedFileService.deleteUploadedFile: File does not exist");
    }
  }
  
  
  /*****************************************
  *
  *  putIncompleteUploadedFile
  *
  *****************************************/

  public void putIncompleteUploadedFile(IncompleteObject template, boolean newObject, String userID)
  {
    putGUIManagedObject(template, SystemTime.getCurrentTime(), newObject, userID);
  }

  /*****************************************
   *
   *  changeFileApplicationId
   *
   *****************************************/

  public void changeFileApplicationId(String fileID, String newApplicationID)
  {
    UploadedFile file = (UploadedFile) getStoredUploadedFile(fileID);
    if(file!=null){
      // "change" applicationId, not "set"
      if(file.getApplicationID()!=null) file.setApplicationID(newApplicationID);
      // as any GUIManagedObject, same information is duplicated, and we use this following one seems to return GUIManager calls...
      if(file.getJSONRepresentation().get("applicationID")!=null) file.getJSONRepresentation().put("applicationID",newApplicationID);
      file.setEpoch(file.getEpoch()+1);//trigger "changes happened"
      putGUIManagedObject(file,file.getUpdatedDate(),false,null);
    }else{
      log.warn("UploadedFileService.changeFileApplicationId: File does not exist");
    }
  }
  

  /*****************************************
  *
  *  interface OfferListener
  *
  *****************************************/

  public interface UploadedFileListener
  {
    public void fileActivated(UploadedFile uploadedFile);
    public void fileDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  uploadedFileListener
    //

    UploadedFileListener uploadedFileListener = new UploadedFileListener()
    {
      @Override public void fileActivated(UploadedFile uploadedFile) { System.out.println("UploadedFile activated: " + uploadedFile.getApplicationID()); }
      @Override public void fileDeactivated(String guiManagedObjectID) { System.out.println("UploadedFile deactivated: " + guiManagedObjectID); }
    };

    //
    //  offerService
    //

    UploadedFileService offerService = new UploadedFileService(Deployment.getBrokerServers(), "uploadedfileservice-001", Deployment.getUploadedFileTopic(), true, uploadedFileListener);
    offerService.start();

    //
    //  sleep forever
    //

    while (true)
      {
        try
          {
            Thread.sleep(Long.MAX_VALUE);
          }
        catch (InterruptedException e)
          {
            //
            //  ignore
            //
          }
      }
  }
}