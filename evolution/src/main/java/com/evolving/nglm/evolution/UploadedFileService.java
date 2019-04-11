/****************************************************************************
*
*  UploadedFileService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.core.SystemTime;

import org.json.simple.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;

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
  private static final String OUTPUT_FOLDER = "/app/uploaded/";

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
    JSONObject result = super.getSummaryJSONRepresentation(guiManagedObject);
    result.put("destinationFileName", guiManagedObject.getJSONRepresentation().get("destinationFileName"));
    result.put("fileType", guiManagedObject.getJSONRepresentation().get("fileType"));
    result.put("fileSize", guiManagedObject.getJSONRepresentation().get("fileSize"));
    result.put("userID", guiManagedObject.getJSONRepresentation().get("userID"));
    
    return result;
  }

  public String generateFileID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredUploadedFile(String fileID) { return getStoredGUIManagedObject(fileID); }

  /*****************************************
  *
  *  putUploadedFile
  *
  *****************************************/

  public void putUploadedFile(GUIManagedObject uploadedFile, InputStream inputStrm, String filename, boolean newObject, String userID) throws GUIManagerException, IOException
  {
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();
    FileOutputStream destFile = null;
    try {

      //
      //  put
      //

      putGUIManagedObject(uploadedFile, now, newObject, userID);

      //
      // store file
      //

      destFile = new FileOutputStream(new File(OUTPUT_FOLDER+filename));
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
      removeGUIManagedObject(uploadedFile.getGUIManagedObjectID(), now, userID);
    }finally {
      if(destFile != null) {
        destFile.flush();
        destFile.close();
      }
    }
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

    File file = new File(OUTPUT_FOLDER+uploadedFile.getDestinationFilename());
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