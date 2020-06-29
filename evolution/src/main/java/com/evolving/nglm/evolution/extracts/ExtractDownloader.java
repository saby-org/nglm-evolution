package com.evolving.nglm.evolution.extracts;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.SubscriberMessageTemplateService;
import com.evolving.nglm.evolution.TargetService;
import com.evolving.nglm.evolution.reports.ReportUtils;
import org.json.simple.JSONObject;

import com.sun.net.httpserver.HttpExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ExtractDownloader implements Runnable
{

  private static final Logger log = LoggerFactory.getLogger(ExtractDownloader.class);

  private static TargetService targetService;

  static
    {
      targetService = new TargetService(Deployment.getBrokerServers(), "extractdownloader-targetservice-001" , Deployment.getSubscriberMessageTemplateTopic(), false);
      targetService.start();
    }

  private String userID;
  private JSONObject jsonRoot;
  private JSONObject jsonResponse;
  private HttpExchange exchange;

  private Thread t;
  private String threadName;

  public ExtractDownloader(String userID, JSONObject jsonRoot, JSONObject jsonResponse, com.sun.net.httpserver.HttpExchange exchange)
  {
    this.userID = userID;
    this.jsonRoot = jsonRoot;
    this.jsonResponse = jsonResponse;
    this.exchange = exchange;
    threadName = JSONUtilities.decodeString(jsonRoot,"userID",true) + JSONUtilities.decodeString(jsonRoot,"extractName",true);
  }
  @Override
  public void run()
  {
    String extractName = JSONUtilities.decodeString(jsonRoot, "extractName", true);
    String jsonUserID = JSONUtilities.decodeString(jsonRoot, "userID", true);
    String responseCode = null;

    try
      {
        //Report report = new Report(target.getJSONRepresentation(), epochServer.getKey(), null);
        //String reportName = report.getName();
        while(ExtractService.isTargetExtractRunning(extractName+"-"+jsonUserID))
          {
            TimeUnit.SECONDS.sleep(1);
          }

        String outputPath = Deployment.getExtractManagerOutputPath()+ File.separator;
        String fileExtension = Deployment.getExtractManagerFileExtension();

        File folder = new File(outputPath);
        String csvFilenameRegex = jsonUserID+"_"+extractName+ "_"+ ".*"+ "\\."+ fileExtension+ ReportUtils.ZIP_EXTENSION;

        File[] listOfFiles = folder.listFiles(new FileFilter(){
          @Override
          public boolean accept(File f) {
            return Pattern.compile(csvFilenameRegex).matcher(f.getName()).matches();
          }});

        File reportFile = null;

        long lastMod = Long.MIN_VALUE;
        if(listOfFiles != null && listOfFiles.length != 0) {
          for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
              if(listOfFiles[i].lastModified() > lastMod) {
                reportFile = listOfFiles[i];
                lastMod = reportFile.lastModified();
              }
            }
          }
        }else {
          responseCode = "Cant find extract with that name";
        }

        if(reportFile != null) {
          if(reportFile.length() > 0) {
            try {
              FileInputStream fis = new FileInputStream(reportFile);
              exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
              exchange.getResponseHeaders().add("Content-Disposition", "attachment; filename=" + reportFile.getName());
              exchange.sendResponseHeaders(200, reportFile.length());
              OutputStream os = exchange.getResponseBody();
              byte data[] = new byte[10_000]; // allow some bufferization
              int length;
              while ((length = fis.read(data)) != -1) {
                os.write(data, 0, length);
              }
              fis.close();
              os.flush();
              os.close();
            } catch (Exception excp) {
              StringWriter stackTraceWriter = new StringWriter();
              excp.printStackTrace(new PrintWriter(stackTraceWriter, true));
              log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
            }
          }else {
            responseCode = "extract size is 0, extract file is empty";
          }
        }else {
          responseCode = "extract is null, cant find this extract";
        }
      }
    catch (Exception e)
      {
        log.info("Exception when building target from "+extractName+" : "+e.getLocalizedMessage());
        responseCode = "internalError";
      }

    if(responseCode != null) {
      try {
        jsonResponse.put("responseCode", responseCode);
        exchange.sendResponseHeaders(200, 0);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(exchange.getResponseBody()));
        writer.write(jsonResponse.toString());
        writer.close();
        exchange.close();
      }catch(Exception e) {
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.warn("Exception processing REST api: {}", stackTraceWriter.toString());
      }
    }
  }

  public void start()
  {
    if (t == null) {
      t = new Thread (this, threadName);
      t.start ();
    }
  }
}
