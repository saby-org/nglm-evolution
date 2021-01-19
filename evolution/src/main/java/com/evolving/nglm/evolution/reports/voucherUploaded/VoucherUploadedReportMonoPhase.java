package com.evolving.nglm.evolution.reports.voucherUploaded;

import java.io.IOException;
import java.text.DateFormat;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.zip.ZipOutputStream;

import javax.swing.text.html.HTMLDocument.HTMLReader.IsindexAction;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.joda.time.LocalDate;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.Supplier;
import com.evolving.nglm.evolution.SupplierService;
import com.evolving.nglm.evolution.Voucher;
import com.evolving.nglm.evolution.VoucherService;
import com.evolving.nglm.evolution.VoucherType;
import com.evolving.nglm.evolution.VoucherTypeService;
import com.evolving.nglm.evolution.reports.ReportCsvFactory;
import com.evolving.nglm.evolution.reports.ReportMonoPhase;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportsCommonCode;
import com.evolving.nglm.evolution.reports.ReportUtils.ReportElement;
import com.evolving.nglm.evolution.reports.journeycustomerstatistics.JourneyCustomerStatisticsReportMonoPhase;


public class VoucherUploadedReportMonoPhase implements ReportCsvFactory
{

  //
  // logger
  //

  private static final Logger log = LoggerFactory.getLogger(VoucherUploadedReportMonoPhase.class);
  final private static String CSV_SEPARATOR = ReportUtils.getSeparator();
  private VoucherService voucherService;
  private SupplierService supplierService;
  private VoucherTypeService voucherTypeService;
  List<String> headerFieldsOrder = new ArrayList<String>();

  public void dumpLineToCsv(Map<String, Object> lineMap, ZipOutputStream writer, boolean addHeaders)
  {
    try
      {
        if (addHeaders)
          {
            addHeaders(writer, lineMap.keySet(), 1);
          }
        String line = ReportUtils.formatResult(lineMap);
        if (log.isTraceEnabled()) log.trace("Writing to csv file : " + line);
        writer.write(line.getBytes());
      } 
    catch (IOException e)
      {
        e.printStackTrace();
      }
  }

  public Map<String, List<Map<String, Object>>> getSplittedReportElementsForFileMono(Map<String, Object> map)
  {
    Map<String, List<Map<String, Object>>> result = new LinkedHashMap<String, List<Map<String, Object>>>();
    Map<String, Object> voucherPersonal = map;
    Date now = SystemTime.getCurrentTime();

    Map<String, Object> voucherInfo = new LinkedHashMap<String, Object>();
    
    if (voucherPersonal != null && !voucherPersonal.isEmpty())
      {
        Voucher voucher = voucherService.getActiveVoucher(voucherPersonal.get("voucherId").toString(), SystemTime.getCurrentTime());

        if (voucherPersonal.get("_id") != null)
          {
            voucherInfo.put("voucherCode", voucherPersonal.get("_id"));
          }
        else
          {
            voucherInfo.put("voucherCode", "");
          }
        if (voucher != null)
          {
            if (voucher.getSupplierID() != null)
              {
                Supplier supplier = (Supplier) supplierService.getStoredSupplier(voucher.getSupplierID());
                voucherInfo.put("supplier", supplier.getGUIManagedObjectDisplay());
              }
            else
              {
                voucherInfo.put("supplier", "");
              }

            if (voucherPersonal.get("expiryDate") != null)
              {
                Object expiryDateObj = voucherPersonal.get("expiryDate");
                if (expiryDateObj instanceof String)
                  {
                    voucherInfo.put("expiryDate", ReportsCommonCode.parseDate((String) expiryDateObj));
                  }
                else
                  {
                    log.info("expiryDate is of wrong type : " + expiryDateObj.getClass().getName());
                  }
              }
            else
              {
                voucherInfo.put("expiryDate", "");
              }
            
            Date expiryDate = null;
            if (voucherPersonal.get("expiryDate") != null)
              {
                Object expiryDateObj = voucherPersonal.get("expiryDate");
                SimpleDateFormat df = new SimpleDateFormat(Deployment.getAPIresponseDateFormat());
                df.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
                try
                  {
                    expiryDate = df.parse(ReportsCommonCode.parseDate((String) expiryDateObj));
                  }
                catch (ParseException e)
                  {
                    if(log.isDebugEnabled()) log.debug("Error in parsing the expiry Date string to Date");
                  }

              }

            if (expiryDate != null && expiryDate.before(now))
              {
                voucherInfo.put("voucherStatus", "expired");
              }
            else if (voucherPersonal.containsKey("subscriberId") && voucherPersonal.get("subscriberId") != null)
              {
                voucherInfo.put("voucherStatus", "delivered");
              }
            else 
              {
                voucherInfo.put("voucherStatus", "available");
              }

            if (voucher.getVoucherTypeId() != null)
              {
                VoucherType voucherType = (VoucherType) voucherTypeService
                    .getStoredVoucherType(voucher.getVoucherTypeId());
                voucherInfo.put("voucherType", voucherType.getGUIManagedObjectDisplay());
              }
            else
              {
                voucherInfo.put("voucherType", "");
              }
            
            JSONObject voucherJSON = voucher.getJSONRepresentation();
            JSONArray voucherFiles = (JSONArray) voucherJSON.get("voucherFiles");
            if (voucherFiles != null && !(voucherFiles.isEmpty()))
              {
                for (int i = 0; i < voucherFiles.size(); i++)
                  {
                    JSONObject voucherFile = (JSONObject) voucherFiles.get(i);
                    String currentFileID = voucherPersonal.get("fileId").toString();
                    if (voucherFile.get("fileId").equals(currentFileID))
                      {
                        voucherInfo.put("fileName", voucherFile.get("fileName").toString());
                        break;
                      }
                  }
              }
            else
              {
                voucherInfo.put("fileName", "");
              }
     
            //
            // result
            //
            List<Map<String, Object>> elements = new ArrayList<Map<String, Object>>();
            elements.add(voucherInfo);
            result.put(voucher.getSupplierID(), elements);

          }
      }
    return result;
  }

  private void addHeaders(ZipOutputStream writer, Set<String> headers, int offset) throws IOException
  {
    if (headers != null && !headers.isEmpty())
      {
        String header = "";
        for (String field : headers)
          {
            header += field + CSV_SEPARATOR;
          }
        header = header.substring(0, header.length() - offset);
        writer.write(header.getBytes());
        if (offset == 1)
          {
            writer.write("\n".getBytes());
          }
      }
  }
  
  public static void main(String[] args, Date reportGenerationDate)
  {
    VoucherUploadedReportMonoPhase voucherUploadedReportMonoPhase = new VoucherUploadedReportMonoPhase();
    voucherUploadedReportMonoPhase.start(args, reportGenerationDate);
  }
  
  private void start(String[] args, final Date reportGenerationDate)
  {
    if(log.isInfoEnabled())
    log.info("received " + args.length + " args");
    for (String arg : args)
      {
        log.info("VoucherUploadedMonoPhase: arg " + arg);
      }

    if (args.length < 4)
      {
        if(log.isWarnEnabled())
        log.warn("Usage : VoucherUploadedMonoPhase <ESNode> <ES customer index> <csvfile> <defaultReportPeriodQuantity> <defaultReportPeriodUnit>");
        return;
      }
    String esNode          = args[0];
    String esIndexVoucher  = args[1];
    String csvfile         = args[2];
    
    
    String supplierTopic = Deployment.getSupplierTopic();
    String voucherTypeTopic = Deployment.getVoucherTypeTopic();

    supplierService = new SupplierService(Deployment.getBrokerServers(), "vdrreportcsvwriter-productService-VDRReportMonoPhase", supplierTopic, false);
    
    voucherTypeService = new VoucherTypeService(Deployment.getBrokerServers(), "vdrreportcsvwriter-productService-VDRReportMonoPhase", voucherTypeTopic, false);
    
    voucherService = new VoucherService(Deployment.getBrokerServers(), "voucherUploadedReport-voucherservice-voucherUploadedReportMonoDriver", Deployment.getVoucherTopic());
   
    
    supplierService.start();
    voucherTypeService.start();
    voucherService.start();

    try {
      Collection<Voucher> activeVouchers = voucherService.getActiveVouchers(reportGenerationDate);
      StringBuilder activeVoucherEsIndex = new StringBuilder();
      boolean firstEntry = true;
      for (Voucher voucher : activeVouchers)
        {
          if (!firstEntry) activeVoucherEsIndex.append(",");
          String indexName = esIndexVoucher + voucher.getSupplierID(); 
          activeVoucherEsIndex.append(indexName);
          firstEntry = false;
        }

      log.info("Reading data from ES in (" + activeVoucherEsIndex.toString() + ") and writing to " + csvfile);
      LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
      esIndexWithQuery.put(activeVoucherEsIndex.toString(), QueryBuilders.matchAllQuery());

      ReportMonoPhase reportMonoPhase = new ReportMonoPhase(
          esNode,
          esIndexWithQuery,
          this,
          csvfile
          );

      if (!reportMonoPhase.startOneToOne(true))
        {
          if(log.isWarnEnabled())
            log.warn("An error occured, the report might be corrupted");
        }
    } finally {
      supplierService.stop();
      voucherTypeService.stop();
      voucherService.stop();

      if(log.isInfoEnabled()) log.info("Finished VoucherUploadedReportESReader");
    }
  }

}
