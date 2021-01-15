/****************************************************************************
 *
 *  ReportsCommonCode.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports;

import com.evolving.nglm.evolution.Deployment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class ReportsCommonCode
{
  
  public static List<SimpleDateFormat> standardDateFormats = new ArrayList<SimpleDateFormat>();
  public static final ThreadLocal<SimpleDateFormat> deploymentDateFormat = ThreadLocal.withInitial(
      () -> {
        SimpleDateFormat sdf = new SimpleDateFormat(Deployment.getAPIresponseDateFormat());
        sdf.setTimeZone(TimeZone.getTimeZone(Deployment.getSystemTimeZone())); // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct or should it be per tenant ???
        return sdf;
      });

  public static final Logger log = LoggerFactory.getLogger(ReportsCommonCode.class);

  /****************************************
  *
  *  initializeDateFormats
  *
  ****************************************/
  
  public static void initializeDateFormats()
  {
    synchronized (standardDateFormats)
    {
      if (standardDateFormats.isEmpty())
        {
          standardDateFormats.add(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSSXXX"));
          standardDateFormats.add(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"));
          standardDateFormats.add(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX"));
          standardDateFormats.add(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSXX"));
          for (SimpleDateFormat standardDateFormat : standardDateFormats)
            {
              standardDateFormat.setTimeZone(TimeZone.getTimeZone(Deployment.getSystemTimeZone())); // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct or should it be per tenant ???
            }
        }
    }
  }

  /****************************************
  *
  *  parseDate
  *
  ****************************************/
  
  public static String parseDate(String dateString)
  {
    String result = dateString;
    if (dateString != null)
      {
        boolean ableToParse = false;
        for (SimpleDateFormat standardDateFormat : standardDateFormats)
          {
            synchronized (standardDateFormat)
              {
                try
                  {
                    Date date = standardDateFormat.parse(dateString.trim());
                    result = deploymentDateFormat.get().format(date);
                    ableToParse = true;
                    break;
                  }
                catch (ParseException e)
                  {
                    // just ignore, and try next format
                  }
              }
          }
        if (!ableToParse)
          {
            log.info("Unable to parse " + dateString);
          }
      }
    else
      {
        result = "";
      }
    return result;
  }
  
  /*****************************************
  *
  *  getDateString
  *
  *****************************************/
 
 public static String getDateString(Date date)
 {
   String result = null;
   if (date == null) return result;
   try
   {
     result = deploymentDateFormat.get().format(date);
   }
   catch (Exception e)
   {
     log.warn(e.getMessage());
   }
   return result;
 }
}
