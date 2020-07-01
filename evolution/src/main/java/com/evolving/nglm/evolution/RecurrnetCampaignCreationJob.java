/****************************************************************************
*
*  RecurrnetCampaignCreationJob.java 
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;

public class RecurrnetCampaignCreationJob extends ScheduledJob
{
  //
  // log
  //

  private static final Logger log = LoggerFactory.getLogger(RecurrnetCampaignCreationJob.class);

  //
  // data
  //

  private JourneyService journeyService;

  /***********************************
   *
   * constructor
   *
   ************************************/

  public RecurrnetCampaignCreationJob(long schedulingUniqueID, String jobName, String periodicGenerationCronEntry, String baseTimeZone, boolean scheduleAtStart, JourneyService journeyService)
  {
    super(schedulingUniqueID, jobName, periodicGenerationCronEntry, baseTimeZone, scheduleAtStart);
    this.journeyService = journeyService;
  }

  /***********************************
   *
   * run
   *
   ************************************/

  @Override
  protected void run()
  {
    if (log.isInfoEnabled()) log.info("creating recurrent campaigns");
    Date now = SystemTime.getCurrentTime();
    now = RLMDateUtils.truncate(now, Calendar.DATE, Deployment.getBaseTimeZone());
    Collection<Journey> recurrentJourneys = journeyService.getActiveRecurrentJourneys(now);
    for (Journey recurrentJourney : recurrentJourneys)
      {
        List<Date> journeyCreationDates = new ArrayList<Date>();
        JourneyScheduler journeyScheduler = recurrentJourney.getJourneyScheduler();
        Journey latestJourney = journeyService.getLatestRecurrentJourney(recurrentJourney.getGUIManagedObjectID());
        int limitCount = journeyScheduler.getNumberOfOccurrences() - latestJourney.getOccurrenceNumber();

        //
        // limit reached
        //

        if (limitCount <= 0) continue;
        log.info("RAJ K creating recurrent campaign limit ok");

        //
        // scheduling
        //

        String scheduling = journeyScheduler.getRunEveryUnit().toLowerCase();
        Integer scheduligInterval = journeyScheduler.getRunEveryDuration();

        log.info("RAJ K creating recurrent campaign for {} and scheduling {} scheduligInterval {}", recurrentJourney.getJourneyID(), scheduling, scheduligInterval);
        if ("week".equalsIgnoreCase(scheduling))
          {
            Date firstDateOfThisWk = getFirstDate(now, Calendar.DAY_OF_WEEK);
            Date lastDateOfThisWk = getLastDate(now, Calendar.DAY_OF_WEEK);

            //
            // nextExpectedDate
            //

            Date nextExpectedDate = RLMDateUtils.addWeeks(recurrentJourney.getEffectiveStartDate(), scheduligInterval, Deployment.getBaseTimeZone());
            while (nextExpectedDate.before(firstDateOfThisWk))
              {
                nextExpectedDate = RLMDateUtils.addWeeks(nextExpectedDate, scheduligInterval, Deployment.getBaseTimeZone());
              }

            //
            // not in this week
            //
            log.info("RAJ K nextExpectedDate {} ", nextExpectedDate);
            if (RLMDateUtils.truncate(nextExpectedDate, Calendar.DATE, Deployment.getBaseTimeZone()).after(lastDateOfThisWk)) continue;
            log.info("RAJ K nextExpectedDate is ok");

            //
            // this is the week
            //

            List<Date> expectedCreationDates = getExpectedCreationDates(firstDateOfThisWk, lastDateOfThisWk, scheduling, journeyScheduler.getRunEveryWeekDay());

            //
            // journeyCreationDates
            //

            Collection<Journey> recurrentSubJourneys = journeyService.getAllRecurrentJourneysByID(recurrentJourney.getJourneyID());
            for (Date expectedDate : expectedCreationDates)
              {
                boolean exists = false;
                for (Journey subJourney : recurrentSubJourneys)
                  {
                    exists = RLMDateUtils.truncatedCompareTo(expectedDate, subJourney.getEffectiveStartDate(), Calendar.DATE, Deployment.getBaseTimeZone()) == 0;
                    if (exists)
                      break;
                  }
                if (!exists && limitCount > 0)
                  {
                    journeyCreationDates.add(expectedDate);
                    limitCount--;
                  }
              }
          } else if ("month".equalsIgnoreCase(scheduling))
          {
            Date firstDateOfThisMonth = getFirstDate(now, Calendar.DAY_OF_MONTH);
            Date lastDateOfThisMonth = getLastDate(now, Calendar.DAY_OF_MONTH);

            //
            // nextExpectedDate
            //

            Date nextExpectedDate = RLMDateUtils.addMonths(recurrentJourney.getEffectiveStartDate(), scheduligInterval, Deployment.getBaseTimeZone());
            while (nextExpectedDate.before(firstDateOfThisMonth))
              {
                nextExpectedDate = RLMDateUtils.addMonths(nextExpectedDate, scheduligInterval, Deployment.getBaseTimeZone());
              }

            //
            // not in this month
            //
            log.info("RAJ K nextExpectedDate {} ", nextExpectedDate);
            if (RLMDateUtils.truncate(nextExpectedDate, Calendar.DATE, Deployment.getBaseTimeZone()).after(lastDateOfThisMonth))
              continue;
            log.info("RAJ K nextExpectedDate is ok");
            //
            // this is the month
            //

            List<Date> expectedCreationDates = getExpectedCreationDates(firstDateOfThisMonth, lastDateOfThisMonth, scheduling, journeyScheduler.getRunEveryMonthDay());

            //
            // journeyCreationDates
            //

            Collection<Journey> recurrentSubJourneys = journeyService.getAllRecurrentJourneysByID(recurrentJourney.getJourneyID());
            for (Date expectedDate : expectedCreationDates)
              {
                boolean exists = false;
                for (Journey subJourney : recurrentSubJourneys)
                  {
                    exists = RLMDateUtils.truncatedCompareTo(expectedDate, subJourney.getEffectiveStartDate(), Calendar.DATE, Deployment.getBaseTimeZone()) == 0;
                    if (exists) break;
                  }
                if (!exists && limitCount > 0)
                  {
                    journeyCreationDates.add(expectedDate);
                    limitCount--;
                  }
              }
          }

        //
        // createJourneys
        //

        createJourneys(recurrentJourney, journeyCreationDates, latestJourney);
      }
    if (log.isInfoEnabled())log.info("created recurrent campaigns");
  }
  
  //
  //  createJourneys
  //
  
  private void createJourneys(Journey recurrentJourney, List<Date> journeyCreationDates, Journey latestJourney)
  {
    log.info("RAJ K createingJourneys of {}, for {}", recurrentJourney.getJourneyID(), journeyCreationDates);
    int daysBetween = RLMDateUtils.daysBetween(recurrentJourney.getEffectiveStartDate(), recurrentJourney.getEffectiveEndDate(), Deployment.getBaseTimeZone());
    int occurrenceNumber = latestJourney.getOccurrenceNumber();
    for (Date startDate : journeyCreationDates)
      {
        JSONObject journeyJSON = (JSONObject) journeyService.getJSONRepresentation(recurrentJourney).clone();
        journeyJSON.put("apiVersion", 1);
        
        //
        //  remove
        //
        
        journeyJSON.remove("recurrence");
        journeyJSON.remove("scheduler");
        journeyJSON.remove("status");
        
        //
        //  add
        //
        
        String journeyID = journeyService.generateJourneyID();
        journeyJSON.put("id", journeyID);
        journeyJSON.put("occurrenceNumber", ++occurrenceNumber);
        journeyJSON.put("name", recurrentJourney.getGUIManagedObjectName() + "_" + occurrenceNumber);
        journeyJSON.put("display", recurrentJourney.getGUIManagedObjectDisplay() + "_" + occurrenceNumber);
        journeyJSON.put("effectiveStartDate", recurrentJourney.formatDateField(startDate));
        journeyJSON.put("effectiveEndDate", recurrentJourney.formatDateField(RLMDateUtils.addDays(startDate, daysBetween, Deployment.getBaseTimeZone())));
        
        //
        //  create and activate
        //
        
        processPutJourney("0", journeyJSON, recurrentJourney.getGUIManagedObjectType());
        processSetActive("0", journeyJSON, recurrentJourney.getGUIManagedObjectType(), true);
      }
  }
  
  //
  //  getExpectedCreationDates
  //
  
  private List<Date> getExpectedCreationDates(Date firstDate, Date lastDate, String scheduling, List<String> runEveryDay)
  {
    List<Date> result = new ArrayList<Date>();
    while (firstDate.before(lastDate) || firstDate.compareTo(lastDate) == 0)
      {
        int day = -1;
        switch (scheduling)
          {
            case "week":
              day = RLMDateUtils.getField(firstDate, Calendar.DAY_OF_WEEK, Deployment.getBaseTimeZone());
              break;
              
            case "month":
              day = RLMDateUtils.getField(firstDate, Calendar.DAY_OF_MONTH, Deployment.getBaseTimeZone());
              break;

            default:
              break;
        }
        String dayOf = String.valueOf(day);
        if (runEveryDay.contains(dayOf)) result.add(new Date(firstDate.getTime()));
        firstDate = RLMDateUtils.addDays(firstDate, 1, Deployment.getBaseTimeZone());
      }
    
    //
    //  handle last date of month
    //
    
    if ("month".equalsIgnoreCase(scheduling))
      {
        int lastDayOfMonth = RLMDateUtils.getField(lastDate, Calendar.DAY_OF_MONTH, Deployment.getBaseTimeZone());
        for (String day : runEveryDay)
          {
            if (Integer.parseInt(day) > lastDayOfMonth) result.add(new Date(lastDate.getTime()));
          }
      }
    log.info("RAJ K getExpectedCreationDates {}", result);
    return result;
  }

  //
  //  getFirstDate
  //
  
  private Date getFirstDate(Date now, int dayOf)
  {
    if (Calendar.DAY_OF_WEEK == dayOf)
      {
        Date firstDateOfNext = RLMDateUtils.ceiling(now, dayOf, Deployment.getBaseTimeZone());
        return RLMDateUtils.addDays(firstDateOfNext, -7, Deployment.getBaseTimeZone());
      }
    else
      {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
        c.setTime(now);
        int dayOfMonth = RLMDateUtils.getField(now, Calendar.DAY_OF_MONTH, Deployment.getBaseTimeZone());
        Date firstDate = RLMDateUtils.addDays(now, -dayOfMonth+1, Deployment.getBaseTimeZone());
        return firstDate;
      }
  }
  
  //
  //  getLastDate
  //
  
  private Date getLastDate(Date now, int dayOf)
  {
    Date firstDateOfNext = RLMDateUtils.ceiling(now, dayOf, Deployment.getBaseTimeZone());
    if (Calendar.DAY_OF_WEEK == dayOf)
      {
        Date firstDateOfthisWk = RLMDateUtils.addDays(firstDateOfNext, -7, Deployment.getBaseTimeZone());
        return RLMDateUtils.addDays(firstDateOfthisWk, 6, Deployment.getBaseTimeZone());
      }
    else
      {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
        c.setTime(now);
        int toalNoOfDays = c.getActualMaximum(Calendar.DAY_OF_MONTH);
        int dayOfMonth = RLMDateUtils.getField(now, Calendar.DAY_OF_MONTH, Deployment.getBaseTimeZone());
        Date firstDate = RLMDateUtils.addDays(now, -dayOfMonth+1, Deployment.getBaseTimeZone());
        Date lastDate = RLMDateUtils.addDays(firstDate, toalNoOfDays-1, Deployment.getBaseTimeZone());
        return lastDate;
      }
  }

}
