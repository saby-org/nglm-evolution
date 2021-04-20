package com.evolving.nglm.evolution.reports;

import java.text.ParseException;
import java.util.Comparator;
import java.util.Date;

import com.evolving.nglm.core.RLMDateUtils;

public class ReportGenerationDateComperator implements Comparator<String>
{

  @Override
  public int compare(String o1, String o2)
  {
    String[] splittedString1 =  o1.split("_", -1);
    String[] splittedString2 =  o2.split("_", -1);
    if (splittedString2.length > 0 && splittedString1.length > 0)
      {
        try
          {
            Date date1 = RLMDateUtils.DAY_FORMAT.get().parse(splittedString1[1]);
            Date date2 = RLMDateUtils.DAY_FORMAT.get().parse(splittedString2[1]);
            return date2.compareTo(date1);
          } 
        catch (ParseException e)
          {
            return 0;
          }        
      }
    else
      {
        return 0;
      }
  }

}
