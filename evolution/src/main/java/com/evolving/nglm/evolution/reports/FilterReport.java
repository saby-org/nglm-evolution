package com.evolving.nglm.evolution.reports;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterReport {

  private static final Logger log = LoggerFactory.getLogger(FilterReport.class);

  public static void filterReport(String InputFileName, String OutputFileName,
      List<String> colsName, List<List<String>> colsValues, String fieldSeparator, String fieldSurrounder) 
  {

    if (colsName.size() == colsValues.size()) 
      {
        int[] numOfColm = new int[colsName.size()];
        try 
        {
          BufferedReader br = new BufferedReader(new FileReader(InputFileName));

          List<String> headerList = new ArrayList<>();
          String headerVerbatim = br.readLine();
          if (headerVerbatim == null)
            {
              log.error("The file "+InputFileName+" is empty");
              br.close();
              return;
            }
          String[] words = headerVerbatim.split(fieldSeparator, -1);

          for (int i = 0; i < words.length; i++) 
            {
              headerList.add(words[i].replaceAll("\\\\'", "'"));
            }

          log.debug("Check the existence of columns: " + colsName);
          boolean colsFound = false;
          int i = 0;
          for (String col : colsName) 
            {
              int colIndex = 0;
              for (String word : headerList) 
                {
                  if (word.equals(col)) 
                    {
                      colsFound = true;
                      numOfColm[i++] = colIndex;
                      break;
                    }
                  colsFound = false;
                  colIndex++;
                }
              if (!colsFound) 
                {
                  log.error("No filter is applied since column (" + col + ") doesn't exist in the report (" + InputFileName+")");
                  break;					
                }
            }

          if (colsFound) 
            {
              log.debug("Look for matching values: "+colsValues);
              BufferedWriter bw = new BufferedWriter(new FileWriter(OutputFileName));
              String line;

              // write header
              bw.write(headerVerbatim + "\n");

              while ((line = br.readLine()) != null) 
                {
                  if (line.length() != 0) {
                    String surrounder = Pattern.quote(fieldSurrounder);
                    String separator = Pattern.quote(fieldSeparator);
                    // :(?=(?:[^X]*X[^X]*X)*[^X]*$)
                    String regex = separator + "(?=(?:[^" + surrounder + "]*" + surrounder + "[^" + surrounder + "]*" + surrounder + ")*[^" + surrounder + "]*$)";
                    String[] cols = line.split(regex);
                    i = 0;
                    boolean filterInvalid = false;
                    for (List<String> listOfColsValues : colsValues) 
                      {
                        String valueToTest = cols[numOfColm[i]].replaceAll("^.|.$", "");
                        if (!listOfColsValues.contains(valueToTest)) 
                          {
                            filterInvalid = true;
                            break;
                          }
                        i++;
                      }
                    if (!filterInvalid) 
                      {
                        bw.write(line + "\n");
                      }
                  }
                }

              br.close();
              bw.close();
            }
        } 
        catch (FileNotFoundException e) 
        {
          log.error("The file "+InputFileName+"  doesn't exist!", e);
        }
        catch (IOException e)
          {
            log.error("Exception while processing " + InputFileName + " or " + OutputFileName, e);
          }
      } 
    else 
      {
        log.error("size of column names ("+colsName.size()+ ") != size of column values ("+colsValues+").");
      }
  }
}
