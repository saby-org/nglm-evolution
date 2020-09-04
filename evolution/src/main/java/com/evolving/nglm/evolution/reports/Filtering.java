package com.evolving.nglm.evolution.reports;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Filtering {
	
	private static final Logger log = LoggerFactory.getLogger(Filtering.class);

	public static void filteringReport(String InputFileName, String OutputFileName,
			List<String> colsName, List<List<String>> colsValues, String delimiter, String separator) 
	{
		if (colsName.size() == colsValues.size())
		{
			int[] numOfColm = new int[colsName.size()];
			BufferedReader br;
			try 
			{
				br = new BufferedReader(new FileReader(InputFileName));
			
			List<String> headerList = new ArrayList<>();
			String[] words = br.readLine().split("delimiter", -1);

			for (int i = 0; i < words.length; i++) 
			{
				headerList.add(words[i].replaceAll("\\\\'", "'"));
			}

			// checking for the column name in the header
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
					colIndex++;
				}
				if (!colsFound) 
				{
					log.error("Noneexistent column!");
				}
			}

			// checking for the column values
			BufferedWriter bw = new BufferedWriter(new FileWriter(OutputFileName));
			String line;
			if(separator == "'")
			{
				bw.write(headerList.toString().substring(1, headerList.toString().length() - 1) + "\n");
				while ((line = br.readLine()) != null) 
				{
					if (line.length() != 0)  
					{
						String regex = delimiter + "(?=(?:[^\\" + separator + "]*\\" + separator + "[^\\" + separator
								+ "]*\\" + separator + ")*[^\\" + separator + "]*$)";
						String[] cols = line.split(regex, -1);

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
			}
			else
			{
				log.error("The separator should be a single quote (').");
			}

			br.close();
			bw.close();
			} 
			catch (FileNotFoundException e) 
			{
				log.info("The file doesn't exist", e);
			} 
			catch (Exception e) 
			{
				e.printStackTrace();
			}
		} 
		else 
		{
			log.error("Column Names and column values should be of the same size.");
		}
	}
}
