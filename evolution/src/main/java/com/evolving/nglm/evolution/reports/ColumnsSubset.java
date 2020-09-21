package com.evolving.nglm.evolution.reports;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnsSubset {

	private static final Logger log = LoggerFactory.getLogger(ColumnsSubset.class);

	public static void subsetOfCols(String InputFileName, String OutputFileName, List<String> columnNames,
			String fieldSeparator, String fieldSurrounder) {

		int[] indexOfColsToExtract = new int[columnNames.size()];

		try 
		{
			List<String> headerList = new ArrayList<>();

			BufferedReader br = new BufferedReader(new FileReader(InputFileName));
			BufferedWriter bw = new BufferedWriter(new FileWriter(OutputFileName));

			if (columnNames.size() != 0) 
			{
				String[] wordsOfHeader = br.readLine().split(fieldSeparator, -1);
				List<String> colNamesInHeader = new ArrayList<>();

				for (int i = 0; i < wordsOfHeader.length; i++) 
				{
					headerList.add(wordsOfHeader[i].replaceAll("\\\\", ""));// remove the \ from the header if exist
				}

				boolean colsFound = false;
				int i = 0;
				for (String col : columnNames) 
				{
					int headerIndex = 0;
					for (String word : headerList) 
					{
						if (word.equals(col)) 
						{
							colNamesInHeader.add(word);
							colsFound = true;

							indexOfColsToExtract[i++] = headerIndex;
							break;
						} 
						else 
						{
							indexOfColsToExtract[i] = -1;
						}
						headerIndex++;
						if (headerIndex == headerList.size()) 
						{
							indexOfColsToExtract[i] = -1;
							i++;
							break;
						}
					}
				}
				if (colsFound) 
				{
					Arrays.sort(indexOfColsToExtract);
					List<String> sortedColsNamesInHeader = new ArrayList<>();
					for (int j = 0; j < indexOfColsToExtract.length; j++) 
					{
						if (indexOfColsToExtract[j] != -1) 
						{
						sortedColsNamesInHeader.add(headerList.get(indexOfColsToExtract[j]));
						}
					}
					
					// keep the same format of the header in the output file
					for (int j = 0; j < sortedColsNamesInHeader.size() - 1; j++) 
					{
						bw.write(sortedColsNamesInHeader.get(j).concat(fieldSeparator));
					}
					bw.write(sortedColsNamesInHeader.get(sortedColsNamesInHeader.size() - 1));

					String colsToExtract = "";
					String line;
					while ((line = br.readLine()) != null) {
						if (line.length() != 0) 
						{
							String regex = fieldSeparator + "(?=(?:[^\\" + fieldSurrounder + "]*\\" + fieldSurrounder + "[^\\"
									+ fieldSurrounder + "]*\\" + fieldSurrounder + ")*[^\\" + fieldSurrounder + "]*$)";
							String[] cols = line.split(regex, -1);

							for (int cpt = 0; cpt < indexOfColsToExtract.length -1; cpt++) 
							{
								if (indexOfColsToExtract[cpt] != -1) 
								{
									colsToExtract = colsToExtract + cols[indexOfColsToExtract[cpt]] + fieldSeparator;
								}
							}
							bw.write("\n");
							bw.write(colsToExtract);
							bw.write(cols[indexOfColsToExtract[indexOfColsToExtract.length-1]]); // to avoid having a comma at the end of each written line
							
							colsToExtract = "";

						}
					}
				} 
				else 
				{
					log.error("The columns: " + columnNames + " don't exist in  "
							+ InputFileName);
				}
			} 
			else 
			{
				log.error("No column names were specified!");
			}
			br.close();
			bw.close();
		} catch (FileNotFoundException e) {
			log.error("The file " + InputFileName + " doesn't exist!", e);
		} catch (Exception e) {
			log.error("The file " + InputFileName + " is empty!", e);
		}
	}
}
