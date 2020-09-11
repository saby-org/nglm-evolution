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

	public static void subsetOfCols(String InputFileName, String OutputFileName, List<String> colsNames,
			String separator, String fileSurrounder) {

		int[] indexOfColsToExtract = new int[colsNames.size()];

		try 
		{
			List<String> headerList = new ArrayList<>();

			BufferedReader br = new BufferedReader(new FileReader(InputFileName));
			BufferedWriter bw = new BufferedWriter(new FileWriter(OutputFileName));

			if (colsNames.size() != 0) 
			{
				String[] words = br.readLine().split(separator, -1);
				List<String> colsNamesFoundInHeader = new ArrayList<>();

				for (int i = 0; i < words.length; i++) 
				{
					headerList.add(words[i].replaceAll("\\\\'", "'"));// replace the \' in the header with an '
				}

				boolean colsFound = false;
				int i = 0;
				for (String col : colsNames) 
				{
					int headerIndex = 0;
					for (String word : headerList) 
					{
						if (word.equals(col)) 
						{
							colsNamesFoundInHeader.add(word);
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
					List<String> sortedColsNamesFoundInHeader = new ArrayList<>();
					for (int j = 0; j < indexOfColsToExtract.length; j++) 
					{
						sortedColsNamesFoundInHeader.add(headerList.get(indexOfColsToExtract[j]));
					}
					bw.write(sortedColsNamesFoundInHeader.toString().substring(1,
							sortedColsNamesFoundInHeader.toString().length() - 1) + "\n");

					String colsToExtract = "";
					String line;
					while ((line = br.readLine()) != null) {
						if (line.length() != 0) 
						{
							String regex = separator + "(?=(?:[^\\" + fileSurrounder + "]*\\" + fileSurrounder + "[^\\"
									+ fileSurrounder + "]*\\" + fileSurrounder + ")*[^\\" + fileSurrounder + "]*$)";
							String[] cols = line.split(regex, -1);

							for (int cpt = 0; cpt < indexOfColsToExtract.length; cpt++) 
							{
								if (indexOfColsToExtract[cpt] != -1) 
								{
									colsToExtract = colsToExtract + cols[indexOfColsToExtract[cpt]] + ",";
								}
							}
							bw.write(colsToExtract);
							bw.write("\n");
							colsToExtract = "";

						}
					}

				} 
				else 
				{
					log.error("None of the demanded columns: " + colsNames + " exist in the report in use: "
							+ InputFileName);
				}
			} 
			else 
			{
				log.error("The column names array is empty!");
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
