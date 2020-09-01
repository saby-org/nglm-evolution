package com.evolving.nglm.evolution.reports;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PercentageOfRandomRows {
	
	private static Logger log = LoggerFactory.getLogger(PercentageOfRandomRows.class);
	
	public static void extractPercentageOfRandomRows(String inputFileName, String outputFileName,
			int percentage) 
	{
		int totalNmbrOfLinesInFile = 0;
		BufferedReader br;
		
		try 
		{
			br = new BufferedReader(new FileReader(inputFileName));
			
					{
						totalNmbrOfLinesInFile++;
					} br.close();
				 
				
		SamplerNRandomRows mySamplerOfRandomRows = new SamplerNRandomRows();

		if (totalNmbrOfLinesInFile < 1) 
		{
			log.info("The file is empty and thus no lines to be displayed!");
		}

		if (percentage < 1 || percentage > 99) 
		{
			log.info("The required percentage of lines is expected to be between 1 and 99 !");
		}

		if ((totalNmbrOfLinesInFile * percentage) / 100 < 1) 
		{
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputFileName));
			List<String> myListOfRandomRows = mySamplerOfRandomRows.sampler(inputFileName, 1);
			for (int index = 0; index < myListOfRandomRows.size(); index++) 
			{
				bw.write(myListOfRandomRows.get(index));
				bw.write("\n");
			}
			bw.close();

		} else 
		{
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputFileName));
			List<String> myListOfRandomRows = mySamplerOfRandomRows.sampler(inputFileName,
					(int) Math.ceil((totalNmbrOfLinesInFile * percentage) / 100));
			for (int index = 0; index < myListOfRandomRows.size(); index++) {
				bw.write(myListOfRandomRows.get(index));
				bw.write("\n");
			}
			bw.close();
		}
		}
		catch (FileNotFoundException e) 
		{
			log.error("The file doesn't exist!", e);
		}
		catch (IOException e) 
		{
			log.error("File is empty!");
		}
	}
}
