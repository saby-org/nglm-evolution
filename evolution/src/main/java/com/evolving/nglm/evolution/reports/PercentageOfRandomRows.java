package com.evolving.nglm.evolution.reports;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PercentageOfRandomRows {
	
	private static Logger log = LoggerFactory.getLogger(PercentageOfRandomRows.class);

	public static void extractPercentageOfRandomRows(String inputFileName, String outputFileName,
			int percentage) {

		int totalNmbrOfLinesInFile = 0;
		BufferedReader br;

		try
		{
			br = new BufferedReader(new FileReader(inputFileName));

			while((br.readLine()) != null) 
			{
				totalNmbrOfLinesInFile++;
			}
			br.close();

			BufferedWriter bw = new BufferedWriter(new FileWriter(outputFileName));
			SamplerNRandomRows mySamplerOfRandomRows = new SamplerNRandomRows();

			if (totalNmbrOfLinesInFile < 1)
			{
				log.info("The file is empty and thus no lines to be displayed!");
			}
			else if (percentage < 1 || percentage > 99)
			{
				log.info("The required percentage of lines is expected to be between 1 and 99 !");
			}
			else
			{

				int nbLinesToExtract = (totalNmbrOfLinesInFile * percentage) / 100;
				int nbLinesEffective = 1;

				if (nbLinesToExtract >= 1)
				{
					nbLinesEffective = (int) Math.ceil(nbLinesToExtract);
				}

				List<String> myListOfRandomRows = mySamplerOfRandomRows.sampler(inputFileName, nbLinesEffective);
				for (int index = 0; index < myListOfRandomRows.size(); index++)
				{
					bw.write(myListOfRandomRows.get(index) + "\n");
				}
			}

			bw.close();
		}
		catch (FileNotFoundException e) 
		{
			log.error("The file doesn't exist!", e);
		}
		catch (Exception e)
		{
			log.error("File is empty!", e);
		}
	}
}
	
	
