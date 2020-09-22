package com.evolving.nglm.evolution.reports;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopRows {
	
	private static Logger log = LoggerFactory.getLogger(TopRows.class);
	
	public static void extractTopRows(String inputFile, String outputFile, int topRows) 
	{
		try
		{
			String strLine;
			int i = 0;
			BufferedReader br = new BufferedReader(new FileReader(inputFile));
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
			while ((strLine = br.readLine()) != null) 
			{
				if (i++ < topRows) 
				{
					bw.write(strLine);
					bw.write("\n");
				}
				else
					break;

			}
			br.close();
			bw.close();

		}
		catch (FileNotFoundException e) 
		{
			log.error("File doesn't exist", e);
		}
		catch (IOException e) 
		{
			log.error("Error processing " + inputFile + " or " + outputFile, e);
		}
	}
}
