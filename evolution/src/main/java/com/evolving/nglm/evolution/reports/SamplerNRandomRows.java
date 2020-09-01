package com.evolving.nglm.evolution.reports;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamplerNRandomRows {
	
	private static Logger log = LoggerFactory.getLogger(SamplerNRandomRows.class);

	public List<String> sampler(String fileName, int nmbrOfRowsToBeExtracted) 
	{
		String currentRow = null;
		List<String> listOFRandomlyExtractedRows = new ArrayList<String>(nmbrOfRowsToBeExtracted);
		int cptCurrentRowNmbr = 0;

		try 
		{
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		int totalNmbrOfRowsInFile = 0;

		while((br.readLine()) != null) {
			totalNmbrOfRowsInFile++;
		}
		br.close();


		Random ra = new Random();
		int randomNumber = 0;
		Scanner sc = new Scanner(new File(fileName)).useDelimiter("\n");

		if (nmbrOfRowsToBeExtracted <= 0) 
		{
			log.info("The number of rows to be extracted should be > 0!");
		}

		if (nmbrOfRowsToBeExtracted > totalNmbrOfRowsInFile) 
		{
			log.info("The number of rows to be extracted should be > 0 but < totalNumberOfLines !");
		}

		if (nmbrOfRowsToBeExtracted > 0 && nmbrOfRowsToBeExtracted < totalNmbrOfRowsInFile) 
		{

			while (sc.hasNext()) 
			{
				currentRow = sc.next();
				cptCurrentRowNmbr++; 

				if (cptCurrentRowNmbr <= nmbrOfRowsToBeExtracted) 
				{
					listOFRandomlyExtractedRows.add(currentRow);

				} 
				else if ((randomNumber = (int) ra.nextInt(cptCurrentRowNmbr)) < nmbrOfRowsToBeExtracted) 
				{
					listOFRandomlyExtractedRows.set(randomNumber, currentRow);
				}
			}
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
		return listOFRandomlyExtractedRows;
	}
}
