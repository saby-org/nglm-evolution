package com.evolving.nglm.evolution.reports;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class PercentageOfRandomLines {
	public static void displayPercentageOfRandomLines(String inputFileName, String outputFileName,
			int percentage) throws IOException {

		ReadFile file = new ReadFile(inputFileName);
		int totalNmbrOfLines = file.readLines();

		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFileName));

		SamplerNRandomLines mySampler = new SamplerNRandomLines();

		if (new File(inputFileName).exists() == false) {
			throw new IllegalArgumentException("ERROR: The file doesn't exist!");
		}

		else if (new File(inputFileName).length() == 0) {
			throw new IllegalArgumentException("ERROR: The file is empty!");
		}

		else if (totalNmbrOfLines < 1) {
			throw new IllegalArgumentException("ERROR: The file is empty and thus no lines to be displayed!");
		}

		else if (percentage < 1) {
			throw new IllegalArgumentException(
					"ERROR: The required percentage of lines is expected to be between 1 and 99 !");
		}

		else if (percentage > 99) {
			throw new IllegalArgumentException(
					"ERROR: The required percentage of lines is expected to be between 1 and 99 !");
		}

		else if ((totalNmbrOfLines * percentage) / 100 < 1) {
			List<String> myList = mySampler.sampler(inputFileName, 1);
			for (int index = 0; index < myList.size(); index++) {
				bw.write(myList.get(index));
				bw.write("\n");
			}
			bw.close();

		} else {
			List<String> myList = mySampler.sampler(inputFileName,
					(int) Math.ceil((totalNmbrOfLines * percentage) / 100));
			for (int index = 0; index < myList.size(); index++) {
				bw.write(myList.get(index));
				bw.write("\n");
			}
			bw.close();
		}
	}
}
