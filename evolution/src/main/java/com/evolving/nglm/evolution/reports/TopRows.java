package com.evolving.nglm.evolution.reports;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class TopRows {

	public static void readFirstNLines(String inputFileName, String outputFileName, int topRows) throws Exception {

		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFileName));

		ReadFile file = new ReadFile(inputFileName);
		String[] aryLines = file.openFile();
		int totalNmbrOfLines = file.readLines();
		int i;
		if (!(new File(inputFileName).exists())) {
			throw new IllegalArgumentException("ERROR: The file doesn't exist!");
		}

		else if (new File(inputFileName).length() == 0) {
			throw new IllegalArgumentException("ERROR: The file is empty!");
		}

		else if (topRows > totalNmbrOfLines || topRows <= 0) {
			throw new IllegalArgumentException(
					"ERROR: The number of rows to be displayed (topRows) should be 0 < topRows < totalNmbrOfLines!");
		} else {
			for (i = 0; i < topRows; i++) {
				bw.write(aryLines[i]);
				bw.write("\n");
			}
			bw.close();
		}

	}

}
