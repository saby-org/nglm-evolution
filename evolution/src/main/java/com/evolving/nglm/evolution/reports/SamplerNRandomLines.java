package com.evolving.nglm.evolution.reports;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

public class SamplerNRandomLines {

	public SamplerNRandomLines() {
	}

	public List<String> sampler(String fileName, int reservoirSize) throws FileNotFoundException, IOException {
		String currentLine = null;
		// reservoirList is where our selected lines are stored
		List<String> reservoirList = new ArrayList<String>(reservoirSize);
		// Use this counter to count the current line number while iterating
		int count = 0;
		
		ReadFile file = new ReadFile(fileName);
		int totalNmbrOfLines = file.readLines();

		Random ra = new Random();
		int randomNumber = 0;
		Scanner sc = new Scanner(new File(fileName)).useDelimiter("\n");

		if (reservoirSize <= 0) {
			throw new IllegalArgumentException("ERROR: The reservoirSize should be > 0!");
//			System.out.println("ERROR: The reservoirSize should be > 0!");
//			System.out.println("\n");
		}


		if (reservoirSize > totalNmbrOfLines) {
			throw new IllegalArgumentException("ERROR: The reservoirSize should be > 0 but < totalNumberOfLines !");
				
//					System.out.println(
//							"The reservoirSize is: " + reservoirSize + " and the totalNmbrOfLines is: " + totalNmbrOfLines);
//			System.out.println("ERROR: The reservoirSize should be > 0 but < totalNumberOfLines !");
//			System.out.println("\n");
		}

		if (reservoirSize > 0 && reservoirSize < totalNmbrOfLines) {

			while (sc.hasNext()) {
				currentLine = sc.next();
				count++; // increase the line number

				if (count <= reservoirSize) {
					reservoirList.add(currentLine);

				} else if ((randomNumber = (int) ra.nextInt(count)) < reservoirSize) {
					reservoirList.set(randomNumber, currentLine);
				}
			}
//			System.out.print("The reservoirList contains: " + reservoirList);
		}
		
		return reservoirList;
	}
}
