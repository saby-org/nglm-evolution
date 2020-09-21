package com.evolving.nglm.evolution.reports;

import java.io.*;
import java.nio.file.*;
import java.util.zip.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipFile {
	private static final Logger log = LoggerFactory.getLogger(ZipFile.class);
	private static final String ZIP_PREFIX = "zip";

	public static File zipFile(String inputFile) {
		try {
			File file = new File(inputFile);
			String zipFileName = file.getAbsolutePath().concat("." + ZIP_PREFIX);
			FileOutputStream fos = new FileOutputStream(zipFileName);
			ZipOutputStream zos = new ZipOutputStream(fos);

			zos.putNextEntry(new ZipEntry(file.getName()));
			zos.setLevel(Deflater.BEST_SPEED);
			
			
			byte data[] = new byte[100 * 1024]; // allow some bufferization
			int length;
			FileInputStream fis = new FileInputStream(file);
            while ((length = fis.read(data)) != -1) {
			zos.write(data, 0, length);
            }
			zos.closeEntry();
			zos.close();
			fos.close();

			return new File(zipFileName);
		} catch (FileNotFoundException ex) {
			log.info("The file does not exist", ex.getLocalizedMessage());
			return null;
		} catch (IOException ex) {
			log.info("error zipping intermediate file : " + ex.getLocalizedMessage());
	          return null;
		}
	}
}