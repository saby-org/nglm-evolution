package com.evolving.nglm.evolution.reports;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
public class ZipFile {
    private static final String ZIP_PREFIX = "zip";
    private static Logger log = LoggerFactory.getLogger(ZipFile.class);
    
    public static File zipFile(String filePath) {
        try {
            File file = new File(filePath);
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
          log.info("file does not exist : " + ex.getLocalizedMessage());
          return null;
        } catch (IOException ex) {
          log.info("error zipping intermediate file : " + ex.getLocalizedMessage());
          return null;
        }
    }
 
	
    public static void zipFile(String inputFile, String outputFile) {
        try {
            File file = new File(inputFile);
 
            FileOutputStream fos = new FileOutputStream(outputFile);
            ZipOutputStream zos = new ZipOutputStream(fos);
 
            zos.putNextEntry(new ZipEntry(file.getName()));
 
            byte[] bytes = Files.readAllBytes(Paths.get(inputFile));
            zos.write(bytes, 0, bytes.length);
            zos.closeEntry();
            zos.close();
 
        } catch (FileNotFoundException ex) {
        	log.error("The file does not exist", ex);
        } catch (IOException ex) {
        	log.error("I/O error creating zip file", ex);
        }
    }
}
