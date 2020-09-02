package com.evolving.nglm.evolution.reports;

import java.io.*;
import java.nio.file.*;
import java.util.zip.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
public class ZipFile {
 
	private static Logger log = LoggerFactory.getLogger(ZipFile.class);
	
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