package com.evolving.nglm.evolution.reports;

import java.io.*;
import java.nio.file.*;
import java.util.zip.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
public class ZipFile {
 
	private static Logger log = LoggerFactory.getLogger(ZipFile.class);
	
    static void zipFile(String filePath) {
        try {
            File file = new File(filePath);
            String zipFileName = file.getName().concat(".zip");
 
            FileOutputStream fos = new FileOutputStream(zipFileName);
            ZipOutputStream zos = new ZipOutputStream(fos);
 
            zos.putNextEntry(new ZipEntry(file.getName()));
 
            byte[] bytes = Files.readAllBytes(Paths.get(filePath));
            zos.write(bytes, 0, bytes.length);
            zos.closeEntry();
            zos.close();
 
        } catch (FileNotFoundException ex) {
        	log.error("The file %s does not exist", filePath);
        } catch (IOException ex) {
        	log.error("I/O error: " + ex);
        }
    }
}