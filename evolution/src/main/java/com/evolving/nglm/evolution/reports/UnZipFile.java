package com.evolving.nglm.evolution.reports;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnZipFile {
  private static final Logger log = LoggerFactory.getLogger(UnZipFile.class);

  public static String unzip(String zipFilePath) {
    try {
      Path destDirPath = Files.createTempDirectory("zipdir");
      String destDir = destDirPath.toString();
      FileInputStream fis;
      byte[] buffer = new byte[100 * 1024];
      File newFile = null;

      fis = new FileInputStream(zipFilePath);
      ZipInputStream zis = new ZipInputStream(fis);
      ZipEntry ze = zis.getNextEntry();
      while(ze != null){
        String fileName = ze.getName();
        newFile = new File(destDir + File.separator + fileName);

        FileOutputStream fos = new FileOutputStream(newFile);
        int len;
        while ((len = zis.read(buffer)) > 0) {
          fos.write(buffer, 0, len);
        }
        fos.close();
        zis.closeEntry();
        ze = zis.getNextEntry();
      }
      zis.closeEntry();
      zis.close();
      fis.close();
      return newFile.getAbsolutePath();
    } catch (IOException ex) {
      log.info("error zipping intermediate file : " + ex.getLocalizedMessage());
      return "";
    }
  }
}
