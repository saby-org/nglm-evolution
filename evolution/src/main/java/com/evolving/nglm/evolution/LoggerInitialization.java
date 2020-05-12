package com.evolving.nglm.evolution;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple Java Log4j example class.
 * @author alvin alexander, devdaily.com
 */
public class LoggerInitialization
{
	// our log4j category reference
  private static final Logger log = LoggerFactory.getLogger(LoggerInitialization.class);

	/**
	 * init function
	 */
	public void initLogger()
	{
		initializeLogger(System.getProperty("log4j.configuration"));
	} 

	private void initializeLogger(String confFile)
	{
		log.debug("LoggerInitialization.initializeLogger("+confFile+")");
		if (confFile != null) {
			try
			{
				// A thread will be created that will periodically check if confFile has been created or modified.
			  String filename = new URL(confFile).toURI().getPath();
			  if (filename.endsWith(".xml"))
			    {
			      DOMConfigurator.configureAndWatch(filename, 10000L); // milliseconds
			    }
			  else if (filename.endsWith(".properties"))
			    {
			      PropertyConfigurator.configureAndWatch(filename, 10000L); // milliseconds
			    }
			  else
			    {
			      throw new RuntimeException("Unknown logging config format" + filename);
			    }
				log.info("LoggerInitialization.initializeLogger("+confFile+") Logging initialized");
			}
			catch(MalformedURLException | URISyntaxException e)
			{
				log.error("LoggerInitialization.initializeLogger("+confFile+") Unable to load logging conf due to "+e,e);
				throw new RuntimeException("Unable to load logging config " + confFile);
			}
		}
	}
}

