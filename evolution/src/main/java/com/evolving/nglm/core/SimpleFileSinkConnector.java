/****************************************************************************
*
*  SimpleFileSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.core;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SimpleFileSinkConnector extends SinkConnector
{
  /****************************************
  *
  *  config
  *
  ****************************************/

  @Override public ConfigDef config()
  {
    ConfigDef result = new ConfigDef();
    result.define("fileNamePattern", Type.STRING, DEFAULT_FILE_NAME_PATTERN, Importance.HIGH, "file name pattern");
    result.define("rootDir", Type.STRING, DEFAULT_ROOT_DIR, Importance.HIGH, "Root directory for created files");
    result.define("fileExtension", Type.STRING, DEFAULT_FILE_EXTENSION, Importance.HIGH, "File extension for created files");
    result.define("maxFileSize", Type.STRING, ""+DEFAULT_MAX_FILE_SIZE, Importance.HIGH, "size of file (bytes) to trigger a file rotation");
    result.define("maxNumRecords", Type.STRING, ""+DEFAULT_MAX_NUM_RECORDS, Importance.HIGH, "number of records written to trigger a file rotation");
    result.define("maxTimeMinutes", Type.STRING, ""+DEFAULT_MAX_TIME_MINUTES, Importance.HIGH, "time (minutes) since file creation to trigger a file rotation");
    return result;
  }

  //
  //  default values
  //

  public static final String DEFAULT_FILE_NAME_PATTERN = "file_%tY%tm%td_%tH%tM%tS_ROTATE_INDEX";
  public static final String DEFAULT_ROOT_DIR = System.getProperty("java.io.tmpdir");
  public static final String DEFAULT_FILE_EXTENSION = "log";
  public static final int DEFAULT_MAX_NUM_RECORDS = 1000;
  public static final int DEFAULT_MAX_FILE_SIZE = 1_000_000;
  public static final int DEFAULT_MAX_TIME_MINUTES = 15;

  //
  //  logger
  //

  protected static final Logger log = LoggerFactory.getLogger(SimpleFileSinkConnector.class);
  
  //
  //  configuration
  //

  private String connectorName = null;
  private String fileNamePattern = null;
  private String rootDir = null;
  private String fileExtension = null;
  private String maxFileSize = null;
  private String maxNumRecords = null;
  private String maxTimeMinutes = null;

  //
  //  version
  //

  final static String SimpleFileSinkVersion = "1.0";
  
  /****************************************
  *
  *  version
  *
  ****************************************/

  @Override public String version()
  {
    return SimpleFileSinkVersion;
  }

  /****************************************
  *
  *  start
  *
  ****************************************/

  @Override public void start(Map<String, String> properties)
  {
    /*****************************************
    *
    *  configuration -- connectorName
    *
    *****************************************/

    connectorName = properties.get("name");

    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("{} -- Connector.start() START", connectorName);

    /*****************************************
    *
    *  configuration
    *
    *****************************************/

    //
    //  configuration -- fileNamePattern
    //

    String fileNamePatternString = properties.get("fileNamePattern");
    fileNamePattern = (fileNamePatternString != null && fileNamePatternString.trim().length() > 0) ? fileNamePatternString : DEFAULT_FILE_NAME_PATTERN;
    if (fileNamePattern == null || fileNamePattern.trim().length() == 0) throw new ConnectException("SimpleFileSinkConnector configuration must specify 'fileNamePattern'");

    //
    //  configuration -- fileExtension
    //

    String fileExtensionString = properties.get("fileExtension");
    fileExtension = (fileExtensionString != null && fileExtensionString.trim().length() > 0) ? fileExtensionString : DEFAULT_FILE_EXTENSION;
    if (fileExtension == null || fileExtension.trim().length() == 0) throw new ConnectException("SimpleFileSinkConnector configuration must specify 'fileExtension'");

    //
    //  configuration -- rootDir
    //

    String rootDirString = properties.get("rootDir");
    rootDir = (rootDirString != null && rootDirString.trim().length() > 0) ? rootDirString : DEFAULT_ROOT_DIR;
    if (rootDir == null || rootDir.trim().length() == 0) throw new ConnectException("SimpleFileSinkConnector configuration must specify 'rootDir'");
    
    // Check if rootDir exists. if not, try to create parent dir
    File rd = new File(rootDir);
    if (!rd.exists()) {
      File parent = rd.getParentFile();
      if (parent == null)        throw new ConnectException("SimpleFileSinkConnector cannot create rootDir '"+rootDir+"' because parent dir is invalid");
      if (!parent.exists())      throw new ConnectException("SimpleFileSinkConnector cannot create rootDir '"+rootDir+"' because parent dir "+parent.getAbsolutePath()+" does not exist");
      if (!parent.isDirectory()) throw new ConnectException("SimpleFileSinkConnector cannot create rootDir '"+rootDir+"' because parent dir "+parent.getAbsolutePath()+" is not a directory");
      if (!rd.mkdir())           throw new ConnectException("SimpleFileSinkConnector cannot create rootDir '"+rootDir+"'");
      log.info("Created rootDir "+rootDir);
    }

    //
    //  configuration -- maxFileSize
    //

    String maxFileSizeString = properties.get("maxFileSize");
    maxFileSize = (maxFileSizeString != null && maxFileSizeString.trim().length() > 0) ? maxFileSizeString : ""+DEFAULT_MAX_FILE_SIZE;
    if (! validIntegerConfig(maxFileSize, true)) throw new ConnectException("SimpleFileSinkConnector configuration field 'maxFileSize' is a required integer");    
    
    //
    //  configuration -- maxNumRecords
    //

    String maxNumRecordsString = properties.get("maxNumRecords");
    maxNumRecords = (maxNumRecordsString != null && maxNumRecordsString.trim().length() > 0) ? maxNumRecordsString : ""+DEFAULT_MAX_NUM_RECORDS;
    if (! validIntegerConfig(maxNumRecords, true)) throw new ConnectException("SimpleFileSinkConnector configuration field 'maxNumRecords' must be an integer");
    
    //
    //  configuration -- maxTimeMinutes
    //

    String maxTimeMinutesString = properties.get("maxTimeMinutes");
    maxTimeMinutes = (maxTimeMinutesString != null && maxTimeMinutesString.trim().length() > 0) ? maxTimeMinutesString : ""+DEFAULT_MAX_TIME_MINUTES;
    if (! validIntegerConfig(maxTimeMinutes, true)) throw new ConnectException("SimpleFileSinkConnector configuration field 'maxTimeMinutes' must be an integer");

    //
    // In case of restart, promote all temp files
    //
    
    SimpleFileSinkTask.promoteTempFiles(rootDir);

    /*****************************************
    *
    *  log
    *
    *****************************************/
    
    log.info("{} -- Connector.start() END", connectorName);
  }

  /****************************************
  *
  *  taskConfigs
  *
  ****************************************/

  @Override public List<Map<String, String>> taskConfigs(int maxTasks)
  {
    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("{} -- taskConfigs() START", connectorName);

    /*****************************************
    *
    *  create N task configs
    *
    *****************************************/
    
    List<Map<String, String>> result = new ArrayList<Map<String,String>>();
    for (int i = 0; i < maxTasks; i++)
      {
        Map<String, String> taskConfig = new HashMap<>();
        taskConfig.put("connectorName", connectorName);
        taskConfig.put("fileNamePattern", fileNamePattern);
        taskConfig.put("rootDir", rootDir);
        taskConfig.put("fileExtension", fileExtension);
        taskConfig.put("maxFileSize", maxFileSize);
        taskConfig.put("maxTimeMinutes", maxTimeMinutes);
        taskConfig.put("maxNumRecords", maxNumRecords);
        taskConfig.put("taskNumber", Integer.toString(i));
        result.add(taskConfig);
      }

    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("{} -- taskConfigs() END", connectorName);

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return result;
  }

  /****************************************
  *
  *  stop
  *
  ****************************************/

  @Override public void stop()
  {
    log.info("{} -- Connector.stop()", connectorName);
  }

  /****************************************************************************
  *
  *  support
  *
  ****************************************************************************/

  /*****************************************
  *
  *  validIntegerConfig
  *
  *****************************************/

  private static boolean validIntegerConfig(String attribute, boolean required)
  {
    boolean valid = true;
    if (attribute == null || attribute.trim().length() == 0)
      {
        if (required) valid = false;
      }
    else
      {
        try
          {
            int result = Integer.parseInt(attribute);
          }
        catch (NumberFormatException e)
          {
            valid = false;
          }
      }
    return valid;
  }

  /****************************************************************************
  *
  *  utilities
  *
  ****************************************************************************/
  
  /*****************************************
  *
  *  parseIntegerConfig
  *
  *****************************************/

  protected static Integer parseIntegerConfig(String attribute)
  {
    try
      {
        return (attribute != null && attribute.trim().length() != 0) ? Integer.parseInt(attribute) : null;
      }
    catch (NumberFormatException e)
      {
        throw new ServerRuntimeException("invalid config", e);
      }
  }

  /**
   * To test
   */
  public static void main(String[] args) {
    try {
      // Does not work because nglm-core does not depend on log4j... 
//      org.apache.log4j.Logger.getRootLogger().getLoggerRepository().resetConfiguration(); // remove all appenders
//      ConsoleAppender console = new ConsoleAppender();
//      console.setLayout(new PatternLayout("[%d] %p %m (%c)%n")); 
//      console.setThreshold(Level.ALL);
//      console.activateOptions();
//      org.apache.log4j.Logger.getRootLogger().addAppender(console);
//      org.apache.log4j.Logger.getRootLogger().setLevel(Level.ALL);

      
      String jsonRootStr = "" +
          "{" +
          "  \"name\" : \"example-file-sink-connector\",\n" + 
          "  \"config\" :\n" + 
          "    {\n" + 
          "     \"connector.class\" : \"com.evolving.nglm.carrierd.ExampleFileSinkConnector\",\n" + 
          "     \"tasks.max\" : 1,\n" + 
          "     \"topics\" : \"examplefilesinkconnectortopic\",\n" + 
          "     \"fileNamePattern\" : \"mk_file_%tY%tm%td_%tH%tM%tS.%tL_ROTATE_INDEX\",\n" + 
          "     \"fileExtension\" : \"log\",\n" + 
          "     \"maxFileSize\" : \"50\",\n" + 
          "     \"maxNumRecords\" : \"5\",\n" + 
          "     \"maxTimeMinutes\" : \"3\",\n" + 
          "     \"rootDir\" : \"C:/tmp/toto/\"\n" + 
          "    }\n" + 
          "}";
        JSONObject jsonRoot = (JSONObject) (new JSONParser()).parse(jsonRootStr);
        Map<String, String> properties = new HashMap<>();
        properties.put("name", (String) jsonRoot.get("name"));
        JSONObject configJSON = (JSONObject) jsonRoot.get("config");
        for (Object k : configJSON.keySet()) {
          String ks = (String) k;
          Object v = configJSON.get(ks);
          if (v instanceof Long)
            properties.put(ks, ((Long) v)+"");
          else
            properties.put(ks, (String) v);
          System.out.println("Adding "+ks+" : "+v);
        }
        SimpleFileSinkConnector sfsk = new SimpleFileSinkConnector() {
          @Override
          public Class<? extends Task> taskClass() {
            return null;
          }
        };
        sfsk.start(properties);
      } catch (ParseException e) {
        e.printStackTrace();
      }
  }
  
}
