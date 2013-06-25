/**
 * 
 * @company dianping.com
 * @author lantao.jin
 */
package com.dianping.duplicate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.duplicate.configurate.BasicConfigurationConstants;
import com.dianping.duplicate.configurate.Context;
import com.dianping.duplicate.configurate.DDConfiguration;

public class Application {
	private static final Logger logger = LoggerFactory.getLogger(Application.class);
	private String[] args;
	private File configurationFile;
	
	public static void main(String[] args) {
		Application application = new Application();

	    application.setArgs(args);

	    try {
	      if (application.parseOptions()) {
	    	  application.loadConfig();
	          application.run();
	      }
	    } catch (ParseException e) {
	      logger.error(e.getMessage());
	    } catch (Exception e) {
	      logger.error("A fatal error occurred while running. Exception follows.",
	          e);
	    }
	}

	public boolean parseOptions() throws ParseException {
	    Options options = new Options();

	    Option option = new Option("f", "conf", true, "specify a conf file");
	    options.addOption(option);

	    CommandLineParser parser = new GnuParser();
	    CommandLine commandLine = parser.parse(options, args);

	    if (commandLine.hasOption('f')) {
	      configurationFile = new File(commandLine.getOptionValue('f'));

	      if (!configurationFile.exists()) {
		      String path = configurationFile.getPath();
		      try {
		        path = configurationFile.getCanonicalPath();
		      } catch (IOException ex) {
		        logger.error("Failed to read canonical path for file: " + path, ex);
		      }
		      throw new ParseException(
		          "The specified configuration file does not exist: " + path);
	      }
	    }

	    return true;
	  }
	
	public void run() throws Exception{
		/* Schedule the configuration file watcher, it is a good feature.
		 * But now, we use to load configuration file one time in this version.*/
//		FileWatcherRunnable fileWatcherTask = new FileWatcherRunnable(configurationFile);
//	    ScheduledExecutorService configExec = Executors.newSingleThreadScheduledExecutor();
//		configExec.scheduleWithFixedDelay(fileWatcherTask, 0, 30, TimeUnit.SECONDS);

		Context appContext; 
		//schedule the check task
		Configuration conf = new Configuration();
		UserGroupInformation.setConfiguration(conf);
		try {
            SecurityUtil.login(conf, "deduplicate.hadoop.keytab.file", "deduplicate.hadoop.principal");
        } catch (IOException e) {
            logger.error("Faild to login with keytab security.");
            e.printStackTrace();
            throw e;
        }
	    FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
		    logger.error("Failed to get FileSystem.");
			e.printStackTrace();
			throw e;
		}
		
		ScheduledExecutorService checkerThreadPool = null;
		
		Set<String> apps = DDConfiguration.getAppConfigMap().keySet();
		if (apps == null) {
            logger.error("Failed to load configuration file.");
            return;
        }
		
		if (apps.contains(BasicConfigurationConstants.COMMOM_CONFIG)) {
		    int maxThreadPoll = DDConfiguration.getConfigurationFor(
		            BasicConfigurationConstants.COMMOM_CONFIG)
		            .getInteger(BasicConfigurationConstants.MAX_THREAD_POOL);
            checkerThreadPool = Executors.newScheduledThreadPool(maxThreadPoll);
        } else {
            logger.error("Necessary configuration {} is missing." +
                    "Procedure is going to shutdown.",
                    BasicConfigurationConstants.COMMOM_CONFIG);
            return;
        }

		for (String appName : apps) {
		    appContext = DDConfiguration.getConfigurationFor(appName);
		    if (appName.equals(BasicConfigurationConstants.COMMOM_CONFIG)) {
                continue;//Continue because that this case has been handled.
            }
		    if (appContext.getString(BasicConfigurationConstants.PARENT_HDFS_PATH) == null) {
	            logger.error("Failed to load parent HDFS path from *.conf");
	            return;
	        }
		    String appPathStr;
		    if (appContext.getString(BasicConfigurationConstants.PARENT_HDFS_PATH).trim().endsWith("/")) {
		        appPathStr = appContext.getString(BasicConfigurationConstants.PARENT_HDFS_PATH).trim() + appName;
            } else {
                appPathStr = appContext.getString(BasicConfigurationConstants.PARENT_HDFS_PATH).trim() + "/"+ appName;
            }
    		HDFSOperater operater = new HDFSOperater(hdfs, appPathStr);
    		DuplicateChecker checker = new DuplicateChecker(appPathStr, operater, appContext);
    		checkerThreadPool.scheduleWithFixedDelay(checker, 0, appContext.getLong(BasicConfigurationConstants.CHECK_PERIOD), TimeUnit.SECONDS);
		}
	}
	
	public void loadConfig() {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(configurationFile));
            Properties properties = new Properties();
            properties.load(reader);
            DDConfiguration conf = new DDConfiguration(properties);
            Enumeration<?> propertyNames = properties.propertyNames();
            while (propertyNames.hasMoreElements()) {
              String name = (String) propertyNames.nextElement();
              String value = properties.getProperty(name);

              if (!conf.addRawProperty(name, value)) {
                logger.warn("Configuration property ignored: " + name + " = " + value);
                continue;
              }
            }
        } catch (IOException e) {
            logger.error("Unable to load file:" + configurationFile
                      + " (I/O failure) - Exception follows.", e);
        } finally {
          if (reader != null) {
            try {
              reader.close();
            } catch (IOException ex) {
                logger.warn(
                  "Unable to close file reader for file: " + configurationFile, ex);
            }
          }
        }
    }
	
	public String[] getArgs() {
		return args;
	}

	public void setArgs(String[] args) {
	    this.args = args;
	}

}
