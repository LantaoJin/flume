/**
 * 
 * @company dianping.com
 * @author lantao.jin
 */
package com.dianping.duplicate;

import java.io.File;
import java.io.IOException;
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
	
	public void run() {
		
		//schedule the config file watcher
		FileWatcherRunnable fileWatcherTask = new FileWatcherRunnable(configurationFile);
		ScheduledExecutorService configExec = Executors.newSingleThreadScheduledExecutor();
		configExec.scheduleWithFixedDelay(fileWatcherTask, 0, 30, TimeUnit.SECONDS);
		Context appContext; 
		//schedule the check task
		Configuration conf = new Configuration();   
	    FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		ScheduledExecutorService checkerThreadPool = null;
		
		Set<String> apps = DDConfiguration.getAppConfigMap().keySet();
		if (apps == null) {
            logger.error("Configuration load fail. Procedure is going to shutdown.");
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
                continue;//This case has been handled.
            }
		    String appPathStr = appContext.getString(BasicConfigurationConstants.PARENT_HDFS_PATH) + "/" + appName;
    		HDFSOperater operater = new HDFSOperater(hdfs, appPathStr);
    		
    		DuplicateChecker checker = new DuplicateChecker(appPathStr, operater, appContext);
    		checkerThreadPool.scheduleWithFixedDelay(checker, 0, appContext.getLong(BasicConfigurationConstants.CHECK_PERIOD), TimeUnit.SECONDS);
		}
	}
	
	public String[] getArgs() {
		return args;
	}

	public void setArgs(String[] args) {
	    this.args = args;
	}

}
