/**
 * This class is not used in this version.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.duplicate.configurate.DDConfiguration;

public class FileWatcherRunnable implements Runnable{
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private File file;
	private long lastChange;
	public FileWatcherRunnable(File configurationFile) {
		file = configurationFile;
	}

	@Override
    public void run() {
	    logger.debug("Checking file:{} for changes", file);

	    long lastModified = file.lastModified();

	    if (lastModified > lastChange) {
	        logger.info("Reloading configuration file:{}", file);

	        lastChange = lastModified;

	        try {
	            doLoad();
	        } catch (Exception e) {
	            logger.error("Failed to load configuration data. Exception follows.", e);
	        } catch (NoClassDefFoundError e) {
	            logger.error("Failed to start agent because dependencies were not " +
	                    "found in classpath. Error follows.", e);
	        } catch (Throwable t) {
	            // caught because the caller does not handle or log Throwables
	            logger.error("Unhandled error", t);
	        }
	    }
	}

	public synchronized void doLoad() {
		File propertiesFile = getFile();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(propertiesFile));
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
			logger.error("Unable to load file:" + propertiesFile
			          + " (I/O failure) - Exception follows.", e);
        } finally {
          if (reader != null) {
            try {
              reader.close();
            } catch (IOException ex) {
            	logger.warn(
                  "Unable to close file reader for file: " + propertiesFile, ex);
            }
          }
        }
	}
	
	public File getFile() {
		return this.file;
	}
}
