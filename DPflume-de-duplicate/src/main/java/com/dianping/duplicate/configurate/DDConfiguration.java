package com.dianping.duplicate.configurate;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;


public class DDConfiguration {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    static Context appContext = null;
    private static Map<String, Context> appConfigMap;
    
    public DDConfiguration (Properties properties) {
        appConfigMap = new HashMap<String, Context>();
    }
    
    public boolean addRawProperty(String name, String value) {
        // Null names and values not supported
        if (name == null || value == null) {
            logger.error("Null names and values not supported");
            return false;
        }

        // Empty values are not supported
        if (value.trim().length() == 0) {
            logger.error("Empty values are not supported");
            return false;
        }

        // Remove leading and trailing spaces
        name = name.trim();
        value = value.trim();

        int index = name.indexOf('.');

        // All configuration keys must have a prefix defined as app name or conf identify
        if (index == -1) {
            logger.error("All configuration keys must have a prefix defined as app name or conf identify");
            return false;
        }
        String appName = name.substring(0, index);

        // App name or conf identify must be specified for all properties
        if (appName.length() == 0) {
            logger.error("App name or conf identify must be specified for all properties");
            return false;
        }

        String configKey = name.substring(index + 1);

        // Configuration key must be specified for every property
        if (configKey.length() == 0) {
            logger.error("Configuration key must be specified for every property");
            return false;
        }

        appContext = appConfigMap.get(appName);

        if (appContext == null) {
            appContext = new Context(appName);
            appConfigMap.put(appName, appContext);
        }

        return addProperty(configKey, value);
    }

    public boolean addProperty(String configKey, String value) {
        appContext.put(configKey, value);
        return true;
    }

    public static Context getConfigurationFor(String appName) {
        return appConfigMap.get(appName);
      }

    public static Map<String, Context> getAppConfigMap() {
        return appConfigMap;
    }
}
