package com.dianping.duplicate.configurate;

public final class BasicConfigurationConstants {

    public static final String COMMOM_CONFIG = "conf";
    public static final String COMMOM_CONFIG_PREFIX = COMMOM_CONFIG + ".";
    public static final String KEYTAB_FILE = "keytab.file";
    
    public static final String CHECK_PERIOD = "checkPeriod";
    public static final String MAX_THREAD_POOL = "maxThreadPool";

    public static final String APP_START_KEY = "start";
    public static final String IDLE_TIME = "idleTimeout";
    public static final String ALARM_BEGIN_TIME = "alarmBeginTime";
    public static final String PARENT_HDFS_PATH = "parentHdfsPath";
    public static final String APP_HOSTNAMES = "appHostnames";
    public static final String COLLECTORS = "collectors";
    private BasicConfigurationConstants() {
      // disable explicit object creation
    }
}
