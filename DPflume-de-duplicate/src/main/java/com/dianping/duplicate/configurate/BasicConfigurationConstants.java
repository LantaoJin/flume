package com.dianping.duplicate.configurate;

public final class BasicConfigurationConstants {

    public static final String COMMOM_CONFIG = "conf";
    public static final String COMMOM_CONFIG_PREFIX = COMMOM_CONFIG + ".";

    public static final String CHECK_PERIOD = "checkPeriod";
    public static final String MAX_THREAD_POOL = "maxThreadPool";

    public static final String PARENT_HDFS_PATH = "parentHdfsPath";
    
    public static final String MACHINES = "machines";
    public static final String MACHINES_PREFIX = MACHINES + ".";
    public static final String PORT = ".port";
    private BasicConfigurationConstants() {
      // disable explicit object creation
    }
}
