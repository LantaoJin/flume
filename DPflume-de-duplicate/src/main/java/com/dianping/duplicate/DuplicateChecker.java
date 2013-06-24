/**
 * 
 * @company dianping.com
 * @author lantao.jin
 */
package com.dianping.duplicate;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.duplicate.configurate.BasicConfigurationConstants;
import com.dianping.duplicate.configurate.Context;
import com.dianping.duplicate.util.DateUtil;

/**
 * A large number of duplicate event will remain in HDFS, due to a fail-over process occur.
 * The duties of this class are as follow: 
 * First, it checks out whether all source agents have delivered current-hour events to end.
 * Then, it checks the whole progress of receiving. 
 * Finally, remove the duplicate data if needed.
 * To consider the future, this class maybe provide a combine interface. 
 */
public class DuplicateChecker implements Runnable {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	private final String appPathStr;
	private HDFSOperater operater;
	private Context appContext;
	
	private boolean findUndone;
	public DuplicateChecker(String appPathStr, HDFSOperater operater, Context appContext) {
	    this.appContext = appContext;
		this.operater = operater;
		this.appPathStr = appPathStr;
	}
	
	@Override
	public void run() {
		try {
			findUndone = false;	//某个目录由于各种原因导致本次执行无法处理
			logger.info("Begin to process {}.", appPathStr);
			String startHourStr, currentHourStr;
			
			DateUtil dateUtil = DateUtil.getInstance();
			//日期的格式处理，为了获取小时数
			currentHourStr = dateUtil.getCurrentDateStr();
			int currentMin = dateUtil.getCurrentMin();
			
			/* Get the startHourStr for processing the oldest failed direction.
			 * On first time, startHour need to load from file(local or HDFS). */
			startHourStr = appContext.getString(BasicConfigurationConstants.APP_START_KEY);
			if (startHourStr == null) {
			    Path appPath = new Path(appPathStr);
			    if (!operater.checkPathExists(appPath) || !operater.checkStartFileExists(appPath)) {
			        // If running to this branch, it means the procedure for this app is running first time.
			        logger.error("App log HDFS direction " + appPathStr +
			        		" is not exist, task end this time." +
			        		" Please mkdir for it and add a file " +
			        		"\"_" + BasicConfigurationConstants.APP_START_KEY + "\"" +
			        		" under this direction manually");
			        return;
			    } else {
			        // If running to this branch, it means the procedure for this app restart.
			        try {
			            startHourStr = operater.readStartFile().trim();
                    } catch (IOException e) {
                        logger.error("Load app_start_str value faid, the start file in " +
                                appPathStr + "may not create, manually create first. Task end this time.");
                        e.printStackTrace();
                        return;
                    }
			        if (startHourStr.length() == 0) {
			            logger.error("App_start_str value is empty string, it should not" +
                                " be happened. Task end this time.");
			            return;
                    }
                }
            }

			//两个日期中间相差几个小时的计算公式，可以跨天
			int intervalHours = dateUtil.getDateInterval(startHourStr, currentHourStr);
			String workingHourStr = startHourStr;
			
			//注意i从0开始，且i<=时间差值
			for (int i = 0; i <= intervalHours; i++) {
                workingHourStr = dateUtil.getWorkingHourStr(startHourStr, i);
				
				//一旦发现有未完成的，findUndone就变为true，此后除非线程重启，否则都无法再移动startHourStr指针
				if (!findUndone) {
					//Need to create a HDFS file to keep start information.
					operater.writeStartFile(workingHourStr);
	                appContext.put(BasicConfigurationConstants.APP_START_KEY, workingHourStr);
				}
				
				Path workingPath = dateUtil.getPathFromStr(workingHourStr, appPathStr);
				logger.info("Processing " + workingPath);
				//WorkPath not exist, it maybe the newest direction or something exception.
				if (!operater.checkPathExists(workingPath)) {
				    logger.warn("Working path {} is not exist. Task end this time.", workingPath);
					break;
				}
				//判断当前目录是否已经创建了success文件。存在则不处理
				if (operater.checkSuccessFileExist(workingHourStr)) {
					continue;
				}
				//判断当前文件
				if (!checkCollectorsAvailable()) {
				    logger.error("Collectors are not all ready. Task end this time.");
					break;
				}
				//判断当前目录正在接受或已经接受了来自所有数据来源机器的文件（同一个应用）
				if(!checkAllSourceReceiving(workingPath)) {
				    if (currentMin > Integer.parseInt(appContext.getString(
				            BasicConfigurationConstants.ALARM_BEGIN_TIME).trim())) {
				        logger.error("Receive data from all source timeout");
                    }
					findUndone = true;
					continue;
				}
				//清理早晚事件
				if (!cleanUpStaleFile(workingPath)) {
					findUndone = true;
					continue;
				}
				//处理TMP文件
				if (!handleTmpFile(workingPath)) {
					findUndone = true;
					continue;
				}
				//全部条件都满足，是时候给该目录设置一个成功标志文件 _success
				if (!operater.touchSuccFile(workingPath)) {
					findUndone = true;
					continue;
				}
			}
		} catch (NumberFormatException e) {
			logger.error("NumberFormat Parse failed. It not shoud" +
					" be happen, please check \".conf\" file.");
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (RuntimeException e) {
			e.printStackTrace(); 
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	   
    class IdentifyFile {
        private String identifyName;
        private long lineNum;
        private long SPRTS;
        private FileStatus st;
        public IdentifyFile(String identifyName, long lineNum, long SPRTS, FileStatus st) {
            this.identifyName = identifyName;
            this.lineNum = lineNum;
            this.SPRTS = SPRTS;
            this.st = st;
        }
        
        public String getIdentifyName() {
            return identifyName;
        }
        
        public long getLineNum() {
            return lineNum;
        }

        public FileStatus getSt() {
            return st;
        }

        public long getSPRTS() {
            return SPRTS;
        }
        
        public boolean isTmp() {
            return st.getPath().getName().endsWith(".tmp");
        }
    }
	
	public boolean cleanUpStaleFile(Path workingPath) {

	    FileStatus[] fileStatus = operater.listStatus(workingPath);
	    if (fileStatus == null) {
            return false;
        }
	    // Begin to handle tmp file, build a file map with sorted set of identify files
        HashMap<String, SortedSet<IdentifyFile>> idFileMap = buildFileMap(fileStatus, true);

	    
	    /* idFileMap is a map. Its key is a identify name strcat by log name and hostname,
	     * and its value is a sorted set sorted by SPRTS from big to small.
	     * When traversing in the same identify bucket, as long as its SPRTS
	     * is less than last one's in sorted set,
	     * it should be delete. */
        for (String identifyName : idFileMap.keySet()) {
            SortedSet<IdentifyFile> sortedSet = idFileMap.get(identifyName);
            long biggestReadTimestamp = sortedSet.last().getSPRTS();
            for (IdentifyFile identifyFile : sortedSet) {
                if (identifyFile.getSPRTS() < biggestReadTimestamp) {
                    try {
                        if (!operater.deleteFile(identifyFile.getSt().getPath())) {
                            return false;
                        }
                    } catch (IOException e) {
                        logger.warn("Failed to delete stale data");
                        e.printStackTrace();
                        return false;
                    }
                }
            }
        }
	    return true;
	}
	
	/**
	 * 
	 * @param workingPath
	 * @return
	 */
	public boolean handleTmpFile(Path workingPath) {
        FileStatus[] fileStatus = operater.listStatus(workingPath);
        if (fileStatus == null) {
            return false;
        }
        int i = 0;
        for (FileStatus st : fileStatus) {
            if (st.getPath().getName().endsWith(".tmp")) {
                if (st.getModificationTime() + appContext.getLong(BasicConfigurationConstants.IDLE_TIME, 40000l)
                        < System.currentTimeMillis() ) {
                    logger.info("It's time to handle stable tmp file" + st.getPath());
                } else {
                    logger.info("The tmp file is not stable. Task end this time.");
                    return false;
                }
            } else {
                i++;
            }
        }
        // There are no tmp in this direction
        if (i == fileStatus.length) {
            return true;
        }
        
        // Begin to handle tmp file, build a file map with sorted set of identify files
        HashMap<String, SortedSet<IdentifyFile>> idFileMap = buildFileMap(fileStatus, false);
        /* Traversing the map keyset, deal with the tmp file.
         * If catch a ArrayIndexOutOfBoundsException, it means the tmp file is one having biggest line number,
         * another works , collector crashed in the idle time. Just change .tmp to normal.
         * Else discard the duplicate data recursively. */
        for (String identifyName : idFileMap.keySet()) {
            SortedSet<IdentifyFile> sortedSet = idFileMap.get(identifyName);
            Object[] idFiles = sortedSet.toArray();
            for (int j = 0; j < idFiles.length; j++) {
                if (((IdentifyFile)idFiles[j]).isTmp()) {
                    try {
                        operater.discardDuplicateContent(((IdentifyFile)idFiles[j]).getSt().getPath(), ((IdentifyFile)idFiles[j+1]).getLineNum() - 1);
                    } catch (ArrayIndexOutOfBoundsException e) {
                        // Just change .tmp to normal
                        if(!operater.retireTmpFile(((IdentifyFile)idFiles[j]).getSt().getPath())) {
                            return false;
                        }
                    } catch (IOException e) {
                        logger.warn("Failed to discard duplicate data");
                        e.printStackTrace();
                        return false;
                    }
                }
            }
        }
        return true;
	}

    private HashMap<String, SortedSet<IdentifyFile>> buildFileMap(FileStatus[] fileStatus, boolean isHandleSPRTS) {
        HashMap<String, SortedSet<IdentifyFile>> idFileMap = new HashMap<String, SortedSet<IdentifyFile>>();
        for (FileStatus st : fileStatus) {
            String fileName = st.getPath().getName();

            long lineNumber = getLineNumberOfLog(fileName);
            long SPRTS = getReadTimestampOfLog(fileName);
            String identifyName = getPureLogName(fileName) + getOriginHostnameOfLog(fileName);
            if (idFileMap.containsKey(identifyName)) {
                idFileMap.get(identifyName).add(new IdentifyFile(identifyName, lineNumber, SPRTS, st));
            } else {
                SortedSet<IdentifyFile> sortedTmpFiles;
                if (isHandleSPRTS) {
                    sortedTmpFiles = new TreeSet<IdentifyFile>(new Comparator<IdentifyFile>() {
                        @Override
                        public int compare(IdentifyFile o1, IdentifyFile o2) {//from small to big
                            return o2.getSPRTS() > o1.getSPRTS() ? -1 : 1;
                        }
                    });
                } else {
                    sortedTmpFiles = new TreeSet<IdentifyFile>(new Comparator<IdentifyFile>() {
                        @Override
                        public int compare(IdentifyFile o1, IdentifyFile o2) {//from small to big
                            return o2.getLineNum() > o1.getLineNum() ? -1 : 1;
                        }
                    });
                }
                
                sortedTmpFiles.add(new IdentifyFile(identifyName, lineNumber, SPRTS, st));
                idFileMap.put(identifyName, sortedTmpFiles);
            }
        }
        return idFileMap;
    }

	public boolean checkCollectorsAvailable() {
		URLConnection connection;
		InputStream is = null;
		if (null == appContext.getString(BasicConfigurationConstants.COLLECTORS)) {
		    logger.warn("There are no collectors info in *.conf. " +
		    		"Availability for collectors hasn't been checked in this task.");
            return true;
        }
		String[] collectors = appContext.getString(BasicConfigurationConstants.COLLECTORS).trim().split(" ");
		for (String collector : collectors) {
		    /* http://<hostname>:<port>/metrics */
			String urlPath = "http://" + collector + "/metrics";
			logger.info("Connecting url {}.", urlPath);
			try {
				URL url = new URL(urlPath);
				connection = url.openConnection();
				is = connection.getInputStream();
				connection.setConnectTimeout(5000);
				connection.connect();
			} catch (Exception e) {
				logger.warn("Connecting to url {} fail, please check whether need to modify" +
						" collectors info in configure file.");
				return false;
			} finally {
			    try {
			        if (is != null) {
			            is.close();
                    }
                } catch (IOException e) {
                    logger.warn("Failed to close input stream of URL connection");
                    e.printStackTrace();
                }
			}
		}
		return true;
	}
	
	public boolean checkAllSourceReceiving(Path workingPath) {
		boolean allFound = false;
		Set actualHostSet = new HashSet<String>();
		Set configHostSet = new HashSet<String>();
		
		String[] hostnames = appContext.getString(BasicConfigurationConstants.APP_HOSTNAMES).trim().split(" ");
		if(!Collections.addAll(configHostSet, hostnames)) {
			throw new Error("Error that collections add faild. It should not be happen.");
		}
		
		FileStatus[] status = operater.listStatus(workingPath);
		if (status == null) {
			return allFound;
		}
		for (FileStatus st : status) {
			actualHostSet.add(getOriginHostnameOfLog(st.getPath().getName()));
		}
		allFound = actualHostSet.containsAll(configHostSet);
		
		return allFound;
	}

	/**
	 * 
	 * @param filemiddle_1370774306.log.2013-06-03.10+test84.hadoop+1370774430330+75730.1371090533152(.tmp)
	 * @return 75730
	 */
	public long getLineNumberOfLog(String filename) {
		String[] spilts = filename.split("\\+");
		return Long.parseLong(spilts[3].substring(0, spilts[3].indexOf('.')));
	}
	
	/**
	 * 
	 * @param filemiddle_1370774306.log.2013-06-03.10+test84.hadoop+1370774430330+75730.1371090533152(.tmp)
	 * @return 1370353700661
	 */
	public long getReadTimestampOfLog(String filename) {
		String[] spilts = filename.split("\\+");
		return Long.parseLong(spilts[2]);
	}
	
	/**
	 * 
	 * @param filemiddle_1370774306.log.2013-06-03.10+test84.hadoop+1370774430330+75730.1371090533152(.tmp)
	 * @return test84.hadoop
	 */
	public String getOriginHostnameOfLog(String filename) {
		String[] spilts = filename.split("\\+");
		return spilts[1];
	}
	
	/**
	 * @param filemiddle_1370774306.log.2013-06-03.10+test84.hadoop+1370774430330+75730.1371090533152(.tmp)
	 * @return accesslog.log.2013-06-04.20
	 */
	public String getPureLogName(String filename) {
		return filename.substring(0, filename.indexOf('+'));
	}
}
