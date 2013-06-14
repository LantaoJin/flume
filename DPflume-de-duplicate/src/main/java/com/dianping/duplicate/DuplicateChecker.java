/**
 * 
 * @company dianping.com
 * @author lantao.jin
 */
package com.dianping.duplicate;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

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
	//TODO review
	private static final String APP_START_KEY = "start";
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
			logger.info("Begin to process {} .", appPathStr);
			String startHourStr, currentHourStr;
			
			DateUtil dateUtil = DateUtil.getInstance();
			//日期的格式处理，为了获取小时数
			currentHourStr = dateUtil.getCurrentDateStr();
			
			/* Get the startHourStr for processing the oldest failed direction.
			 * On first time, startHour equal to the currentHour. */
			startHourStr = appContext.getString(APP_START_KEY);
			if (startHourStr == null) {
			    startHourStr = currentHourStr;
            }

			//两个日期中间相差几个小时的计算公式，可以跨天
			int intervalHours = dateUtil.getDateInterval(startHourStr, currentHourStr);
			String workingHourStr = startHourStr;
			
			//注意i从0开始，且i<=时间差值
			for (int i = 0; i <= intervalHours; i++) {
				workingHourStr = dateUtil.getWorkingHourStr(startHourStr, i);
				
				//一旦发现有未完成的，findUndone就变为true，此后除非线程重启，否则都无法再移动startHourStr指针
				if (!findUndone) {
					startHourStr = workingHourStr;
					//Need to create a HDFS file to keep start information.
					operater.writeStartFile(APP_START_KEY, startHourStr);
	                appContext.put(APP_START_KEY, startHourStr);
				}
				
				Path workingPath = dateUtil.getPathFromStr(workingHourStr, appPathStr);
				
				//如果workPath不存在，说明路径信息错误，抛出异常，程序停止ֹ
				if (!operater.checkPathExists(workingPath)) {
					throw new Exception("Working path is illegal.");
				}
				//判断当前目录是否已经创建了success文件。存在则不处理
				if (operater.checkSuccessFileExist(workingHourStr)) {
					continue;
				}
				//判断当前文件
				if (!checkCollectorsAvailable()) {
					//TODO Alarm! Cause by collectors are unrecovered.
					break;
				}
				//判断当前目录正在接受或已经接受了来自所有数据来源机器的文件（同一个应用）
				if(!checkAllSourceReceiving(workingPath)) {
					//TODO Alarm!! out of time 
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
			logger.error("HDFS file name format is illegal! It not shoud be happen.");
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace(); 
		} catch (Exception e) {
			e.printStackTrace(); 
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param workingPath
	 * @return
	 */
	public boolean cleanUpStaleFile(Path workingPath) {
		boolean allCleanUpSucc = true;
		HashMap<String, SortedMap<FileStatus, Long>> fileMapper = new HashMap<String, SortedMap<FileStatus,Long>>();
		FileStatus[] status = operater.listStatus(workingPath);
		//为每一个文件排序，按文件名分桶，并使用sortedmap对具有相同纯粹文件名的文件根据读文件的时间戳进行排序。
		for (FileStatus st : status) {
			String fileName = st.getPath().getName();
			if (fileMapper.containsKey(getPureLogName(fileName))) {
				fileMapper.get(fileName).put(st, getReadTimestampOfLog(fileName));
			} else {
				SortedMap<FileStatus, Long> fileSelector = new TreeMap<FileStatus, Long>();
				fileSelector.put(st, getReadTimestampOfLog(fileName));
				fileMapper.put(getPureLogName(fileName), fileSelector);
			}
		}
		//具有相同纯粹文件名的文件只要读取时间戳小于最大的时间戳，则删除。这里要注意的是具有相同纯粹文件名的多个可能具有相同时间戳，这是由于文件roll产生的
		Collection<SortedMap<FileStatus,Long>> pureFileValues = fileMapper.values();
		for (SortedMap<FileStatus, Long> fileSelector : pureFileValues) {
			Set<FileStatus> fileStatus = fileSelector.keySet();
			for (FileStatus st : fileStatus) {
				if (fileSelector.get(st) < fileSelector.get(fileSelector.lastKey())) {
					if (!operater.deleteFileOnRetry(st.getPath())) {
						allCleanUpSucc = false;
						break;
					}
				}
			}
		}
		return allCleanUpSucc;
	}
	
	/**
	 * 
	 * @param workingPath
	 * @return
	 */
	public boolean handleTmpFile(Path workingPath) {
		boolean allTmpHandleSucc = true;
		SortedMap<FileStatus, Long> fileSelector = new TreeMap<FileStatus, Long>();
		FileStatus[] status = operater.listStatus(workingPath);
		for (FileStatus st : status) {
			String fileName = st.getPath().getName();
			fileSelector.put(st, getLineNumberOfLog(fileName));
		}
		//tmp文件是最后一个，直接mv成正常文件
		if (fileSelector.lastKey().getPath().getName().endsWith(".tmp")) {
			if(!operater.retireTmpFile(fileSelector.lastKey().getPath())) {
				allTmpHandleSucc = false;
			}
		}
		
		Set<Entry<FileStatus, Long>> entries = fileSelector.entrySet();
		for (Iterator<Entry<FileStatus, Long>> it = entries.iterator(); it.hasNext(); ) {
			Entry<FileStatus, Long> entry = it.next();
			if (entry.getKey().getPath().getName().endsWith(".tmp")) {
				try {
					operater.discardDuplicateContent(entry.getKey().getPath(), it.next().getValue() - 1);
				} catch (IOException e) {
					e.printStackTrace();
					allTmpHandleSucc = false;
				}
			}
		}
		return allTmpHandleSucc;
	}

	public boolean checkCollectorsAvailable() {
		URLConnection connection;
		String[] hostnames = appContext.getString(BasicConfigurationConstants.MACHINES).split(" ");
		for (String hostname : hostnames) {
			//example: port key is "machines.hostname1.port"
		    String port = appContext.getString(BasicConfigurationConstants.MACHINES_PREFIX + 
		            hostname + BasicConfigurationConstants.PORT);
		    /* http://<hostname>:<port>/metrics */
			String urlPath = "http://" + hostname + ":" + port + "/metrics";
			logger.info("Connecting url {}.", urlPath);
			try {
				URL url = new URL(urlPath);
				connection = url.openConnection();
				connection.setConnectTimeout(5000);
				connection.connect();
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}
	
	public boolean checkAllSourceReceiving(Path workingPath) {
		boolean allFound = false;
		Set actualHostSet = new HashSet<String>();
		Set configHostSet = new HashSet<String>();
		
		String[] hostnames = appContext.getString(BasicConfigurationConstants.MACHINES).split(" ");
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
