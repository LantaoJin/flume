/**
 * 
 * @company dianping.com
 * @author lantao.jin
 */
package com.dianping.duplicate;

import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.duplicate.util.DateUtil;

public class HDFSOperater {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private FileSystem fs;
	private FSDataInputStream in;
	private FSDataOutputStream out;
	private String appPathStr;
	//TODO for compression
//	private CompressionOutputStream cmpOut;
//	private CompressionOutputStream cmpIn;

	public HDFSOperater(FileSystem fs, String appPathStr) {
		this.fs = fs;
		this.appPathStr = appPathStr;
	}

	public boolean retireTmpFile(Path tmpPath) {
		Path parentPath = tmpPath.getParent();
		String tmpName = tmpPath.getName();
		if (!tmpName.endsWith("tmp")) {
            return false;
        }
		String dstName = tmpName.substring(0, tmpName.lastIndexOf('.'));
		Path dstPath = new Path(parentPath, dstName);
		try {
			return fs.rename(tmpPath, dstPath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	public boolean touchSuccFile(Path parentPath) {
	    logger.debug("testhaha");
		Path sucesssFile = new Path(parentPath, "_success");
		try {
			out = fs.create(sucesssFile);
		} catch (IOException e) {
			logger.error("Touch success flag file fail!");
			e.printStackTrace();
			return false;
		} finally {
			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
				logger.warn("Close hdfs out put stream fail!");
			}
		}
		return true;
	}
	
	public boolean checkPathExists(Path path) {
		try {
			return fs.exists(path);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	

	public boolean checkSuccessFileExist(String hourStr) {
		DateUtil dateUtil = DateUtil.getInstance();
		try {
			Path f = new Path(dateUtil.getPathFromStr(hourStr, appPathStr), new Path("_success"));
			return fs.exists(f);
		} catch (ParseException e) {
			// TODO: handle exception
			e.printStackTrace();
		} catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return false;
	}
	
	//TODO check not null
	public FileStatus[] listStatus(Path path) {
		try {
			return fs.listStatus(path);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public boolean deleteFile(Path path) {
		try {
			return fs.deleteOnExit(path);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	public boolean deleteFileOnRetry(Path path) {
		int retryCount = 0;
		try {
			while (retryCount++ < 3) {
				if (fs.deleteOnExit(path)) {
					break;
				}
				TimeUnit.SECONDS.sleep(30);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (retryCount > 3) {
			return false;
		} else {
			return true;
		}
		
	}
	
	public void discardDuplicateContent(Path tmpPath, long endLineNumber) throws IOException{
		String tmpName = tmpPath.getName();
		String dstName = tmpName.substring(0, tmpName.lastIndexOf('.'));
		Path completePath = new Path(tmpPath.getParent(), dstName);
		try {
			out = fs.create(completePath);
			in = fs.open(tmpPath);
			//TODO to be review, If the file is a compression file, then what?
			//We can use a buffer to writing first instead of flushing the whole content.
			//To refer the SpoolingFileLineRead.java
			for (int i = 0; i < endLineNumber; i++ ) {
				byte[] byteOfline = in.readLine().getBytes();
				out.write(byteOfline);
				out.write('\n');
				if (i % 5000 == 0) {
					out.flush();
					out.sync();
				}
			}
			out.flush();
			out.sync();
		} finally {
			try {
				out.close();
				in.close();
			} catch (IOException e) {
				logger.warn("Unable to close in and out stream for file: " + tmpName, e);
			}
		}
	}
	
	public void writeStartFile(final String appStartKey, String appStartValue) {
		try {
			Path startKeyPath = new Path(appPathStr, "_" + appStartKey);
			out = fs.create(startKeyPath, true);
			out.write(appStartValue.getBytes());
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	//TODO should to be check return value not ""
	public String readStartFile(final String appStartKey) {
		Path startKeyPath = new Path(appPathStr, "_" + appStartKey);
		byte[] appStartValue = new byte[10];
		try {
			in = fs.open(startKeyPath);
			in.read(appStartValue);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return (new String(appStartValue));
	}
}
