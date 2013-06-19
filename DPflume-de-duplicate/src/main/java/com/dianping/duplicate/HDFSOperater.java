/**
 * 
 * @company dianping.com
 * @author lantao.jin
 */
package com.dianping.duplicate;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.duplicate.configurate.BasicConfigurationConstants;
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
		if (tmpName.endsWith(".tmp") || tmpName.endsWith(".buf")) {
		    String dstName = tmpName.substring(0, tmpName.lastIndexOf('.'));
	        Path dstPath = new Path(parentPath, dstName);
	        try {
	            return fs.rename(tmpPath, dstPath);
	        } catch (IOException e) {
	            logger.warn("Failed to rename {}", tmpPath);
	            e.printStackTrace();
	            return false;
	        }
        }
		return true;
	}
	
	public boolean touchSuccFile(Path parentPath) {
		Path sucesssFile = new Path(parentPath, "_success");
		try {
			out = fs.create(sucesssFile);
		} catch (IOException e) {
			logger.warn("Failed to touch success flag file.");
			e.printStackTrace();
			return false;
		} finally {
			try {
			    if (out != null) {
			        out.close();
                }
			} catch (IOException e) {
				e.printStackTrace();
				logger.warn("Close hdfs out put stream fail!");
			}
		}
		return true;
	}
	
	public boolean mkdir(Path path) {
        try {
            return fs.mkdirs(path);
        } catch (IOException e) {
            logger.warn("HDFS may not work in operating.");
            e.printStackTrace();
        }
        return false;
    }
	
	public boolean checkPathExists(Path path) {
		try {
			return fs.exists(path);
		} catch (IOException e) {
			logger.warn("HDFS may not work in operating.");
			e.printStackTrace();
		}
		return false;
	}
	
	public boolean checkStartFileExists(Path path) {
        return checkPathExists(new Path(path, "_" + BasicConfigurationConstants.APP_START_KEY));
    }
	

	public boolean checkSuccessFileExist(String hourStr) {
		DateUtil dateUtil = DateUtil.getInstance();
		try {
			Path f = new Path(dateUtil.getPathFromStr(hourStr, appPathStr), new Path("_success"));
			return fs.exists(f);
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	//the caller must check that result is not null
	public FileStatus[] listStatus(Path path) {
		try {
			return fs.listStatus(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public boolean deleteFile(Path path) throws IOException{
		return fs.delete(path, false);
	}
	
	public void discardDuplicateContent(Path tmpPath, long endLineNumber) throws IOException{
		String tmpName = tmpPath.getName();
		String bufName = tmpName.substring(0, tmpName.lastIndexOf('.')).concat(".buf");
		Path bufferedPath = new Path(tmpPath.getParent(), bufName);
		try {
			out = fs.create(bufferedPath);
			in = fs.open(tmpPath);
			//TODO If the file is a compression file, then what?
			//We can use a buffer to writing first instead of flushing the whole content.
			//To refer the SpoolingFileLineRead.java
			long beginNumber = getLineNumberOfLog(tmpName) - 1;
			for (long i = beginNumber; i < endLineNumber; i++ ) {
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
			//rename .buf to normal
			if (!retireTmpFile(bufferedPath)) {
			    logger.warn("Failed to rename file from \".buf\" to normal.");
                throw new IOException();
            }
			if (!deleteFile(tmpPath)) {
                logger.warn("Failed to delete old tmp file.");
            }
		} finally {
			try {
			    if (out != null) {
			        out.close();
                }
				if (in != null) {
				    in.close();
                }
			} catch (IOException e) {
				logger.warn("Unable to close in and out stream for file: " + tmpName, e);
			}
		}
	}
	
	public void writeStartFile(String appStartValue) {
	    Path startKeyPath = new Path(appPathStr, "_" + BasicConfigurationConstants.APP_START_KEY + ".buf");
	    try {
			out = fs.create(startKeyPath, true);
			out.write(appStartValue.getBytes());
			out.flush();
		} catch (IOException e) {
		    logger.warn("Exception occur in write start file: " + startKeyPath);
			e.printStackTrace();
		} finally {
			try {
			    if (out != null) {
			        out.close();
                }
				retireTmpFile(startKeyPath);
			} catch (IOException e) {
			    logger.warn("Unable to close out stream for file: " + startKeyPath);
			}
		}
	}
	
	//should to be check return value not ""
	public String readStartFile() throws IOException{
		Path startKeyPath = new Path(appPathStr, "_" + BasicConfigurationConstants.APP_START_KEY);
		byte[] appStartValue = new byte[10];
		try {
			in = fs.open(startKeyPath);
			in.read(appStartValue);
		} finally {
			try {
			    if (in != null) {
			        in.close();
                }
			} catch (IOException e) {
			    logger.warn("Unable to close in stream for file: " + startKeyPath);
			}
		}
		return (new String(appStartValue));
	}
	
    private long getLineNumberOfLog(String filename) {
        String[] spilts = filename.split("\\+");
        return Long.parseLong(spilts[3].substring(0, spilts[3].indexOf('.')));
    }
}
