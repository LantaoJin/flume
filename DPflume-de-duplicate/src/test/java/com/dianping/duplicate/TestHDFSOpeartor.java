package com.dianping.duplicate;

import static org.junit.Assert.*;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dianping.duplicate.HDFSOperater;
import com.dianping.duplicate.util.DateUtil;

public class TestHDFSOpeartor {
	static boolean ret;
	static FileSystem fs = null;
	static final String FILE_NAME = "filemiddle_1370774126.log.2013-06-03.10+test84.hadoop+1370774412283+1.1370774325295.tmp";
	static final String TMP_FILE_NAME = FILE_NAME + ".tmp";
	static final String WORKING_HOUR_STR = "13060310"; 
	static final String appPathStr = "hdfs://10.1.77.86:/user/workcron/openAPI/";
	static DateUtil dateUtil = DateUtil.getInstance();
	static Path appPath;
	static Path appPathTmp;
	static HDFSOperater operater;
	@Before
	public void setUp() throws Exception {
		Configuration configuration = new Configuration();
		this.fs = FileSystem.get(configuration);
		appPath = new Path(appPathStr + "/" + FILE_NAME);
		appPathTmp = new Path(appPathStr + "/" + TMP_FILE_NAME);
		operater = new HDFSOperater(fs, appPathStr);
		//for test
//		FileStatus[] status = this.fs.listStatus(new Path("hdfs://10.1.77.86:/user/workcron/openAPI/13-05-28/19"));
//		for (FileStatus fileStatus : status) {
//			System.out.println(fileStatus.getPath().getName());
//		}
	}

	@After
	public void tearDown() throws Exception {
		System.out.println(ret);
		FileStatus[] status = fs.listStatus(new Path("hdfs://10.1.77.86:/user/workcron/lantao/2013-06-03/10/"));
		for (FileStatus fileStatus : status) {
			System.out.println(fileStatus.getPath().getName());
		}
	}

	@Test
	public void testRetireTmpFile() {
//	    System.out.println(appPathTmp);
		System.out.println(operater.retireTmpFile(appPathTmp)); 
	}

	@Test
	public void testTouchSuccFile() throws ParseException {
		Path parentPath = dateUtil.getPathFromStr(WORKING_HOUR_STR, appPathStr);
		ret = operater.touchSuccFile(parentPath);
	}

	@Test
	public void testCheckPathExists() throws ParseException {
		Path workingPath = dateUtil.getPathFromStr("13060310", appPathStr);
		ret = operater.checkPathExists(workingPath);
	}

	@Test
	public void testCheckSuccessFileExist() {
//		Path parentPath = new Path("hdfs://10.1.77.86:/user/workcron/lantao/2013-06-03/10/");
		ret = operater.checkSuccessFileExist(WORKING_HOUR_STR);
	}

	@Test
	public void testDeleteFile() throws ParseException, IOException {
		ret = operater.deleteFile(new Path(dateUtil.getPathFromStr(WORKING_HOUR_STR, appPathStr), "_success"));
	}

	@Test
	public void testDiscardDuplicateContent() throws IOException, ParseException {
		operater.discardDuplicateContent(new Path(dateUtil.getPathFromStr(WORKING_HOUR_STR, appPathStr), FILE_NAME), 70000l);
	}

	@Test
	public void testWriteStartFile() {
		operater.writeStartFile("13060310");
		operater.writeStartFile("13060311");
	}

	@Test
	public void testReadStartFile() {
		try {
            System.out.println(operater.readStartFile());
        } catch (IOException e) {
            e.printStackTrace();
        }
	}

}
