package com.dianping.duplicate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dianping.duplicate.HDFSOperater;
import com.dianping.duplicate.util.DateUtil;

public class TestHDFSOpeartor {
	static boolean ret;
	static FileSystem fs = null;
	static final String APP_PATH_STR       = "hdfs://10.1.77.86:/user/workcron/openAPI";
	static final String HDFSOP_PATH_STR    = "/hdfsOPTest";
	static final String ORIGIN_FILE_NAME   = APP_PATH_STR + HDFSOP_PATH_STR + "/testfile";
	static final String FILE_NAME          = APP_PATH_STR + HDFSOP_PATH_STR + "/accesslog.log.2013-06-14.19+host_5-94+1371211233511+1.1371211267051";
	static final String TMP_FILE_NAME      = FILE_NAME + ".tmp";
	static final String WORKING_HOUR_STR   = "13060310"; 
	
	static DateUtil dateUtil = DateUtil.getInstance();
	static Path appPath;
	static Path appPathTmp;
	static HDFSOperater operater;
	
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
      BufferedReader input = null;
      Process p = null;
      try {
          SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy-MM-dd");
          Date curruent = new Date();
          String currentDayStr = dayFormat.format(curruent);
          Runtime runtime = Runtime.getRuntime();
          String line;
          p = runtime.exec("hadoop fs -rm " + TMP_FILE_NAME);
          input = new BufferedReader(new InputStreamReader(p.getInputStream()));  
          while ((line = input.readLine()) != null) {  
          // System.out.println(line);  
          }
          p = runtime.exec("hadoop fs -rm " + FILE_NAME);
          input = new BufferedReader(new InputStreamReader(p.getInputStream()));  
          while ((line = input.readLine()) != null) {  
          // System.out.println(line);  
          }
          p = runtime.exec("hadoop fs -cp " + ORIGIN_FILE_NAME + " " + TMP_FILE_NAME);
          input = new BufferedReader(new InputStreamReader(p.getInputStream()));  
          while ((line = input.readLine()) != null) {  
          // System.out.println(line);  
          }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (input != null) {
                input.close();
            }
            if (p != null) {
                p.destroy();
            }
        }
    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        
    }
    
	@Before
	public void setUp() throws Exception {
		Configuration configuration = new Configuration();
		this.fs = FileSystem.get(configuration);
		operater = new HDFSOperater(fs, APP_PATH_STR);
		//for test
//		FileStatus[] status = this.fs.listStatus(new Path("hdfs://10.1.77.86:/user/workcron/openAPI/13-05-28/19"));
//		for (FileStatus fileStatus : status) {
//			System.out.println(fileStatus.getPath().getName());
//		}
		
	}

	@After
	public void tearDown() throws Exception {
		System.out.println(ret);
		FileStatus[] status = fs.listStatus(new Path(HDFSOP_PATH_STR));
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
		Path parentPath = dateUtil.getPathFromStr(WORKING_HOUR_STR, APP_PATH_STR);
		ret = operater.touchSuccFile(parentPath);
	}

	@Test
	public void testCheckPathExists() throws ParseException {
		Path workingPath = dateUtil.getPathFromStr("13060310", APP_PATH_STR);
		ret = operater.checkPathExists(workingPath);
	}

	@Test
	public void testCheckSuccessFileExist() {
//		Path parentPath = new Path("hdfs://10.1.77.86:/user/workcron/lantao/2013-06-03/10/");
		ret = operater.checkSuccessFileExist(WORKING_HOUR_STR);
	}

	@Test
	public void testDeleteFile() throws ParseException, IOException {
		ret = operater.deleteFile(new Path(dateUtil.getPathFromStr(WORKING_HOUR_STR, APP_PATH_STR), "_success"));
	}

	@Test
	public void testDiscardDuplicateContent() throws IOException, ParseException {
	    appPath = new Path(TMP_FILE_NAME);
	    Runtime runtime = Runtime.getRuntime();
	    Process p = runtime.exec("hadoop fs -tail " + TMP_FILE_NAME);
        BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line;
        while ((line = input.readLine()) != null) {  
            System.out.println(line);  
        }
		operater.discardDuplicateContent(appPath, 70000l);
		p = runtime.exec("hadoop fs -tail " + FILE_NAME);
        input = new BufferedReader(new InputStreamReader(p.getInputStream()));
        while ((line = input.readLine()) != null) {  
            System.out.println(line);  
        }
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
