package com.dianping.duplicate;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dianping.duplicate.DuplicateChecker;
import com.dianping.duplicate.HDFSOperater;
import com.dianping.duplicate.configurate.Context;

public class TestDuplicateChecker {
	static final String FILE_NAME = "filemiddle_1370774306.log.2013-06-03.10+test84.hadoop+1370774430330+75730.1371090533152.tmp";
	static final String appPathStr = "hdfs://10.1.77.86:/user/workcron/lantao/";
	static final String APP_NAME = "lantao";
	static boolean ret = false;
	static Context context = null;
	static DuplicateChecker dc;
	static HDFSOperater operater;
	@Before
	public void setUp() throws Exception {
	    context = new Context(APP_NAME);
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(configuration);
		operater = new HDFSOperater(fs, appPathStr);
		context.put(APP_NAME + ".start", "13060310");
		context.put("app." + APP_NAME +".machines", "10.1.77.85 10.1.77.86");
		context.put("app." + APP_NAME +".machines." + "10.1.77.85" + ".port", 34545);
		context.put("app." + APP_NAME +".machines." + "10.1.77.86" + ".port", 34545);
		dc = new DuplicateChecker(APP_NAME, operater, context);
	}

	@After
	public void tearDown() throws Exception {
		System.out.println(ret);
	}

	@Test
	public void testDuplicateChecker() {
		fail("Not yet implemented");
	}

	@Test
	public void testRun() {
		fail("Not yet implemented");
	}

	@Test
	public void testCleanUpStaleFile() {
		fail("Not yet implemented");
	}

	@Test
	public void testHandleTmpFile() {
		fail("Not yet implemented");
	}

	@Test
	public void testCheckCollectorsAvailable() {
		ret = dc.checkCollectorsAvailable();
	}

	@Test
	public void testCheckAllSourceReceiving() {
		System.out.println(dc.checkCollectorsAvailable());
	}

	@Test
	public void testGetLineNumberOfLog() {
		System.out.println(dc.getLineNumberOfLog(FILE_NAME));
	}

	@Test
	public void testGetReadTimestampOfLog() {
		System.out.println(dc.getReadTimestampOfLog(FILE_NAME));
	}

	@Test
	public void testGetOriginHostnameOfLog() {
		System.out.println(dc.getOriginHostnameOfLog(FILE_NAME));
	}

	@Test
	public void testGetPureLogName() {
		System.out.println(dc.getPureLogName(FILE_NAME));
	}
}
