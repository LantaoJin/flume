package com.dianping.duplicate.util;

import static org.junit.Assert.*;

import java.text.ParseException;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dianping.duplicate.util.DateUtil;

public class TestDateUtil {
	static DateUtil dateUtil = DateUtil.getInstance();
	static final String appPathStr = "hdfs://10.1.77.86:/user/workcron/lantao/";

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetInstance() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetCurrentDateStr() {
		System.out.println(dateUtil.getCurrentDateStr());
	}

	@Test
	public void testGetPathFromStr() throws ParseException {
		Path path = dateUtil.getPathFromStr(dateUtil.getCurrentDateStr(), appPathStr);
		System.out.println(path.getParent());
	}

	@Test
	public void testGetDateInterval() throws ParseException {
		int interval = dateUtil.getDateInterval("13061221", dateUtil.getCurrentDateStr());
		System.out.println(interval);
	}

	@Test
	public void testGetWorkingHourStr() throws ParseException {
		System.out.println(dateUtil.getWorkingHourStr(dateUtil.getCurrentDateStr(), 0));
	}

}
