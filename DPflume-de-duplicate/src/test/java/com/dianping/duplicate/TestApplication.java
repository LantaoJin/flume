package com.dianping.duplicate;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.time.DateUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.duplicate.util.DateUtil;


public class TestApplication {
    private static final Logger logger = LoggerFactory.getLogger(TestApplication.class);
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        BufferedReader input = null;
        Process p = null;
        String line;
        try {
            SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date curruent = new Date();
            String currentDayStr = dayFormat.format(curruent);
            Runtime runtime = Runtime.getRuntime();
            p = runtime.exec("hadoop fs -rm /user/workcron/openAPI/_start");//
            input = new BufferedReader(new InputStreamReader(p.getInputStream()));  
            while ((line = input.readLine()) != null) {  
            // System.out.println(line);  
            }
            
            p = runtime.exec("hadoop fs -put ./_start /user/workcron/openAPI");
            input = new BufferedReader(new InputStreamReader(p.getInputStream()));  
            while ((line = input.readLine()) != null) {  
            // System.out.println(line);  
            }  
            p = runtime.exec("hadoop fs -rmr /user/workcron/openAPI/" + currentDayStr);
            input = new BufferedReader(new InputStreamReader(p.getInputStream()));  
            while ((line = input.readLine()) != null) {  
            // System.out.println(line);  
            }  
            p = runtime.exec("hadoop fs -mkdir /user/workcron/openAPI/" + currentDayStr);
            input = new BufferedReader(new InputStreamReader(p.getInputStream()));  
            while ((line = input.readLine()) != null) {  
            // System.out.println(line);  
            }  
            for(int i = 0; i<24;i++) {
                String hours;
                if (i < 10) {
                    hours = "0".concat(String.valueOf(i));
                } else {
                    hours = String.valueOf(i);
                }
                p = runtime.exec("hadoop fs -mkdir /user/workcron/openAPI/" + currentDayStr +  "/" + hours);
                input = new BufferedReader(new InputStreamReader(p.getInputStream()));  
                while ((line = input.readLine()) != null) {  
                // System.out.println(line);  
                }  
                p = runtime.exec("hadoop fs -cp /user/workcron/openAPI/fortest/* /user/workcron/openAPI/" + currentDayStr +  "/" + hours);
                input = new BufferedReader(new InputStreamReader(p.getInputStream()));  
                while ((line = input.readLine()) != null) {  
                // System.out.println(line);  
                } 
                logger.info(currentDayStr + "hdfs dir create successful.");
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
	    
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testMain() {
	    Application application = new Application();
	    String[] args = {"-f", "test_deduplicate.conf"};
	    application.setArgs(args);
	    try {
          if (application.parseOptions()) {
              application.loadConfig();
              application.run();
          }
        } catch (ParseException e) {
          logger.error(e.getMessage());
        } catch (Exception e) {
          logger.error("A fatal error occurred while running. Exception follows.",
              e);
        }
	}

	@Test
	public void testParseOptions() {
		fail("Not yet implemented");
	}

	@Test
    public void testLoadConfig() {
        Application application = new Application();
        application.loadConfig();
    }
	
	@Test
	public void testRun() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetArgs() {
		fail("Not yet implemented");
	}

	@Test
	public void testSetArgs() {
		fail("Not yet implemented");
	}

}
