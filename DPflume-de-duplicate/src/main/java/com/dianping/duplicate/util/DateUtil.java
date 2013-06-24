/**
 * 
 * @company dianping.com
 * @author lantao.jin
 */
package com.dianping.duplicate.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.fs.Path;
//TODO need to review
/**
 * 
 * DateUtil is a thread safety singleton class
 */
public class DateUtil {
	private SimpleDateFormat formatForCount;
	private SimpleDateFormat formatForPath;
	private SimpleDateFormat formatForAlarm;
	private Calendar calendar;
	
	
	/**
	 * Initialization on demand holder
	 * The java language specification will guarantee the thread safety using an internal class
	 */
	private static class SingletonHolder {
		private static DateUtil instance = new DateUtil();
	}
	
	public static DateUtil getInstance() {
		return SingletonHolder.instance;
	}
	
	private DateUtil() {
		this.formatForCount = new SimpleDateFormat("yyMMddHH");
		this.formatForPath = new SimpleDateFormat("/yyyy-MM-dd/HH");
		this.formatForAlarm = new SimpleDateFormat("mm");
		this.calendar =  Calendar.getInstance();
	}
	
	public String getCurrentDateStr() {
		return formatForCount.format(new Date());
	}
	
	public int getCurrentMin() {
        return Integer.parseInt(formatForAlarm.format(new Date()));
    }
	
	public Path getPathFromStr(String str, String appPathStr) throws ParseException {
		return new Path(appPathStr + 
				formatForPath.format(formatForCount.parse(str)));
	}

	public int getDateInterval(final String startDateStr,final String endDateStr) throws ParseException {
		return (int)((formatForCount.parse(endDateStr).getTime() -
				formatForCount.parse(startDateStr).getTime())/(60*60*1000));
	}
	
	/**
	 * 获得给定时间字符串之后n个小时的时间字符串
	 * 例如给出的开始时间字符串为13061010，进过30小时以后，时间为13061116
	 * @param startHour
	 * @param i
	 * @return
	 * @throws ParseException
	 */
	public String getWorkingHourStr(String beginHourStr, int n) throws ParseException {
		Date oldDate = formatForCount.parse(beginHourStr);
		calendar.setTime(oldDate);
		calendar.add(Calendar.HOUR_OF_DAY, n);
		Date newDate = calendar.getTime();
		return formatForCount.format(newDate);
	}
}
