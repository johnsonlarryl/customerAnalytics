package com.statefarm.hackday;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class Test {
	public static void main(String[] args) {
		Calendar cal = new GregorianCalendar();
		cal.set(Calendar.MONTH, Calendar.JUNE);
		cal.set(Calendar.YEAR, 2015);
		cal.set(Calendar.DAY_OF_MONTH, 1);
		System.out.println("Time: " + cal.getTimeInMillis());
		cal.setTimeInMillis(1421560800000L);
		System.out.println(cal);
		
		System.out.println(cal.getTime());
	}
}
