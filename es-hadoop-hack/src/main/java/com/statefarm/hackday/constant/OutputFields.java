package com.statefarm.hackday.constant;

import org.apache.hadoop.io.Text;

public class OutputFields {
	public static Text CUSTOMER_ID = new Text("customerId");
	public static Text SEGMENT = new Text("segment");
	public static Text LOYALTY_INDEX = new Text("loyaltyIndex");
	public static Text AUTO_MOMTHLY_PREMIUM = new Text("autoMonthlyPremium");
	public static Text LIFE_MONTHLY_PREMIUM = new Text("lifeMonthlyPremium");
	public static Text FIRE_MONTHLY_PREMIUM = new Text("fireMonthlyPremium");
	public static Text TOTAL_MONTHLY_PREMIUM = new Text("totalMonthlyPremium");
	public static Text LAST_CONTACT_WITH_COMPANY = new Text("lastContactWithCompany");
	public static Text LOCATION = new Text("geoPoint");
	public static Text LATITUDE = new Text("lat");
	public static Text LONGITUDE = new Text("lon");
	public static Text START_DATE = new Text("startDate");
	public static Text PRODUCT_FOOT_PRINT = new Text("productFootPrint");
}
