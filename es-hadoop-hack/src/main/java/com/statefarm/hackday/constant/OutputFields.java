package com.statefarm.hackday.constant;

import org.apache.hadoop.io.Text;

public interface OutputFields {
	Text CUSTOMER_ID = new Text("customerId");
	Text SEGMENT = new Text("segment");
	Text LOYALTY_INDEX = new Text("loyaltyIndex");
	Text AUTO_MOMTHLY_PREMIUM = new Text("autoMonthlyPremium");
	Text LIFE_MONTHLY_PREMIUM = new Text("lifeMonthlyPremium");
	Text FIRE_MONTHLY_PREMIUM = new Text("fireMonthlyPremium");
	Text TOTAL_MONTHLY_PREMIUM = new Text("totalMonthlyPremium");
	Text LAST_CONTACT_WITH_COMPANY = new Text("lastContactWithCompany");
	Text LOCATION = new Text("geoPoint");
	Text LATITUDE = new Text("lat");
	Text LONGITURE = new Text("lon");
	Text START_DATE = new Text("startDate");
}
