package org.kickusanalytics.service;

import org.kickusanalytics.model.Address;
import org.kickusanalytics.model.Location;

public interface geoLocationService {
	
	public Location getGeoCode(Address address);
	public Location getGeoCode(String address);
}
