package org.aoa.customeranalytics;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

@Path("/geolocationservice")
public interface GeoLocationService {
	
	@GET
	@Path("/geolocation/{zipCode}")
	public GeoPoint getGeoLocation(String zipCode);

}
