package org.kickusanalytics.service;

import org.kickusanalytics.model.Address;
import org.kickusanalytics.model.Location;

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;


@Path("/geolocationservice/")
@Produces("text/xml")
public class geoLocationServiceImpl implements geoLocationService{

	@Override
	public Location getGeoCode(Address address) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Location getGeoCode(String address) {
		// TODO Auto-generated method stub
		return null;
	}

}
