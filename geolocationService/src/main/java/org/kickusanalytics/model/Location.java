package org.kickusanalytics.model;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "Location")
public class Location {
	private double latitude;
	private double longitude;
	public double getLongitude() {
		return longitude;
	}
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	
	
	
}
