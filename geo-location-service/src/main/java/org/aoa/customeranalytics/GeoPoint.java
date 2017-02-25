package org.aoa.customeranalytics;


import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "GeoPoint")
public class GeoPoint {

	private long lat;
	private long lon;
	
	public long getLat() {
		return lat;
	}
	public void setLat(long lat) {
		this.lat = lat;
	}
	public long getLon() {
		return lon;
	}
	public void setLon(long lon) {
		this.lon = lon;
	}
	
}
