package org.aoa.customeranalytics;


import javax.ws.rs.PathParam;

public class GeoLocationServiceImpl implements GeoLocationService {

    /**
     * This method is mapped to an HTTP GET of 'http://localhost:8181/cxf/crm/customerservice/customers/{id}'.  The value for
     * {id} will be passed to this message as a parameter, using the @PathParam annotation.
     * <p/>
     * The method returns a Customer object - for creating the HTTP response, this object is marshaled into XML using JAXB.
     * <p/>
     * For example: surfing to 'http://localhost:8181/cxf/crm/customerservice/customers/123' will show you the information of
     * customer 123 in XML format.
     */
	@Override
	public GeoPoint getGeoLocation(@PathParam("zipCode") String zipCode) {
		System.out.println(zipCode);
		return new GeoPoint();
	}
	

	
}
