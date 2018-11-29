package edu.zju.gis.test;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class TestDifference {

	public static void main(String[] args) throws ParseException {
		contains();
	}
	
	public static void difference() throws ParseException {
		WKTReader reader = new WKTReader();
		String wkt1 = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))";
		Geometry geometry1 = reader.read(wkt1);
		String wkt2 = "POLYGON((1 1, 2 1, 2 2, 1 2, 1 1))";
		Geometry geometry2 = reader.read(wkt2);
		System.out.println(geometry1.difference(geometry2));
		System.out.println(geometry1.intersection(geometry2));
	}

	public static void contains() throws ParseException {
		WKTReader reader = new WKTReader();
		String wkt1 = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))";
		Geometry geometry1 = reader.read(wkt1);
		String wkt2 = "POLYGON((-1 0, 2 0, 2 2, -1 2, -1 0))";
		Geometry geometry2 = reader.read(wkt2);
		System.out.println(geometry1.contains(geometry2));
	}
}
