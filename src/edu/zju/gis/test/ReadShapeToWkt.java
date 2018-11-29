package edu.zju.gis.test;

import edu.zju.gis.utils.ReadConfigUtils;

public class ReadShapeToWkt {

	public static void main(String[] args) {
		String wkt = ReadConfigUtils.readSingleFeatureShape("config/singleFeatureShape");
		System.out.println(wkt);
	}

}
