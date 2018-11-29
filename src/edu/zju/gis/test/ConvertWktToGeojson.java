package edu.zju.gis.test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.geotools.geojson.geom.GeometryJSON;
import org.json.simple.JSONObject;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;


/**
 * 将wkt的数据转化成geojson的数据格式
 * @author hyr
 *
 */
public class ConvertWktToGeojson {

	private static WKTReader reader = new WKTReader();
	
	public static void main(String[] args) throws IOException, ParseException {
		String wkt = "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)))";
		Geometry geometry = reader.read(wkt);
		GeometryJSON geoJson = new GeometryJSON();
		StringWriter writer = new StringWriter();
		geoJson.write(geometry, writer);
		System.out.println(writer.toString());
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("geometry",writer);
		map.put("type","Feature");
		String jsonString = JSONObject.toJSONString(map);
		System.out.println(jsonString);

	}
	

}
