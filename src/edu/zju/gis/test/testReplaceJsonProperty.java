package edu.zju.gis.test;

import java.io.IOException;
import java.io.StringWriter;

import org.geotools.geojson.geom.GeometryJSON;
import org.json.JSONObject;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import edu.zju.gis.utils.GeoJsonUtils;

/**
 * 修改json数据中某个属性数据
 * @author hyr
 *
 */
public class testReplaceJsonProperty {

	public static void main(String[] args) throws ParseException, IOException {
		String geojson = "{\"type\":\"Feature\",\"geometry\":{\"type\":\"MultiPolygon\",\"coordinates\":[[[[4.0569587098299995E7,3383330.2681],[4.0569510170899995E7,3383320.3999],[4.056950296689999E7,3383337.6266],[4.05694928057E7,3383361.0753],[4.05694849893E7,3383378.6619],[4.05694849893E7,3383383.3516],[4.05694888974E7,3383385.6965],[4.0569499449300006E7,3383391.168],[4.05695084379E7,3383394.6854],[4.0569509610400006E7,3383396.2486],[4.05695064838E7,3383403.2833],[4.0569500389299996E7,3383416.5167],[4.0569520145799994E7,3383418.4789],[4.0569568761E7,3383423.3073],[4.0569580712299995E7,3383361.2737],[4.0569584870799996E7,3383342.4523],[4.0569587098299995E7,3383330.2681]]]]},\"properties\":{\"BSM\":11029,\"YSDM\":\"2003010100\",\"TBBH\":\"683\",\"XZQDM\":\"330424102206\",\"DLMC\":\"水田\",\"DLBZ\":\"99\",\"DLDM\":\"111\",\"SQBM\":\"330424102206\",\"SQMC\":\"桃北村\",\"XZQMC\":\"桃北村\",\"QSXZ\":\"32\",\"PDJB\":\"1\",\"PZWH\":\"\",\"HQRQ\":\"1769-12-31T16:00:00.000+0000\",\"TKXS\":0.0,\"TBMJ\":7621.7265,\"TBDLMJ\":6583.6615,\"KSXM\":1038.065,\"KLWM\":0.0,\"KKSM\":0.0,\"SJCJ\":\"03\"},\"id\":\"JQDLTB.2\"}";
		String wkt = "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)))";
		JSONObject jsonObject = GeoJsonUtils.replaceJsonValue(geojson, "geometry", wkt);
		System.out.println(jsonObject);
	}
	
	public static void modifyJson() {
		String json = "{\r\n" + 
				"   \"name\":\"jack\",\r\n" + 
				"   \"age\":18,\r\n" + 
				"   \"address\":{\r\n" + 
				"      \"province\":\"浙江省\",\r\n" + 
				"      \"city\":\"杭州\"\r\n" + 
				"   }\r\n" + 
				"}";
		JSONObject jsonObject = new JSONObject(json);
		System.out.println(jsonObject);
		jsonObject.getJSONObject("address").put("city", "嘉兴市");
		System.out.println(jsonObject);
	}
	
	public static void modifyJson2() throws Exception {
		String geojson = "{\"type\":\"Feature\",\"geometry\":{\"type\":\"MultiPolygon\",\"coordinates\":[[[[4.0569587098299995E7,3383330.2681],[4.0569510170899995E7,3383320.3999],[4.056950296689999E7,3383337.6266],[4.05694928057E7,3383361.0753],[4.05694849893E7,3383378.6619],[4.05694849893E7,3383383.3516],[4.05694888974E7,3383385.6965],[4.0569499449300006E7,3383391.168],[4.05695084379E7,3383394.6854],[4.0569509610400006E7,3383396.2486],[4.05695064838E7,3383403.2833],[4.0569500389299996E7,3383416.5167],[4.0569520145799994E7,3383418.4789],[4.0569568761E7,3383423.3073],[4.0569580712299995E7,3383361.2737],[4.0569584870799996E7,3383342.4523],[4.0569587098299995E7,3383330.2681]]]]},\"properties\":{\"BSM\":11029,\"YSDM\":\"2003010100\",\"TBBH\":\"683\",\"XZQDM\":\"330424102206\",\"DLMC\":\"水田\",\"DLBZ\":\"99\",\"DLDM\":\"111\",\"SQBM\":\"330424102206\",\"SQMC\":\"桃北村\",\"XZQMC\":\"桃北村\",\"QSXZ\":\"32\",\"PDJB\":\"1\",\"PZWH\":\"\",\"HQRQ\":\"1769-12-31T16:00:00.000+0000\",\"TKXS\":0.0,\"TBMJ\":7621.7265,\"TBDLMJ\":6583.6615,\"KSXM\":1038.065,\"KLWM\":0.0,\"KKSM\":0.0,\"SJCJ\":\"03\"},\"id\":\"JQDLTB.2\"}";
		System.out.println(geojson);
		JSONObject jsonObject = new JSONObject(geojson);
		String wkt = "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)))";
		WKTReader reader = new WKTReader();
		Geometry geometry = reader.read(wkt);
		GeometryJSON geoJson = new GeometryJSON();
		StringWriter writer = new StringWriter();
		geoJson.write(geometry, writer);
		System.out.println(writer.toString());
		jsonObject.put("geometry", new JSONObject(writer.toString()));
		System.out.println(jsonObject.toString());
	}

}
