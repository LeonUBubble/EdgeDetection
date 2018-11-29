package edu.zju.gis.test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;

import edu.zju.gis.bean.MyFeature;
import edu.zju.gis.bean.Rectangle;
import edu.zju.gis.utils.FormatUtils;
import edu.zju.gis.utils.GeometryUtils;



public class ReadShapeFromHdfs {

	public static void main(String[] args) throws Exception{
		
//		String path = "C:/Users/hyr/Desktop/test/test2.shp";
//		FeatureIterator<SimpleFeature> featureIterator = GeometryUtils.getFreatureIterator(path);
//		String wkt = null;
//		while(featureIterator.hasNext()) {
//			SimpleFeature feature = featureIterator.next();
//			wkt = feature.getDefaultGeometryProperty().getValue().toString();
//			System.out.println(wkt);
//		}
		
		MyFeature ghqFeature = new MyFeature();
		String wkt = "MULTIPOLYGON (((40570714.7289 3384225.1284000035, 40572343.287 3384390.6787, 40572209.6889 3383589.253800001, 40570771.8373 3383557.3401999995, 40570714.7289 3384225.1284000035)))";
		ghqFeature.wkt = wkt;
		ghqFeature.envelop = GeometryUtils.createEnvelope(ghqFeature.wkt);
		ghqFeature.rectangle = new Rectangle(ghqFeature.envelop);
		System.out.println(ghqFeature.rectangle);
		
		
		
	}

}
