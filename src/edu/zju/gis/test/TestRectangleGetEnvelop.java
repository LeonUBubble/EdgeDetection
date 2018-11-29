package edu.zju.gis.test;

import java.util.List;

import com.vividsolutions.jts.geom.Geometry;

import edu.zju.gis.bean.Point;
import edu.zju.gis.bean.Rectangle;
import edu.zju.gis.utils.GeometryUtils;

public class TestRectangleGetEnvelop {

	public static void main(String[] args) throws Exception {
		Point p1 = new Point(0,0);
		Point p2 = new Point(10,10);
		Rectangle rectangle = new Rectangle(p1, p2);
		Geometry envelop = rectangle.createEnvelop();
		System.out.println(envelop);
		List<Rectangle> quaterTreeSplit = GeometryUtils.quaterTreeSplit(rectangle);
		for (Rectangle rectangle2 : quaterTreeSplit) {
			System.out.println(rectangle2);
			System.out.println(rectangle2.envelop);
		}

	}

}
