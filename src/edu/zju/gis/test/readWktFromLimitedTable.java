package edu.zju.gis.test;

import edu.zju.gis.utils.HbaseUtils;

public class readWktFromLimitedTable {

	public static void main(String[] args) {
		String data = HbaseUtils.getTableData("LimitedTable", "GeometryFamily", "Wkt");
		System.out.println(data);

	}

}
