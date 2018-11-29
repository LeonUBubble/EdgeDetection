package edu.zju.gis.test;

public class StringBuildDele {

	public static void main(String[] args) {
		StringBuilder sbBuilder = new StringBuilder("1234,");
		sbBuilder.delete(sbBuilder.length()-1, sbBuilder.length());
		System.out.println(sbBuilder.toString());
	}

}
