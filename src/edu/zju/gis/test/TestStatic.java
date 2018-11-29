package edu.zju.gis.test;

public class TestStatic {

	public static final int a = 1;
	public static Object object;
	public static Object object1;
	static {
		object = new Object();
		init();
		System.out.println("¾²Ì¬´úÂë¿é");
	}
	
	public static void init() {
		System.out.println("hello");
		object1 =  new Object();
	}
	
	public static void main(String[] args) {
		

	}

}
