package edu.zju.gis.test;

public class Two {

	public static void main(String[] args) {
		for(int level=1;level<6;level++) {
			int count = (int) Math.pow(2, level-1);
			System.out.println(count);
		}
		

	}

}
