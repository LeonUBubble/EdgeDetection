package edu.zju.gis.test;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;

public class textEncode {

	public static void main(String[] args) throws IOException {
//		OutputStream os = new BufferedOutputStream(new FileOutputStream(("H:/a.txt")));
//		os.write("¹þ¹þ".getBytes());
//		os.flush();
		String string = new String("¹þà¶");
		System.out.println(string);
		byte[] bytes = string.getBytes("GBK");
		String string2 = new String(bytes,"UTF-8");
		System.out.println(string2);

	}

}
