package edu.zju.gis.test;

import java.io.IOException;
import java.io.StringWriter;

public class StringWriteTest {

	public static void main(String[] args) throws IOException {
		StringWriter writer = new StringWriter();
		writer.write("123");
		System.out.println(writer);
		writer.flush();
		writer.close();
		writer.write("456");
		System.out.println(writer);

	}

}
