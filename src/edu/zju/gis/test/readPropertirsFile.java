package edu.zju.gis.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

public class readPropertirsFile {

	public static void main(String[] args) throws IOException {
		Properties properties = new Properties();
	    // ʹ��ClassLoader����properties�����ļ����ɶ�Ӧ��������
	    InputStream in = readPropertirsFile.class.getClassLoader().getResourceAsStream("config/zookeeper");
	    // ʹ��properties�������������
	    properties.load(in);
	    //��ȡkey��Ӧ��valueֵ
	    String property = properties.getProperty("server.1");
	    System.out.println(property);
	    Set<Object> keySet = properties.keySet();
	    for (Object key : keySet) {
			String value = properties.getProperty(key.toString());
			System.out.println(value);
		}

	}

}
