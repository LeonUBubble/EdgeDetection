package edu.zju.gis.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

public class readPropertirsFile {

	public static void main(String[] args) throws IOException {
		Properties properties = new Properties();
	    // 使用ClassLoader加载properties配置文件生成对应的输入流
	    InputStream in = readPropertirsFile.class.getClassLoader().getResourceAsStream("config/zookeeper");
	    // 使用properties对象加载输入流
	    properties.load(in);
	    //获取key对应的value值
	    String property = properties.getProperty("server.1");
	    System.out.println(property);
	    Set<Object> keySet = properties.keySet();
	    for (Object key : keySet) {
			String value = properties.getProperty(key.toString());
			System.out.println(value);
		}

	}

}
