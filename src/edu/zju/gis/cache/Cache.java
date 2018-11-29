package edu.zju.gis.cache;

import java.io.StringWriter;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.geotools.geojson.feature.FeatureJSON;

import com.vividsolutions.jts.geom.Geometry;

import edu.zju.gis.bean.Rectangle;
import edu.zju.gis.utils.GeometryUtils;
import edu.zju.gis.utils.ReadConfigUtils;

//整个系统的缓存工具箱
public class Cache {
	public static int row = 8;//空间索引第二层的行数
	public static int col = 8;//空间索引第二层的列数
	public static Rectangle provinceRectangle = null;
	public static Configuration config = null;
	public static Connection connection = null;
	public static Table indexTable = null;//空间索引表
	public static Table featureTable = null;//要素信息表
	public static Table xzqTable = null;//行政区表
	public static Table staticTable = null;//行政区表
	public static Table ghqTable = null;//规划区表
	public static String top = "0000000000000";//空间索引第一层，即顶层
	public static FeatureJSON fJson = null;
	
	/**
	 * 表信息
	 */
	//空间索引表
	public static final String tableName0 = "IndexTable";//空间索引表
	public static final String tableName0_colf = "FeatureFamaily";//列族
	//要素信息表
	public static final String tableName1 = "FeatureTable";//表名1
	public static final String tableName1_colf = "GeometryFamily";//列族名
	public static final String tableName1_colf_prop1 = "Geojson";//列名
	public static final String tableName1_colf_prop2 = "Wkt";//列名
	//行政区表
	public static final String tableName2 = "xzqTable";//表名
	public static final String tableName2_colf = "GeometryFamily";//列族名
	public static final String tableName2_colf_prop = "Geojson";//列名
	//统计结果表
	public static final String tableName3 = "StaticTable";//结果统计表
	public static final String tableName3_colf = "StaticFamily";
	public static final String tableName3_colf_pro = "area";
	//规划结果表
	public static final String tableName4 = "GHQTable";
	public static final String tableName4_colf = "GeometryFamily";
	public static final String tableName4_colf_pro = "Wkt";
	static {
		try {
			init();
			initTB();
			//获得浙江省的外包矩形
			String wkt = "POLYGON ((40304819.23290604 3005514.0952151217, 40304819.23290604 3452222.997718748, 40705006.92309791 3452222.997718748, 40705006.92309791 3005514.0952151217, 40304819.23290604 3005514.0952151217))";
			Geometry provinceGeometry;
			provinceGeometry = GeometryUtils.createEnvelope(wkt);
			provinceRectangle = new Rectangle(provinceGeometry);
			fJson = new FeatureJSON();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void init() throws Exception {
		config = HBaseConfiguration.create();// 配置
		config.set("hbase.zookeeper.quorum", ReadConfigUtils.readWholeProperties("config/zookeeper"));// zookeeper地址
		config.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
		connection = ConnectionFactory.createConnection(config);
	}
	
	/**
	 * 初始化表结构，及其数据
	 */
	public static void initTB() {
		HBaseAdmin admin=null;
		try {
			admin = new HBaseAdmin(config);//创建表管理
			System.out.println(admin);
			/*删除表*/
//			if (admin.tableExists(tableName0)) {
//				System.out.println(tableName0 + "table is already exists!");
//				admin.disableTable(tableName0);
//				admin.deleteTable(tableName0);
//			}
//			if (admin.tableExists(tableName1)) {
//				System.out.println(tableName1 + "table is already exists!");
//				admin.disableTable(tableName1);
//				admin.deleteTable(tableName1);
//			}
			/*创建表*/
			if(!admin.tableExists(tableName0)) {
				HTableDescriptor desc = new HTableDescriptor(tableName0);
				HColumnDescriptor family = new HColumnDescriptor(tableName0_colf);
				desc.addFamily(family);
				admin.createTable(desc);
			}
			if(!admin.tableExists(tableName1)) {
				HTableDescriptor desc = new HTableDescriptor(tableName1);
				HColumnDescriptor family = new HColumnDescriptor(tableName1_colf);
				desc.addFamily(family);
				admin.createTable(desc);
			}
			//连接表
			indexTable = connection.getTable(TableName.valueOf(tableName0));
			featureTable = connection.getTable(TableName.valueOf(tableName1));
			xzqTable = connection.getTable(TableName.valueOf(tableName2));
			staticTable = connection.getTable(TableName.valueOf(tableName3));
			ghqTable = connection.getTable(TableName.valueOf(tableName4));
		} catch (Exception e) {
			e.printStackTrace();
		} 
	} 
	
	
	
	
}
