package edu.zju.gis.test;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;

import edu.zju.gis.cache.Cache;
import edu.zju.gis.utils.GeoJsonUtils;
import edu.zju.gis.utils.GeometryUtils;
import edu.zju.gis.utils.HbaseUtils;
import net.sf.ehcache.config.PinningConfiguration.Store;

public class TestHbaseEncoding {

	/**����Ϣ**/
	public static Table testTable = null;//��������
	public static final String tableName = "testTable";//����
	public static final String tableName_colf = "GeometryFamily";//������
	public static final String tableName_colf_prop = "Geojson";//����
	
	
	
	/**���ʼ��**/
	public static void initTable() throws Exception {
		HBaseAdmin admin = new HBaseAdmin(Cache.config);//���������
		if(admin.tableExists(tableName)) {
			System.out.println(tableName + "table is already exists!");
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		if(!admin.tableExists(tableName)) {
			HTableDescriptor desc = new HTableDescriptor(tableName);
			HColumnDescriptor family = new HColumnDescriptor(tableName_colf);
			desc.addFamily(family);
			admin.createTable(desc);
		}
		//���ӱ�
		testTable = Cache.connection.getTable(TableName.valueOf(tableName));
	}
	
	public static void main(String[] args) throws Exception {
		initTable();
		Store();
		Query();
		HbaseUtils.closeTable(testTable);
	}
	
	//�洢����
	public static void Store() throws Exception {
		String path = "H:/geodata/20180112tohxr/2016������/�����أ�330424��/�ٲ���330424102���滮����/3.�滮ͼ������/XZQ.shp";
		FeatureIterator<SimpleFeature> featureIterator = GeometryUtils.getFreatureIterator(path);
		SimpleFeature feature = featureIterator.next();
		StringWriter writer = new StringWriter();
		Cache.fJson.writeFeature(feature, writer);
		String XZQMC = GeoJsonUtils.getPropertyValue(writer.toString(), GeoJsonUtils.PROPERTIES, "XZQMC");
		System.out.println(XZQMC);
		Put put = new Put("1".getBytes());
		put.addColumn(tableName_colf.getBytes(), tableName_colf_prop.getBytes(), XZQMC.getBytes("utf-8"));
		testTable.put(put);
	}
	
	//��ѯ����
	public static void Query() throws Exception {
		Scan scan = new Scan();
		ResultScanner scanner = testTable.getScanner(scan);
		for (Result result : scanner) {
			byte[] bytes = result.getValue(tableName_colf.getBytes(), tableName_colf_prop.getBytes());
			System.out.println(new String(bytes, "UTF-8"));
			System.out.println(new String(bytes, "GBK"));
			System.out.println(Bytes.toString(bytes));
		}
		
	}
}
