package edu.zju.gis.test;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;

import edu.zju.gis.cache.Cache;
import edu.zju.gis.utils.FormatUtils;
import edu.zju.gis.utils.GeometryUtils;
import edu.zju.gis.utils.HbaseUtils;

public class readXZQDate {
	/**����Ϣ**/
	public static Table xzqTable = null;//��������
	public static final String tableName = "xzqTable";//����
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
		xzqTable = Cache.connection.getTable(TableName.valueOf(tableName));
	}
	
	public static void main(String[] args) throws Exception {
		initTable();
		String path = "H:/geodata/20180112tohxr/2016������/�����أ�330424��/��ɽ�ֵ���330424104���滮����/3.�滮ͼ������/XZQ.shp";
		FeatureIterator<SimpleFeature> featureIterator = GeometryUtils.getFreatureIterator(path,"GBK");
		int count = 0;
		List<Put> list = new ArrayList<>();
		while(featureIterator.hasNext()) {
			SimpleFeature feature = featureIterator.next();
			InsertFeature(list,feature);
			count++;
		}
		if(!list.isEmpty()) {
			xzqTable.put(list);
		}
		System.out.println("������"+count+"�����ݣ�");
		HbaseUtils.closeTable(xzqTable);

	}
	
	private static void InsertFeature(List<Put> list, SimpleFeature feature) throws Exception {
		//rowkey����ƣ�����������+3λ��ʶ�루����λǰ�油�㣩
		String XZQDM = feature.getAttribute("XZQDM").toString();
		String BYM = FormatUtils.leftZeroArrang(3, feature.getAttribute("BSM").toString());
		String rowkey = XZQDM + BYM;
		System.out.println("rowkey:"+rowkey);
		StringWriter writer = new StringWriter();
		Cache.fJson.writeFeature(feature, writer);
		System.out.println(writer.toString());
		Put put = new Put(rowkey.getBytes());
		put.addColumn(tableName_colf.getBytes("UTF-8"), tableName_colf_prop.getBytes("UTF-8"), writer.toString().getBytes("utf-8"));
		list.add(put);
	}

}
