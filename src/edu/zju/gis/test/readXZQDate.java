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
	/**表信息**/
	public static Table xzqTable = null;//行政区表
	public static final String tableName = "xzqTable";//表名
	public static final String tableName_colf = "GeometryFamily";//列族名
	public static final String tableName_colf_prop = "Geojson";//列名
	
	/**表初始化**/
	public static void initTable() throws Exception {
		HBaseAdmin admin = new HBaseAdmin(Cache.config);//创建表管理
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
		//连接表
		xzqTable = Cache.connection.getTable(TableName.valueOf(tableName));
	}
	
	public static void main(String[] args) throws Exception {
		initTable();
		String path = "H:/geodata/20180112tohxr/2016年数据/海盐县（330424）/秦山街道（330424104）规划数据/3.规划图形数据/XZQ.shp";
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
		System.out.println("共插入"+count+"条数据！");
		HbaseUtils.closeTable(xzqTable);

	}
	
	private static void InsertFeature(List<Put> list, SimpleFeature feature) throws Exception {
		//rowkey的设计：行政区代码+3位标识码（不足位前面补零）
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
