package edu.zju.gis.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.vividsolutions.jts.geom.Geometry;

import edu.zju.gis.bean.MyFeature;
import edu.zju.gis.bean.Rectangle;
import edu.zju.gis.utils.GeoJsonUtils;
import edu.zju.gis.utils.GeometryUtils;
import edu.zju.gis.utils.ReadConfigUtils;
import net.sf.ehcache.search.aggregator.Count;

/**
 * 不要使用，有错误
 * @author hyr
 *
 */
public class GHQStatistics2_2 {
	
	private static MyFeature ghqFeature = null;//规划区的要素
	/**
	 * 创建hbase配置
	 */
	static Configuration config = null;
	static {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", ReadConfigUtils.readWholeProperties("config/zookeeper"));
		config.set("hbase.zookeeper.property.clientPort", "2181");
		System.out.println("init static doce====>>>>GHQStatistics2_2");
		//规划区的wkt数据--每次通过手动指定或从外界计算得到结果
		String ghqWkt = ReadConfigUtils.readSingleFeatureShape("config/singleFeatureShape");
		try {
			ghqFeature = new MyFeature();
			ghqFeature.wkt = ghqWkt;
			ghqFeature.envelop = GeometryUtils.createEnvelope(ghqFeature.wkt);
			ghqFeature.rectangle = new Rectangle(ghqFeature.envelop);
			ghqFeature.geometry = GeometryUtils.createGeometry(ghqWkt);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 表信息
	 */
	public static final String tableName0 = "FeatureTable";//表名1
	public static final String tableName0_colf = "GeometryFamily";//列族
	public static final String tableName0_colf_pro1 = "Geojson";//表名1-列名1
	public static final String tableName0_colf_pro2 = "Wkt";//表名1-列名2
	public static final String tableName1 = "FeatureTempTable";//表名2
	public static final String tableName1_colf = "NullFamaily";//列族
	public static final String tableName1_colf_pro = "NullValue";//表名2-列名
	public static final String tableName2 = "StaticTable";//表名3  结果统计表
	//表名3  结果统计表
	//rowkey设计 
	//以1开头表示符合规划 + 用地类型
	//以0开头表示违反规划 + 用地类型
	public static final String tableName2_colf = "StaticFamily";//列族
	public static final String tableName2_colf_pro = "area";//表名3-列名
	
	/**
	 * 初始化表结构
	 */
	public static void initTB() {
		
		HBaseAdmin admin=null;
		try {
			admin = new HBaseAdmin(config);//创建表管理
			/*删除表*/
			System.out.println(admin.tableExists(tableName2));
			if (admin.tableExists(tableName2)) {
				System.out.println(tableName2 + " is already exists!");
				admin.disableTable(tableName2);
				admin.deleteTable(tableName2);
			}
			/*创建表*/
			HTableDescriptor desc = new HTableDescriptor(tableName2);
			HColumnDescriptor family = new HColumnDescriptor(tableName2_colf);
			desc.addFamily(family);
			admin.createTable(desc);
			System.out.println("创建表"+ tableName2 +"成功！");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static class StatisticsMapper extends TableMapper<Text, DoubleWritable>{
		private static Connection connection = null;
		private static Table featureable = null;
		
		//Called once at the beginning of the task.即初始化
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			try {
				connection = ConnectionFactory.createConnection(config);
				featureable = connection.getTable(TableName.valueOf(tableName0));	
			} catch (Exception e) {
				e.printStackTrace();
			}
			super.setup(context);
		}
		private static final String ACCORD_PREFIX = "1";//符合规划用地
		private static final String INACCORD_PREFIX = "0";//违反规划用地
		private static Text planLandType = new Text();
		private static DoubleWritable area = new DoubleWritable();//该土地类型的面积
		
		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {
			
			Get get = new Get(value.getRow());
			Result featureResult = featureable.get(get);
			String featureRowKey = Bytes.toString(featureResult.getRow());//将来判断是否符合规划用地
			String geojson = Bytes.toString(featureResult.getValue(Bytes.toBytes(tableName0_colf), Bytes.toBytes(tableName0_colf_pro1)));
			String landType = GeoJsonUtils.getPropertyValue(geojson,GeoJsonUtils.PROPERTIES,"DLMC");
			String wkt = Bytes.toString(featureResult.getValue(Bytes.toBytes(tableName0_colf), Bytes.toBytes(tableName0_colf_pro2)));
			try {
				Geometry featureGeometry = GeometryUtils.createGeometry(wkt);
				if(featureGeometry.intersects(ghqFeature.geometry)) {
					String mapKey = ACCORD_PREFIX + landType;
					planLandType.set(mapKey);
					area.set(featureGeometry.intersection(ghqFeature.geometry).getArea());
					context.write(planLandType, area);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}	
			
		}
	}
	
	public static class StatisticsReducer extends TableReducer<Text, DoubleWritable, ImmutableBytesWritable>{
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			//对面积求和
			double sum = 0;
			for (DoubleWritable val : values) {//叠加
				sum += val.get();
			}
			Put put = new Put(Bytes.toBytes(key.toString()));
			// 封装数据
			put.addColumn(Bytes.toBytes(tableName2_colf), Bytes.toBytes(tableName2_colf_pro),Bytes.toBytes(""+sum));
			//写到hbase,需要指定rowkey、put
			context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);
		}
	}
		
	public static void main(String[] args) throws Exception {
		initTB();
		
		//创建job
		Job job = Job.getInstance(config);//job
		job.setJar("G:/linux/ghq2.jar");
		
		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob(tableName1, scan, StatisticsMapper.class,Text.class, DoubleWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(tableName2, StatisticsReducer.class, job);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public  void executor() throws Exception {
		initTB();
		
		//创建job
		Job job = Job.getInstance(config);//job
		job.setJar("G:/linux/ghq.jar");
		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob(tableName1, scan, StatisticsMapper.class,Text.class, DoubleWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(tableName2, StatisticsReducer.class, job);
		job.waitForCompletion(true);
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
